package com.vmturbo.topology.processor.cost;

import static com.vmturbo.topology.processor.cost.DiscoveredCloudCostUploader.CLOUD_COST_PRICES_SECTION;
import static com.vmturbo.topology.processor.cost.DiscoveredCloudCostUploader.CLOUD_COST_UPLOAD_TIME;
import static com.vmturbo.topology.processor.cost.DiscoveredCloudCostUploader.UPLOAD_REQUEST_BUILD_STAGE;
import static com.vmturbo.topology.processor.cost.DiscoveredCloudCostUploader.UPLOAD_REQUEST_UPLOAD_STAGE;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Function;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpecInfo;
import com.vmturbo.common.protobuf.cost.Pricing.GetPriceTableChecksumRequest;
import com.vmturbo.common.protobuf.cost.Pricing.GetPriceTableChecksumResponse;
import com.vmturbo.common.protobuf.cost.Pricing.OnDemandPriceTable;
import com.vmturbo.common.protobuf.cost.Pricing.PriceTable;
import com.vmturbo.common.protobuf.cost.Pricing.ProbePriceTableSegment;
import com.vmturbo.common.protobuf.cost.Pricing.ProbePriceTableSegment.ProbePriceTableChunk;
import com.vmturbo.common.protobuf.cost.Pricing.ProbePriceTableSegment.ProbePriceTableHeader;
import com.vmturbo.common.protobuf.cost.Pricing.ProbePriceTableSegment.ProbeRISpecPriceChunk;
import com.vmturbo.common.protobuf.cost.Pricing.ReservedInstanceSpecPrice;
import com.vmturbo.common.protobuf.cost.Pricing.UploadPriceTablesResponse;
import com.vmturbo.common.protobuf.cost.PricingServiceGrpc.PricingServiceStub;
import com.vmturbo.platform.sdk.common.CloudCostDTO;
import com.vmturbo.platform.sdk.common.PricingDTO;
import com.vmturbo.platform.sdk.common.PricingDTO.PriceTable.OnDemandPriceTableByRegionEntry;
import com.vmturbo.platform.sdk.common.PricingDTO.PriceTable.ReservedInstancePriceEntry;
import com.vmturbo.platform.sdk.common.PricingDTO.PriceTable.ReservedInstancePriceTableByRegionEntry;
import com.vmturbo.platform.sdk.common.PricingDTO.ReservedInstancePrice;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.proactivesupport.DataMetricTimer;

/**
 * Uploads price tables discovered by the cloud cost probes, to the cost component.
 *
 * We expect to see one price table per probe type. We will cache the last discovered price table
 * for each probe type, and upload them to the cost probe during the cloud cost upload topology
 * pipeline stage.
 *
 * Because price data is expected to change infrequently, we will do a checksum comparison with the
 * latest price table checksum from the cost component before initiating a new upload.
 */
public class PriceTableUploader {

    private static final Logger logger = LogManager.getLogger();

    protected static final DataMetricSummary PRICE_TABLE_HASH_CALCULATION_TIME = DataMetricSummary.builder()
            .withName("tp_price_table_hash_calculation_seconds")
            .withHelp("Time taken to calculate the hash code for a potential price table upload.")
            .build()
            .register();

    /**
     * grpc client for price service
     */
    private final PricingServiceStub priceServiceClient;

    private final Clock clock;

    private final int riSpecPriceChunkSize;

    /**
     * a cache of last price table received per probe type. We assume any price tables returned by
     * any target of the same probe type will be equivalent, so we will keep one table per probe
     * type, rather than one per-target.
     *
     * Not using a concurrent hash map like we do in the RIandExpenses uploader, because we expect
     * fewer cost probe targets as well as fewer discoveries due to the lower frequency of data
     *  updates.
     */
    private final Map<SDKProbeType, PricingDTO.PriceTable> sourcePriceTableByProbeType
            = Collections.synchronizedMap(new HashMap<>());

    public PriceTableUploader(@Nonnull final PricingServiceStub priceServiceClient,
                              @Nonnull final Clock clock,
                              final int riSpecPriceChunkSize) {
        this.priceServiceClient = priceServiceClient;
        this.clock = clock;
        this.riSpecPriceChunkSize = riSpecPriceChunkSize;
    }

    /**
     * Cache the price table for this probe type, for potentially later upload.
     *
     * @param probeType the probe type this price table was discovered by
     * @param priceTable the discovered price table
     */
    public void recordPriceTable(SDKProbeType probeType, PricingDTO.PriceTable priceTable) {
        if (probeType.getProbeCategory() != ProbeCategory.COST) {
            logger.warn("Skipping price tables for non-Cost probe {}.", probeType.getProbeType());
            return;
        }

        // how big is this object?
        if (priceTable != null) {
            logger.debug("Received price table [{} bytes serialized] for probe type {}",
                    priceTable.getSerializedSize(), probeType);
        }

        sourcePriceTableByProbeType.put(probeType, priceTable);
    }

    /**
     * When a target is removed, we will clear the cached price table for it's probe type.
     *
     * @param targetId the ID of a target that was just removed
     * @param probeType the probe type of the removed target
     */
    public void targetRemoved(long targetId, @Nonnull SDKProbeType probeType) {
        if (probeType.getProbeCategory() != ProbeCategory.COST) {
            return;
        }

        // if there were multiple cost probe targets of the same probe type, it's possible we don't
        // need to remove the price table yet since it would theoretically be discovered by the other
        // target(s). We can handle that a number of ways, but not going to worry about that case
        // until we know we need to support it.
        if (sourcePriceTableByProbeType.containsKey(probeType)) {
            logger.info("Clearing cached source price table for probe type {}", probeType);
            sourcePriceTableByProbeType.remove(probeType);
        }
    }

    /**
     * Upload whatever price tables we have gathered, into the cost component.
     *
     * @param cloudEntitiesMap the {@link CloudEntitiesMap} containing local id's to oids
     */
    public void uploadPriceTables(@Nonnull CloudEntitiesMap cloudEntitiesMap) {

        // calculate a hashcode for the new data being assembled so we can see if we even need to
        // update the price tables. We'll create a new hash code based on the source price tables
        // and cloud entities map, since if those two are the same then the resulting upload data
        // should also be the same.
        DataMetricTimer timer = PRICE_TABLE_HASH_CALCULATION_TIME.startTimer();
        long newHashcode = cloudEntitiesMap.hashCode() * 31 + sourcePriceTableByProbeType.hashCode();
        double hashCalculationSecs = timer.observe();
        logger.debug("Price table hash code calculation took {} secs", hashCalculationSecs);

        // check if the data has changed since our last upload by comparing the new hash against
        // the hash of the last successfully saved price table from the cost component
        CompletableFuture<Long> lastConfirmedHashFuture = new CompletableFuture<>();
        priceServiceClient.getPriceTableChecksum(
                GetPriceTableChecksumRequest.getDefaultInstance(),
                new StreamObserver<GetPriceTableChecksumResponse>() {
                    @Override
                    public void onNext(final GetPriceTableChecksumResponse getPriceTableChecksumResponse) {
                        lastConfirmedHashFuture.complete(getPriceTableChecksumResponse.getPriceTableChecksum());
                    }

                    @Override
                    public void onError(final Throwable throwable) {
                        lastConfirmedHashFuture.completeExceptionally(throwable);
                    }

                    @Override
                    public void onCompleted() {}
                });

        long lastConfirmedHashCode;
        try {
            lastConfirmedHashCode = lastConfirmedHashFuture.get();
        } catch (ExecutionException|InterruptedException e) {
            logger.error("Could not retrieve last persisted price table hash. Will not proceed with price table upload.", e);
            return;
        }
        if (newHashcode == lastConfirmedHashCode) {
            logger.info("Last processed upload hash is the same as this one [{}], skipping this upload.",
                    Long.toUnsignedString(newHashcode));
            return;
        }
        logger.info("Price table upload hash check: new upload {} last upload {}. We will upload.",
                Long.toUnsignedString(newHashcode), Long.toUnsignedString(lastConfirmedHashCode));

        // build a new set of price objects to upload.
        DataMetricTimer buildTimer = CLOUD_COST_UPLOAD_TIME.labels(CLOUD_COST_PRICES_SECTION,
                UPLOAD_REQUEST_BUILD_STAGE).startTimer();
        List<ProbePriceData> probePricesList = new ArrayList<>();
        synchronized (sourcePriceTableByProbeType) {
            sourcePriceTableByProbeType.forEach((probeType, priceTable) -> {
                ProbePriceData probePriceData = new ProbePriceData();
                probePriceData.probeType = probeType.getProbeType();

                // convert the price table for this probe type
                probePriceData.priceTable = priceTableToCostPriceTable(priceTable, cloudEntitiesMap, probeType);
                // add the RI price table for this probe type
                probePriceData.riSpecPrices = getRISpecPrices(priceTable, cloudEntitiesMap);
                probePricesList.add(probePriceData);
            });
        }
        buildTimer.observe();
        logger.debug("Build of {} price tables took {} secs", sourcePriceTableByProbeType.size(),
                buildTimer.getTimeElapsedSecs());

        DataMetricTimer uploadTimer = CLOUD_COST_UPLOAD_TIME.labels(CLOUD_COST_PRICES_SECTION,
                UPLOAD_REQUEST_UPLOAD_STAGE).startTimer();
        try {
            logger.info("Uploading price tables for {} probe types", probePricesList.size());
            // since we want this to be a synchronous upload, we will wait for the upload to complete
            // in this thread.
            CountDownLatch uploadLatch = new CountDownLatch(1);
            ProbePriceDataSender priceDataSender = new ProbePriceDataSender(priceServiceClient.updatePriceTables(new StreamObserver<UploadPriceTablesResponse>() {
                @Override
                public void onNext(final UploadPriceTablesResponse uploadPriceTablesResponse) {}

                @Override
                public void onError(final Throwable throwable) {
                    logger.warn("Error while uploading price tables.", throwable);
                    uploadLatch.countDown();
                }

                @Override
                public void onCompleted() {
                    uploadLatch.countDown();
                }
            }));

            priceDataSender.sendProbePrices(probePricesList, newHashcode);

            uploadLatch.await();
        } catch (InterruptedException | RuntimeException e) {
            logger.error("Error uploading price tables.", e);
        }
        uploadTimer.observe();
        logger.info("Upload of {} price tables took {} secs",
                sourcePriceTableByProbeType.size(), uploadTimer.getTimeElapsedSecs() );
    }

    /**
     * Build an XL protobuf price table based on the price table received from a probe.
     * Note that we are modifying the price table that is passed in, and if an
     * exception occurs during this method, it's possible the price table will be partially updated.
     *
     * If this is a concern, we can change the semantics to return a modified copy of the source
     * price table rather than modify the source in-place, so that exceptions can cause a copy of
     * the original object to be returned, if that is preferred.
     *
     * @param sourcePriceTable the input {@link PriceTable}
     * @param cloudEntitiesMap the map of cloud entity id's -> oids
     * @param probeType the probe type that discovered this price table
     */
    @VisibleForTesting
    PriceTable priceTableToCostPriceTable(@Nonnull PricingDTO.PriceTable sourcePriceTable,
                                          @Nonnull final Map<String, Long>  cloudEntitiesMap,
                                          SDKProbeType probeType) {
        logger.debug("Processing price table for probe type {}", probeType);

        PriceTable.Builder priceTableBuilder = PriceTable.newBuilder();
        // we only know on-demand and license prices for now.
        // TODO: reckon the spot instance prices
        // structure to track missing tiers - we want to log about this but not spam the log.
        Multimap<String,String> missingTiers = ArrayListMultimap.create();

        // the price tables are demselves broken up into tables within the table, with one table per
        // type of "good" (on-demand, spot instance, license) per region.
        for (OnDemandPriceTableByRegionEntry onDemandPriceTableForRegion : sourcePriceTable.getOnDemandPriceTableList()) {
            // find the region oid assigned by the TP
            String regionInternalId = onDemandPriceTableForRegion.getRelatedRegion().getId();
            if (!cloudEntitiesMap.containsKey(regionInternalId)) {
                // !kosher
                logger.error("On-demand price table reader: OID not found for region local id {}."
                        + " Skipping this table.", regionInternalId);
                continue;
            }
            logger.debug("Processing price tables for region {}", regionInternalId);
            Long regionOid = cloudEntitiesMap.get(regionInternalId);
            Map<Long, OnDemandPriceTable> onDemandPriceTablesByRegionId = priceTableBuilder.getOnDemandPriceByRegionIdMap();
            // get an on-demand price table builder to populate
            OnDemandPriceTable.Builder onDemandPricesBuilder
                    = onDemandPriceTablesByRegionId.getOrDefault(regionOid,
                    OnDemandPriceTable.getDefaultInstance())
                    .toBuilder();

            // add Compute prices
            addPriceEntries(onDemandPriceTableForRegion.getComputePriceTableList(),
                    cloudEntitiesMap,
                    entry -> entry.getRelatedComputeTier().getId(),
                    oid -> onDemandPricesBuilder.containsComputePricesByTierId(oid),
                    (oid, entry) -> onDemandPricesBuilder.putComputePricesByTierId(oid,
                                    entry.getComputeTierPriceList()),
                    missingTiers
            );

            // add DB prices
            addPriceEntries(onDemandPriceTableForRegion.getDatabasePriceTableList(),
                    cloudEntitiesMap,
                    entry -> entry.getRelatedDatabaseTier().getId(),
                    oid -> onDemandPricesBuilder.containsDbPricesByInstanceId(oid),
                    (oid, entry) -> onDemandPricesBuilder.putDbPricesByInstanceId(oid,
                            entry.getDatabaseTierPriceList()),
                    missingTiers
                    );

            // add Storage prices
            addPriceEntries(onDemandPriceTableForRegion.getStoragePriceTableList(),
                    cloudEntitiesMap,
                    entry -> {
                        // The storage id's being sent in the price table data don't match the
                        // billing and topology probe-assigned local id's we are using to identify
                        // our storage entities. They are close, but missing the probe prefix. So
                        // we will convert their id's in the price table to the probe syntax before
                        // looking up the entity oid in our cloudEntitiesMap.
                        return CloudCostUtils.storageTierLocalNameToId(
                                entry.getRelatedStorageTier().getId(), probeType);
                    },
                    oid -> onDemandPricesBuilder.containsCloudStoragePricesByTierId(oid),
                    (oid, entry) -> onDemandPricesBuilder.putCloudStoragePricesByTierId(oid,
                            entry.getStorageTierPriceList()),
                    missingTiers
            );

            // add the ip prices
            onDemandPricesBuilder.setIpPrices(onDemandPriceTableForRegion.getIpPrices());
            // add the on demand prices
            priceTableBuilder.putOnDemandPriceByRegionId(regionOid, onDemandPricesBuilder.build());
        }
        if (missingTiers.size() > 0) {
            logger.warn("Couldnt find oids for: {}", missingTiers);
        }

        // Populate the new Price table with license costs
        priceTableBuilder.addAllLicensePrices(sourcePriceTable.getLicensePriceTableList());

        return priceTableBuilder.build();
    }

    /**
     * This is used to try to avoid repetitive code when populating the prices table. Not sure if
     * it was worth the extra abstraction.
     *
     * @param priceEntries the set of "price lists per tier" we will be looking for new prices in.
     * @param cloudEntityOidByLocalId the local id -> entity oid map
     * @param localIdGetter a method to use for extracting the local id from the priceEntries
     * @param duplicateChecker a function that should check if prices already exist in the destination.
     * @param putMethod The method that adds the new prices to the map using the oid.
     * @param <PricesForTier> The type of objects contained in priceEntries
     * @return
     */
    private static <PricesForTier> void addPriceEntries(@Nonnull List<PricesForTier> priceEntries,
                      @Nonnull final Map<String, Long> cloudEntityOidByLocalId,
                      Function<PricesForTier, String> localIdGetter,
                      Function<Long, Boolean> duplicateChecker,
                      BiConsumer<Long, PricesForTier> putMethod, Multimap missingTiers) {
        for (PricesForTier priceEntry : priceEntries) {
            // find the cloud tier oid this set of prices is associated to.
            String localID = localIdGetter.apply(priceEntry);
            Long oid = cloudEntityOidByLocalId.get(localID);
            if (oid == null) {
                // track for logging purposes
                missingTiers.put(priceEntry.getClass().getSimpleName(), localID);
            } else {
                // if there is no price list for this "tier" yet, add these prices to the table.
                // otherwise, if a price list already exists for the tier in this region, then
                // skip adding our prices. we will NOT overwrite or even attempt to merge the data.
                // We'll just log a warning and come back to this case if needed.
                if (duplicateChecker.apply(oid)) {
                    logger.warn("A price list of {} already exists for {} - skipping.",
                            priceEntry.getClass().getSimpleName(), localID);
                } else {
                    // no pre-existing prices table found -- add ours to the map
                    logger.trace("Adding price list of {} for {}",
                            priceEntry.getClass().getSimpleName(), localID);
                    putMethod.accept(oid, priceEntry);
                }
            }
        }
    }


    /**
     * Create the collection of {@link com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec} prices
     * based on the RI Price table for a given probe.
     *
     * @param sourcePriceTable The source price table to read.
     * @param cloudEntitiesMap The probe local id -> oid map
     * @return a list of {@link ReservedInstanceSpecPrice} based on the source price table
     */
    private static List<ReservedInstanceSpecPrice> getRISpecPrices(@Nonnull PricingDTO.PriceTable sourcePriceTable,
                                                                   @Nonnull final Map<String, Long>  cloudEntitiesMap) {

        // keep a map of spec info's to prices we've visited so far
        Map<ReservedInstanceSpecInfo, ReservedInstancePrice> riPricesBySpec = new HashMap<>();

        // we are seeing a lot of conflicting RI prices in the price data -- enough that logging
        // each occurrence spams the log. Instead, we'll keep a count and log something at the end
        // if any were detected.
        int numConflictingPrices = 0;

        List<ReservedInstanceSpecPrice> retVal = new ArrayList<>();

        Set<String> missingRITiers = new HashSet<>(); // track any RI tiers we can't match
        Set<String> missingRegions = new HashSet<>(); // track missing regions
        for (ReservedInstancePriceTableByRegionEntry riPriceTableForRegion
                : sourcePriceTable.getReservedInstancePriceTableList()) {
            // look up the region for this set of prices
            String regionId = riPriceTableForRegion.getRelatedRegion().getId();
            Long regionOid = cloudEntitiesMap.get(regionId);
            if (regionOid == null) {
                missingRegions.add(regionId);
                continue;
            }
            // construct/find an RISpecInfo for each set of prices
            for (ReservedInstancePriceEntry riPriceEntry : riPriceTableForRegion.getReservedInstancePriceMapList()) {
                CloudCostDTO.ReservedInstanceSpec sourceRiSpec = riPriceEntry.getReservedInstanceSpec();
                // verify region id matches, just in case.
                if (! regionId.equals(sourceRiSpec.getRegion().getId())) {
                    // if the RI Spec has a different region, it's not clear which is correct, so
                    // skip this entry and log an error.
                    logger.warn("cost probe RISpec region {} doesn't match RI Price entry region {}",
                            sourceRiSpec.getRegion().getId(), regionId);
                    continue;
                }
                Long tierOid = cloudEntitiesMap.get(sourceRiSpec.getTier().getId());
                if (tierOid == null) {
                    missingRITiers.add(sourceRiSpec.getTier().getId());
                    continue;

                }
                // we'll start off by constructing an RISpecInfo based on the price table entry data
                ReservedInstanceSpecInfo riSpecInfo = ReservedInstanceSpecInfo.newBuilder()
                        .setRegionId(regionOid)
                        .setTierId(tierOid)
                        .setOs(sourceRiSpec.getOs())
                        .setTenancy(sourceRiSpec.getTenancy())
                        .setType(sourceRiSpec.getType())
                        .build();
                // does this already exist? If so, log a warning
                if (riPricesBySpec.containsKey(riSpecInfo)) {
                    // increase the "conflicting prices" counter and move on to the next.
                    numConflictingPrices += 1;
                    continue;
                }
                // add to the map of already priced RI specs
                riPricesBySpec.put(riSpecInfo, riPriceEntry.getReservedInstancePrice());
                // add to the return list
                retVal.add(ReservedInstanceSpecPrice.newBuilder()
                        .setRiSpecInfo(riSpecInfo)
                        .setPrice(riPriceEntry.getReservedInstancePrice())
                        .build());
            }
        }
        if (missingRITiers.size() > 0) {
            logger.warn("couldn't find compute tier oid for RI Spec tier ids {}", missingRITiers);
        }
        if (missingRegions.size() > 0) {
            logger.warn("Couldn't find regions for RI Spec Pricing: {}", missingRegions);
        }
        if (numConflictingPrices > 0) {
            logger.warn("{} conflicting RI Prices were skipped. Only the first price found was "
                            +"kept for each RI Spec.", numConflictingPrices);
        }
        return retVal;
    }

    // Utility class for assembling price upload data
    private static class ProbePriceData {
        public String probeType;
        public PriceTable priceTable;
        public List<ReservedInstanceSpecPrice> riSpecPrices;
    }

    // Helper class for sending the probe price tables over a GRPC stream.
    private class ProbePriceDataSender {
        private final StreamObserver<ProbePriceTableSegment> uploadStream;
        long totalBytesSent = 0;
        long numSegmentsSent = 0;

        public ProbePriceDataSender(StreamObserver<ProbePriceTableSegment> uploadStream) {
            this.uploadStream = uploadStream;
        }

        public void sendProbePrices(List<ProbePriceData> probePriceDataList, long checksum) {
            // first send the header
            sendSegment(ProbePriceTableSegment.newBuilder()
                    .setHeader(ProbePriceTableHeader.newBuilder()
                            .setCreatedTime(clock.millis())
                            .setChecksum(checksum))
                    .build());
            // send the upload segments
            for (ProbePriceData probePriceData : probePriceDataList) {
                if (probePriceData.priceTable != null) {
                    logger.debug("Sending price table for probe {}", probePriceData.probeType);
                    sendSegment(ProbePriceTableSegment.newBuilder()
                            .setProbePriceTable(ProbePriceTableChunk.newBuilder()
                                .setProbeType(probePriceData.probeType)
                                .setPriceTable(probePriceData.priceTable))
                            .build());
                }

                if (CollectionUtils.isNotEmpty(probePriceData.riSpecPrices)) {
                    logger.debug("Sending RI prices for probe {}", probePriceData.probeType);
                    int x = 0;
                    ProbePriceTableSegment.Builder nextSegmentBuilder = ProbePriceTableSegment.newBuilder()
                            .setProbeRiSpecPrices(ProbeRISpecPriceChunk.newBuilder()
                                .setProbeType(probePriceData.probeType));
                    while (x < probePriceData.riSpecPrices.size()) {
                        // add to the next chunk
                        nextSegmentBuilder.getProbeRiSpecPricesBuilder().addReservedInstanceSpecPrices(probePriceData.riSpecPrices.get(x));
                        x++;
                        // if we have hit the threshold, then send this chunk and start a new one.
                        if (x % riSpecPriceChunkSize == 0) {
                            sendSegment(nextSegmentBuilder.build());
                            nextSegmentBuilder = ProbePriceTableSegment.newBuilder()
                                    .setProbeRiSpecPrices(ProbeRISpecPriceChunk.newBuilder()
                                            .setProbeType(probePriceData.probeType));
                        }
                    }
                    // if there is still an unfinished segment to send, send it now.
                    if (nextSegmentBuilder.getProbeRiSpecPrices().getReservedInstanceSpecPricesCount() > 0) {
                        sendSegment(nextSegmentBuilder.build());
                    }

                }
            }
            uploadStream.onCompleted();
            logger.debug("Sent {} total bytes over {} segments.", totalBytesSent, numSegmentsSent);
        }

        private void sendSegment(ProbePriceTableSegment segment) {
            numSegmentsSent++;
            totalBytesSent += segment.getSerializedSize();
            if (segment.hasHeader()) {
                logger.debug("Sending header with checksum {} and creation time {}",
                        segment.getHeader().getChecksum(), segment.getHeader().getCreatedTime());
            } else if (segment.hasProbePriceTable()) {
                logger.debug("Sending price table segment.");
            } else if (segment.hasProbeRiSpecPrices()) {
                logger.debug("Sending ri price segment with {} ri spec prices.", segment.getProbeRiSpecPrices().getReservedInstanceSpecPricesCount());
            }
            uploadStream.onNext(segment);
        }

    }
}
