package com.vmturbo.topology.processor.cost;

import static com.vmturbo.topology.processor.cost.DiscoveredCloudCostUploader.CLOUD_COST_PRICES_SECTION;
import static com.vmturbo.topology.processor.cost.DiscoveredCloudCostUploader.CLOUD_COST_UPLOAD_TIME;
import static com.vmturbo.topology.processor.cost.DiscoveredCloudCostUploader.UPLOAD_REQUEST_BUILD_STAGE;
import static com.vmturbo.topology.processor.cost.DiscoveredCloudCostUploader.UPLOAD_REQUEST_UPLOAD_STAGE;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpecInfo;
import com.vmturbo.common.protobuf.cost.Pricing.GetPriceTableChecksumRequest;
import com.vmturbo.common.protobuf.cost.Pricing.OnDemandPriceTable;
import com.vmturbo.common.protobuf.cost.Pricing.PriceTable;
import com.vmturbo.common.protobuf.cost.Pricing.ProbePriceTable;
import com.vmturbo.common.protobuf.cost.Pricing.ProbePriceTable.ReservedInstanceSpecPrice;
import com.vmturbo.common.protobuf.cost.Pricing.UploadPriceTablesRequest;
import com.vmturbo.common.protobuf.cost.Pricing.UploadPriceTablesResponse;
import com.vmturbo.common.protobuf.cost.PricingServiceGrpc.PricingServiceBlockingStub;
import com.vmturbo.platform.sdk.common.CloudCostDTO;
import com.vmturbo.platform.sdk.common.PricingDTO;
import com.vmturbo.platform.sdk.common.PricingDTO.PriceTable.OnDemandPriceTableByRegionEntry;
import com.vmturbo.platform.sdk.common.PricingDTO.PriceTable.ReservedInstancePriceEntry;
import com.vmturbo.platform.sdk.common.PricingDTO.PriceTable.ReservedInstancePriceTableByRegionEntry;
import com.vmturbo.platform.sdk.common.PricingDTO.ReservedInstancePrice;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
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

    /**
     * grpc client for price service
     */
    private final PricingServiceBlockingStub priceServiceClient;

    private final Clock clock;

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

    public PriceTableUploader(@Nonnull final PricingServiceBlockingStub priceServiceClient,
                              @Nonnull final Clock clock) {
        this.priceServiceClient = priceServiceClient;
        this.clock = clock;
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
     * @param targetId
     */
    public void targetRemoved(long targetId, SDKProbeType probeType) {
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

        // first build the price tables upload request
        DataMetricTimer buildTimer = CLOUD_COST_UPLOAD_TIME.labels(CLOUD_COST_PRICES_SECTION,
                UPLOAD_REQUEST_BUILD_STAGE).startTimer();

        UploadPriceTablesRequest.Builder uploadRequestBuilder = UploadPriceTablesRequest.newBuilder();
        synchronized (sourcePriceTableByProbeType) {
            sourcePriceTableByProbeType.forEach((probeType, priceTable) -> {
                ProbePriceTable.Builder probePriceTableBuilder = ProbePriceTable.newBuilder();

                // add the price table for this probe type
                probePriceTableBuilder.setPriceTable(priceTableToCostPriceTable(priceTable, cloudEntitiesMap, probeType));
                // add the RI price table for this probe type
                probePriceTableBuilder.addAllReservedInstanceSpecPrices(getRISpecPrices(priceTable, cloudEntitiesMap));

                uploadRequestBuilder.putProbePriceTables(probeType.getProbeType(),
                        probePriceTableBuilder.build());
            });
        }
        buildTimer.observe();
        logger.debug("Build of {} price tables took {} secs", sourcePriceTableByProbeType.size(),
                buildTimer.getTimeElapsedSecs());

        // get the hash of the last successfully saved price table from the cost component
        long lastConfirmedHash = priceServiceClient.getPriceTableChecksum(
                GetPriceTableChecksumRequest.getDefaultInstance())
                .getPriceTableChecksum();

        // check if the data has changed since our last upload
        long requestHash = uploadRequestBuilder.build().hashCode();
        if (requestHash == lastConfirmedHash) {
            logger.info("Last processed upload hash is the same as this one [{}], skipping this upload.",
                    Long.toUnsignedString(requestHash));
            return;
        }
        logger.info("Price table upload hash check: new upload {} last upload {}. We will upload.",
                Long.toUnsignedString(requestHash), Long.toUnsignedString(lastConfirmedHash));

        // add the hash and current time to the upload
        uploadRequestBuilder.setChecksum(requestHash);
        uploadRequestBuilder.setCreatedTime(clock.millis());
        UploadPriceTablesRequest request = uploadRequestBuilder.build();

        DataMetricTimer uploadTimer = CLOUD_COST_UPLOAD_TIME.labels(CLOUD_COST_PRICES_SECTION,
                UPLOAD_REQUEST_UPLOAD_STAGE).startTimer();
        try {
            UploadPriceTablesResponse response = priceServiceClient.uploadPriceTables(request);
            lastConfirmedHash = requestHash;
        } catch (RuntimeException rte) {
            logger.error("Error uploading price tables.", rte);
        }
        uploadTimer.observe();
        logger.info("Upload of {} price tables [{} bytes] took {} secs",
                sourcePriceTableByProbeType.size(), request.getSerializedSize(),
                uploadTimer.getTimeElapsedSecs() );
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
     * @param sourcePriceTable the input {@link com.vmturbo.platform.sdk.common.PricingDTO.PriceTable}
     * @param cloudEntitiesMap the map of cloud entity id's -> oids
     * @param probeType the probe type that discovered this price table
     */
    @VisibleForTesting
    PriceTable priceTableToCostPriceTable(@Nonnull PricingDTO.PriceTable sourcePriceTable,
                                          @Nonnull final Map<String, Long>  cloudEntitiesMap,
                                          SDKProbeType probeType) {
        logger.debug("Processing price table for probe type {}", probeType);

        PriceTable.Builder priceTableBuilder = PriceTable.newBuilder();
        // we only knows on-demand prices for now.
        // TODO: reckon the spot instance prices
        // TODO: savvy the license prices
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
                                    entry.getComputeTierPriceList())
            );

            // add DB prices
            addPriceEntries(onDemandPriceTableForRegion.getDatabasePriceTableList(),
                    cloudEntitiesMap,
                    entry -> entry.getRelatedDatabaseTier().getId(),
                    oid -> onDemandPricesBuilder.containsDbPricesByInstanceId(oid),
                    (oid, entry) -> onDemandPricesBuilder.putDbPricesByInstanceId(oid,
                            entry.getDatabaseTierPriceList())
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
                            entry.getStorageTierPriceList())
            );

            // TODO: Get IP Prices too

            // add the on demand prices
            priceTableBuilder.putOnDemandPriceByRegionId(regionOid, onDemandPricesBuilder.build());
        }
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
                      BiConsumer<Long, PricesForTier> putMethod) {
        for (PricesForTier priceEntry : priceEntries) {
            // find the cloud tier oid this set of prices is associated to.
            String localID = localIdGetter.apply(priceEntry);
            Long oid = cloudEntityOidByLocalId.get(localID);
            if (oid == null) {
                logger.warn("Can't find oid for {} {} -- skipping price table for it.",
                        priceEntry.getClass().getSimpleName(), localID);
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

        for (ReservedInstancePriceTableByRegionEntry riPriceTableForRegion
                : sourcePriceTable.getReservedInstancePriceTableList()) {
            // look up the region for this set of prices
            String regionId = riPriceTableForRegion.getRelatedRegion().getId();
            Long regionOid = cloudEntitiesMap.get(regionId);
            if (regionOid == null) {
                logger.warn("RI Spec Prices: Region oid not found for region id {} - will not add RI Spec prices for this region.",
                        regionId);
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
                    logger.warn("could find compute tier oid for RI Spec tier id {}",
                            sourceRiSpec.getTier().getId());
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
                riPricesBySpec.put( riSpecInfo, riPriceEntry.getReservedInstancePrice());
                // add to the return list
                retVal.add(ReservedInstanceSpecPrice.newBuilder()
                        .setRiSpecInfo(riSpecInfo)
                        .setPrice(riPriceEntry.getReservedInstancePrice())
                        .build());
            }
        }
        if (numConflictingPrices > 0) {
            logger.warn("{} conflicting RI Prices were skipped. Only the first price found was "
                            +"kept for each RI Spec.", numConflictingPrices);
        }
        return retVal;
    }

}
