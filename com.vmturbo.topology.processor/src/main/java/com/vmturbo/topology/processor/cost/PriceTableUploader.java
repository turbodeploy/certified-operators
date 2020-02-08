package com.vmturbo.topology.processor.cost;

import static com.vmturbo.topology.processor.cost.DiscoveredCloudCostUploader.CLOUD_COST_PRICES_SECTION;
import static com.vmturbo.topology.processor.cost.DiscoveredCloudCostUploader.CLOUD_COST_UPLOAD_TIME;
import static com.vmturbo.topology.processor.cost.DiscoveredCloudCostUploader.UPLOAD_REQUEST_BUILD_STAGE;
import static com.vmturbo.topology.processor.cost.DiscoveredCloudCostUploader.UPLOAD_REQUEST_UPLOAD_STAGE;

import java.lang.reflect.Type;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;

import io.grpc.stub.StreamObserver;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpecInfo;
import com.vmturbo.common.protobuf.cost.Pricing;
import com.vmturbo.common.protobuf.cost.Pricing.GetPriceTableChecksumRequest;
import com.vmturbo.common.protobuf.cost.Pricing.GetPriceTableChecksumResponse;
import com.vmturbo.common.protobuf.cost.Pricing.OnDemandPriceTable;
import com.vmturbo.common.protobuf.cost.Pricing.PriceTable;
import com.vmturbo.common.protobuf.cost.Pricing.PriceTableChecksum;
import com.vmturbo.common.protobuf.cost.Pricing.PriceTableKey;
import com.vmturbo.common.protobuf.cost.Pricing.PriceTableKey.Builder;
import com.vmturbo.common.protobuf.cost.Pricing.ProbePriceTableSegment;
import com.vmturbo.common.protobuf.cost.Pricing.ProbePriceTableSegment.ProbePriceTableChunk;
import com.vmturbo.common.protobuf.cost.Pricing.ProbePriceTableSegment.ProbePriceTableHeader;
import com.vmturbo.common.protobuf.cost.Pricing.ProbePriceTableSegment.ProbeRISpecPriceChunk;
import com.vmturbo.common.protobuf.cost.Pricing.ReservedInstanceSpecPrice;
import com.vmturbo.common.protobuf.cost.Pricing.UploadPriceTablesResponse;
import com.vmturbo.common.protobuf.cost.PricingServiceGrpc.PricingServiceStub;
import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.components.common.diagnostics.DiagnosticsAppender;
import com.vmturbo.components.common.diagnostics.DiagnosticsException;
import com.vmturbo.components.common.diagnostics.DiagsRestorable;
import com.vmturbo.platform.common.dto.CommonDTO.PricingIdentifier;
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
import com.vmturbo.topology.processor.targets.TargetStore;

/**
 * Uploads price tables discovered by the cloud cost probes, to the cost component.
 *
 * <p>We expect to see one price table per probe type. We will cache the last discovered price table
 * for each probe type, and upload them to the cost probe during the cloud cost upload topology
 * pipeline stage.
 *</p>
 * Because price data is expected to change infrequently, we will do a checksum comparison with the
 * latest price table checksum from the cost component before initiating a new upload.
 */
public class PriceTableUploader implements DiagsRestorable {
    /**
     * File name inside diagnostics to store price table info.
     */
    public static final String PRICE_TABLE_NAME = "PriceTables";
    private static final Logger logger = LogManager.getLogger();
    private static final String END_OF_TARGET_ID_MAPPINGS = "END_OF_TARGET_ID_TO_PRICETABLE_MAPPING";

    private static final DataMetricSummary PRICE_TABLE_HASH_CALCULATION_TIME = DataMetricSummary.builder()
            .withName("tp_price_table_hash_calculation_seconds")
            .withHelp("Time taken to calculate the hash code for a potential price table upload.")
            .build()
            .register();

    /**
     * grpc client for price service.
     */
    private final PricingServiceStub priceServiceClient;

    private final Clock clock;

    private final int riSpecPriceChunkSize;

    private final TargetStore targetStore;

    private final SpotPriceTableConverter spotPriceTableConverter;

    /**
     * a cache of last price table received per probe type. We assume any price tables returned by
     * any target of the same probe type will be equivalent, so we will keep one table per probe
     * type, rather than one per-target.
     *
     * <p>Not using a concurrent hash map like we do in the RIandExpenses uploader, because we expect
     * fewer cost probe targets as well as fewer discoveries due to the lower frequency of data
     *  updates.
     *  </p>
     */
    private final Map<Long, PricingDTO.PriceTable> sourcePriceTableByTargetId
            = Collections.synchronizedMap(new HashMap<>());

    public PriceTableUploader(@Nonnull final PricingServiceStub priceServiceClient,
                              @Nonnull final Clock clock,
                              final int riSpecPriceChunkSize,
                              @Nonnull TargetStore targetStore,
                              @Nonnull final SpotPriceTableConverter spotPriceTableConverter) {
        this.priceServiceClient = priceServiceClient;
        this.clock = clock;
        this.riSpecPriceChunkSize = riSpecPriceChunkSize;
        this.targetStore = targetStore;
        this.spotPriceTableConverter = spotPriceTableConverter;
    }

    /**
     * Cache the price table for this probe type, for potentially later upload.
     *
     * @param targetId   record priceTables by {@link this.sourcePriceTableByTargetId}
     * @param probeType  the probe type this price table was discovered by
     * @param priceTable the discovered price table
     */

    public void recordPriceTable(final long targetId, @Nonnull SDKProbeType probeType,
                                 @Nullable PricingDTO.PriceTable priceTable) {
        if (probeType.getProbeCategory() != ProbeCategory.COST) {
            logger.warn("Skipping price tables for non-Cost probe {}.", probeType.getProbeType());
            return;
        }

        // how big is this object?
        if (priceTable != null) {
            logger.debug("Received price table [{} bytes serialized] for probe type {}",
                    priceTable.getSerializedSize(), probeType);
        }

        sourcePriceTableByTargetId.put(targetId, priceTable);
    }

    @Nonnull
    Map<Long, PricingDTO.PriceTable> getSourcePriceTables() {
        return Collections.unmodifiableMap(sourcePriceTableByTargetId);
    }

    /**
     * When a target is removed, we will clear the cached price table for it's probe type.
     *
     * @param targetId target Id used for indexing sourcePriceTableByTargetId.
     * @param probeType the probe type of the removed target
     */
    public void targetRemoved(@Nonnull Long targetId, final SDKProbeType probeType) {
        if (probeType.getProbeCategory() != ProbeCategory.COST) {
            return;
        }

        // if there were multiple cost probe targets of the same probe type, it's possible we don't
        // need to remove the price table yet since it would theoretically be discovered by the other
        // target(s). We can handle that a number of ways, but not going to worry about that case
        // until we know we need to support it.
        if (sourcePriceTableByTargetId.containsKey(targetId)) {
            logger.info("Clearing cached source price table for target id : {} with probe type {}",
                    targetId, probeType);
            sourcePriceTableByTargetId.remove(targetId);
        }
    }

    /**
     * Will first check if something has changed according to the hash.
     * Then build a list of ProbePriceData based on the {@link CloudEntitiesMap}.
     * Upload whatever price tables we have gathered, into the cost component.
     *
     * @param probeTypesForTargetId map of probeTypes indexed by targetID.
     * @param cloudEntitiesMap the {@link CloudEntitiesMap} containing local id's to oids
     */
    public void checkForUpload(@Nonnull final Map<Long, SDKProbeType> probeTypesForTargetId,
                               @Nonnull CloudEntitiesMap cloudEntitiesMap) {

        DataMetricTimer timer = PRICE_TABLE_HASH_CALCULATION_TIME.startTimer();
        final List<ProbePriceData> probePricesList = buildPricesToUpload(probeTypesForTargetId, cloudEntitiesMap);
        int cloudEntitiesMapHash = cloudEntitiesMap.hashCode();
        // Get a mapping of hash codes to probe price data
        Map<Long, ProbePriceData> hashCodesToProbePriceDataMap = probePricesList.stream()
                .collect(Collectors.toMap(s -> ((long)cloudEntitiesMapHash * 31
                + s.hashCode()), s -> s));
        // Calculate a hashcode for the new data being assembled based on the source price tables
        // and cloud entities map.
        double hashCalculationSecs = timer.observe();
        logger.debug("Price table hash code calculation took {} secs", hashCalculationSecs);
        Map<PriceTableKey, Long> previousPriceTableKeyToChecksumMap;
        try {
            // Get the mapping of PriceTableKey to checksum
            previousPriceTableKeyToChecksumMap = getCheckSum();
        } catch (ExecutionException | InterruptedException e) {
            logger.error("Could not retrieve last persisted price table hash. Will not proceed with price table upload.", e);
            return;
        }
        Map<Long, ProbePriceData> priceTablesToUpload = new HashMap<>();
        // See if we even need to update the price tables.
        if (!previousPriceTableKeyToChecksumMap.isEmpty()) {
            for (Map.Entry<Long, ProbePriceData> entry : hashCodesToProbePriceDataMap.entrySet()) {
                long hashcode = entry.getKey();
                ProbePriceData probePriceData = entry.getValue();
                if (probePriceData.probeType != null) {
                    Long previousHashCode = previousPriceTableKeyToChecksumMap.get(probePriceData.priceTableKey);
                    if (previousHashCode != null && previousHashCode == hashcode) {
                        logger.info("Last processed upload hash is the same as this one [{}], skipping upload" +
                                        " for this price table.",
                                Long.toUnsignedString(hashcode));
                    } else {
                        logger.info("Price table upload hash check: new upload {} not found in database." +
                                " We will upload.", hashcode);
                        priceTablesToUpload.put(hashcode, hashCodesToProbePriceDataMap.get(hashcode));
                    }
                }
            }
        } else {
            priceTablesToUpload = hashCodesToProbePriceDataMap;
        }

        // Upload the price tables
        if (!priceTablesToUpload.isEmpty()) {
            uploadPrices(priceTablesToUpload);
        }
    }

    private Map<PriceTableKey, Long> getCheckSum() throws ExecutionException, InterruptedException {
        // Check if the data has changed since our last upload by comparing the new hash against
        // the hash of the last successfully saved price table from the cost component
        CompletableFuture<Map<PriceTableKey, Long>> lastConfirmedHashFuture = new CompletableFuture<>();
        priceServiceClient.getPriceTableChecksum(
            GetPriceTableChecksumRequest.getDefaultInstance(),
            new StreamObserver<GetPriceTableChecksumResponse>() {
                @Override
                public void onNext(final GetPriceTableChecksumResponse getPriceTableChecksumResponse) {
                    Map<PriceTableKey, Long> previousCheckSumOptional = getPriceTableChecksumResponse.getPricetableToChecksumList().stream()
                                    .collect(Collectors.toMap(s -> s.getPriceTableKey(), s -> s.getCheckSum()));
                    lastConfirmedHashFuture.complete(previousCheckSumOptional);
                }

                @Override
                public void onError(final Throwable throwable) {
                    lastConfirmedHashFuture.completeExceptionally(throwable);
                }

                @Override
                public void onCompleted() {}
            });
        return lastConfirmedHashFuture.get();
    }

    @VisibleForTesting
    @Nonnull
    List<ProbePriceData> buildPricesToUpload(@Nonnull final Map<Long, SDKProbeType> probeTypesForTargetId,
                                             @Nonnull CloudEntitiesMap cloudEntitiesMap) {
        // Build a new set of price objects to upload.
        DataMetricTimer buildTimer = CLOUD_COST_UPLOAD_TIME.labels(CLOUD_COST_PRICES_SECTION,
            UPLOAD_REQUEST_BUILD_STAGE).startTimer();
        List<ProbePriceData> probePricesList = new ArrayList<>();
        synchronized (sourcePriceTableByTargetId) {
            sourcePriceTableByTargetId.forEach((targetId, priceTable) -> {
                SDKProbeType sdkProbeType = probeTypesForTargetId.get(targetId);
                ProbePriceData probePriceData = new ProbePriceData();
                probePriceData.probeType = sdkProbeType.getProbeType();
                // convert the price table for this probe type
                probePriceData.priceTable = priceTableToCostPriceTable(priceTable, cloudEntitiesMap,
                        SDKProbeType.create(sdkProbeType.getProbeType()));
                // add the RI price table for this probe type
                probePriceData.riSpecPrices = getRISpecPrices(priceTable, cloudEntitiesMap);

                final Optional<PriceTableKey> optPriceTableKey =
                        generatePriceTableKey(priceTable, targetId);

                if (optPriceTableKey.isPresent()) {
                    probePriceData.priceTableKey = optPriceTableKey.get();
                    probePricesList.add(probePriceData);
                } else {
                    logger.error("Unable to create price table key for price table (TargetID={})",
                            targetId);
                }
            });
        }
        buildTimer.observe();
        logger.debug("Build of {} price tables took {} secs", sourcePriceTableByTargetId.size(),
            buildTimer.getTimeElapsedSecs());
        return probePricesList;
    }

    /**
     * Generate {@link PriceTableKey} from a given priceTableKeyList and its probeType.
     *
     * @param priceTable used to extract priceTableKeys.
     * @param targetId  targetId to which priceTable belongs to. Used to determine {@link SDKProbeType}.
     * @return {@link PriceTableKey} which is sent to cost component.
     */
    @Nonnull
    private Optional<PriceTableKey> generatePriceTableKey(@Nonnull final PricingDTO.PriceTable priceTable,
                                                @Nonnull final Long targetId) {
        Builder priceTableKeyBuilder = PriceTableKey.newBuilder();
        priceTable.getPriceTableKeysList().forEach(pricingIdentifier -> {
            priceTableKeyBuilder.putProbeKeyMaterial(pricingIdentifier.getIdentifierName().name(),
                    pricingIdentifier.getIdentifierValue());
        });

        return targetStore.getProbeTypeForTarget(targetId)
                .map(PricingGroupMapper::getPricingGroupForProbeType)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(priceTableKeyBuilder::setPricingGroup)
                .map(PriceTableKey.Builder::build);
    }

    @VisibleForTesting
    void uploadPrices(Map<Long, ProbePriceData> priceTablesToUpload) {
        DataMetricTimer uploadTimer = CLOUD_COST_UPLOAD_TIME.labels(CLOUD_COST_PRICES_SECTION,
            UPLOAD_REQUEST_UPLOAD_STAGE).startTimer();
        try {
            logger.info("Uploading price tables for {} probe types", priceTablesToUpload.keySet().size());          // upload the data
            // Since we want this to be a synchronous upload, we will wait for the upload to complete
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

            priceDataSender.sendProbePrices(priceTablesToUpload);

            uploadLatch.await();
        } catch (InterruptedException | RuntimeException e) {
            logger.error("Error uploading price tables.", e);
        }
        uploadTimer.observe();
        logger.info("Upload of {} price tables took {} secs",
            sourcePriceTableByTargetId.size(), uploadTimer.getTimeElapsedSecs() );
    }

    /**
     * Build an XL protobuf price table based on the price table received from a probe.
     * Note that we are modifying the price table that is passed in, and if an
     * exception occurs during this method, it's possible the price table will be partially updated.
     *
     * <p>If this is a concern, we can change the semantics to return a modified copy of the source
     * price table rather than modify the source in-place, so that exceptions can cause a copy of
     * the original object to be returned, if that is preferred.
     *</p>
     * @param sourcePriceTable the input {@link PriceTable}
     * @param cloudEntitiesMap the map of cloud entity id's -> oids
     * @param probeType the probe type that discovered this price table
     * @return the resulted protobuf price table
     */
    @VisibleForTesting
    PriceTable priceTableToCostPriceTable(@Nonnull PricingDTO.PriceTable sourcePriceTable,
                                          @Nonnull final Map<String, Long> cloudEntitiesMap,
                                          SDKProbeType probeType) {
        logger.debug("Processing price table for probe type {}", probeType);

        PriceTable.Builder priceTableBuilder = PriceTable.newBuilder();
        // we only know on-demand and license prices for now.
        // structure to track missing tiers - we want to log about this but not spam the log.
        Multimap<String, String> missingTiers = ArrayListMultimap.create();

        // The price tables are themselves broken up into tables within the table, with one table per
        // type of "good" (on-demand, spot instance, license) per region.
        for (OnDemandPriceTableByRegionEntry onDemandPriceTableForRegion : sourcePriceTable.getOnDemandPriceTableList()) {
            // find the region oid assigned by the TP
            String regionInternalId = onDemandPriceTableForRegion.getRelatedRegion().getId();
            if (!cloudEntitiesMap.containsKey(regionInternalId)) {
                // !kosher
                logger.warn("On-demand price table reader: OID not found for region local id {}."
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
                    onDemandPricesBuilder::containsComputePricesByTierId,
                    (oid, entry) -> onDemandPricesBuilder.putComputePricesByTierId(oid,
                                    entry.getComputeTierPriceList()),
                    missingTiers);

            // add DB prices
            addPriceEntries(onDemandPriceTableForRegion.getDatabasePriceTableList(),
                    cloudEntitiesMap,
                    entry -> entry.getRelatedDatabaseTier().getId(),
                    onDemandPricesBuilder::containsDbPricesByInstanceId,
                    (oid, pricesForTier) -> dbOnDemandPriceTableAdder(oid, pricesForTier, onDemandPricesBuilder),
                    missingTiers);

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
                    }, onDemandPricesBuilder::containsCloudStoragePricesByTierId,
                    (oid, entry) -> onDemandPricesBuilder.putCloudStoragePricesByTierId(oid,
                            entry.getStorageTierPriceList()),
                    missingTiers);

            // add the ip prices
            onDemandPricesBuilder.setIpPrices(onDemandPriceTableForRegion.getIpPrices());
            // add the on demand prices
            priceTableBuilder.putOnDemandPriceByRegionId(regionOid, onDemandPricesBuilder.build());
        }
        if (missingTiers.size() > 0) {
            logger.warn("Couldn't find oids for: {}", missingTiers);
        }

        // Populate the new Price table with license costs
        priceTableBuilder.addAllOnDemandLicensePrices(sourcePriceTable.getOnDemandLicensePriceTableList());
        priceTableBuilder.addAllReservedLicensePrices(sourcePriceTable.getReservedLicensePriceTableList());

        // Populate Spot prices
        priceTableBuilder.putAllSpotPriceByZoneOrRegionId(spotPriceTableConverter
                .convertSpotPrices(sourcePriceTable, cloudEntitiesMap));

        return priceTableBuilder.build();
    }

    private void dbOnDemandPriceTableAdder(Long oid,
                                           OnDemandPriceTableByRegionEntry.DatabasePriceTableByTierEntry pricesForTier,
                                           OnDemandPriceTable.Builder onDemandPricesBuilder) {
        Pricing.DbTierOnDemandPriceTable.Builder dbPriceTableBuilder =
            Pricing.DbTierOnDemandPriceTable.newBuilder();

        pricesForTier.getDatabaseTierPriceListList().forEach(priceList -> {
            if (priceList.hasDeploymentType()) {
                dbPriceTableBuilder.putDbPricesByDeploymentType(
                    priceList.getDeploymentType().getNumber(), priceList);
            } else {
                dbPriceTableBuilder.setOnDemandPricesNoDeploymentType(priceList);
            }
        });

        onDemandPricesBuilder.putDbPricesByInstanceId(oid, dbPriceTableBuilder.build());
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
     * @param missingTiers used for logging
     */
    private static <PricesForTier> void addPriceEntries(@Nonnull List<PricesForTier> priceEntries,
                      @Nonnull final Map<String, Long> cloudEntityOidByLocalId,
                      Function<PricesForTier, String> localIdGetter,
                      Function<Long, Boolean> duplicateChecker,
                      BiConsumer<Long, PricesForTier> putMethod,
                      Multimap<String, String> missingTiers) {
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
    static List<ReservedInstanceSpecPrice> getRISpecPrices(@Nonnull PricingDTO.PriceTable sourcePriceTable,
                                                           @Nonnull final Map<String, Long> cloudEntitiesMap) {

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
                if (!regionId.equals(sourceRiSpec.getRegion().getId())) {
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
                        .setSizeFlexible(sourceRiSpec.getSizeFlexible())
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
                            + "kept for each RI Spec.", numConflictingPrices);
        }
        return retVal;
    }

    /**
     * Create a stream of pairs of JSON-formatted String values corresponding to the probe type and
     * corresponding priceTable to be saved to the diagnostics file.
     */
    @Override
    public void collectDiags(@Nonnull DiagnosticsAppender diagnostics)
            throws DiagnosticsException {
        final Gson gson = ComponentGsonFactory.createGsonNoPrettyPrint();
        synchronized (sourcePriceTableByTargetId) {
            final JsonFormat.Printer printer =
                    JsonFormat.printer().omittingInsignificantWhitespace();
            Map<Collection<PricingIdentifier>, PricingDTO.PriceTable> priceTableToKeyMap =
                    new HashMap<>();
            sourcePriceTableByTargetId.values()
                    .forEach(priceTable -> priceTableToKeyMap.putIfAbsent(
                            priceTable.getPriceTableKeysList(), priceTable));
            for (Entry<Long, PricingDTO.PriceTable> probeTypePriceTableEntry : sourcePriceTableByTargetId
                    .entrySet()) {
                final long targetId = probeTypePriceTableEntry.getKey();
                final PricingDTO.PriceTable priceTable = probeTypePriceTableEntry.getValue();
                diagnostics.appendString(String.valueOf(targetId));
                diagnostics.appendString(gson.toJson(priceTable.getPriceTableKeysList()));
            }
            diagnostics.appendString(END_OF_TARGET_ID_MAPPINGS);
            for (Entry<Collection<PricingIdentifier>, PricingDTO.PriceTable> pricingData : priceTableToKeyMap
                    .entrySet()) {
                final Collection<PricingIdentifier> pricingIdentifiers = pricingData.getKey();
                final PricingDTO.PriceTable priceTable = pricingData.getValue();
                try {
                    diagnostics.appendString(gson.toJson(pricingIdentifiers));
                    diagnostics.appendString(printer.print(priceTable));
                } catch (InvalidProtocolBufferException ex) {
                    throw new DiagnosticsException(
                            "Error mapping targetId " + pricingIdentifiers + " priceTable " +
                                    priceTable, ex);
                }
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void restoreDiags(@Nonnull final List<String> collectedDiags) throws DiagnosticsException {
        if (collectedDiags.size() % 2 != 1) {
            //odd length because of one extra line for END_OF_TARGET_ID_MAPPINGS.
            throw new DiagnosticsException("Unexpected diags - should be odd length.");
        }

        final Gson gson = ComponentGsonFactory.createGsonNoPrettyPrint();
        synchronized (sourcePriceTableByTargetId) {
            final JsonFormat.Parser parser = JsonFormat.parser().ignoringUnknownFields();
            Map<Long, Collection<String>> targetIdToPriceTableKey = Maps.newHashMap();
            Map<Collection<String>, PricingDTO.PriceTable.Builder> priceTableKeyToData = Maps.newHashMap();

            final Type typeRef = new TypeToken<Collection<String>>() {}.getType();
            boolean endOfTargetMappingSeen = false;
            for (int i = 0; i + 2 <= collectedDiags.size(); i += 2) {
                if (collectedDiags.get(i).equals(END_OF_TARGET_ID_MAPPINGS)) {
                    i--; // to align the offset of the counter which reads the list.
                    endOfTargetMappingSeen = true;
                    continue;
                }
                try {
                    if (endOfTargetMappingSeen) {
                        String priceTableKeys = collectedDiags.get(i);
                        Collection<String> pricingIdentifier = gson.fromJson(priceTableKeys, typeRef);
                        String priceTable = collectedDiags.get(i + 1);
                        PricingDTO.PriceTable.Builder priceTableBldr = PricingDTO.PriceTable.newBuilder();
                        parser.merge(priceTable, priceTableBldr);
                        priceTableKeyToData.put(pricingIdentifier, priceTableBldr);
                    } else {
                        Long targetId = Long.valueOf(collectedDiags.get(i));
                        String priceTableKeys = collectedDiags.get(i + 1);
                        Collection<String> pricingIdentifier = gson.fromJson(priceTableKeys, typeRef);
                        targetIdToPriceTableKey.put(targetId, pricingIdentifier);
                    }
                } catch (NumberFormatException e) {
                    logger.error("Failed to find targetId for : {}", collectedDiags.get(i));
                } catch (InvalidProtocolBufferException e) {
                    logger.error("Failed to deserialize price table. Error: {}", e.getMessage());
                } catch (JsonSyntaxException e) {
                    throw new DiagnosticsException("Failed to deserialize JSON: " + priceTableKeyToData);
                }
            }
            targetIdToPriceTableKey.forEach((key, priceTableKey) -> sourcePriceTableByTargetId.put(key,
                    priceTableKeyToData.getOrDefault(priceTableKey,
                            PricingDTO.PriceTable.newBuilder()).build()));
        }
    }

    @Nonnull
    @Override
    public String getFileName() {
        return PRICE_TABLE_NAME;
    }

    /**
     * Utility class for assembling price upload data.
     */
    public static class ProbePriceData {
        public String probeType;
        public  PriceTable priceTable;
        public PriceTableKey priceTableKey;
        public List<ReservedInstanceSpecPrice> riSpecPrices;

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }

            if (!(other instanceof ProbePriceData)) {
                return false;
            }

            ProbePriceData otherProbePriceData = (ProbePriceData)other;

            return this.probeType.equals(otherProbePriceData.probeType) &&
                this.priceTable.equals(otherProbePriceData.priceTable) &&
                    this.riSpecPrices.equals(otherProbePriceData.riSpecPrices) &&
                    this.priceTableKey.equals(otherProbePriceData.priceTableKey);
        }

        @Override
        public int hashCode() {
            return Objects.hash(probeType, priceTable, riSpecPrices, priceTableKey);
        }
    }

    /**
     * Helper class for sending the probe price tables over a GRPC stream.
     */
    private class ProbePriceDataSender {
        private final StreamObserver<ProbePriceTableSegment> uploadStream;
        long totalBytesSent = 0;
        long numSegmentsSent = 0;

        ProbePriceDataSender(StreamObserver<ProbePriceTableSegment> uploadStream) {
            this.uploadStream = uploadStream;
        }

        public void sendProbePrices(Map<Long, ProbePriceData> priceTablesToUpload) {
            // send the header and the upload segments
            for (Map.Entry<Long, ProbePriceData> entry : priceTablesToUpload.entrySet()) {
                long checksum = entry.getKey();
                ProbePriceData probePriceData = entry.getValue();
                if (probePriceData.priceTable != null) {
                    logger.debug("Sending price table for probe {}", probePriceData.probeType);
                    sendSegment(ProbePriceTableSegment.newBuilder().setHeader((ProbePriceTableHeader.newBuilder()
                            .setCreatedTime(clock.millis())
                            .addPriceTableChecksums(PriceTableChecksum.newBuilder()
                                    .setCheckSum(checksum))))
                            .setProbePriceTable(ProbePriceTableChunk.newBuilder()
                                    .setPriceTableKey(probePriceData.priceTableKey)
                                .setPriceTable(probePriceData.priceTable))
                            .build());
                }

                if (CollectionUtils.isNotEmpty(probePriceData.riSpecPrices)) {
                    logger.debug("Sending RI prices for probe {}", probePriceData.probeType);
                    int x = 0;
                    ProbePriceTableSegment.Builder nextSegmentBuilder = ProbePriceTableSegment.newBuilder()
                            .setProbeRiSpecPrices(ProbeRISpecPriceChunk.newBuilder()
                                .setPriceTableKey(probePriceData.priceTableKey));
                    while (x < probePriceData.riSpecPrices.size()) {
                        // add to the next chunk
                        nextSegmentBuilder.getProbeRiSpecPricesBuilder()
                            .addReservedInstanceSpecPrices(probePriceData.riSpecPrices.get(x));
                        x++;
                        // if we have hit the threshold, then send this chunk and start a new one.
                        if (x % riSpecPriceChunkSize == 0) {
                            sendSegment(nextSegmentBuilder.build());
                            nextSegmentBuilder = ProbePriceTableSegment.newBuilder()
                                    .setProbeRiSpecPrices(ProbeRISpecPriceChunk.newBuilder()
                                            .setPriceTableKey(probePriceData.priceTableKey));
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
                        segment.getHeader().getPriceTableChecksumsList(), segment.getHeader().getCreatedTime());
            }
            if (segment.hasProbePriceTable()) {
                logger.debug("Sending price table segment.");
            }
            if (segment.hasProbeRiSpecPrices()) {
                logger.debug("Sending ri price segment with {} ri spec prices.",
                    segment.getProbeRiSpecPrices().getReservedInstanceSpecPricesCount());
            }
            uploadStream.onNext(segment);
        }

    }
}
