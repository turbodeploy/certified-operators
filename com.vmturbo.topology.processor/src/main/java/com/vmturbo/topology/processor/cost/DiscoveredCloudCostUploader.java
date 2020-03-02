package com.vmturbo.topology.processor.cost;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.StampedLock;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.components.common.diagnostics.DiagnosticsAppender;
import com.vmturbo.components.common.diagnostics.DiagnosticsException;
import com.vmturbo.components.common.diagnostics.DiagsRestorable;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.NonMarketDTO.CostDataDTO;
import com.vmturbo.platform.common.dto.NonMarketDTO.NonMarketEntityDTO;
import com.vmturbo.platform.common.dto.NonMarketDTO.NonMarketEntityDTO.NonMarketEntityType;
import com.vmturbo.platform.sdk.common.PricingDTO.PriceTable;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.topology.processor.operation.discovery.Discovery;
import com.vmturbo.topology.processor.stitching.StitchingContext;
import com.vmturbo.topology.processor.stitching.TopologyStitchingEntity;

/**
 * This class is responsible for extracting the cloud cost data and
 * sending it to the Cost Component.
 */
public class DiscoveredCloudCostUploader implements DiagsRestorable {
    /**
     * File name inside diagnostics to store discovered cloud costs.
     */
    public static final String DISCOVERED_CLOUD_COST_NAME = "DiscoveredCloudCost";
    private static final Logger logger = LogManager.getLogger();

    public static final long MILLIS_PER_YEAR = 31536000000L; // ms per year

    public static final long MILLIS_PER_3_YEAR = 3 * MILLIS_PER_YEAR;

    protected static final DataMetricSummary CLOUD_COST_UPLOAD_TIME = DataMetricSummary.builder()
            .withName("tp_cloud_cost_upload_seconds")
            .withHelp("Time taken to perform cloud cost upload (in seconds). This metric is separated"+
                    " into two dimensions -- \"Section\", which represents the type of cloud cost data"+
                    " being uploaded in a request, and will be either 'prices' or 'expenses';"+
                    " and \"Stage\", which delineates time spent in either the 'build' or 'upload'"+
                    " stage of the request.")
            .withLabelNames("section", "stage")
            .build()
            .register();

    protected static final String CLOUD_COST_EXPENSES_SECTION = "expenses";
    protected static final String CLOUD_COST_PRICES_SECTION = "prices";
    protected static final String RI_DATA_SECTION = "ri_data";

    protected static final String UPLOAD_REQUEST_BUILD_STAGE = "build";
    protected static final String UPLOAD_REQUEST_UPLOAD_STAGE = "upload";
    private static final Gson GSON = ComponentGsonFactory.createGsonNoPrettyPrint();

    private final RICostDataUploader riCostDataUploader;

    private final AccountExpensesUploader accountExpensesUploader;

    private final PriceTableUploader priceTableUploader;


    private final BusinessAccountPriceTableKeyUploader businessAccountPriceTableKeyUploader;

    // a cache of all the cloud service non-market entities and cost dto's discovered by cloud
    // probes. The concurrent map is probably overkill, but the idea is to support concurrent writes.
    // When we read during the upload stage, we will make a defensive copy anyways.
    private final Map<Long, TargetCostData> costDataByTargetId = new ConcurrentHashMap<>();

    // we'll be using a stamped lock to guard the target cost data map. However, we are inverting
    // the operations -- we are aiming to support concurrent puts (it's a concurrent map) but
    // lock for a single reader when we make a copy of the map.
    private StampedLock targetCostDataCacheLock = new StampedLock();

    private final Map<Long, SDKProbeType> probeTypesForTargetId = new HashMap<>();

    public DiscoveredCloudCostUploader(@Nonnull RICostDataUploader riCostDataUploader,
                                       @Nonnull AccountExpensesUploader accountExpensesUploader,
                                       @Nonnull PriceTableUploader priceTableUploader,
                                       @Nonnull final BusinessAccountPriceTableKeyUploader
                                               businessAccountPriceTableKeyUploader) {
        this.riCostDataUploader = riCostDataUploader;
        this.accountExpensesUploader = accountExpensesUploader;
        this.priceTableUploader = priceTableUploader;
        this.businessAccountPriceTableKeyUploader = businessAccountPriceTableKeyUploader;
    }


    /**
     * Add the {@link TargetCostData} object to the per-target cache. If there is an existing entry
     * with the same target it, it will get replaced.
     *
     * @param costData the TargetCostData instance to add
     */
    private void cacheCostData(@Nonnull final TargetCostData costData) {
        logger.trace("Getting read lock for target cost data map");
        long stamp = targetCostDataCacheLock.readLock();
        logger.trace("Got read lock for target cost data map");
        try {
            costDataByTargetId.put(costData.targetId, costData);
        } finally {
            logger.trace("Releasing read lock for target cost data map");
            targetCostDataCacheLock.unlock(stamp);
        }
    }

    @VisibleForTesting
    Map<Long, SDKProbeType> getProbeTypesForTargetId() {
        return Collections.unmodifiableMap(probeTypesForTargetId);
    }

    /**
     * Get an immutable snapshot of the cost data map in it's current state
     *
     * @return an {@link ImmutableMap} of the cost data objects, by target id.
     */
    @VisibleForTesting
    Map<Long, TargetCostData> getCostDataByTargetIdSnapshot() {
        logger.trace("Getting write lock for target cost data map");
        long stamp = targetCostDataCacheLock.writeLock();
        logger.trace("Got write lock for target cost data map");
        try {
            return ImmutableMap.copyOf(this.costDataByTargetId);
        } finally {
            logger.trace("Releasing write lock for target cost data map");
            targetCostDataCacheLock.unlock(stamp);
        }
    }

    /**
     * This is called when a discovery completes.
     * <p>
     * Set aside any cloud cost data contained in the discovery response for the given target.
     * We will use this data later, in the topology pipeline.
     *
     * @param targetId
     * @param discovery
     * @param nonMarketEntityDTOS
     */
    public void recordTargetCostData(long targetId,
                                     @Nonnull final Optional<SDKProbeType> optionalSDKProbeType,
                                     @Nonnull Discovery discovery,
                                     @Nonnull final List<NonMarketEntityDTO> nonMarketEntityDTOS,
                                     @Nonnull final List<CostDataDTO> costDataDTOS,
                                     @Nullable final PriceTable priceTable) {
        SDKProbeType probeType = optionalSDKProbeType.orElse(null);
        if (probeType == null) {
            logger.warn("Skipping price tables for unknown probeType for targetId {}.", targetId);
            return;
        }

        // add the probe type to our discovered probe types list
        probeTypesForTargetId.put(targetId, probeType);

        // cache the cost data objects and non-market entities so our uploader helpers can mine them
        // for data
        TargetCostData costData = new TargetCostData();
        costData.targetId = targetId;
        costData.discovery = discovery;
        costData.cloudServiceEntities = nonMarketEntityDTOS.stream()
                .filter(nme -> NonMarketEntityType.CLOUD_SERVICE == nme.getEntityType())
                .collect(Collectors.toList());
        costData.costDataDTOS = costDataDTOS;
        cacheCostData(costData);

        // the price table helper will cache it's own data
        priceTableUploader.recordPriceTable(targetId, probeType, priceTable);
    }

    /**
     * When a target is removed, we will remove any cached cloud cost data associated with it.
     *
     * @param targetId
     */
    public void targetRemoved(long targetId) {
        // Try to retrieve the probe type for the target that was just removed. This may be null--
        // if discovery has not completed for the removed target, it won't be in the map yet.
        // If the target is not in the probe type map yet, then no data will have been stored for it.
        final SDKProbeType probeType = probeTypesForTargetId.get(targetId);
        if (probeType != null) {
            priceTableUploader.targetRemoved(targetId, probeType);
            probeTypesForTargetId.remove(targetId);
            long stamp = targetCostDataCacheLock.readLock();
            try {
                costDataByTargetId.remove(targetId);
            } finally {
                logger.trace("Releasing read lock for target cost data map");
                targetCostDataCacheLock.unlock(stamp);
            }
        }
    }

    /**
     * <p>Upload the cloud cost data.
     * </p>
     * Called in the topology pipeline after the stitching context has been created, but before
     * it has been converted to a topology map. Ths is because a lot of the data we need is in the
     * raw cloud entity data, much of which we lose in the conversion to topology map.
     *
     * <p>We will be cross-referencing data from the cost DTO's, non-market entities, and topology
     * entities (in stitching entity form), from the billing and discovery probes. So there may be
     * some sensitivity to discovery mismatches between billing and discovery probe data.
     * </p>
     *
     * @param topologyInfo
     * @param stitchingContext
     */
    public synchronized void uploadCostData(TopologyInfo topologyInfo,
                                            StitchingContext stitchingContext) {

        // create a copy of the cost data map so we have a stable data set for this upload step.
        Map<Long, TargetCostData> costDataByTargetIdSnapshot = getCostDataByTargetIdSnapshot();

        // build a map allowing easy translation from cloud service local id's to TP oids
        CloudEntitiesMap cloudEntitiesMap = new CloudEntitiesMap(stitchingContext, probeTypesForTargetId);
        try {
            // call the upload methods of our helper objects.
            try {
                accountExpensesUploader.uploadAccountExpenses(costDataByTargetIdSnapshot, topologyInfo,
                    stitchingContext, cloudEntitiesMap);
            } catch (RuntimeException e) {
                logger.error("Failed to upload account expenses data.", e);
            }

            try {
                riCostDataUploader.uploadRIData(costDataByTargetIdSnapshot, topologyInfo,
                    stitchingContext, cloudEntitiesMap);
            } catch (RuntimeException e) {
                logger.error("Failed to upload RI data.", e);
            }

            try {
                businessAccountPriceTableKeyUploader.uploadAccountPriceTableKeys(stitchingContext,
                    probeTypesForTargetId);
            } catch (RuntimeException e) {
                logger.error("Failed to upload price table keys.", e);
            }

            try {
                priceTableUploader.checkForUpload(probeTypesForTargetId, cloudEntitiesMap);
            } catch (RuntimeException e) {
                logger.error("Failed to upload price table.", e);
            }

        } finally {
            // there will be exceptions if cost component is not running, we should remove
            // ReservedInstance from topology regardless of whether cost component is started or
            // not, since they are only used for cost, and should not be broadcast to other
            // components (e.g. repository)
            List<TopologyStitchingEntity> riEntitiesToRemove = stitchingContext
                    .getEntitiesOfType(EntityType.RESERVED_INSTANCE)
                    .collect(Collectors.toList());
            riEntitiesToRemove.forEach(stitchingContext::removeEntity);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public void collectDiags(@Nonnull DiagnosticsAppender appender) throws DiagnosticsException {
        final Map<Long, String> strProbeTypesForTargetId = probeTypesForTargetId.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getProbeType()));

        // return the type -> targets map serialized, and then each cost data item serialized
        appender.appendString(GSON.toJson(strProbeTypesForTargetId));
        for (TargetCostData costData: costDataByTargetId.values()) {
            appender.appendString(GSON.toJson(costData));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void restoreDiags(@Nonnull final List<String> collectedDiags) {
        if (collectedDiags.isEmpty()) {
            logger.info("Empty diags - not restoring anything.");
            return;
        }

        final Map<Long, String> strProbeTypesForTargetId =
            GSON.fromJson(collectedDiags.get(0), new TypeToken<Map<Long, String>>() {}.getType());
        strProbeTypesForTargetId.forEach((targetId, probeType) -> {
            final SDKProbeType sdkProbeType = SDKProbeType.create(probeType);
            if (sdkProbeType != null) {
                probeTypesForTargetId.put(targetId, sdkProbeType);
            } else {
                logger.error("Failed to restore SDK probe type mapping for probe type: {}", probeType);
            }
        });

        for (int i = 1; i < collectedDiags.size(); ++i) {
            final TargetCostData costData = GSON.fromJson(collectedDiags.get(i), TargetCostData.class);
            costDataByTargetId.put(costData.targetId, costData);
        }
    }

    @Nonnull
    @Override
    public String getFileName() {
        return DISCOVERED_CLOUD_COST_NAME;
    }

    /**
     * Caches a target's cost data + discovery information.
     */
    public static class TargetCostData {
        public long targetId;

        public Discovery discovery;

        public List<NonMarketEntityDTO> cloudServiceEntities;

        public List<CostDataDTO> costDataDTOS;
    }

}
