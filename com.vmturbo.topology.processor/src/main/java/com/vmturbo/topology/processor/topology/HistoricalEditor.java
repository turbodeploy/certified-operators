package com.vmturbo.topology.processor.topology;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableSet;
import com.google.gson.Gson;
import com.google.protobuf.InvalidProtocolBufferException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProjectType;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange;
import com.vmturbo.common.protobuf.topology.HistoricalInfo.HistoricalInfoDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO.Builder;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.HistoricalValues;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.proactivesupport.DataMetricTimer;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.processor.historical.Conversions;
import com.vmturbo.topology.processor.historical.HistoricalCommodityInfo;
import com.vmturbo.topology.processor.historical.HistoricalInfo;
import com.vmturbo.topology.processor.historical.HistoricalServiceEntityInfo;
import com.vmturbo.topology.processor.historical.HistoricalUtilizationDatabase;

/**
 * Editor to calculate the correct used and peak values for the commodities,
 * considering also their historical values.
 * We want to smooth steep changes in the used and peak values, thus the real
 * values we use in market 2 (for used and peak values) are weighted values
 * where we consider both the current and historical values.
 */
public class HistoricalEditor {

    private static final Logger logger = LogManager.getLogger();

    private final HistoricalUtilizationDatabase historicalUtilizationDatabase;

    private final ExecutorService executorService;

    private HistoricalInfo historicalInfo;

    // commodity types already logged as missing historical info - recreated on
    // each invocation of applyCommodityEdits()
    private Set<Integer> commodityTypesAlreadyLoggedAsMissingHistory = null;

    // the weight of the historical used value in the calculation of the weighted used value
    public static final float globalUsedHistoryWeight = 0.5f;

    // the weight of the historical peak value in the calculation of the weighted peak value
    public static final float globalPeakHistoryWeight = 0.99f;

    public static final float E = 0.00001f; // to compare floats for equality

    private static final String EMPTY_JSON = "{}";

    private boolean isPlan;

    /**
     * A metric that tracks the time taken to load the historical used and peak values.
     */
    private static final DataMetricSummary HISTORICAL_USED_AND_PEAK_VALUES_LOAD_TIME_SUMMARY =
            DataMetricSummary.builder()
                    .withName("historical_used_and_peak_values_load_time_seconds")
                    .withHelp("Time taken to load the historical used and peak values from history.")
                    .build();

    /**
     * A set that holds the commodities that do not use historical values.
     * These include access commodities, and commodities that do not exhibit spikes, volatility,
     * or any other types of noises so data smoothing is not needed.
     */
    private static final ImmutableSet<Integer> COMMODITIES_TO_SKIP_HISTORICAL_EDITOR =
            ImmutableSet.<Integer>builder()
            // Access commodities
            .add(CommonDTO.CommodityDTO.CommodityType.CLUSTER_VALUE)
            .add(CommonDTO.CommodityDTO.CommodityType.SEGMENTATION_VALUE)
            .add(CommonDTO.CommodityDTO.CommodityType.DATASTORE_VALUE)
            .add(CommonDTO.CommodityDTO.CommodityType.VDC_VALUE)
            .add(CommonDTO.CommodityDTO.CommodityType.DSPM_ACCESS_VALUE)
            .add(CommonDTO.CommodityDTO.CommodityType.DATACENTER_VALUE)
            .add(CommonDTO.CommodityDTO.CommodityType.APPLICATION_VALUE)
            .add(CommonDTO.CommodityDTO.CommodityType.NETWORK_VALUE)
            .add(CommonDTO.CommodityDTO.CommodityType.STORAGE_CLUSTER_VALUE)
            .add(CommonDTO.CommodityDTO.CommodityType.ACCESS_VALUE)
            .add(CommonDTO.CommodityDTO.CommodityType.VAPP_ACCESS_VALUE)
            .add(CommonDTO.CommodityDTO.CommodityType.DRS_SEGMENTATION_VALUE)
            .add(CommonDTO.CommodityDTO.CommodityType.VMPM_ACCESS_VALUE)
            .add(CommonDTO.CommodityDTO.CommodityType.DISK_ARRAY_ACCESS_VALUE)
            .add(CommonDTO.CommodityDTO.CommodityType.SERVICE_LEVEL_CLUSTER_VALUE)
            .add(CommonDTO.CommodityDTO.CommodityType.PROCESSING_UNITS_VALUE)
            .add(CommonDTO.CommodityDTO.CommodityType.TENANCY_ACCESS_VALUE)
            .add(CommonDTO.CommodityDTO.CommodityType.LICENSE_ACCESS_VALUE)
            .add(CommonDTO.CommodityDTO.CommodityType.TEMPLATE_ACCESS_VALUE)
            .add(CommonDTO.CommodityDTO.CommodityType.ZONE_VALUE)
            // Commodities that do not exhibit volatility
            .add(CommonDTO.CommodityDTO.CommodityType.VCPU_REQUEST_VALUE)
            .add(CommonDTO.CommodityDTO.CommodityType.VMEM_REQUEST_VALUE)
            .add(CommonDTO.CommodityDTO.CommodityType.STORAGE_PROVISIONED_VALUE)
            .add(CommonDTO.CommodityDTO.CommodityType.MEM_PROVISIONED_VALUE)
            .add(CommonDTO.CommodityDTO.CommodityType.CPU_PROVISIONED_VALUE)
            .add(CommonDTO.CommodityDTO.CommodityType.COUPON_VALUE)
            .add(CommonDTO.CommodityDTO.CommodityType.STORAGE_AMOUNT_VALUE)
            .add(CommonDTO.CommodityDTO.CommodityType.INSTANCE_DISK_SIZE_VALUE)
            .add(CommonDTO.CommodityDTO.CommodityType.INSTANCE_DISK_TYPE_VALUE)
            .build();

    public HistoricalEditor(HistoricalUtilizationDatabase historicalUtilizationDatabase, ExecutorService executorService) {
        this.historicalUtilizationDatabase = historicalUtilizationDatabase;
        this.executorService = executorService;
        historicalInfo = new HistoricalInfo();

        final DataMetricTimer loadDurationTimer =
            HISTORICAL_USED_AND_PEAK_VALUES_LOAD_TIME_SUMMARY.startTimer();
        byte[] bytes = historicalUtilizationDatabase.getInfo();
        loadDurationTimer.observe();

        if (bytes != null) {
            HistoricalInfoDTO histInfo = null;
            try {
                histInfo = HistoricalInfoDTO.parseFrom(bytes);
            } catch (InvalidProtocolBufferException e) {
                logger.error(e.getMessage());
            }
            historicalInfo = Conversions.convertFromDto(histInfo);
        }
        logger.info("Time taken to load historical utilization data is " + loadDurationTimer.getTimeElapsedSecs());
    }

    /**
     * This method calculates the used and peak values for all the commodities
     * considering the values read from mediation and the historical values from
     * the previous cycle.
     * (Note : This method is synchronized because HistoricalEditor class object is currently
     * being shared by plan and realtime topology pipeline)
     *
     * @param graph The topology graph which contains all the SEs
     * @param changes to iterate over and find relevant changes (e.g baseline change)
     * @param topologyInfo to identify if it is a cluster headroom plan
     */
    public synchronized void applyCommodityEdits(@Nonnull final TopologyGraph<TopologyEntity> graph,
                                    @Nonnull final List<ScenarioChange> changes,
                                    @Nonnull final TopologyDTO.TopologyInfo topologyInfo) {
        // Don't allow historical editor to update commodities for headroom or historicalBaseline plan.
        if (TopologyDTOUtil.isPlanType(PlanProjectType.CLUSTER_HEADROOM, topologyInfo) ||
            changes.stream().anyMatch(change -> change.getPlanChanges().hasHistoricalBaseline())) {
            return;
        }

        isPlan = TopologyDTOUtil.isPlan(topologyInfo);

        if (isPlan) {
            // This set is created so that we don't process one entity more than once.
            // This can happen in the case when we have many clones from same original entity.
            final Set<Long> entityOidsProcessed = new HashSet<>();
            graph.entities().forEach(entity -> {
                TopologyEntityDTO.Builder entityDTO = entity.getTopologyEntityDtoBuilder();
                if (entity.getClonedFromEntity().isPresent()) {
                    entityDTO = entity.getClonedFromEntity().get();
                }
                if (!entityOidsProcessed.contains(entityDTO.getOid())) {
                    processCommoditySoldList(entityDTO);
                    processCommodityBoughtList(entityDTO);
                    entityOidsProcessed.add(entityDTO.getOid());
                }
            });
            copyHistoricalValuesToClonedEntities(graph, changes);
        } else {
            Stream<TopologyEntity> entities = graph.entities();
            Iterable<TopologyEntity> entitiesIterable = entities::iterator;
            this.commodityTypesAlreadyLoggedAsMissingHistory = new HashSet<>();

            // Clean historical utilization data structure
            // Construct the set of all oids existing from previous iterations
            Set<Long> histOids = new HashSet<Long>(historicalInfo.keySet());
            // Remove the set of oids existing in this iteration.
            // The oids not existing any more remaining
            for (TopologyEntity entity : entitiesIterable) {
                histOids.remove(entity.getOid());
            }

            // Remove the oids not existing any more.
            for (Long oid : histOids) {
                historicalInfo.remove(oid);
            }

            entities = graph.entities();
            entitiesIterable = entities::iterator;

            for (TopologyEntity entity : entitiesIterable) {
                if (!historicalInfo.containsKey(entity.getOid())) {
                    HistoricalServiceEntityInfo histSeInfo = new HistoricalServiceEntityInfo();
                    histSeInfo.setSeOid(entity.getOid());
                    historicalInfo.put(entity.getOid(), histSeInfo);
                }

                processCommoditySoldList(entity.getTopologyEntityDtoBuilder());

                processCommodityBoughtList(entity.getTopologyEntityDtoBuilder());

                // Clean historical data for commodities not existing any more (e.g. storage)
                // and reinitailize commodities for the next topology cycle
                postProcessingHistoricalInfo(historicalInfo.get(entity.getOid()));
            }
        }
        this.commodityTypesAlreadyLoggedAsMissingHistory = null;

        executorService.submit(() -> historicalUtilizationDatabase.saveInfo(historicalInfo));

    }

    /**
     * Remove the commodities doesnt exist any more from the historical data structure and reset
     * commodity attributes to get ready for next cycle.
     *
     * @param histSeInfo the HistoricalServiceEntityInfo representing the entity
     */
    private void postProcessingHistoricalInfo(final @Nullable HistoricalServiceEntityInfo histSeInfo) {
        if (histSeInfo != null) {
            // Examine sold commodities
            List<HistoricalCommodityInfo> histSoldInfoList = histSeInfo.getHistoricalCommoditySold();
            Iterator<HistoricalCommodityInfo> iterSold = histSoldInfoList.iterator();
            while (iterSold.hasNext()) {
                HistoricalCommodityInfo histSoldInfo = iterSold.next();
                if (!histSoldInfo.getUpdated()) {
                    // the commodity is not updated with smoothed value, which means it no longer
                    // needed
                    iterSold.remove();
                }
            }
            // Examine bought commodities
            List<HistoricalCommodityInfo> histBoughtInfoList = histSeInfo.getHistoricalCommodityBought();
            Iterator<HistoricalCommodityInfo> iterBought = histBoughtInfoList.iterator();
            while (iterBought.hasNext()) {
                HistoricalCommodityInfo histBoughtInfo = iterBought.next();
                if (!histBoughtInfo.getUpdated()) {
                    // the commodity is not updated with smoothed value, which means it no longer
                    // needed
                    iterBought.remove();
                }
            }

            // Set matched and updated attributes to false to prepare for next market cycle
            // It is a kind of initialization to have all the commodities unmatched by commodities
            // of the next cycle and considering that they possibly not exist in the next cycle
            for (int i = 0; i < histSoldInfoList.size(); i++) {
                reinitializingCommodity(histSoldInfoList.get(i));
            }

            for (int i = 0; i < histBoughtInfoList.size(); i++) {
                reinitializingCommodity(histBoughtInfoList.get(i));
            }
        }
    }

    /**
     * Initializting the matched and updated attributes to false to prepare for the next cycle.
     *
     * @param histComm the given HistoricalCommodityInfo object
     */
    private void reinitializingCommodity(final @Nonnull HistoricalCommodityInfo histComm) {
        histComm.setMatched(false);
        histComm.setUpdated(false);
    }

    /**
     * Returns the provider or volume id.
     *
     * @param commBoughtProvider the given commodities bought bundle
     * @return oid of the provider or volume
     */
    private long resolveSourceId(final @Nonnull CommoditiesBoughtFromProvider.Builder commBoughtProvider) {
        long sourceId = -1;
        if (commBoughtProvider.hasVolumeId()) {
            sourceId = commBoughtProvider.getVolumeId();
        } else if (commBoughtProvider.hasProviderId()) {
            sourceId = commBoughtProvider.getProviderId();
        } else {
            logger.error("No volumeId or providerId exists for a bought commodity");
        }
        return sourceId;
    }

    /**
     * Whether the commodity has to use historical value or not.
     *
     * @param commodityType the commodity type
     * @return true if commodity has to use historical value, otherwise false.
     */
    private boolean useHistoricalValues(int commodityType) {
        return !COMMODITIES_TO_SKIP_HISTORICAL_EDITOR.contains(commodityType);
    }

    /**
     * A setter method for populating attributes of a HistoricalCommodityInfo object.
     *
     * @param histCommInfo the commodity object to hold historical utilization data
     * @param commType the commodity type
     * @param used the historical used to be set on commodity
     * @param peak the historical peak to be set on commodity
     * @param sourceId the provider or volume id of the commodity
     * @param isMatched whether the commodity is matched with one in previous cycle
     * @param isUpdated whether the commodity's historical values is up-to-date
     * @return the commodity object
     */
    private HistoricalCommodityInfo populateHistoricalCommodityInfo(final @Nonnull HistoricalCommodityInfo histCommInfo,
                                                                    final @Nonnull CommodityType commType,
                                                                    final float used, final float peak,
                                                                    final long sourceId, final boolean isMatched,
                                                                    final boolean isUpdated) {
        histCommInfo.setCommodityTypeAndKey(commType);
        histCommInfo.setHistoricalUsed(used);
        histCommInfo.setHistoricalPeak(peak);
        histCommInfo.setSourceId(sourceId);
        histCommInfo.setMatched(isMatched);
        histCommInfo.setUpdated(isUpdated);
        return histCommInfo;
    }

    /**
     * Calculates and set new historical values on new HistoricalCommodityInfo.
     *
     * @param newCommInfo the HistoricalCommodityInfo object to be set with new values
     * @param oldCommInfo the previous HistoricalCommodityInfo object whose historical values will
     *                    be used to calculate the new historical values in the current cycle
     * @param histSeInfo the HistoricalServiceEntityInfo representing the entity
     * @param used current used value
     * @param peak current peak value
     * @param oid the oid of the entity
     */
    private float[] calculateAndSetNewHistoricalValues(final @Nonnull HistoricalCommodityInfo newCommInfo,
                                                    final @Nonnull HistoricalCommodityInfo oldCommInfo,
                                                    final @Nonnull HistoricalServiceEntityInfo histSeInfo,
                                                    final float used, final float peak, final long oid) {
        // Initialize newUsed and newPeak to the current values.
        float newUsed = used;
        float newPeak = peak;
        if (oldCommInfo.getHistoricalUsed() > 0) {
            newUsed = calculateSmoothedValue(true, used, oldCommInfo);
        }
        if (oldCommInfo.getHistoricalPeak() > 0) {
            newPeak = calculateSmoothedValue(false, peak, oldCommInfo);
        }
        if (!isPlan) {
            populateHistoricalCommodityInfo(newCommInfo,
                newCommInfo.getCommodityTypeAndKey(), newUsed, newPeak,
                newCommInfo.getSourceId(), newCommInfo.getMatched(), true);
            historicalInfo.replace(oid, histSeInfo);
        }
        logger.trace("Entity={}, Bought commodity={}, previous Historical used={}," +
                " previous Historical peak={}, Calculated used={}, Calculated peak={}",
            oid,  newCommInfo.getCommodityTypeAndKey(),
            oldCommInfo.getHistoricalUsed(), oldCommInfo.getHistoricalPeak(),
            newUsed, newPeak);
        return new float[]{newUsed, newPeak};
    }

    /**
     * This method calculates the used and peak values for all the sold commodities
     * considering the values read from mediation and the historical values from
     * the previous cycle.
     * @param topoEntity current entity's builder.
     */
    private void processCommoditySoldList(TopologyEntityDTO.Builder topoEntity) {
        // Check if historicalCommoditySold exists in historical data structure.
        // If not, add it. Otherwise, add new commodities and match the existing ones.
        HistoricalServiceEntityInfo histSeInfo = historicalInfo.get(topoEntity.getOid());
        if (histSeInfo == null) {
            if (isPlan) {
                return;
            }
            logger.error("A HistoricalServiceEntityInfo data structure is missing for the service entity {}", topoEntity.getOid());
        } else if (histSeInfo.getHistoricalCommoditySold().size() == 0) {
            // Add all the sold commodities info
            List<HistoricalCommodityInfo> histSoldInfoList = new ArrayList<>();
            for (CommoditySoldDTO.Builder commSold : topoEntity.getCommoditySoldListBuilderList()) {
                if (useHistoricalValues(commSold.getCommodityType().getType())) {
                    HistoricalCommodityInfo histSoldInfo = populateHistoricalCommodityInfo(new HistoricalCommodityInfo(),
                            commSold.getCommodityType(), -1.0f, -1.0f, -1, false, false);
                    histSoldInfoList.add(histSoldInfo);
                }
            }
            // Add new sold commodity list to entity
            histSeInfo.setHistoricalCommoditySold(histSoldInfoList);
            historicalInfo.replace(topoEntity.getOid(), histSeInfo);
        } else {
            // Add new sold commodities info, match the old ones
            for (CommoditySoldDTO.Builder commSold : topoEntity.getCommoditySoldListBuilderList()) {
                if (useHistoricalValues(commSold.getCommodityType().getType())) {
                    List<HistoricalCommodityInfo>  histSoldInfoList =
                            histSeInfo.getHistoricalCommoditySold();
                    boolean isMatched = false;
                    for (int i = 0; i < histSoldInfoList.size(); i++) {
                        HistoricalCommodityInfo histSoldInfo = histSoldInfoList.get(i);
                        if (histSoldInfo.getCommodityTypeAndKey().equals(commSold.getCommodityType())) {
                            histSoldInfo.setMatched(true);
                            isMatched = true;
                            break;
                        }
                    }
                    if (!isMatched) {
                        HistoricalCommodityInfo histSoldInfo =
                                populateHistoricalCommodityInfo(new HistoricalCommodityInfo(),
                                        commSold.getCommodityType(), -0.1f, -0.1f, -1, false, false);
                        histSoldInfoList.add(histSoldInfo);
                    }
                }
            }
            historicalInfo.replace(topoEntity.getOid(), histSeInfo);
        }

        for (CommoditySoldDTO.Builder commSold : topoEntity.getCommoditySoldListBuilderList()) {
            calculateSmoothedValuesForCommoditySold(topoEntity, commSold);
        }
    }

    /**
     * Calculates the historical values for used or peak.
     *
     * @param isUsed true if to calculate used value, otherwise peak value.
     * @param currentValue the current used or peak value
     * @param comm the commodity whose historical utilization will be used for smoothing
     * @return the smoothed value
     */
    private float calculateSmoothedValue(final boolean isUsed, final float currentValue,
                                         final @Nonnull HistoricalCommodityInfo comm) {
        return isUsed ? (globalUsedHistoryWeight * comm.getHistoricalUsed() + (1 - globalUsedHistoryWeight)
                * currentValue) : (globalPeakHistoryWeight * comm.getHistoricalPeak()
                + (1 - globalPeakHistoryWeight) * currentValue);
    }

    /**
     * Calculates smoothed historical values for commodity sold.
     *
     * @param topoEntity the topology entity
     * @param topoCommSold the given commodity sold
     */
    private void calculateSmoothedValuesForCommoditySold(TopologyEntityDTO.Builder topoEntity, Builder topoCommSold) {
        if (topoCommSold == null) {
            logger.error("The topoCommSold is null for the entity {}", topoEntity.getOid());
            return;
        }
        final CommodityType commodityType = topoCommSold.getCommodityType();
        // Using historical values in calculation of used and peak
        if (!useHistoricalValues(commodityType.getType())) {
            return;
        }
        float used = (float) topoCommSold.getUsed();
        float peak = (float) topoCommSold.getPeak();
        logger.trace("Entity={}, Sold commodity={}, Used from mediation={}, Peak from mediation={}", topoEntity.getOid(),
                topoCommSold.getCommodityType().getType(), used, peak);


        HistoricalServiceEntityInfo histSeInfo = historicalInfo.get(topoEntity.getOid());
        if (histSeInfo == null) {
            if (isPlan) {
                return;
            }
            logger.error("A HistoricalServiceEntityInfo data structure is missing for the service entity {}", topoEntity.getOid());
        } else {
            boolean commSoldFound = false;
            List<HistoricalCommodityInfo> histSoldInfoList = histSeInfo.getHistoricalCommoditySold();
            for (int i = 0; i < histSoldInfoList.size(); i++) {
                HistoricalCommodityInfo histSoldInfo = histSoldInfoList.get(i);
                if (histSoldInfo.getCommodityTypeAndKey().equals(commodityType)) {
                    commSoldFound = true;
                    if (histSoldInfo.getMatched()) {
                        float[] newValues = calculateAndSetNewHistoricalValues(histSoldInfo, histSoldInfo, histSeInfo,
                                used, peak, topoEntity.getOid());
                        used = newValues[0];
                        peak = newValues[1];
                    } else if (!histSoldInfo.getMatched() && !isPlan){
                        populateHistoricalCommodityInfo(histSoldInfo, commodityType, used, peak,
                            histSoldInfo.getSourceId(), histSoldInfo.getMatched(), true);
                        historicalInfo.replace(topoEntity.getOid(), histSeInfo);
                    }
                    topoCommSold.getHistoricalUsedBuilder().setHistUtilization(used);
                    topoCommSold.getHistoricalPeakBuilder().setHistUtilization(peak);
                    break;
                }
            }
            if (!commSoldFound) {
                int type = commodityType.getType();
                // don't repeat log messages within a cycle
                if (!commodityTypesAlreadyLoggedAsMissingHistory.contains(type)) {
                    logger.error("A sold commodity with type {} is missing in HistoricalServiceEntityInfo", type);
                    commodityTypesAlreadyLoggedAsMissingHistory.add(type);
                }
            }
        }
    }

    /*
     * This method calculates the used and peak values for all the bought commodities
     * considering the values read from mediation and the historical values from
     * the previous cycle.
     * @param topoEntity current entity's builder.
     */
    private void processCommodityBoughtList(TopologyEntityDTO.Builder topoEntity) {
        // Check if histCommBought exists in historical data structure.
        // If not, add it. Otherwise, add new commodities and match the existing ones.
        HistoricalServiceEntityInfo histSeInfo = historicalInfo.get(topoEntity.getOid());
        if (histSeInfo == null) {
            if (isPlan) {
                return;
            }
            logger.error("A HistoricalServiceEntityInfo data structure is missing for the service entity {}", topoEntity.getOid());
        } else if (histSeInfo.getHistoricalCommodityBought().size() == 0) {
            // Add all the bought commodities info
            List<HistoricalCommodityInfo> histBoughtInfoList = new ArrayList<>();
            for (CommoditiesBoughtFromProvider.Builder commBoughtProvider
                    : topoEntity.getCommoditiesBoughtFromProvidersBuilderList()) {
                long sourceId = resolveSourceId(commBoughtProvider);
                for (CommodityBoughtDTO.Builder commBought
                        : commBoughtProvider.getCommodityBoughtBuilderList()) {
                    if (useHistoricalValues(commBought.getCommodityType().getType())) {
                        HistoricalCommodityInfo histBoughtInfo = populateHistoricalCommodityInfo(
                                new HistoricalCommodityInfo(), commBought.getCommodityType(),
                                -1.0f, -1.0f, sourceId, false, false);
                        histBoughtInfoList.add(histBoughtInfo);
                    }
                }
            }
            histSeInfo.setHistoricalCommodityBought(histBoughtInfoList);
            historicalInfo.replace(topoEntity.getOid(), histSeInfo);
        } else {
            // Add new bought commodities info, match the old ones
            for (CommoditiesBoughtFromProvider.Builder commBoughtProvider
                    : topoEntity.getCommoditiesBoughtFromProvidersBuilderList()) {
                long sourceId = resolveSourceId(commBoughtProvider);
                for (CommodityBoughtDTO.Builder commBought : commBoughtProvider
                        .getCommodityBoughtBuilderList()) {
                    if (useHistoricalValues(commBought.getCommodityType().getType())) {
                        boolean isMatched = false;
                        List<HistoricalCommodityInfo> histBoughtInfoList =
                                histSeInfo.getHistoricalCommodityBought();
                        for (HistoricalCommodityInfo histBoughtInfo : histBoughtInfoList) {
                            if (histBoughtInfo.getCommodityTypeAndKey()
                                    .equals(commBought.getCommodityType()) &&
                                    (histBoughtInfo.getSourceId() == sourceId) &&
                                    !histBoughtInfo.getMatched()) {
                                histBoughtInfo.setMatched(true);
                                isMatched = true;
                                break;
                            }
                        }
                        if (!isMatched) {
                            HistoricalCommodityInfo histBoughtInfo = populateHistoricalCommodityInfo(
                                    new HistoricalCommodityInfo(), commBought.getCommodityType(),
                                    -1.0f, -1.0f, sourceId, false, false);
                            histBoughtInfoList.add(histBoughtInfo);
                        }
                    }
                }
            }
            historicalInfo.replace(topoEntity.getOid(), histSeInfo);
        }

        for (CommoditiesBoughtFromProvider.Builder commBoughtProvider : topoEntity
                .getCommoditiesBoughtFromProvidersBuilderList()) {
            long sourceId = (commBoughtProvider.hasVolumeId() ? commBoughtProvider.getVolumeId()
                    : commBoughtProvider.getProviderId());
            for (CommodityBoughtDTO.Builder commBought : commBoughtProvider.getCommodityBoughtBuilderList()) {
                calculateSmoothedValuesForCommodityBought(topoEntity, commBought, sourceId);
            }
        }
    }

    /**
     * Returns true if the commodity type matches and the commodity is only initialized.
     *
     * @param histBoughtInfo the HistoricalCommodityInfo
     * @param commodityType the commodity type to be matched
     * @return true if the commodity type matches and the commodity is only initialized.
     */
    private boolean checkIsNewComodityToBeSet(@Nonnull final HistoricalCommodityInfo histBoughtInfo,
                                              @Nonnull final CommodityType commodityType) {
        return histBoughtInfo.getCommodityTypeAndKey().equals(commodityType) &&
                !histBoughtInfo.getMatched() && !histBoughtInfo.getUpdated();
    }

    /**
     * Returns true when both historical used and peak are the default negative values.
     *
     * @param histBoughtInfo the HistoricalCommodityInfo
     * @return true when both historical used and peak are the default negative values.
     */
    private boolean isHistoricalValuesUnset(@Nonnull final HistoricalCommodityInfo histBoughtInfo) {
        return (Math.abs(histBoughtInfo.getHistoricalUsed() + 1.0f) < E) &&
                (Math.abs(histBoughtInfo.getHistoricalPeak() + 1.0f) < E);
    }

    /**
     * Update historical values for commodity that exist in the previous cycle.
     *
     * @param histBoughtInfoList the list of HistoricalCommodityInfo
     * @param histSeInfo HistoricalServiceEntityInfo
     * @param topoEntity the topology entity
     * @param topoCommBought the commodity bought DTO
     * @param sourceId provider or volume id
     * @return true if topoCommBought matches to existing commodity and is being updated.
     */
    private boolean updateAlreadyExistingCommodity(@Nonnull final List<HistoricalCommodityInfo> histBoughtInfoList,
                                                   @Nonnull final HistoricalServiceEntityInfo histSeInfo,
                                                   @Nonnull final TopologyEntityDTO.Builder topoEntity,
                                                   @Nonnull final CommodityBoughtDTO.Builder topoCommBought,
                                                   final long sourceId) {
        for (HistoricalCommodityInfo histBoughtInfo : histBoughtInfoList) {
            if (histBoughtInfo.getCommodityTypeAndKey().equals(topoCommBought.getCommodityType()) &&
                    (histBoughtInfo.getSourceId() == sourceId) && histBoughtInfo.getMatched() &&
                    !histBoughtInfo.getUpdated()) {
                float[] newValues = calculateAndSetNewHistoricalValues(histBoughtInfo, histBoughtInfo, histSeInfo,
                        (float)topoCommBought.getUsed(), (float)topoCommBought.getPeak(), topoEntity.getOid());
                topoCommBought.getHistoricalUsedBuilder().setHistUtilization(newValues[0]);
                topoCommBought.getHistoricalPeakBuilder().setHistUtilization(newValues[1]);
                return true;
            }
        }
        return false;
    }

    /**
     * Update historical values for newly added commodities.
     *
     * @param histBoughtInfoList the list of HistoricalCommodityInfo
     * @param histSeInfo HistoricalServiceEntityInfo
     * @param topoEntity the topology entity
     * @param topoCommBought the commodity bought DTO
     * @param commType commodity type
     * @param sourceId provider or volume id
     */
    private void updateNewlyAddedCommodity(@Nonnull final List<HistoricalCommodityInfo> histBoughtInfoList,
                                           @Nonnull final HistoricalServiceEntityInfo histSeInfo,
                                           @Nonnull final TopologyEntityDTO.Builder topoEntity,
                                           @Nonnull final CommodityBoughtDTO.Builder topoCommBought,
                                           @Nonnull final CommodityType commType,
                                           final long sourceId) {
        float usedQuantity = (float) topoCommBought.getUsed();
        float peakQuantity = (float) topoCommBought.getPeak();
        // Check if there is a unique provider for the same commodity type
        // Else there is no match
        int numberNew = 0;      // the number of new unmatched commodities of the same type
        int numberPrevious = 0; // the number of the previous unmatched commodities of the same type
        int indexCurrent = -1;  // the index of the current commodity in histBoughtInfoList
        int indexMatching = -1; // the index of the previous commodity to be matched in histBoughtInfoList
        for (int i = 0; i < histBoughtInfoList.size(); i++) {
            HistoricalCommodityInfo histBoughtInfo = histBoughtInfoList.get(i);
            if (checkIsNewComodityToBeSet(histBoughtInfo, commType)) {
                if (isHistoricalValuesUnset(histBoughtInfo)) {
                    // It is a new commodity
                    numberNew++;
                    if (histBoughtInfo.getSourceId() == sourceId) {
                        // It corresponds to the current topology entity
                        indexCurrent = i;
                    }
                } else {
                    // It is an old commodity
                    numberPrevious++;
                    indexMatching = i;
                }
            }
        }
        if ((numberNew == 1) && (numberPrevious == 1)) {
            // Match the new commodity with the previous one
            HistoricalCommodityInfo newComm = histBoughtInfoList.get(indexCurrent);
            HistoricalCommodityInfo previousComm = histBoughtInfoList.get(indexMatching);
            float[] newValues = calculateAndSetNewHistoricalValues(newComm, previousComm, histSeInfo,
                previousComm.getHistoricalUsed(), previousComm.getHistoricalPeak(),
                topoEntity.getOid());
            usedQuantity = newValues[0];
            peakQuantity = newValues[1];
        } else {
            // Don't match
            for (int i = 0; i < histBoughtInfoList.size(); i++) {
                HistoricalCommodityInfo histBoughtInfo = histBoughtInfoList.get(i);
                if (checkIsNewComodityToBeSet(histBoughtInfo, commType) &&
                    isHistoricalValuesUnset(histBoughtInfo) &&
                    (histBoughtInfo.getSourceId() == sourceId)) {
                    populateHistoricalCommodityInfo(histBoughtInfo,
                        histBoughtInfo.getCommodityTypeAndKey(), usedQuantity,
                        peakQuantity, histBoughtInfo.getSourceId(),
                        histBoughtInfo.getMatched(), true);
                    historicalInfo.replace(topoEntity.getOid(), histSeInfo);
                    break;
                }
            }
        }
        topoCommBought.getHistoricalUsedBuilder().setHistUtilization(usedQuantity);
        topoCommBought.getHistoricalPeakBuilder().setHistUtilization(peakQuantity);
    }

    /**
     * Calculates smoothed historical values for commodity bought.
     *
     * @param topoEntity the topology entity
     * @param topoCommBought the given commodity bought
     * @param sourceId the provider or volume id of the commodity bought
     */
    private void calculateSmoothedValuesForCommodityBought(TopologyEntityDTO.Builder topoEntity, CommodityBoughtDTO.Builder topoCommBought, long sourceId) {
        float usedQuantity = (float) topoCommBought.getUsed();
        float peakQuantity = (float) topoCommBought.getPeak();
        CommodityType commType = topoCommBought.getCommodityType();
        logger.trace("Entity={}, Bought commodity={}, Used from mediation={}, Peak from mediation={}", topoEntity.getOid(),
            commType.getType(), usedQuantity, peakQuantity);

        if (!useHistoricalValues(commType.getType())) {
            return;
        }
        // Using historical values in calculation of used and peak
        HistoricalServiceEntityInfo histSeInfo = historicalInfo.get(topoEntity.getOid());
        if (histSeInfo == null) {
            if (isPlan) {
                return;
            }
            logger.error("A HistoricalServiceEntityInfo data structure is missing for the service" +
                    " entity {}", topoEntity.getOid());
        } else {
            List<HistoricalCommodityInfo> histBoughtInfoList = histSeInfo.getHistoricalCommodityBought();
            if (!updateAlreadyExistingCommodity(histBoughtInfoList, histSeInfo, topoEntity, topoCommBought, sourceId)) {
                updateNewlyAddedCommodity(histBoughtInfoList,  histSeInfo, topoEntity, topoCommBought, commType, sourceId);
            }
        }
    }

    /**
     * Copy historical values from original entities to cloned entities.
     *
     * @param graph a topology graph which contains all entities
     * @param changes a list of changes in plan
     */
    @VisibleForTesting
    static void copyHistoricalValuesToClonedEntities(
            @Nonnull final TopologyGraph<TopologyEntity> graph,
            @Nonnull final List<ScenarioChange> changes) {
        // Skip if it's not add workload plan.
        if (changes.stream().filter(Objects::nonNull).noneMatch(ScenarioChange::hasTopologyAddition)) {
           return;
        }

        final Stopwatch stopwatch = Stopwatch.createStarted();
        for (TopologyEntity entity : (Iterable<TopologyEntity>)graph.entities()::iterator) {
            // Skip if it's not a cloned entity.
            if (!entity.getClonedFromEntity().isPresent()) {
                continue;
            }


            final TopologyEntityDTO.Builder entityBuilder = entity.getTopologyEntityDtoBuilder();
            final TopologyEntityDTO.Builder originalEntityBuilder = entity.getClonedFromEntity().get();

            copyCommSoldHistoricalValuesToClonedEntities(entityBuilder, originalEntityBuilder);
            copyCommBoughtHistoricalValuesToClonedEntities(entityBuilder, originalEntityBuilder);
        }

        stopwatch.stop();
        logger.info("Copy historical values from original entities to cloned entities took {} ms.",
            stopwatch.elapsed(TimeUnit.MILLISECONDS));
    }

    /**
     * Copy commSold historical values from original entities to cloned entities.
     *
     * @param clonedEntityBuilder the cloned entity builder
     * @param originalEntityBuilder the original entity builder
     */
    private static void copyCommSoldHistoricalValuesToClonedEntities(
            @Nonnull final TopologyEntityDTO.Builder clonedEntityBuilder,
            @Nonnull final TopologyEntityDTO.Builder originalEntityBuilder) {
        // Construct the commType to commSold map of the cloned entity.
        final Map<CommodityType, CommoditySoldDTO.Builder> clonedCommTypeToCommSold =
            getCommTypeToCommSold(clonedEntityBuilder);

        // Construct the commType to commSold map of the original entity.
        final Map<CommodityType, CommoditySoldDTO.Builder> originalCommTypeToCommSold =
            getCommTypeToCommSold(originalEntityBuilder);

        copyCommodityHistoricalValuesToClonedEntities(
            clonedEntityBuilder, originalEntityBuilder,
            clonedCommTypeToCommSold, originalCommTypeToCommSold,
            CommoditySoldDTO.Builder::getHistoricalUsedBuilder,
            CommoditySoldDTO.Builder::getHistoricalPeakBuilder);
    }

    /**
     * Construct the commType to commSold map of the given entity.
     *
     * @param entityBuilder an entity builder
     * @return the commType to commSold map
     */
    private static Map<CommodityType, CommoditySoldDTO.Builder> getCommTypeToCommSold(
            @Nonnull final TopologyEntityDTO.Builder entityBuilder) {
        return entityBuilder.getCommoditySoldListBuilderList().stream()
            .collect(Collectors.toMap(CommoditySoldDTO.Builder::getCommodityType, Function.identity(),
                (commSold1, commSold2) -> {
                    logger.warn("Two commSold {} and {} with same commType {} of entity {} ({}) appear. " +
                        "Keep the first one.", commSold1.getDisplayName(), commSold2.getDisplayName(),
                        entityBuilder.getDisplayName(), entityBuilder.getOid(),
                        commSold1.getCommodityType());
                    return commSold1;
                }));
    }

    /**
     * Copy commBought historical values from original entities to cloned entities.
     *
     * @param clonedEntityBuilder the cloned entity builder
     * @param originalEntityBuilder the original entity builder
     */
    private static void copyCommBoughtHistoricalValuesToClonedEntities(
            @Nonnull final TopologyEntityDTO.Builder clonedEntityBuilder,
            @Nonnull final TopologyEntityDTO.Builder originalEntityBuilder) {
        // Construct the provider oid to commType to commBought map of the cloned entity.
        final Map<Long, Map<CommodityType, CommodityBoughtDTO.Builder>> clonedProviderToCommTypeToCommBought =
            getProviderToCommTypeToCommBought(clonedEntityBuilder);

        // Construct the provider oid to commType to commBought map of the original entity.
        final Map<Long, Map<CommodityType, CommodityBoughtDTO.Builder>> originalProviderToCommTypeToCommBought =
            getProviderToCommTypeToCommBought(originalEntityBuilder);

        // Get the providerOidOfClonedEntity to providerOidOfOriginalEntity map.
        @SuppressWarnings("unchecked")
        final Map<String, Double> oldProviders = new Gson().fromJson(clonedEntityBuilder
            .getEntityPropertyMapMap().getOrDefault("oldProviders", EMPTY_JSON), Map.class);
        final Map<Long, Long> oldProvidersMap;
        try {
            oldProvidersMap = oldProviders.entrySet().stream()
                .collect(Collectors.toMap(e -> Long.decode(e.getKey()),
                    e -> e.getValue().longValue()));
        } catch (NumberFormatException e) {
            logger.error("Failed to get oldProvidersMap.", e);
            return;
        }

        for (Entry<Long, Long> entry : oldProvidersMap.entrySet()) {
            final long providerOidOfClonedEntity = entry.getKey();
            final long providerOidOfOriginalEntity = entry.getValue();

            if (!clonedProviderToCommTypeToCommBought.containsKey(providerOidOfClonedEntity)) {
                logger.warn("Entity {} ({}) is not buying from entity {}.",
                    clonedEntityBuilder.getDisplayName(), clonedEntityBuilder.getOid(), providerOidOfClonedEntity);
                continue;
            }

            if (!originalProviderToCommTypeToCommBought.containsKey(providerOidOfOriginalEntity)) {
                logger.warn("Entity {} ({}) is not buying from entity {}.",
                    originalEntityBuilder.getDisplayName(), originalEntityBuilder.getOid(), providerOidOfOriginalEntity);
                continue;
            }

            copyCommodityHistoricalValuesToClonedEntities(
                clonedEntityBuilder, originalEntityBuilder,
                clonedProviderToCommTypeToCommBought.get(providerOidOfClonedEntity),
                originalProviderToCommTypeToCommBought.get(providerOidOfOriginalEntity),
                CommodityBoughtDTO.Builder::getHistoricalUsedBuilder,
                CommodityBoughtDTO.Builder::getHistoricalPeakBuilder);
        }
    }

    /**
     * Construct the provider oid to commType to commBought map of the given entity.
     *
     * @param entityBuilder an entity builder
     * @return the provider oid to commType to commBought map
     */
    private static Map<Long, Map<CommodityType, CommodityBoughtDTO.Builder>> getProviderToCommTypeToCommBought(
            @Nonnull final TopologyEntityDTO.Builder entityBuilder) {
        return entityBuilder.getCommoditiesBoughtFromProvidersBuilderList().stream()
            .collect(Collectors.toMap(CommoditiesBoughtFromProvider.Builder::getProviderId,
                commBought -> commBought.getCommodityBoughtBuilderList().stream()
                    .collect(Collectors.toMap(CommodityBoughtDTO.Builder::getCommodityType,
                        Function.identity(), (commBought1, commBought2) -> {
                            logger.warn("Two commBought {} and {} with same commType {} of entity {} ({}) appear. " +
                                    "Keep the first one.", commBought1.getDisplayName(), commBought2.getDisplayName(),
                                entityBuilder.getDisplayName(), entityBuilder.getOid(),
                                commBought1.getCommodityType());
                            return commBought1;
                        }))));
    }

    /**
     * Copy commBought historical values from original entities to cloned entities.
     *
     * @param clonedEntityBuilder the cloned entity builder
     * @param originalEntityBuilder the original entity builder
     * @param clonedCommTypeToCommodity commType to CommBought map of cloned entity
     * @param originalCommTypeToCommodity commType to CommBought map of original entity
     * @param historicalUsedExtractor extract historical used values from T
     * @param historicalPeakExtractor extract historical peak values from T
     * @param <T> the type parameter of commodity.
     *            It can be CommoditySoldDTO.Builder or CommodityBoughtDTO.Builder.
     */
    private static <T> void copyCommodityHistoricalValuesToClonedEntities(
            @Nonnull final TopologyEntityDTO.Builder clonedEntityBuilder,
            @Nonnull final TopologyEntityDTO.Builder originalEntityBuilder,
            @Nonnull final Map<CommodityType, T> clonedCommTypeToCommodity,
            @Nonnull final Map<CommodityType, T> originalCommTypeToCommodity,
            @Nonnull final Function<T, HistoricalValues.Builder> historicalUsedExtractor,
            @Nonnull final Function<T, HistoricalValues.Builder> historicalPeakExtractor) {
        // Iterate over the map to copy historical values from original entities to cloned entities.
        for (Entry<CommodityType, T> entry : clonedCommTypeToCommodity.entrySet()) {
            if (!originalCommTypeToCommodity.containsKey(entry.getKey())) {
                logger.warn("Original commodity of commType {} of entity {} ({}) not found.",
                    entry.getKey(), clonedEntityBuilder.getDisplayName(), clonedEntityBuilder.getOid());
                continue;
            }

            if (logger.isTraceEnabled()) {
                logger.trace("Coping historical value of commodity {} from entity {} ({}) to entity {} ({}).",
                    entry.getKey(), originalEntityBuilder.getDisplayName(), originalEntityBuilder.getOid(),
                    clonedEntityBuilder.getDisplayName(), clonedEntityBuilder.getOid());
            }

            // Copy historical used values.
            final HistoricalValues.Builder clonedHistoricalUsedBuilder =
                historicalUsedExtractor.apply(entry.getValue());
            clonedHistoricalUsedBuilder.clear();
            clonedHistoricalUsedBuilder.mergeFrom(
                historicalUsedExtractor.apply(originalCommTypeToCommodity.get(entry.getKey())).build());

            // Copy historical peak values.
            final HistoricalValues.Builder clonedHistoricalPeakBuilder =
                historicalPeakExtractor.apply(entry.getValue());
            clonedHistoricalPeakBuilder.clear();
            clonedHistoricalPeakBuilder.mergeFrom(
                historicalPeakExtractor.apply(originalCommTypeToCommodity.get(entry.getKey())).build());
        }
    }
}
