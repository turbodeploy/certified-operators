package com.vmturbo.topology.processor.topology;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.ImmutableSet;
import com.google.protobuf.InvalidProtocolBufferException;

import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProjectType;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange;
import com.vmturbo.common.protobuf.topology.HistoricalInfo.HistoricalInfoDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
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
import com.vmturbo.topology.processor.historical.HistoricalInfoRecord;
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
            .build();

    public HistoricalEditor(HistoricalUtilizationDatabase historicalUtilizationDatabase, ExecutorService executorService) {
        this.historicalUtilizationDatabase = historicalUtilizationDatabase;
        this.executorService = executorService;
        historicalInfo = new HistoricalInfo();

        final DataMetricTimer loadDurationTimer =
            HISTORICAL_USED_AND_PEAK_VALUES_LOAD_TIME_SUMMARY.startTimer();
        HistoricalInfoRecord record = historicalUtilizationDatabase.getInfo();
        loadDurationTimer.observe();

        if (record != null) {
            byte[] bytes = record.getInfo();
            if (bytes != null) {
                HistoricalInfoDTO histInfo = null;
                try {
                    histInfo = HistoricalInfoDTO.parseFrom(bytes);
                } catch (InvalidProtocolBufferException e) {
                    logger.error(e.getMessage());
                }
                historicalInfo = Conversions.convertFromDto(histInfo);
            }
        }
    }

    /**
     * This method calculates the used and peak values for all the commodities
     * considering the values read from mediation and the historical values from
     * the previous cycle.
     *
     * @param graph The topology graph which contains all the SEs
     * @param changes to iterate over and find relevant changes (e.g baseline change)
     * @param topologyInfo to identify if it is a cluster headroom plan
     */
    public void applyCommodityEdits(@Nonnull final TopologyGraph<TopologyEntity> graph,
                                    @Nonnull final List<ScenarioChange> changes,
                                    @Nonnull final TopologyDTO.TopologyInfo topologyInfo) {
        // Don't allow historical editor to update commodities for headroom or historicalBaseline plan.
        if (TopologyDTOUtil.isPlanType(PlanProjectType.CLUSTER_HEADROOM, topologyInfo) ||
            changes.stream().anyMatch(change -> change.getPlanChanges().hasHistoricalBaseline())) {
            return;
        }

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
                histSeInfo.setUsedHistoryWeight(globalUsedHistoryWeight);
                histSeInfo.setPeakHistoryWeight(globalPeakHistoryWeight);
                historicalInfo.put(entity.getOid(), histSeInfo);
            }

            processCommoditySoldList(entity);

            processCommodityBoughtList(entity);

            // Clean historical data for commodities not existing any more (e.g. storage)
            HistoricalServiceEntityInfo histSeInfo = historicalInfo.get(entity.getOid());
            if (histSeInfo != null) {
                // Examine sold commodities
                List<HistoricalCommodityInfo> histSoldInfoList = histSeInfo.getHistoricalCommoditySold();
                Iterator<HistoricalCommodityInfo> iterSold = histSoldInfoList.iterator();
                while (iterSold.hasNext()) {
                    HistoricalCommodityInfo histSoldInfo = iterSold.next();
                    if (!histSoldInfo.getExisting()) {
                        iterSold.remove();
                    }
                }

                // Examine bought commodities
                List<HistoricalCommodityInfo> histBoughtInfoList = histSeInfo.getHistoricalCommodityBought();
                Iterator<HistoricalCommodityInfo> iterBought = histBoughtInfoList.iterator();
                while (iterBought.hasNext()) {
                    HistoricalCommodityInfo histBoughtInfo = iterBought.next();
                    if (!histBoughtInfo.getExisting()) {
                        iterBought.remove();
                    }
                }

                // Set all matched and existing values to false to prepare for next market cycle
                // It is a kind of initialization to have all the commodities unmatched by commodities
                // of the next cycle and considering that they possibly not exist in the next cycle
                for (int i = 0; i < histSoldInfoList.size(); i++) {
                    HistoricalCommodityInfo histCommSold = histSoldInfoList.get(i);
                    histCommSold.setMatched(false);
                    histCommSold.setExisting(false);
                    histSoldInfoList.set(i, histCommSold);
                }

                for (int i = 0; i < histBoughtInfoList.size(); i++) {
                    HistoricalCommodityInfo histCommBought = histBoughtInfoList.get(i);
                    histCommBought.setMatched(false);
                    histCommBought.setExisting(false);
                    histBoughtInfoList.set(i, histCommBought);
                }

                histSeInfo.setHistoricalCommoditySold(histSoldInfoList);
                histSeInfo.setHistoricalCommodityBought(histBoughtInfoList);
            }

        }
        this.commodityTypesAlreadyLoggedAsMissingHistory = null;

        executorService.submit(() -> historicalUtilizationDatabase.saveInfo(historicalInfo));
    }

    private boolean useHistoricalValues(int commodityType) {
        return !COMMODITIES_TO_SKIP_HISTORICAL_EDITOR.contains(commodityType);
    }

    /**
     * This method calculates the used and peak values for all the sold commodities
     * considering the values read from mediation and the historical values from
     * the previous cycle.
     */
    private void processCommoditySoldList(TopologyEntity topoEntity) {
        final TopologyEntityDTO.Builder entityBuilder = topoEntity.getTopologyEntityDtoBuilder();

        // Check if historicalCommoditySold exists in historical data structure.
        // If not, add it. Otherwise, add new commodities and match the existing ones.
        HistoricalServiceEntityInfo histSeInfo = historicalInfo.get(topoEntity.getOid());
        if (histSeInfo == null) {
            logger.error("A HistoricalServiceEntityInfo data structure is missing for the service entity {}", topoEntity.getOid());
        } else if (histSeInfo.getHistoricalCommoditySold().size() == 0) {
            // Add all the sold commodities info
            List<HistoricalCommodityInfo> histSoldInfoList = new ArrayList<>();
            for (CommoditySoldDTO.Builder commSold : entityBuilder.getCommoditySoldListBuilderList()) {
                if (useHistoricalValues(commSold.getCommodityType().getType())) {
                    HistoricalCommodityInfo histSoldInfo = new HistoricalCommodityInfo();
                    histSoldInfo.setCommodityTypeAndKey(commSold.getCommodityType());
                    histSoldInfo.setHistoricalUsed(-1.0f);
                    histSoldInfo.setHistoricalPeak(-1.0f);
                    histSoldInfo.setSourceId(-1);
                    histSoldInfo.setMatched(false);
                    histSoldInfo.setExisting(false);
                    histSoldInfoList.add(histSoldInfo);
                }
            }
            histSeInfo.setHistoricalCommoditySold(histSoldInfoList);
            historicalInfo.replace(topoEntity.getOid(), histSeInfo);
        } else {
            // Add new sold commodities info, match the old ones
            List<HistoricalCommodityInfo> histSoldInfoList = null;
            for (CommoditySoldDTO.Builder commSold : entityBuilder.getCommoditySoldListBuilderList()) {
                if (useHistoricalValues(commSold.getCommodityType().getType())) {
                    histSoldInfoList = histSeInfo.getHistoricalCommoditySold();
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
                        HistoricalCommodityInfo histSoldInfo = new HistoricalCommodityInfo();
                        histSoldInfo.setCommodityTypeAndKey(commSold.getCommodityType());
                        histSoldInfo.setHistoricalUsed(-1.0f);
                        histSoldInfo.setHistoricalPeak(-1.0f);
                        histSoldInfo.setSourceId(-1);
                        histSoldInfo.setMatched(false);
                        histSoldInfo.setExisting(false);
                        histSoldInfoList.add(histSoldInfo);
                        histSeInfo.setHistoricalCommoditySold(histSoldInfoList);
                    }
                }
            }
            historicalInfo.replace(topoEntity.getOid(), histSeInfo);
        }

        for (CommoditySoldDTO.Builder commSold : entityBuilder.getCommoditySoldListBuilderList()) {
            calculateSmoothValuesForCommoditySold(topoEntity, commSold);
        }
    }

    private void calculateSmoothValuesForCommoditySold(TopologyEntity topoEntity, CommoditySoldDTO.Builder topoCommSold) {
        if (topoCommSold == null) {
            logger.error("The topoCommSold is null for the entity {}", topoEntity.getOid());
            return;
        }
        final CommodityType commodityType = topoCommSold.getCommodityType();
        float used = (float) topoCommSold.getUsed();
        float peak = (float) topoCommSold.getPeak();
        logger.trace("Entity={}, Sold commodity={}, Used from mediation={}, Peak from mediation={}", topoEntity.getOid(),
                topoCommSold.getCommodityType().getType(), used, peak);

        // Using historical values in calculation of used and peak
        if (useHistoricalValues(commodityType.getType())) {
            HistoricalServiceEntityInfo histSeInfo = historicalInfo.get(topoEntity.getOid());
            if (histSeInfo == null) {
                logger.error("A HistoricalServiceEntityInfo data structure is missing for the service entity {}", topoEntity.getOid());
            } else {
                boolean commSoldFound = false;
                List<HistoricalCommodityInfo> histSoldInfoList = histSeInfo.getHistoricalCommoditySold();
                for (int i = 0; i < histSoldInfoList.size(); i++) {
                    HistoricalCommodityInfo histSoldInfo = histSoldInfoList.get(i);
                    if (histSoldInfo.getCommodityTypeAndKey().equals(commodityType)) {
                        commSoldFound = true;
                        if (histSoldInfo.getMatched()) {
                            float usedHistWeight = histSeInfo.getUsedHistoryWeight();
                            float peakHistWeight = histSeInfo.getPeakHistoryWeight();
                            if (histSoldInfo.getHistoricalUsed() > 0) {
                                used = usedHistWeight * histSoldInfo.getHistoricalUsed() + (1 - usedHistWeight) * used;
                            }
                            if (histSoldInfo.getHistoricalPeak() > 0) {
                                peak = peakHistWeight * histSoldInfo.getHistoricalPeak() + (1 - peakHistWeight) * peak;
                            }
                            logger.trace("Entity={}, Sold commodity={}, Historical used={}, Historical peak={}", topoEntity.getOid(),
                                    topoCommSold.getCommodityType().getType(), histSoldInfo.getHistoricalUsed(), histSoldInfo.getHistoricalPeak());
                            logger.trace("Entity={}, Sold commodity={}, Calculated used={}, Calculated peak={}", topoEntity.getOid(),
                                    topoCommSold.getCommodityType().getType(), used, peak);
                        }
                        histSoldInfo.setHistoricalUsed(used);
                        histSoldInfo.setHistoricalPeak(peak);
                        histSoldInfo.setExisting(true);
                        histSoldInfoList.set(i, histSoldInfo);
                        histSeInfo.setHistoricalCommoditySold(histSoldInfoList);
                        historicalInfo.replace(topoEntity.getOid(), histSeInfo);
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
    }

    /**
     * This method calculates the used and peak values for all the bought commodities
     * considering the values read from mediation and the historical values from
     * the previous cycle.
     */
    private void processCommodityBoughtList(TopologyEntity topoEntity) {
        final TopologyEntityDTO.Builder entityBuilder = topoEntity.getTopologyEntityDtoBuilder();

        // Check if histCommBought exists in historical data structure.
        // If not, add it. Otherwise, add new commodities and match the existing ones.
        HistoricalServiceEntityInfo histSeInfo = historicalInfo.get(topoEntity.getOid());
        if (histSeInfo == null) {
            logger.error("A HistoricalServiceEntityInfo data structure is missing for the service entity {}", topoEntity.getOid());
        } else if (histSeInfo.getHistoricalCommodityBought().size() == 0) {
            // Add all the bought commodities info
            List<HistoricalCommodityInfo> histBoughtInfoList = new ArrayList<>();
            for (CommoditiesBoughtFromProvider.Builder commBoughtProvider : entityBuilder.getCommoditiesBoughtFromProvidersBuilderList()) {
                long sourceId = -1;
                if (commBoughtProvider.hasVolumeId()) {
                    sourceId = commBoughtProvider.getVolumeId();
                } else if (commBoughtProvider.hasProviderId()) {
                    sourceId = commBoughtProvider.getProviderId();
                } else {
                    logger.error("No volumeId or providerId exists for a bought commodity");
                }
                for (CommodityBoughtDTO.Builder commBought : commBoughtProvider.getCommodityBoughtBuilderList()) {
                    if (useHistoricalValues(commBought.getCommodityType().getType())) {
                        HistoricalCommodityInfo histBoughtInfo = new HistoricalCommodityInfo();
                        histBoughtInfo.setCommodityTypeAndKey(commBought.getCommodityType());
                        histBoughtInfo.setHistoricalUsed(-1.0f);
                        histBoughtInfo.setHistoricalPeak(-1.0f);
                        histBoughtInfo.setSourceId(sourceId);
                        histBoughtInfo.setMatched(false);
                        histBoughtInfo.setExisting(false);
                        histBoughtInfoList.add(histBoughtInfo);
                    }
                }
            }
            histSeInfo.setHistoricalCommodityBought(histBoughtInfoList);
            historicalInfo.replace(topoEntity.getOid(), histSeInfo);
        } else {
            // Add new bought commodities info, match the old ones
            for (CommoditiesBoughtFromProvider.Builder commBoughtProvider : entityBuilder.getCommoditiesBoughtFromProvidersBuilderList()) {
                long sourceId = -1;
                if (commBoughtProvider.hasVolumeId()) {
                    sourceId = commBoughtProvider.getVolumeId();
                } else if (commBoughtProvider.hasProviderId()) {
                    sourceId = commBoughtProvider.getProviderId();
                } else {
                    logger.error("No volumeId or providerId exists for a bought commodity");
                }
                for (CommodityBoughtDTO.Builder commBought : commBoughtProvider.getCommodityBoughtBuilderList()) {
                    if (useHistoricalValues(commBought.getCommodityType().getType())) {
                        boolean isMatched = false;
                        List<HistoricalCommodityInfo> histBoughtInfoList = histSeInfo.getHistoricalCommodityBought();
                        for (int i = 0; i < histBoughtInfoList.size(); i++) {
                            HistoricalCommodityInfo histBoughtInfo = histBoughtInfoList.get(i);
                            if (histBoughtInfo.getCommodityTypeAndKey().equals(commBought.getCommodityType()) &&
                                    (histBoughtInfo.getSourceId() == sourceId) &&
                                    !histBoughtInfo.getMatched()) {
                                histBoughtInfo.setMatched(true);
                                histBoughtInfoList.set(i, histBoughtInfo);
                                histSeInfo.setHistoricalCommodityBought(histBoughtInfoList);
                                isMatched = true;
                                break;
                            }
                        }
                        if (!isMatched) {
                            HistoricalCommodityInfo histBoughtInfo = new HistoricalCommodityInfo();
                            histBoughtInfo.setCommodityTypeAndKey(commBought.getCommodityType());
                            histBoughtInfo.setHistoricalUsed(-1.0f);
                            histBoughtInfo.setHistoricalPeak(-1.0f);
                            histBoughtInfo.setSourceId(sourceId);
                            histBoughtInfo.setMatched(false);
                            histBoughtInfo.setExisting(false);
                            histBoughtInfoList.add(histBoughtInfo);
                            histSeInfo.setHistoricalCommodityBought(histBoughtInfoList);
                        }
                    }
                }
            }
            historicalInfo.replace(topoEntity.getOid(), histSeInfo);
        }

        for (CommoditiesBoughtFromProvider.Builder commBoughtProvider : entityBuilder.getCommoditiesBoughtFromProvidersBuilderList()) {
            long sourceId = (commBoughtProvider.hasVolumeId() ? commBoughtProvider.getVolumeId() : commBoughtProvider.getProviderId());
            for (CommodityBoughtDTO.Builder commBought : commBoughtProvider.getCommodityBoughtBuilderList()) {
                calculateSmoothValuesForCommodityBought(topoEntity, commBought, sourceId);
            }
        }
    }

    private void calculateSmoothValuesForCommodityBought(TopologyEntity topoEntity, CommodityBoughtDTO.Builder topoCommBought, long sourceId) {
        float usedQuantity = (float) topoCommBought.getUsed();
        float peakQuantity = (float) topoCommBought.getPeak();
        logger.trace("Entity={}, Bought commodity={}, Used from mediation={}, Peak from mediation={}", topoEntity.getOid(),
                topoCommBought.getCommodityType().getType(), usedQuantity, peakQuantity);

        if (useHistoricalValues(topoCommBought.getCommodityType().getType())) {
            // Using historical values in calculation of used and peak
            HistoricalServiceEntityInfo histSeInfo = historicalInfo.get(topoEntity.getOid());
            if (histSeInfo == null) {
                logger.error("A HistoricalServiceEntityInfo data structure is missing for the service entity {}", topoEntity.getOid());
            } else {
                boolean commBoughtFound = false;
                List<HistoricalCommodityInfo> histBoughtInfoList = histSeInfo.getHistoricalCommodityBought();
                for (int i = 0; i < histBoughtInfoList.size(); i++) {
                    HistoricalCommodityInfo histBoughtInfo = histBoughtInfoList.get(i);
                    if (histBoughtInfo.getCommodityTypeAndKey().equals(topoCommBought.getCommodityType()) &&
                        (histBoughtInfo.getSourceId() == sourceId) &&
                        histBoughtInfo.getMatched() && !histBoughtInfo.getExisting()) {
                        commBoughtFound = true;
                        float usedHistWeight = histSeInfo.getUsedHistoryWeight();
                        float peakHistWeight = histSeInfo.getPeakHistoryWeight();
                        if (histBoughtInfo.getHistoricalUsed() > 0) {
                            usedQuantity = usedHistWeight * histBoughtInfo.getHistoricalUsed() + (1 - usedHistWeight) * usedQuantity;
                        }
                        if (histBoughtInfo.getHistoricalPeak() > 0) {
                            peakQuantity = peakHistWeight * histBoughtInfo.getHistoricalPeak() + (1 - peakHistWeight) * peakQuantity;
                        }
                        logger.trace("Entity={}, Bought commodity={}, Historical used={}, Historical peak={}", topoEntity.getOid(),
                                topoCommBought.getCommodityType().getType(), histBoughtInfo.getHistoricalUsed(), histBoughtInfo.getHistoricalPeak());
                        logger.trace("Entity={}, Bought commodity={}, Calculated used={}, Calculated peak={}", topoEntity.getOid(),
                                topoCommBought.getCommodityType().getType(), usedQuantity, peakQuantity);
                        histBoughtInfo.setHistoricalUsed(usedQuantity);
                        histBoughtInfo.setHistoricalPeak(peakQuantity);
                        histBoughtInfo.setExisting(true);
                        histBoughtInfoList.set(i, histBoughtInfo);
                        histSeInfo.setHistoricalCommodityBought(histBoughtInfoList);
                        historicalInfo.replace(topoEntity.getOid(), histSeInfo);
                        break;
                    }
                }
                if (!commBoughtFound) {
                    // Check if there is a unique provider for the same commodity type
                    // Else there is no match
                    int numberNew = 0;      // the number of new unmatched commodities of the same type
                    int numberPrevious = 0; // the number of the previous unmatched commodities of the same type
                    int indexCurrent = -1;  // the index of the current commodity in histBoughtInfoList
                    int indexMatching = -1; // the index of the previous commodity to be matched in histBoughtInfoList
                    for (int i = 0; i < histBoughtInfoList.size(); i++) {
                        HistoricalCommodityInfo histBoughtInfo = histBoughtInfoList.get(i);
                        if (histBoughtInfo.getCommodityTypeAndKey().equals(topoCommBought.getCommodityType()) &&
                            !histBoughtInfo.getMatched() && !histBoughtInfo.getExisting()) {
                            if ((Math.abs(histBoughtInfo.getHistoricalUsed() + 1.0f) < E) &&
                                (Math.abs(histBoughtInfo.getHistoricalPeak() + 1.0f) < E)) {
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
                        float usedHistWeight = histSeInfo.getUsedHistoryWeight();
                        float peakHistWeight = histSeInfo.getPeakHistoryWeight();
                        if (previousComm.getHistoricalUsed() > 0) {
                            usedQuantity = usedHistWeight * previousComm.getHistoricalUsed() + (1 - usedHistWeight) * usedQuantity;
                        }
                        if (previousComm.getHistoricalPeak() > 0) {
                            peakQuantity = peakHistWeight * previousComm.getHistoricalPeak() + (1 - peakHistWeight) * peakQuantity;
                        }
                        logger.trace("Entity={}, Bought commodity={}, Historical used={}, Historical peak={}", topoEntity.getOid(),
                                topoCommBought.getCommodityType().getType(), previousComm.getHistoricalUsed(), previousComm.getHistoricalPeak());
                        logger.trace("Entity={}, Bought commodity={}, Calculated used={}, Calculated peak={}", topoEntity.getOid(),
                                topoCommBought.getCommodityType().getType(), usedQuantity, peakQuantity);
                        newComm.setHistoricalUsed(usedQuantity);
                        newComm.setHistoricalPeak(peakQuantity);
                        newComm.setExisting(true);
                        histBoughtInfoList.set(indexCurrent, newComm);
                        histSeInfo.setHistoricalCommodityBought(histBoughtInfoList);
                        historicalInfo.replace(topoEntity.getOid(), histSeInfo);
                    } else {
                        // Don't match
                        for (int i = 0; i < histBoughtInfoList.size(); i++) {
                            HistoricalCommodityInfo histBoughtInfo = histBoughtInfoList.get(i);
                            if (histBoughtInfo.getCommodityTypeAndKey().equals(topoCommBought.getCommodityType()) &&
                                (Math.abs(histBoughtInfo.getHistoricalUsed() + 1.0f) < E) &&
                                (Math.abs(histBoughtInfo.getHistoricalPeak() + 1.0f) < E) &&
                                (histBoughtInfo.getSourceId() == sourceId) &&
                                !histBoughtInfo.getMatched() && !histBoughtInfo.getExisting()) {
                                histBoughtInfo.setHistoricalUsed(usedQuantity);
                                histBoughtInfo.setHistoricalPeak(peakQuantity);
                                histBoughtInfo.setExisting(true);
                                histBoughtInfoList.set(i, histBoughtInfo);
                                histSeInfo.setHistoricalCommodityBought(histBoughtInfoList);
                                historicalInfo.replace(topoEntity.getOid(), histSeInfo);
                                break;
                            }
                        }
                    }
                }
                topoCommBought.getHistoricalUsedBuilder().setHistUtilization(usedQuantity);
                topoCommBought.getHistoricalPeakBuilder().setHistUtilization(peakQuantity);
            }
        }
    }

}
