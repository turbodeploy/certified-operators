package com.vmturbo.extractor.action.commodity;

import static com.vmturbo.extractor.action.ActionAttributeExtractor.ACTION_TYPES_TO_POPULATE_TARGET_METRIC_IMPACT;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.longs.Long2ObjectMaps;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionType;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.UnsupportedActionException;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc.SettingPolicyServiceBlockingStub;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.components.api.FormattedString;
import com.vmturbo.components.common.utils.MultiStageTimer;
import com.vmturbo.components.common.utils.MultiStageTimer.Detail;
import com.vmturbo.extractor.action.commodity.PercentileSettingsRetriever.PercentileSettings;
import com.vmturbo.extractor.action.commodity.PercentileSettingsRetriever.PercentileSettings.PercentileSetting;
import com.vmturbo.extractor.export.ExportUtils;
import com.vmturbo.extractor.models.Constants;
import com.vmturbo.extractor.schema.json.common.ActionImpactedEntity.ImpactedMetric;
import com.vmturbo.extractor.schema.json.common.CommodityPercentileChange;
import com.vmturbo.extractor.topology.DataProvider;
import com.vmturbo.extractor.topology.ITopologyWriter;
import com.vmturbo.extractor.topology.WriterConfig;

/**
 * Retrieves commodity-related data for actions.
 *
 * <p/>We need three things to add commodity information to actions:
 * - Source commodity information, collected from the topologies being ingested by the extractor.
 * - Projected commodities, retrieved from the history component.
 *     - We do this to avoid having to listen for the projected topology in the extractor, and
 *       because projected stats are in memory, and should be fast to retrieve.
 *  - Percentile-related settings, retrieved from the group component.
 *     - We need this because without knowing the aggressiveness and observation period, the
 *       raw percentile number is not useful.
 */
public class ActionCommodityDataRetriever implements ITopologyWriter  {

    private static final Logger logger = LogManager.getLogger();

    private final ProjectedTopologyCommodityDataRetriever projectedTopologyCommodityDataRetriever;

    private final PercentileSettingsRetriever percentileSettingsRetriever;

    private final IntSet commoditiesWhitelist;

    private volatile TopologyActionCommodityData sourceTopologyActionCommodityData = null;

    private volatile TopologyActionCommodityData newSourceTopologyActionCommodityData = null;

    private volatile PercentileSettings percentileSettings = null;

    /**
     * Construct a new decorator.
     *
     * @param statsHistoryServiceStub Stub to access history component for projected stats.
     * @param settingPolicyServiceStub Stub to access group component for settings.
     * @param commoditiesWhitelist Commodities to cache from source topology - and retrieve from
     *    projected topology - to help track impact of actions.
     */
    public ActionCommodityDataRetriever(@Nonnull final StatsHistoryServiceBlockingStub statsHistoryServiceStub,
                                     @Nonnull final SettingPolicyServiceBlockingStub settingPolicyServiceStub,
            @Nonnull final Set<Integer> commoditiesWhitelist) {
        this(new ProjectedTopologyCommodityDataRetriever(statsHistoryServiceStub), new PercentileSettingsRetriever(settingPolicyServiceStub), commoditiesWhitelist);
    }

    @VisibleForTesting
    ActionCommodityDataRetriever(@Nonnull final ProjectedTopologyCommodityDataRetriever projectedTopologyCommodityDataRetriever,
                              @Nonnull final PercentileSettingsRetriever percentileSettingsRetriever,
            @Nonnull final Set<Integer> commoditiesWhitelist) {
        this.projectedTopologyCommodityDataRetriever = projectedTopologyCommodityDataRetriever;
        this.percentileSettingsRetriever = percentileSettingsRetriever;
        this.commoditiesWhitelist = new IntOpenHashSet(commoditiesWhitelist);
    }

    @Override
    public Consumer<TopologyEntityDTO> startTopology(TopologyInfo topologyInfo,
            WriterConfig writerConfig, MultiStageTimer timer) {
        this.newSourceTopologyActionCommodityData = new TopologyActionCommodityData();
        return entity -> {
            for (CommoditySoldDTO commSold : entity.getCommoditySoldListList()) {
                if (commoditiesWhitelist.contains(commSold.getCommodityType().getType())) {
                    newSourceTopologyActionCommodityData.processSoldCommodity(entity.getOid(),
                            commSold);
                }
            }

            // we don't want to process all the bought commodities, since most of them are not useful
            // currently only the bought commodities of VM from compute tier is needed, since they
            // are shown when scale vm action is expanded in UI
            final Map<Integer, Set<Integer>> boughtCommoditiesByProviderType =
                    Constants.ACTION_IMPACT_BOUGHT_COMMODITIES_WHITELIST.get(entity.getEntityType());
            if (boughtCommoditiesByProviderType != null) {
                entity.getCommoditiesBoughtFromProvidersList().forEach(commoditiesBoughtFromProvider -> {
                    Set<Integer> boughtCommodities = boughtCommoditiesByProviderType.get(
                            commoditiesBoughtFromProvider.getProviderEntityType());
                    if (boughtCommodities != null) {
                        commoditiesBoughtFromProvider.getCommodityBoughtList().forEach(commodityBoughtDTO -> {
                            if (boughtCommodities.contains(commodityBoughtDTO.getCommodityType().getType())) {
                                newSourceTopologyActionCommodityData.processBoughtCommodity(
                                        entity.getOid(), commoditiesBoughtFromProvider.getProviderId(), commodityBoughtDTO);
                            }
                        });
                    }
                });
            }
        };
    }

    @Override
    public int finish(DataProvider dataProvider) {
        newSourceTopologyActionCommodityData.populateCapacityForBoughtCommodities();
        this.sourceTopologyActionCommodityData = newSourceTopologyActionCommodityData;
        this.sourceTopologyActionCommodityData.finish();
        logger.debug("Extracted percentile and commodity data from topology: {}",
                sourceTopologyActionCommodityData.toString());
        this.newSourceTopologyActionCommodityData = null;
        return 0;
    }

    /**
     * Get the commodity-related data for a collection of actions.
     * Makes the minimum necessary remote calls to get all the data.
     *
     * @param actionSpecs The {@link ActionSpec}s.
     * @return The {@link ActionCommodityData} object which can be used to look up commodity
     *         and percentile information.
     */
    @Nonnull
    public ActionCommodityData getActionCommodityData(@Nonnull final List<ActionSpec> actionSpecs) {
        // Grab the reference to the current source percentile data.
        final TopologyActionCommodityData sourceCommData = sourceTopologyActionCommodityData;
        if (sourceCommData == null || actionSpecs.isEmpty()) {
            return new ActionCommodityData(Long2ObjectMaps.emptyMap(),
                    new ActionPercentileData(Long2ObjectMaps.emptyMap()));
        }
        final LongSet entitiesToRetrieve = new LongOpenHashSet();
        final LongSet projectedProvidersToRetrieve = new LongOpenHashSet();
        final IntSet commoditiesToRetrieve = sourceCommData.getCommodityTypes();
        actionSpecs.forEach(actionSpec -> {
            visitCommodityData(actionSpec, (entity, commodityType) -> {
                entitiesToRetrieve.add(entity.getId());
                commoditiesToRetrieve.add(commodityType.getType());
            }, (entity) -> {
                entitiesToRetrieve.add(entity.getId());
            });

            // only need to get bought commodities from the providers in scale actions
            final ActionType actionType = ActionDTOUtil.getActionInfoActionType(
                    actionSpec.getRecommendation());
            if (actionType == ActionType.SCALE) {
                ActionDTOUtil.getChangeProviderList(actionSpec.getRecommendation())
                        .forEach(change -> projectedProvidersToRetrieve.add(change.getDestination().getId()));
            }
        });
        logger.debug("Retrieving percentile data for {} actions. {} target entities, {} total commodities",
            actionSpecs.size(), entitiesToRetrieve.size(), commoditiesToRetrieve.size());

        final MultiStageTimer timer = new MultiStageTimer(logger);
        timer.start("Retrieving percentile settings");
        this.percentileSettings = percentileSettingsRetriever.getPercentileSettingsData(entitiesToRetrieve);
        timer.start("Retrieving projected commodities");
        final TopologyActionCommodityData projectedCommData = projectedTopologyCommodityDataRetriever
                .fetchProjectedCommodityData(entitiesToRetrieve, commoditiesToRetrieve, projectedProvidersToRetrieve);
        timer.stop();
        timer.info(FormattedString.format("Retrieved percentile and commodity data for {} actions", actionSpecs.size()), Detail.STAGE_SUMMARY);

        final Long2ObjectOpenHashMap<Object2ObjectOpenHashMap<String, ImpactedMetric>> commodityChanges = new Long2ObjectOpenHashMap<>();

        final Long2ObjectOpenHashMap<Map<CommodityType, CommodityPercentileChange>> retMap = new Long2ObjectOpenHashMap<>();
        actionSpecs.forEach(spec -> {
            visitCommodityData(spec, ((entity, commodityType) -> {
                // We assume each commodity only appears once.
                ApiEntityType type = ApiEntityType.fromType(entity.getType());
                final Optional<PercentileSetting> percentileSetting = percentileSettings.getEntitySettings(entity.getId(), type);
                final Optional<Double> srcPercentile = sourceCommData.getSoldPercentile(entity.getId(), commodityType);
                final Optional<Double> projPercentile = projectedCommData.getSoldPercentile(entity.getId(), commodityType);
                // If we do not have the source percentile or the settings used to compute
                // percentiles, there is no point writing the data.
                if (percentileSetting.isPresent() && srcPercentile.isPresent()) {
                    CommodityPercentileChange percentileChange = new CommodityPercentileChange();
                    percentileChange.setBefore(srcPercentile.get());
                    percentileChange.setAggressiveness(percentileSetting.get().getAggresiveness());
                    percentileChange.setObservationPeriodDays(percentileSetting.get().getObservationPeriod());

                    // Write the projected percentile if we have it. If we don't, the source
                    // percentile is still useful to tell what drove us to take the action.
                    projPercentile.ifPresent(percentileChange::setAfter);

                    retMap.computeIfAbsent(entity.getId(), k -> new HashMap<>()).put(
                            commodityType, percentileChange);
                }
            }), ((entity) -> {
                final long entityId = entity.getId();
                sourceCommData.visitDifferentCommodities(entityId, projectedCommData, (commType, src, proj) -> {
                    final String commTypeStr = ExportUtils.getCommodityTypeJsonKey(commType);
                    if (src != null && proj != null && commTypeStr != null) {
                        final ImpactedMetric impactedMetric = new ImpactedMetric();
                        // add before/after percentile data for commodity (ignoring key)
                        final Optional<Double> srcPercentile = sourceCommData.getSoldPercentile(
                                entityId, CommodityType.newBuilder().setType(commType).build());
                        final Optional<Double> projPercentile = projectedCommData.getSoldPercentile(
                                entityId, CommodityType.newBuilder().setType(commType).build());
                        // percentile before/after action
                        if (srcPercentile.isPresent()) {
                            src.setPercentileUtilization(srcPercentile.get());
                            projPercentile.ifPresent(proj::setPercentileUtilization);
                        }
                        impactedMetric.setBeforeActions(src);
                        impactedMetric.setAfterActions(proj);
                        commodityChanges.computeIfAbsent(entityId, v -> new Object2ObjectOpenHashMap<>())
                            .put(commTypeStr, impactedMetric);
                    }
                });
            }));
        });
        commodityChanges.trim();
        commodityChanges.values().forEach(Object2ObjectOpenHashMap::trim);
        retMap.trim();

        final ActionCommodityData commData = new ActionCommodityData(commodityChanges,
                new ActionPercentileData(retMap));
        logger.info("Retrieved action commodities for {} entities across {} actions. Percentiles for {} entities.",
                commodityChanges.size(), actionSpecs.size(), retMap.size());
        return commData;
    }

    /**
     * Get percentile settings for the given entity.
     *
     * @param entityId entity oid
     * @param entityType type of entity
     * @return optional percentile settings
     */
    public Optional<PercentileSetting> getPercentileSetting(long entityId, int entityType) {
        return Optional.ofNullable(percentileSettings).flatMap(setting ->
                setting.getEntitySettings(entityId, ApiEntityType.fromType(entityType)));
    }

    /**
     * Utility to accept percentile-related entity-commodity combinations in an {@link ActionSpec}.
     */
    @FunctionalInterface
    interface PercentileVisitor {
        void visit(ActionEntity entity, CommodityType commodityType);
    }

    /**
     * Utility to accept entity-commodity combinations in an {@link ActionSpec}.
     */
    @FunctionalInterface
    interface CommodityVisitor {
        void visit(ActionEntity entity);
    }

    @VisibleForTesting
    void visitCommodityData(@Nonnull final ActionSpec actionSpec,
            @Nonnull final PercentileVisitor percentileVisitor,
            @Nonnull final CommodityVisitor commodityVisitor) {
        final ActionInfo actionInfo = actionSpec.getRecommendation().getInfo();
        final ActionType actionType = ActionDTOUtil.getActionInfoActionType(actionSpec.getRecommendation());
        // TODO (roman, Feb 12 2021): Handle percentiles for move/scale actions.
        //
        // When we add support for percentiles for move (and maybe scale) actions, we need to
        // double-check that we do not need to look at commodities bought. There may be certain
        // edge cases where we want to show the percentiles for bought commodities (e.g. VDI?).
        // In that case we will need to add bought commodities to the TopologyPercentileData, and
        // have logic to determine when to look for bought vs. sold commodity percentiles.
        if (actionInfo.hasResize()) {
            ActionEntity targetEntity = actionSpec.getRecommendation().getInfo().getResize().getTarget();
            percentileVisitor.visit(targetEntity,
                    actionInfo.getResize().getCommodityType());
            commodityVisitor.visit(targetEntity);
        } else if (actionInfo.hasAtomicResize()) {
            actionInfo.getAtomicResize().getResizesList().forEach(resize -> {
                percentileVisitor.visit(resize.getTarget(), resize.getCommodityType());

                // TODO: May need more logic here (e.g. get stats on related entities? Or get a smaller subset of
                // stats depending on which commodity is being resized).
                commodityVisitor.visit(resize.getTarget());
            });
        } else if (actionInfo.hasMove()) {
            actionInfo.getMove().getChangesList().forEach(changeProvider -> {
                final ActionEntity dest = changeProvider.getDestination();
                commodityVisitor.visit(dest);
                if (changeProvider.hasSource()) {
                    commodityVisitor.visit(changeProvider.getSource());
                }
            });
        } else if (ACTION_TYPES_TO_POPULATE_TARGET_METRIC_IMPACT.contains(actionType)) {
            try {
                commodityVisitor.visit(ActionDTOUtil.getPrimaryEntity(actionSpec.getRecommendation()));
            } catch (UnsupportedActionException e) {
                // this should not happen
                logger.error("Unable to get primary entity for unsupported action {}", actionSpec, e);
            }
        }
    }
}
