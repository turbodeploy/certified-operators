package com.vmturbo.extractor.action.percentile;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
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
import com.vmturbo.extractor.action.percentile.PercentileSettingsRetriever.PercentileSettings;
import com.vmturbo.extractor.action.percentile.PercentileSettingsRetriever.PercentileSettings.PercentileSetting;
import com.vmturbo.extractor.schema.json.common.CommodityPercentileChange;
import com.vmturbo.extractor.topology.DataProvider;
import com.vmturbo.extractor.topology.ITopologyWriter;
import com.vmturbo.extractor.topology.WriterConfig;

/**
 * Retrieves percentile-related data for actions.
 *
 * <p/>We need three things to add percentile information to actions:
 * - Source percentiles, collected from the topologies being ingested by the extractor.
 * - Projected percentiles, retrieved from the history component.
 *     - We do this to avoid having to listen for the projected topology in the extractor, and
 *       because projected stats are in memory, and should be fast to retrieve.
 *  - Percentile-related settings, retrieved from the group component.
 *     - We need this because without knowing the aggressiveness and observation period, the
 *       raw percentile number is not useful.
 */
public class ActionPercentileDataRetriever implements ITopologyWriter  {

    private static final Logger logger = LogManager.getLogger();

    private final ProjectedTopologyPercentileDataRetriever projectedTopologyPercentileDataRetriever;

    private final PercentileSettingsRetriever percentileSettingsRetriever;

    private volatile TopologyPercentileData sourceTopologyPercentileData = null;

    private volatile TopologyPercentileData newSourceTopologyPercentileData = null;

    /**
     * Construct a new decorator.
     *
     * @param statsHistoryServiceStub Stub to access history component for projected percentiles.
     * @param settingPolicyServiceStub Stub to access group component for settings.
     */
    public ActionPercentileDataRetriever(@Nonnull final StatsHistoryServiceBlockingStub statsHistoryServiceStub,
                                     @Nonnull final SettingPolicyServiceBlockingStub settingPolicyServiceStub) {
        this(new ProjectedTopologyPercentileDataRetriever(statsHistoryServiceStub), new PercentileSettingsRetriever(settingPolicyServiceStub));
    }

    @VisibleForTesting
    ActionPercentileDataRetriever(@Nonnull final ProjectedTopologyPercentileDataRetriever projectedTopologyPercentileDataRetriever,
                              @Nonnull final PercentileSettingsRetriever percentileSettingsRetriever) {
        this.projectedTopologyPercentileDataRetriever = projectedTopologyPercentileDataRetriever;
        this.percentileSettingsRetriever = percentileSettingsRetriever;
    }

    @Override
    public Consumer<TopologyEntityDTO> startTopology(TopologyInfo topologyInfo,
            WriterConfig writerConfig, MultiStageTimer timer) {
        this.newSourceTopologyPercentileData = new TopologyPercentileData();
        return entity -> {
            for (CommoditySoldDTO commSold : entity.getCommoditySoldListList()) {
                if (commSold.getHistoricalUsed().hasPercentile()) {
                    newSourceTopologyPercentileData.putSoldPercentile(entity.getOid(),
                            commSold.getCommodityType(),
                            commSold.getHistoricalUsed().getPercentile());
                }
            }
        };
    }

    @Override
    public int finish(DataProvider dataProvider) {
        this.sourceTopologyPercentileData = newSourceTopologyPercentileData;
        logger.debug("Extracted percentile data from topology: {}",
                sourceTopologyPercentileData.toString());
        this.newSourceTopologyPercentileData = null;
        return 0;
    }

    /**
     * Get the percentile-related data for a collection of actions.
     * Makes the minimum necessary remote calls to get all the data.
     *
     * @param actionSpecs The {@link ActionSpec}s.
     * @return The {@link ActionPercentileData} object which can be used to look up
     *         {@link CommodityPercentileChange}s for specific entity-commodity pairs.
     */
    @Nonnull
    public ActionPercentileData getActionPercentiles(@Nonnull final List<ActionSpec> actionSpecs) {
        // Grab the reference to the current source percentile data.
        final TopologyPercentileData sourcePercentileData = sourceTopologyPercentileData;
        final LongSet entitiesToRetrieve = new LongOpenHashSet();
        final IntSet commoditiesToRetrieve = new IntOpenHashSet();
        actionSpecs.forEach(actionSpec -> {
            visitPercentileInfo(actionSpec, (entity, commodityType) -> {
                entitiesToRetrieve.add(entity.getId());
                commoditiesToRetrieve.add(commodityType.getType());
            });
        });
        logger.debug("Retrieving percentile data for {} actions. {} target entities, {} total commodities",
            actionSpecs.size(), entitiesToRetrieve.size(), commoditiesToRetrieve.size());

        final MultiStageTimer timer = new MultiStageTimer(logger);
        timer.start("Retrieving percentile settings");
        final PercentileSettings percentileData = percentileSettingsRetriever.getPercentileSettingsData(entitiesToRetrieve);
        timer.start("Retrieving projected percentiles");
        final TopologyPercentileData projectedTopologyData = projectedTopologyPercentileDataRetriever
                .fetchPercentileData(entitiesToRetrieve, commoditiesToRetrieve);
        timer.stop();
        timer.info(FormattedString.format("Retrieved percentile data for {} actions", actionSpecs.size()), Detail.STAGE_SUMMARY);

        final Long2ObjectMap<Map<CommodityType, CommodityPercentileChange>> retMap = new Long2ObjectOpenHashMap<>();
        actionSpecs.forEach(spec -> {
            visitPercentileInfo(spec, ((entity, commodityType) -> {
                // We assume each commodity only appears once.
                ApiEntityType type = ApiEntityType.fromType(entity.getType());
                final Optional<PercentileSetting> percentileSetting = percentileData.getEntitySettings(entity.getId(), type);
                final Optional<Double> srcPercentile = sourcePercentileData.getSoldPercentile(entity.getId(), commodityType);
                final Optional<Double> projPercentile = projectedTopologyData.getSoldPercentile(entity.getId(), commodityType);
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
            }));
        });
        return new ActionPercentileData(retMap);
    }

    /**
     * Utility to accept percentile-related entity-commodity combinations in an {@link ActionSpec}.
     */
    @FunctionalInterface
    interface PercentileVisitor {
        void visit(ActionEntity entity, CommodityType commodityType);
    }

    @VisibleForTesting
    void visitPercentileInfo(@Nonnull final ActionSpec actionSpec, @Nonnull final PercentileVisitor consumer) {
        final ActionInfo actionInfo = actionSpec.getRecommendation().getInfo();
        // TODO (roman, Feb 12 2021): Handle scale actions.
        //
        // When we add support for percentiles for move (and maybe scale) actions, we need to
        // double-check that we do not need to look at commodities bought. There may be certain
        // edge cases where we want to show the percentiles for bought commodities (e.g. VDI?).
        // In that case we will need to add bought commodities to the TopologyPercentileData, and
        // have logic to determine when to look for bought vs. sold commodity percentiles.
        if (actionInfo.hasResize()) {
            consumer.visit(actionSpec.getRecommendation().getInfo().getResize().getTarget(),
                    actionInfo.getResize().getCommodityType());
        } else if (actionInfo.hasAtomicResize()) {
            actionInfo.getAtomicResize().getResizesList().forEach(resize -> {
                consumer.visit(resize.getTarget(), resize.getCommodityType());
            });
        }
    }
}
