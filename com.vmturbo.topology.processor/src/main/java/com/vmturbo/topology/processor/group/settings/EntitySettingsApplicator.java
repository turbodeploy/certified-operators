package com.vmturbo.topology.processor.group.settings;

import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.annotations.VisibleForTesting;

import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Builder;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProviderOrBuilder;
import com.vmturbo.group.api.SettingPolicySetting;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.topology.processor.topology.TopologyGraph;
import com.vmturbo.topology.processor.topology.TopologyGraph.Vertex;
import com.vmturbo.topology.processor.topology.pipeline.TopologyPipeline;

/**
 * The {@link EntitySettingsApplicator} is responsible for applying resolved settings
 * to a {@link TopologyGraph}.
 *
 * It's separated from {@link EntitySettingsResolver} (which resolves settings) for clarity, and ease
 * of tracking, debugging, and measuring. It's separated from {@link GraphWithSettings} (even
 * though it works on the graph and settings) so that the {@link GraphWithSettings} can be
 * a simple data object passed between stages in the {@link TopologyPipeline}.
 *
 * As the number of settings in the system goes up, we may need to rework this class to be
 * more efficient.
 */
public class EntitySettingsApplicator {

    /**
     * All of the applicators for settings that modify the topology.
     */
    private static final Map<SettingPolicySetting, SingleSettingApplicator> APPLICATORS;

    /**
     * Function that provides the applicator for a particular setting, if any.
     * Each setting will have at most one applicator.
     *
     * If the applicator lookup returns null, the setting will have no effect on the
     * topology (i.e. skip application).
     */
    private final Function<SettingPolicySetting, SingleSettingApplicator> applicatorLookup;

    private final Logger logger = LogManager.getLogger(getClass());

    static {
        final Map<SettingPolicySetting, SingleSettingApplicator> applicators =
                new EnumMap<>(SettingPolicySetting.class);
        applicators.put(SettingPolicySetting.Move, new MoveApplicator());
        applicators.put(SettingPolicySetting.Suspend, new SuspendApplicator());
        applicators.put(SettingPolicySetting.Provision, new ProvisionApplicator());
        applicators.put(SettingPolicySetting.Resize, new ResizeApplicator());

        applicators.put(SettingPolicySetting.CpuUtilization,
                new UtilizationThresholdApplicator(CommodityType.CPU));
        applicators.put(SettingPolicySetting.MemoryUtilization,
                new UtilizationThresholdApplicator(CommodityType.MEM));
        applicators.put(SettingPolicySetting.IoThroughput,
                new UtilizationThresholdApplicator(CommodityType.IO_THROUGHPUT));
        applicators.put(SettingPolicySetting.NetThroughput,
                new UtilizationThresholdApplicator(CommodityType.NET_THROUGHPUT));
        applicators.put(SettingPolicySetting.SwappingUtilization,
                new UtilizationThresholdApplicator(CommodityType.SWAPPING));
        applicators.put(SettingPolicySetting.ReadyQueueUtilization,
                new UtilizationThresholdApplicator(CommodityType.QN_VCPU));
        applicators.put(SettingPolicySetting.StorageAmountUtilization,
                new UtilizationThresholdApplicator(CommodityType.STORAGE_AMOUNT));
        applicators.put(SettingPolicySetting.IopsUtilization,
                new UtilizationThresholdApplicator(CommodityType.STORAGE_ACCESS));
        applicators.put(SettingPolicySetting.LatencyUtilization,
                new UtilizationThresholdApplicator(CommodityType.STORAGE_LATENCY));

        applicators.put(SettingPolicySetting.UtilTarget, new UtilTargetApplicator());
        applicators.put(SettingPolicySetting.TargetBand, new TargetBandApplicator());

        APPLICATORS = Collections.unmodifiableMap(applicators);
    }

    public EntitySettingsApplicator() {
        applicatorLookup = APPLICATORS::get;
    }

    @VisibleForTesting
    EntitySettingsApplicator(
            @Nonnull final Function<SettingPolicySetting, SingleSettingApplicator> applicatorLookup) {
        this.applicatorLookup = applicatorLookup;
    }

    /**
     * Applies the settings contained in a {@link GraphWithSettings} to the topology graph
     * contained in it.
     *
     * @param graphWithSettings A {@link TopologyGraph} and the settings that apply to it.
     */
    public void applySettings(@Nonnull final GraphWithSettings graphWithSettings) {
        graphWithSettings.getTopologyGraph().vertices()
            .map(Vertex::getTopologyEntityDtoBuilder)
            .forEach(entity -> {
                final Collection<Setting> settingsForEntity =
                        graphWithSettings.getSettingsForEntity(entity.getOid());
                settingsForEntity.forEach((setting) -> processSetting(entity, setting));
            });
    }

    private void processSetting(@Nonnull TopologyEntityDTO.Builder entity,
            @Nonnull Setting setting) {
        final Optional<SettingPolicySetting> groupSetting =
                SettingPolicySetting.getSettingByName(setting.getSettingSpecName());
        if (groupSetting.isPresent()) {
            final SingleSettingApplicator applicator = applicatorLookup.apply(groupSetting.get());
            if (applicator != null) {
                applicator.apply(entity, setting);
            }
        } else {
            logger.warn("Unknown setting {} for entity {}", setting.getSettingSpecName(),
                    entity.getOid());
            return;
        }
    }

    /**
     * The applicator of a single {@link Setting} to a single {@link TopologyEntityDTO.Builder}.
     */
    @VisibleForTesting
    interface SingleSettingApplicator {
        void apply(@Nonnull final TopologyEntityDTO.Builder entity, @Nonnull final Setting setting);
    }

    /**
     * Applies the "move" setting to a {@link TopologyEntityDTO.Builder}. In particular,
     * if the "move" is disabled, set the commodities purchased from a host to non-movable.
     */
    @VisibleForTesting
    static class MoveApplicator implements SingleSettingApplicator {
        @Override
        public void apply(@Nonnull final TopologyEntityDTO.Builder entity,
                          @Nonnull final Setting setting) {
            final boolean movable = !setting.getEnumSettingValue().getValue().equals("DISABLED");
            entity.getCommoditiesBoughtFromProvidersBuilderList().stream()
                // Only disable moves for placed entities (i.e. those that have providers).
                // Doesn't make sense to disable them for unplaced ones.
                .filter(CommoditiesBoughtFromProviderOrBuilder::hasProviderId)
                .filter(CommoditiesBoughtFromProviderOrBuilder::hasProviderEntityType)
                // The "move" setting controls host moves. So we only want to set the group
                // of commodities bought from hosts (physical machines) to non-movable.
                .filter(commBought -> commBought.getProviderEntityType() ==
                        EntityType.PHYSICAL_MACHINE_VALUE)
                .forEach(commBought -> commBought.setMovable(movable));
        }
    }

    /**
     * Applies the "suspend" setting to a {@link TopologyEntityDTO.Builder}.
     */
    static class SuspendApplicator implements SingleSettingApplicator {
        @Override
        public void apply(@Nonnull final TopologyEntityDTO.Builder entity,
                          @Nonnull final Setting setting) {
            entity.getAnalysisSettingsBuilder().setSuspendable(
                    !setting.getEnumSettingValue().getValue().equals("DISABLED"));
        }
    }

    /**
     * Applies the "provision" setting to a {@link TopologyEntityDTO.Builder}.
     */
    static class ProvisionApplicator implements SingleSettingApplicator {
        @Override
        public void apply(@Nonnull final TopologyEntityDTO.Builder entity,
                          @Nonnull final Setting setting) {
            entity.getAnalysisSettingsBuilder().setCloneable(
                    !setting.getEnumSettingValue().getValue().equals("DISABLED"));
        }
    }

    /**
     * Applies the "resize" setting to a {@link TopologyEntityDTO.Builder}.
     */
    static class ResizeApplicator implements SingleSettingApplicator {
        @Override
        public void apply(@Nonnull final TopologyEntityDTO.Builder entity,
                          @Nonnull final Setting setting) {
            final boolean resizeable =
                    !setting.getEnumSettingValue().getValue().equals("DISABLED");
            entity.getCommoditySoldListBuilderList().forEach(commSoldBuilder -> {
                commSoldBuilder.setIsResizeable(resizeable);
            });
        }
    }

    /**
     * Applicator for utilization threshold settings. Utilization thresholds work by setting
     * effective capacity percentage, so Market reads this value to create effective capacity as
     * {@code effectiveCapacity = capacity * effectiveCapacityPercentage / 100}
     * The effective capacity is later used to calculate resources price.
     */
    @ThreadSafe
    private static class UtilizationThresholdApplicator implements SingleSettingApplicator {

        private final CommodityType commodityType;

        private UtilizationThresholdApplicator(@Nonnull final CommodityType commodityType) {
            this.commodityType = Objects.requireNonNull(commodityType);
        }

        @Override
        public void apply(@Nonnull Builder entity, @Nonnull Setting setting) {
            final float settingValue = setting.getNumericSettingValue().getValue();
            entity.getCommoditySoldListBuilderList()
                    .stream()
                    .filter(commodity -> commodity.getCommodityType().getType() ==
                            commodityType.getNumber())
                    .forEach(commodityBuilder -> {
                        commodityBuilder.setEffectiveCapacityPercentage(settingValue);
                    });
        }
    }

    /**
     *  Applies the "utilTarget" setting to an entity.
     */
    private static class UtilTargetApplicator implements SingleSettingApplicator {
        @Override
        public void apply(@Nonnull final TopologyEntityDTO.Builder entity,
                          @Nonnull final Setting setting) {

            entity.getAnalysisSettingsBuilder()
                .setDesiredUtilizationTarget(setting.getNumericSettingValue().getValue());
        }
    }

    /**
     *  Applies the "targetBand" setting to an entity.
     */
    private static class TargetBandApplicator implements SingleSettingApplicator {
        @Override
        public void apply(@Nonnull final TopologyEntityDTO.Builder entity,
                          @Nonnull final Setting setting) {

            entity.getAnalysisSettingsBuilder()
                .setDesiredUtilizationRange(setting.getNumericSettingValue().getValue());
        }
    }
}
