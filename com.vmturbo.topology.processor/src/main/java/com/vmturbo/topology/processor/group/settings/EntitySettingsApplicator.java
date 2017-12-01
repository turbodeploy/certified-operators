package com.vmturbo.topology.processor.group.settings;

import java.util.Collection;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.collect.ImmutableList;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.action.ActionDTOREST.ActionMode;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
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
     * List of the applicators that modify the topology.
     */
    private static final List<SettingApplicator> APPLICATORS =
            ImmutableList.of(new MoveApplicator(), new SuspendApplicator(),
                    new ProvisionApplicator(), new ResizeApplicator(),
                    new UtilizationThresholdApplicator(SettingPolicySetting.IoThroughput,
                            CommodityType.IO_THROUGHPUT),
                    new UtilizationThresholdApplicator(SettingPolicySetting.NetThroughput,
                            CommodityType.NET_THROUGHPUT),
                    new UtilizationThresholdApplicator(SettingPolicySetting.SwappingUtilization,
                            CommodityType.SWAPPING),
                    new UtilizationThresholdApplicator(SettingPolicySetting.ReadyQueueUtilization,
                            CommodityType.QN_VCPU), new UtilizationThresholdApplicator(
                            SettingPolicySetting.StorageAmountUtilization,
                            CommodityType.STORAGE_AMOUNT),
                    new UtilizationThresholdApplicator(SettingPolicySetting.IopsUtilization,
                            CommodityType.STORAGE_ACCESS),
                    new UtilizationThresholdApplicator(SettingPolicySetting.LatencyUtilization,
                            CommodityType.STORAGE_LATENCY), new UtilTargetApplicator(),
                    new TargetBandApplicator(), new HaDependentUtilizationApplicator());

    private final Logger logger = LogManager.getLogger(getClass());

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
                final Map<SettingPolicySetting, Setting> settingsForEntity =
                        new EnumMap<>(SettingPolicySetting.class);
                for (final Setting setting : graphWithSettings.getSettingsForEntity(
                        entity.getOid())) {
                    final Optional<SettingPolicySetting> policySetting =
                            SettingPolicySetting.getSettingByName(setting.getSettingSpecName());
                    if (policySetting.isPresent()) {
                        settingsForEntity.put(policySetting.get(), setting);
                    } else {
                        logger.warn("Unknown setting {} for entity {}",
                                setting.getSettingSpecName(), entity.getOid());
                        return;
                    }
                }
                processSettings(entity, settingsForEntity);
            });
    }

    private static void processSettings(@Nonnull TopologyEntityDTO.Builder entity,
            @Nonnull Map<SettingPolicySetting, Setting> settingsMap) {
        for (SettingApplicator applicator: APPLICATORS) {
            applicator.apply(entity, settingsMap);
        }
    }

    private static Collection<CommoditySoldDTO.Builder> getCommoditySoldBuilders(
            TopologyEntityDTO.Builder entity, CommodityType commodityType) {
        return entity.getCommoditySoldListBuilderList()
                .stream()
                .filter(commodity -> commodity.getCommodityType().getType() ==
                        commodityType.getNumber())
                .collect(Collectors.toList());
    }

    private static void applyUtilizationChanges(@Nonnull TopologyEntityDTO.Builder entity,
            @Nonnull CommodityType commodityType, @Nullable Setting setting, boolean ignoreHa) {
        for (CommoditySoldDTO.Builder commodity : getCommoditySoldBuilders(entity, commodityType)) {
            if (ignoreHa) {
                if (setting != null) {
                    commodity.setEffectiveCapacityPercentage(
                            setting.getNumericSettingValue().getValue());
                } else {
                    commodity.clearEffectiveCapacityPercentage();
                }
            } else {
                if (setting != null) {
                    commodity.setEffectiveCapacityPercentage(
                            setting.getNumericSettingValue().getValue());
                }
            }
        }
    }

    /**
     * The applicator of a single {@link Setting} to a single {@link TopologyEntityDTO.Builder}.
     */
    private static abstract class SingleSettingApplicator implements SettingApplicator {

        private final SettingPolicySetting setting;

        private SingleSettingApplicator(@Nonnull SettingPolicySetting setting) {
            this.setting = Objects.requireNonNull(setting);
        }

        protected abstract void apply(@Nonnull final TopologyEntityDTO.Builder entity,
                @Nonnull final Setting setting);

        @Override
        public void apply(@Nonnull Builder entity,
                @Nonnull Map<SettingPolicySetting, Setting> settings) {
            final Setting settingObject = settings.get(setting);
            if (settingObject != null) {
                apply(entity, settingObject);
            }
        }
    }

    /**
     * Settings applicator, that requires multiple settings to be processed.
     */
    private interface SettingApplicator {
        /**
         * Applies settings to the specified entity.
         *
         * @param entity entity to apply settings to
         * @param settings settings to apply
         */
        void apply(@Nonnull final TopologyEntityDTO.Builder entity,
                @Nonnull final Map<SettingPolicySetting, Setting> settings);
    }

    /**
     * Applies the "move" setting to a {@link TopologyEntityDTO.Builder}. In particular,
     * if the "move" is disabled, set the commodities purchased from a host to non-movable.
     */
    private static class MoveApplicator extends SingleSettingApplicator {

        private MoveApplicator() {
            super(SettingPolicySetting.Move);
        }

        @Override
        protected void apply(@Nonnull final TopologyEntityDTO.Builder entity,
                          @Nonnull final Setting setting) {
            final boolean movable =
                    !setting.getEnumSettingValue().getValue().equals(ActionMode.DISABLED.name());
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
    private static class SuspendApplicator extends SingleSettingApplicator {

        private SuspendApplicator() {
            super(SettingPolicySetting.Suspend);
        }

        @Override
        protected void apply(@Nonnull final TopologyEntityDTO.Builder entity,
                          @Nonnull final Setting setting) {
            entity.getAnalysisSettingsBuilder().setSuspendable(
                    !setting.getEnumSettingValue().getValue().equals("DISABLED"));
        }
    }

    /**
     * Applies the "provision" setting to a {@link TopologyEntityDTO.Builder}.
     */
    private static class ProvisionApplicator extends SingleSettingApplicator {

        private ProvisionApplicator() {
            super(SettingPolicySetting.Provision);
        }

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
    private static class ResizeApplicator extends SingleSettingApplicator {

        private ResizeApplicator() {
            super(SettingPolicySetting.Resize);
        }

        @Override
        public void apply(@Nonnull final TopologyEntityDTO.Builder entity,
                          @Nonnull final Setting setting) {
            final boolean resizeable =
                    !setting.getEnumSettingValue().getValue().equals("DISABLED");
            entity.getCommoditySoldListBuilderList()
                    .forEach(commSoldBuilder -> commSoldBuilder.setIsResizeable(resizeable));
        }
    }

    /**
     * Applicator for utilization threshold settings. Utilization thresholds work by setting
     * effective capacity percentage, so Market reads this value to create effective capacity as
     * {@code effectiveCapacity = capacity * effectiveCapacityPercentage / 100}
     * The effective capacity is later used to calculate resources price.
     */
    @ThreadSafe
    private static class UtilizationThresholdApplicator extends SingleSettingApplicator {

        private final CommodityType commodityType;

        private UtilizationThresholdApplicator(@Nonnull SettingPolicySetting setting,
                @Nonnull final CommodityType commodityType) {
            super(setting);
            this.commodityType = Objects.requireNonNull(commodityType);
        }

        @Override
        public void apply(@Nonnull Builder entity, @Nonnull Setting setting) {
            final float settingValue = setting.getNumericSettingValue().getValue();
            for (CommoditySoldDTO.Builder commodity : getCommoditySoldBuilders(entity, commodityType)) {
                commodity.setEffectiveCapacityPercentage(settingValue);
            }
        }
    }

    /**
     * HA related commodities applicator. This applicator will process ignoreHA setting and cpu/mem
     * utilization threshold. Both of the commodities are calculated on top of appropriate settings
     * and HA ignorance configuration.
     */
    @ThreadSafe
    private static class HaDependentUtilizationApplicator implements SettingApplicator {

        @Override
        public void apply(@Nonnull final TopologyEntityDTO.Builder entity,
                @Nonnull final Map<SettingPolicySetting, Setting> settings) {
            final Setting ignoreHaSetting = settings.get(SettingPolicySetting.IgnoreHA);
            final boolean ignoreHa =
                    ignoreHaSetting != null && ignoreHaSetting.getBooleanSettingValue().getValue();
            final Setting cpuUtilSetting = settings.get(SettingPolicySetting.CpuUtilization);
            final Setting memUtilSetting = settings.get(SettingPolicySetting.MemoryUtilization);
            applyUtilizationChanges(entity, CommodityType.CPU, cpuUtilSetting, ignoreHa);
            applyUtilizationChanges(entity, CommodityType.MEM, memUtilSetting, ignoreHa);
        }
    }

    /**
     *  Applies the "utilTarget" setting to an entity.
     */
    private static class UtilTargetApplicator extends SingleSettingApplicator {

        private UtilTargetApplicator() {
            super(SettingPolicySetting.UtilTarget);
        }

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
    private static class TargetBandApplicator extends SingleSettingApplicator {

        private TargetBandApplicator() {
            super(SettingPolicySetting.TargetBand);
        }

        @Override
        public void apply(@Nonnull final TopologyEntityDTO.Builder entity,
                          @Nonnull final Setting setting) {

            entity.getAnalysisSettingsBuilder()
                .setDesiredUtilizationRange(setting.getNumericSettingValue().getValue());
        }
    }
}
