package com.vmturbo.topology.processor.group.settings;

import java.util.Collection;
import java.util.Map;
import java.util.function.Function;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;

import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProviderOrBuilder;
import com.vmturbo.platform.common.dto.CommonDTOREST.EntityDTO.EntityType;
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
    private static final Map<String, SingleSettingApplicator> APPLICATORS =
            ImmutableMap.<String, SingleSettingApplicator>builder()
                .put("move", new MoveApplicator())
                .build();

    /**
     * Function that provides the applicator for a particular setting, if any.
     * Each setting will have at most one applicator.
     *
     * If the applicator lookup returns null, the setting will have no effect on the
     * topology (i.e. skip application).
     */
    private final Function<String, SingleSettingApplicator> applicatorLookup;

    public EntitySettingsApplicator() {
        applicatorLookup = APPLICATORS::get;
    }

    @VisibleForTesting
    EntitySettingsApplicator(@Nonnull final Function<String, SingleSettingApplicator> applicatorLookup) {
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
                final Map<String, Setting> settingsForEntity =
                        graphWithSettings.getSettingsForEntity(entity.getOid());
                settingsForEntity.forEach((settingName, setting) -> {
                    final SingleSettingApplicator applicator = applicatorLookup.apply(settingName);
                    if (applicator != null) {
                        applicator.apply(entity, setting);
                    }
                });
            });
    }

    /**
     * The applicator of a single {@link Setting} to a single {@link TopologyEntityDTO.Builder}.
     */
    @VisibleForTesting
    abstract static class SingleSettingApplicator {
        public abstract void apply(@Nonnull final TopologyEntityDTO.Builder entity,
                                   @Nonnull final Setting setting);
    }

    /**
     * Applies the "move" setting to a {@link TopologyEntityDTO.Builder}. In particular,
     * if the "move" is disabled, set the commodities purchased from a host to non-movable.
     */
    @VisibleForTesting
    static class MoveApplicator extends SingleSettingApplicator {
        @Override
        public void apply(@Nonnull final TopologyEntityDTO.Builder entity,
                          @Nonnull final Setting setting) {
            if (setting.getSettingSpecName().equals("move") &&
                    setting.getEnumSettingValue().getValue().equals("DISABLED")) {
                entity.getCommoditiesBoughtFromProvidersBuilderList().stream()
                    // Only disable moves for placed entities (i.e. those that have providers).
                    // Doesn't make sense to disable them for unplaced ones.
                    .filter(CommoditiesBoughtFromProviderOrBuilder::hasProviderId)
                    .filter(CommoditiesBoughtFromProviderOrBuilder::hasProviderEntityType)
                    // The "move" setting controls host moves. So we only want to set the group
                    // of commodities bought from hosts (physical machines) to non-movable.
                    .filter(commBought -> commBought.getProviderEntityType() ==
                            EntityType.PHYSICAL_MACHINE.getValue())
                    .forEach(commBought -> commBought.setMovable(false));
            }
        }
    }
}
