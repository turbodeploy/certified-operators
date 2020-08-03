package com.vmturbo.topology.processor.group.settings;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettings;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettings.SettingToPolicyId;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy;
import com.vmturbo.stitching.EntitySettingsCollection;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.processor.topology.pipeline.TopologyPipeline;
import com.vmturbo.topology.processor.topology.pipeline.TopologyPipelineContext;

/**
 * Represents the conceptual pair of a {@link TopologyGraph<TopologyEntity>} and the set of settings resolved
 * for entities in the graph. Various stages of the {@link TopologyPipeline} after
 * setting resolution need access to settings in order to compute things correctly, and this
 * structure facilitates that.
 *
 * We use a special class so that it can be passed between the stages of the pipeline. The
 * alternative was to put the graph and settings in the {@link TopologyPipelineContext}, but
 * we wanted fields in the context to be available across all stages, instead of
 * null-until-some-stage.
 */
public class GraphWithSettings {

    /**
     * The {@link TopologyGraph<TopologyEntity>}. This will continue to be mutated by stages in the pipeline.
     */
    private final TopologyGraph<TopologyEntity> topologyGraph;

    /**
     * The default setting policies in the system, as retrieved during setting resolution.
     * We want to work with the same snapshot of default settings at all stages in the pipeline
     * to avoid inconsistencies.
     *
     * Maps policy id -> policy
     *
     * This is immutable.
     */
    private final Map<Long, SettingPolicy> defaultSettingPolicies;

    /**
     * The {@link EntitySettings} for each entity in the topology graph that has settings
     * defined on it.
     *
     * Maps entity id -> settings, applied to entity
     *
     * This is immutable.
     */
    private final Map<Long, EntitySettings> settingsByEntity;

    public GraphWithSettings(@Nonnull final TopologyGraph<TopologyEntity> topologyGraph,
                             @Nonnull final Map<Long, EntitySettings> entitySettings,
                             @Nonnull final Map<Long, SettingPolicy> defaultSettingPolicies) {
        this.topologyGraph = Objects.requireNonNull(topologyGraph);
        this.defaultSettingPolicies = Collections.unmodifiableMap(
                Objects.requireNonNull(defaultSettingPolicies));
        this.settingsByEntity = Collections.unmodifiableMap(
                Objects.requireNonNull(entitySettings));
    }

    @Nonnull
    public TopologyGraph<TopologyEntity> getTopologyGraph() {
        return topologyGraph;
    }

    @Nonnull
    public Collection<EntitySettings> getEntitySettings() {
        return settingsByEntity.values();
    }

    @Nonnull
    public EntitySettingsCollection constructEntitySettingsCollection() {
        return new EntitySettingsCollection(defaultSettingPolicies, settingsByEntity);
    }

    @Nonnull
    public Collection<Setting> getSettingsForEntity(final long entityId) {
        final EntitySettings settingsForEntity = settingsByEntity.get(entityId);
        if (settingsForEntity == null || !settingsForEntity.hasDefaultSettingPolicyId()) {
            return Collections.emptyList();
        }

        final SettingPolicy defaultSettingPolicy =
                defaultSettingPolicies.get(settingsForEntity.getDefaultSettingPolicyId());

        // Make a copy of the default settings because we'll need to modify it
        // to put in the user settings.
        //
        // Right now we do this every time this method gets called. If it becomes computationally
        // expensive, we can do it once at construction and save the
        // (entityID -> settingName -> setting) map, or save each settings lookup so that
        // we only do this once per entity.
        final Collection<Setting> settingsByName =
                new ArrayList<>(defaultSettingPolicy.getInfo().getSettingsList());
        // Override defaults with user-specific settings.
        settingsByName.addAll(settingsForEntity.getUserSettingsList()
                .stream()
                .map(SettingToPolicyId::getSetting)
                .collect(Collectors.toList()));

        return settingsByName;
    }
}
