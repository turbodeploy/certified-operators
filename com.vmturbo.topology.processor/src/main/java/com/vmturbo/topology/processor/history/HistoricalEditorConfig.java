package com.vmturbo.topology.processor.history;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.EntitySettingsCollection;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.processor.group.settings.GraphWithSettings;

/**
 * Base config values for all historical editor sub-types.
 * Provides basic settings/policies access functionality, to be extended.
 */
public class HistoricalEditorConfig {
    private EntitySettingsCollection entitySettings;
    private TopologyGraph<TopologyEntity> graph;

    /**
     * Entity settings only get available way after construction at certain pipeline stage.
     * Which is supposed to run before history calculations stage.
     * This should be initialized when the stage begins.
     *
     * @param graph topology graph with settings
     */
    public void initSettings(@Nonnull GraphWithSettings graph) {
        this.entitySettings = graph.constructEntitySettingsCollection();
        this.graph = graph.getTopologyGraph();
    }

    /**
     * Get the setting for an entity.
     *
     * @param <T> setting value type
     * @param oid topology entity identifier
     * @param settingSpec setting specification
     * @param cls setting value class
     * @return setting value, null if not present
     * @throws HistoryCalculationException when settings are not initialized
     */
    @Nullable
    public <T> T getEntitySetting(long oid,
                                  @Nonnull EntitySettingSpecs settingSpec,
                                  @Nonnull Class<T> cls) throws HistoryCalculationException {
        if (entitySettings == null) {
            throw new HistoryCalculationException("Settings are not initialized for history calculation");
        }
        return entitySettings.getEntitySettingValue(oid, settingSpec, cls);
    }

    /**
     * Locate the entity type in the graph being broadcasted by oid.
     *
     * @param oid entity oid
     * @return entity oid
     * @throws HistoryCalculationException when not initialized
     */
    @Nonnull
    protected EntityType getEntityType(long oid) throws HistoryCalculationException {
        if (graph == null) {
            throw new HistoryCalculationException("Settings are not initialized for history calculation");
        }
        return graph.getEntity(oid).map(TopologyEntity::getEntityType).map(EntityType::forNumber)
                        .orElseThrow(() -> new HistoryCalculationException("Entity not found by oid " + oid));
    }
}
