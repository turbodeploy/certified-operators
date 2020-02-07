package com.vmturbo.topology.processor.history;

import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
    private static final Logger logger = LogManager.getLogger();
    private EntitySettingsCollection entitySettings;
    private TopologyGraph<TopologyEntity> graph;
    private boolean isPlan;

    /**
     * Entity settings only get available way after construction at certain pipeline stage.
     * Which is supposed to run before history calculations stage.
     * This should be initialized when the stage begins.
     *
     * @param graph topology graph with settings
     * @param isPlan whether invoked in plan broadcast context
     */
    public void initSettings(@Nonnull GraphWithSettings graph, boolean isPlan) {
        this.entitySettings = graph.constructEntitySettingsCollection();
        this.graph = graph.getTopologyGraph();
        this.isPlan = isPlan;
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
     * Whether invoked in plan broadcast context.
     *
     * @return true when for plan
     */
    public boolean isPlan() {
        return isPlan;
    }

    /**
     * Locate the entity type in the graph being broadcasted by oid.
     *
     * @param oid entity oid
     * @return entity oid
     * @throws HistoryCalculationException when not initialized
     */
    @Nullable
    protected EntityType getEntityType(long oid) throws HistoryCalculationException {
        if (graph == null) {
            throw new HistoryCalculationException("Settings are not initialized for history calculation");
        }
        Optional<TopologyEntity> entity = graph.getEntity(oid);
        if (!entity.isPresent()) {
            logger.debug(
                    "{} There is no entity with id {} in topology graph during historical data calculations . Returning null.",
                    getClass().getSimpleName(), oid);
            return null;
        }
        return entity.map(TopologyEntity::getEntityType).map(EntityType::forNumber)
                        .orElseThrow(() -> new HistoryCalculationException("Entity type calculation failed for entity with oid " + oid));
    }

    /**
     * Release the references.
     */
    public void deinit() {
        graph = null;
        entitySettings = null;
    }
}
