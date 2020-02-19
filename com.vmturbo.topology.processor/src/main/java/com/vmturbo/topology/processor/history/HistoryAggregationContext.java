package com.vmturbo.topology.processor.history;

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

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
 * Per-pipeline invocation context for the history aggregations.
 * Aggregations may be invoked in parallel with different contexts.
 */
public class HistoryAggregationContext {
    private static final Logger logger = LogManager.getLogger();

    private final TopologyGraph<TopologyEntity> graph;
    private final ICommodityFieldAccessor accessor;
    private final EntitySettingsCollection entitySettings;
    private final boolean isPlan;

    /**
     * Construct the context.
     *
     * @param graphWithSettings topology graph
     * @param isPlan whether the pipeline is for plan
     */
    public HistoryAggregationContext(@Nonnull GraphWithSettings graphWithSettings, boolean isPlan) {
        this.graph = graphWithSettings.getTopologyGraph();
        this.accessor = new CommodityFieldAccessor(graph);
        this.isPlan = isPlan;
        this.entitySettings = graphWithSettings.constructEntitySettingsCollection();
    }

    /**
     * Get the setting for an entity.
     *
     * @param <T> setting value type
     * @param oid topology entity identifier
     * @param settingSpec setting specification
     * @param cls setting value class
     * @return setting value, null if not present
     */
    @Nullable
    public <T> T getEntitySetting(long oid,
                                  @Nonnull EntitySettingSpecs settingSpec,
                                  @Nonnull Class<T> cls) {
        return entitySettings.getEntitySettingValue(oid, settingSpec, cls);
    }

    /**
     * Locate the entity type in the graph being broadcasted by oid.
     *
     * @param oid entity oid
     * @return entity type, null and log if not present
     */
    @Nullable
    public EntityType getEntityType(long oid) {
        Optional<TopologyEntity> entity = graph.getEntity(oid);
        if (!entity.isPresent()) {
            logger.debug(
                    "There is no entity with id {} in topology graph during historical data calculations",
                    oid);
            return null;
        }
        EntityType type = EntityType.forNumber(entity.get().getEntityType());
        if (type == null) {
            logger.debug("Unknown entity type {} for entity {}", entity.get().getEntityType(), oid);
        }
        return type;
    }

    /**
     * Map filtered graph entities to a given setting value.
     *
     * @param <SettingT> setting type
     * @param filter filter entities by that
     * @param settingExtractor setting getter for an oid
     * @return entity to setting value
     */
    @Nonnull
    public <SettingT> Map<Long, SettingT>
           entityToSetting(@Nonnull Predicate<TopologyEntity> filter,
                           @Nonnull Function<TopologyEntity, SettingT> settingExtractor) {
        return graph.entities().filter(filter)
                        .collect(Collectors.toMap(TopologyEntity::getOid, settingExtractor));
    }

    /**
     * Updater for the commodity historical value fields.
     *
     * @return commodity accessor
     */
    @Nonnull
    public ICommodityFieldAccessor getAccessor() {
        return accessor;
    }

    public boolean isPlan() {
        return isPlan;
    }

}
