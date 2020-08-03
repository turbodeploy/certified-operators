package com.vmturbo.topology.processor.history;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.util.Pair;
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

    private final TopologyInfo topologyInfo;
    private final TopologyGraph<TopologyEntity> graph;
    private final ICommodityFieldAccessor accessor;
    private final EntitySettingsCollection entitySettings;
    private final boolean isPlan;
    private final Map<Pair<Long, EntitySettingSpecs>, Number> oidSettingToValue =
                    new ConcurrentHashMap<>();

    /**
     * Construct the context.
     *
     * @param topologyInfo information about currently processing topology snapshot.
     * @param graphWithSettings topology graph
     * @param isPlan whether the pipeline is for plan
     */
    public HistoryAggregationContext(@Nonnull TopologyInfo topologyInfo,
                    @Nonnull GraphWithSettings graphWithSettings, boolean isPlan) {
        this.topologyInfo = topologyInfo;
        this.graph = graphWithSettings.getTopologyGraph();
        this.accessor = new CommodityFieldAccessor(graph);
        this.isPlan = isPlan;
        this.entitySettings = graphWithSettings.constructEntitySettingsCollection();
    }

    /**
     * Returns setting value from cache in case it is present or from the entity settings
     * collection. In case entity has no explicit setting value, then default setting value will be
     * returned.
     *
     * @param relatedId entity identifier which setting value we want to get.
     * @param settingSpecs setting specification which value we need to get.
     * @param settingValueType raw type of the setting that we are going to get.
     * @param converter converts raw setting value into integer.
     * @param defaultValueProvider provider for default setting value.
     * @param <V> type of raw setting value.
     * @param <D> type of the converted setting value.
     * @return value of the setting.
     */
    public <V, D extends Number> Number getSettingValue(long relatedId,
                                               @Nonnull EntitySettingSpecs settingSpecs,
                    @Nonnull Class<V> settingValueType, @Nonnull Function<V, D> converter,
                    Function<SettingSpec, V> defaultValueProvider) {
        return oidSettingToValue.computeIfAbsent(Pair.create(relatedId, settingSpecs), k -> {
            final V rawValue = entitySettings.getEntitySettingValue(relatedId, settingSpecs,
                            settingValueType);
            if (rawValue == null) {
                return converter.apply(defaultValueProvider.apply(settingSpecs.getSettingSpec()));
            }
            return converter.apply(rawValue);
        });
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

    /**
     * Returns topology identifier for which history aggregation is running for.
     *
     * @return topology identifier.
     */
    public long getTopologyId() {
        return topologyInfo.getTopologyId();
    }

    public boolean isPlan() {
        return isPlan;
    }

    /**
     * Returns the oid of the live topology of input entity oid.
     *
     * @param entityOid the input entity oid.
     * @return the oid of the live topology oid and empty otherwise.
     */
    @Nonnull
    public Optional<Long> getClonedFromEntityOid(@Nullable Long entityOid) {
        return Optional.ofNullable(entityOid)
                .flatMap(graph::getEntity)
                .flatMap(TopologyEntity::getClonedFromEntity)
                .map(TopologyEntityDTO.Builder::getOid);
    }
}
