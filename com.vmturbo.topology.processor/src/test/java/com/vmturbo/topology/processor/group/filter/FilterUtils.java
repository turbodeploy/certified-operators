package com.vmturbo.topology.processor.group.filter;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.tag.Tag.TagValuesDTO;
import com.vmturbo.common.protobuf.tag.Tag.Tags;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Origin;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.DiscoveryOriginBuilder;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.processor.topology.TopologyGraph;

/**
 * A set of utilities for testing filters.
 */
public class FilterUtils {
    /**
     * Prevent instantiation because contains only static helpers
     */
    private FilterUtils() { }

    /**
     * Filter a list of OIDs over a graph using a given filter to generate collection of other OIDs.
     *
     * @param filter The filter to use.
     * @param graph The graph to filter over.
     * @param startingPoints The starting points of the filter operation.
     *
     * @return A collection of OIDs for the vertices in the graph that matched the filter.
     */
    public static Collection<Long> filterOids(@Nonnull final TopologyFilter filter,
                                        @Nonnull final TopologyGraph graph, long... startingPoints) {
        Stream<TopologyEntity> startingVertices = Arrays.stream(startingPoints)
            .mapToObj(l -> l)
            .map(graph::getEntity)
            .filter(Optional::isPresent)
            .map(Optional::get);

        return filter.apply(startingVertices, graph)
            .map(TopologyEntity::getOid)
            .collect(Collectors.toList());
    }


    /**
     * Create a minimal topology entity builder.
     *
     * @param oid The OID of the topology entity.
     * @param entityType The entity type for the entity.
     * @param producers The OIDs of the producers that the created entity should be consuming from.
     *                  Does not actually associate any commodities with the producers.
     * @return A {@link TopologyEntityDTO} with the given properties.
     */
    public static TopologyEntity.Builder topologyEntity(long oid, EntityType entityType, long... producers) {
        return topologyEntity(oid, 0, 0, entityType, producers);
    }

    public static TopologyEntity.Builder connectedTopologyEntity(long oid, EntityType entityType, long... connectedToEntities) {
        return connectedTopologyEntity(oid, 0, 0, entityType, connectedToEntities);
    }

    /**
     * Create a minimal topology entity builder.
     *
     * @param oid The OID of the topology entity.
     * @param discoveringTargetId The ID of the target that discovered the entity.
     * @param lastUpdatedTime last updated time of the topology entity
     * @param entityType The entity type for the entity.
     * @param producers The OIDs of the producers that the created entity should be consuming from.
     *                  Does not actually associate any commodities with the producers.
     * @return A {@link TopologyEntityDTO} with the given properties.
     */
    public static TopologyEntity.Builder topologyEntity(long oid,
                                                        long discoveringTargetId,
                                                        long lastUpdatedTime,
                                                        EntityType entityType,
                                                        long... producers) {
        final TopologyEntity.Builder builder = TopologyEntity.newBuilder(
            TopologyEntityDTO.newBuilder()
                .setOid(oid)
                .setEntityType(entityType.getNumber())
                .setOrigin(Origin.newBuilder()
                    .setDiscoveryOrigin(DiscoveryOriginBuilder.discoveredBy(discoveringTargetId)
                        .lastUpdatedAt(lastUpdatedTime))));

        addCommodityBoughtMap(builder.getEntityBuilder(), producers);
        return builder;
    }

    /**
     * Create a minimal topology entity builder.
     *
     * @param oid The OID of the topology entity.
     * @param discoveringTargetId The ID of the target that discovered the entity.
     * @param lastUpdatedTime last updated time of the topology entity
     * @param entityType The entity type for the entity.
     * @param connectedToEntities The OIDs of the entities that the created entity should be
     *                            connected to in the topology
     * @return A {@link TopologyEntityDTO} with the given properties.
     */
    public static TopologyEntity.Builder connectedTopologyEntity(long oid,
                                                        long discoveringTargetId,
                                                        long lastUpdatedTime,
                                                        EntityType entityType,
                                                        long... connectedToEntities) {
        final TopologyEntity.Builder builder = TopologyEntity.newBuilder(
            TopologyEntityDTO.newBuilder()
                .setOid(oid)
                .setEntityType(entityType.getNumber())
                .setOrigin(Origin.newBuilder()
                    .setDiscoveryOrigin(DiscoveryOriginBuilder.discoveredBy(discoveringTargetId)
                        .lastUpdatedAt(lastUpdatedTime))));

        for (long connectedTo : connectedToEntities) {
            builder.getEntityBuilder().addConnectedEntityList(ConnectedEntity.newBuilder()
                .setConnectedEntityId(connectedTo)
                .setConnectionType(ConnectionType.NORMAL_CONNECTION)
                .build());
        }
        return builder;
    }

    /**
     * Create a minimal topology entity builder that was never discovered.
     *
     * @param oid The OID of the topology entity.
     * @param entityType The entity type for the entity.
     * @param producers The OIDs of the producers that the created entity should be consuming from.
     *                  Does not actually associate any commodities with the producers.
     * @return A {@link TopologyEntityDTO} with the given properties.
     */
    public static TopologyEntity.Builder neverDiscoveredTopologyEntity(long oid,
                                                        EntityType entityType,
                                                        long... producers) {
        final TopologyEntity.Builder builder = TopologyEntity.newBuilder(
            TopologyEntityDTO.newBuilder()
                .setOid(oid)
                .setEntityType(entityType.getNumber()));

        addCommodityBoughtMap(builder.getEntityBuilder(), producers);
        return builder;
    }

    /**
     * Create a minimal topology entity with display name.
     *
     * @param oid The OID of the topology entity.
     * @param entityType The entity type for the entity.
     * @param name The name of the topology entity
     * @param producers The OIDs of the producers that the created entity should be consuming from.
     *                  Does not actually associate any commodities with the producers.
     * @return A {@link TopologyEntityDTO} with the given properties.
     */
    public static TopologyEntity.Builder topologyEntityWithName(long oid,
                                                           EntityType entityType,
                                                           String name,
                                                           long... producers) {
        final TopologyEntity.Builder builder = TopologyEntity.newBuilder(TopologyEntityDTO.newBuilder()
                .setOid(oid)
                .setDisplayName(name)
                .setEntityType(entityType.getNumber()));

        addCommodityBoughtMap(builder.getEntityBuilder(), producers);
        return builder;
    }

    /**
     * Create a minimal topology entity with entity tags.
     *
     * @param oid The OID of the topology entity.
     * @param entityType The entity type for the entity.
     * @param tags The tags.
     * @return A {@link TopologyEntityDTO} with the given properties.
     */
    public static TopologyEntity.Builder topologyEntityWithTags(
            long oid, @Nonnull EntityType entityType, @Nonnull Map<String, TagValuesDTO> tags) {
        return TopologyEntity.newBuilder(
                TopologyEntityDTO.newBuilder()
                    .setOid(oid)
                    .setEntityType(entityType.getNumber())
                    .setTags(Tags.newBuilder().putAllTags(tags).build()));
    }

    /**
     * Add each producer to builder commodity bought map
     * @param builder The builder of the topology entity
     * @param producers The OIDs of the producers that the created entity should be consuming from.
     *                  Does not actually associate any commodities with the producers.
     */
    private static void addCommodityBoughtMap(TopologyEntityDTO.Builder builder, long... producers) {
        for (long producer : producers) {
            builder.addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                .setProviderId(producer)
                .build());
        }
    }
}
