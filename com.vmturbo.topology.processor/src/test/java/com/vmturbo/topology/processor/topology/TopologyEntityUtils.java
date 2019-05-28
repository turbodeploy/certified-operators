package com.vmturbo.topology.processor.topology;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.tag.Tag.TagValuesDTO;
import com.vmturbo.common.protobuf.tag.Tag.Tags;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.DiscoveryOrigin;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Origin;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.DiscoveryOriginBuilder;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;

/**
 * Utilities for generating {@link TopologyEntity} objects for tests.
 */
public class TopologyEntityUtils {
    /**
     * Create a {@link TopologyEntity.Builder}.
     *
     * @param entityBuilder The entity builder the {@link TopologyEntity} should wrap.
     * @return A builder for a {@link TopologyEntity}.
     */
    public static TopologyEntity.Builder topologyEntityBuilder(@Nonnull final TopologyEntityDTO.Builder entityBuilder) {
        return TopologyEntity.newBuilder(entityBuilder);
    }

    /**
     * Create a {@link TopologyEntity.Builder}.
     *
     * @param entityBuilder The entity builder the {@link TopologyEntity} should wrap.
     * @return A {@link TopologyEntity} wrapping the input DTO builder.
     */
    public static TopologyEntity topologyEntity(@Nonnull final TopologyEntityDTO.Builder entityBuilder) {
        return topologyEntityBuilder(entityBuilder).build();
    }

    /**
     * Create a new {@link TopologyEntity} with the given OID and {@link EntityType} buying
     * from the given providerIds.
     *
     * All providers are set to be physical machines and the created entity is set to buy CPU from the providers.
     *
     * @param oid The oid of the entity.
     * @param entityType The entity type of the entity.
     * @param providerIds The oids of the providers to this entity.
     * @return a new {@link TopologyEntity} with the given OID and {@link EntityType} buying
     *         from the given providerIds.
     */
    @Nonnull
    public static TopologyEntity.Builder topologyEntityBuilder(final long oid,
                                                               final EntityType entityType,
                                                               @Nonnull final List<Long> providerIds) {
        return topologyEntityBuilder(oid, entityType, DiscoveryOrigin.getDefaultInstance(), providerIds);
    }

    /**
     * Create a new {@link TopologyEntity} with the given OID and {@link EntityType} buying
     * from the given providerIds.
     *
     * All providers are set to be physical machines and the created entity is set to buy CPU from the providers.
     *
     * @param oid The oid of the entity.
     * @param entityType The entity type of the entity.
     * @param discoveryOrigin The builder for the entity origin.
     * @param providerIds The oids of the providers to this entity.
     * @return a new {@link TopologyEntity} with the given OID and {@link EntityType} buying
     *         from the given providerIds.
     */
    @Nonnull
    public static TopologyEntity.Builder topologyEntityBuilder(final long oid,
                                                               final EntityType entityType,
                                                               @Nonnull final DiscoveryOrigin discoveryOrigin,
                                                               @Nonnull final List<Long> providerIds) {
        return topologyEntityBuilder(TopologyEntityDTO.newBuilder()
                .setOid(oid)
                .setEntityType(entityType.getNumber())
                .setOrigin(Origin.newBuilder().setDiscoveryOrigin(discoveryOrigin))
                .addAllCommoditiesBoughtFromProviders(
                    providerIds.stream()
                        .map(providerId ->
                                CommoditiesBoughtFromProvider.newBuilder()
                                    .setProviderId(providerId)
                                    .setProviderEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
                                    .addCommodityBought(
                                        CommodityBoughtDTO.newBuilder()
                                            .setCommodityType(
                                                CommodityType.newBuilder()
                                                    .setType(CommodityDTO.CommodityType.CPU_VALUE)
                                                    .build()
                                            )
                                    ).build()
                        ).collect(Collectors.toList())
                )
        );
    }

    @Nonnull
    public static TopologyGraph<TopologyEntity> topologyGraphOf(@Nonnull final TopologyEntity.Builder... entityBuilders) {
        return TopologyEntityTopologyGraphCreator.newGraph(Stream.of(entityBuilders)
            .collect(Collectors.toMap(TopologyEntity.Builder::getOid, Function.identity())));
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
