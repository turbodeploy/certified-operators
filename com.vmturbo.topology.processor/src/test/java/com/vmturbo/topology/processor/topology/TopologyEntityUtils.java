package com.vmturbo.topology.processor.topology;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.TextFormat;
import com.google.protobuf.util.JsonFormat;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyPOJO;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommodityBoughtImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommoditySoldImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommoditySoldView;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommodityTypeImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommodityTypeView;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.HistoricalValuesImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.HistoricalValuesView;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl.CommoditiesBoughtFromProviderImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl.ConnectedEntityImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl.ConnectedEntityView;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl.DiscoveryOriginView;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl.OriginImpl;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityOrigin;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;
import com.vmturbo.platform.sdk.common.util.Pair;
import com.vmturbo.stitching.DiscoveryOriginImplBuilder;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;

/**
 * Utilities for generating {@link TopologyEntity} objects for tests.
 */
public class TopologyEntityUtils {

    /**
     * Create a {@link TopologyEntity.Builder}.
     *
     * @param topologyEntityImpl The entity builder the {@link TopologyEntityImpl} should wrap.
     * @return A builder for a {@link TopologyEntity}.
     */
    public static TopologyEntity.Builder topologyEntityBuilder(@Nonnull final TopologyEntityImpl topologyEntityImpl) {
        return TopologyEntity.newBuilder(topologyEntityImpl);
    }

    /**
     * Create a {@link TopologyEntity.Builder}.
     *
     * @param entityBuilder The entity builder the {@link TopologyEntity} should wrap.
     * @return A builder for a {@link TopologyEntity}.
     */
    public static TopologyEntity.Builder topologyEntityBuilder(@Nonnull final TopologyEntityDTO.Builder entityBuilder) {
        return TopologyEntity.newBuilder(TopologyEntityImpl.fromProto(entityBuilder));
    }

    /**
     * Create a {@link TopologyEntity}.
     *
     * @param entityBuilder The entity builder the {@link TopologyEntity} should wrap.
     * @return A {@link TopologyEntity} wrapping the input DTO builder.
     */
    public static TopologyEntity topologyEntity(@Nonnull final TopologyEntityImpl entityBuilder) {
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
        return topologyEntityBuilder(oid, entityType, DiscoveryOriginView.getDefaultInstance(), providerIds);
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
                                                           @Nonnull final DiscoveryOriginView discoveryOrigin,
                                                           @Nonnull final List<Long> providerIds) {
        return topologyEntityBuilder(new TopologyEntityImpl()
            .setOid(oid)
            .setEntityType(entityType.getNumber())
            .setOrigin(new OriginImpl().setDiscoveryOrigin(discoveryOrigin))
            .addAllCommoditiesBoughtFromProviders(
                providerIds.stream()
                    .map(providerId ->
                        new CommoditiesBoughtFromProviderImpl()
                            .setProviderId(providerId)
                            .setProviderEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
                            .addCommodityBought(
                                new CommodityBoughtImpl()
                                    .setCommodityType(
                                        new CommodityTypeImpl()
                                            .setType(CommodityDTO.CommodityType.CPU_VALUE)
                                    )
                            )
                    ).collect(Collectors.toList())
            )
        );
    }

    /**
     * Create TopologyGraph of {@link TopologyEntity}'s.
     *
     * @param entityBuilders entity builders
     * @return graph.
     */
    @Nonnull
    public static TopologyGraph<TopologyEntity> pojoGraphOf(@Nonnull final TopologyEntity.Builder... entityBuilders) {
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
     * @return A {@link TopologyEntity.Builder} with the given properties.
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
     * @return A {@link TopologyEntity.Builder} with the given properties.
     */
    public static TopologyEntity.Builder topologyEntity(long oid,
                                                    long discoveringTargetId,
                                                    long lastUpdatedTime,
                                                    EntityType entityType,
                                                    long... producers) {
        return topologyEntity(oid, discoveringTargetId, lastUpdatedTime, "", entityType, producers);
    }

    /**
     * Create a minimal topology entity builder.
     *
     * @param oid The OID of the topology entity.
     * @param discoveringTargetId The ID of the target that discovered the entity.
     * @param lastUpdatedTime last updated time of the topology entity.
     * @param displayName topology entity display name.
     * @param entityType The entity type for the entity.
     * @param producers The OIDs of the producers that the created entity should be consuming from.
     *                  Does not actually associate any commodities with the producers.
     * @return A {@link TopologyEntity.Builder} with the given properties.
     */
    public static TopologyEntity.Builder topologyEntity(long oid,
                                                    long discoveringTargetId,
                                                    long lastUpdatedTime,
                                                    String displayName,
                                                    EntityType entityType,
                                                    long... producers) {
        return topologyEntity(oid, discoveringTargetId, lastUpdatedTime,
                displayName, entityType, Collections.emptyList(), producers);
    }

    /**
     * Create a minimal topology entity builder.
     *
     * @param oid The OID of the topology entity.
     * @param discoveringTargetId The ID of the target that discovered the entity.
     * @param lastUpdatedTime last updated time of the topology entity.
     * @param displayName topology entity display name.
     * @param entityType The entity type for the entity.
     * @param soldComms is the list of {@link CommodityType} sold.
     * @param producers The OIDs of the producers that the created entity should be consuming from.
     *                  Does not actually associate any commodities with the producers.
     * @return A {@link TopologyEntityImpl} with the given properties.
     */
    public static TopologyEntity.Builder topologyEntity(long oid,
                                                    long discoveringTargetId,
                                                    long lastUpdatedTime,
                                                    String displayName,
                                                    EntityType entityType,
                                                    List<CommodityTypeView> soldComms,
                                                    long... producers) {
        final TopologyEntity.Builder builder = TopologyEntity.newBuilder(
                new TopologyEntityImpl()
                        .setOid(oid)
                        .setEntityType(entityType.getNumber())
                        .setDisplayName(displayName)
                        .setOrigin(new TopologyEntityImpl.OriginImpl()
                                    .setDiscoveryOrigin(
                                            DiscoveryOriginImplBuilder.discoveredBy(discoveringTargetId,
                                                                            null, EntityOrigin.DISCOVERED)
                                                    .lastUpdatedAt(lastUpdatedTime))));
        for (CommodityTypeView ct : soldComms) {
            builder.getTopologyEntityImpl().addCommoditySoldList(new CommoditySoldImpl().setCommodityType(ct));
        }

        addCommodityBoughtMap(builder.getTopologyEntityImpl(), producers);
        return builder;
    }

    /**
     * Create a minimal topology entity builder.
     *
     * @param oid The OID of the topology entity.
     * @param discoveringTargetId The ID of the target that discovered the entity.
     * @param lastUpdatedTime last updated time of the topology entity.
     * @param displayName topology entity display name.
     * @param entityType The entity type for the entity.
     * @param producers The is mapping of OIDs of the producers that the created entity should be consuming from.
     *                  associated with commodities bought from the provider.
     * @param soldComms is the list of {@link CommodityType} sold.
     * @return A {@link TopologyEntityDTO} with the given properties.
     */
    public static TopologyEntity.Builder topologyEntity(long oid,
                                                    long discoveringTargetId,
                                                    long lastUpdatedTime,
                                                    String displayName,
                                                    EntityType entityType,
                                                    Map<Long, List<TopologyPOJO.CommodityTypeView>> producers,
                                                    List<CommodityTypeView> soldComms) {
        final TopologyEntity.Builder builder = TopologyEntity.newBuilder(
                new TopologyEntityImpl()
                        .setOid(oid)
                        .setEntityType(entityType.getNumber())
                        .setDisplayName(displayName)
                        .setOrigin(new TopologyEntityImpl.OriginImpl()
                                .setDiscoveryOrigin(
                                        DiscoveryOriginImplBuilder.discoveredBy(discoveringTargetId,
                                                        null, EntityOrigin.DISCOVERED)
                                                .lastUpdatedAt(lastUpdatedTime))));
        for (CommodityTypeView ct : soldComms) {
            builder.getTopologyEntityImpl().addCommoditySoldList(new CommoditySoldImpl().setCommodityType(ct));
        }

        addCommodityBoughtFromProviderMap(builder.getTopologyEntityImpl(), producers);
        return builder;
    }

    /**
     * Create a minimal topology entity builder.
     *
     * @param oid The OID of the topology entity.
     * @param discoveringTargetId The ID of the target that discovered the entity.
     * @param lastUpdatedTime last updated time of the topology entity.
     * @param displayName topology entity display name.
     * @param entityType The entity type for the entity.
     * @param soldComms is the list of {@link CommodityType} sold.
     * @param producers The is mapping of OIDs of the producers that the created entity should be consuming from.
     *                  associated with commodities bought from the provider.
     * @return A {@link TopologyEntityDTO} with the given properties.
     */
    public static TopologyEntity.Builder topologyEntity(long oid,
                                                        long discoveringTargetId,
                                                        long lastUpdatedTime,
                                                        String displayName,
                                                        EntityType entityType,
                                                        List<CommodityTypeView> soldComms,
                                                        Map<Long, Pair<Integer, List<CommodityTypeView>>> producers) {
        final TopologyEntity.Builder builder = TopologyEntity.newBuilder(
            new TopologyEntityImpl()
                .setOid(oid)
                .setEntityType(entityType.getNumber())
                .setDisplayName(displayName)
                .setOrigin(new OriginImpl()
                    .setDiscoveryOrigin(
                        DiscoveryOriginImplBuilder.discoveredBy(discoveringTargetId,
                            null, EntityOrigin.DISCOVERED)
                            .lastUpdatedAt(lastUpdatedTime))));
        for (CommodityTypeView ct : soldComms) {
            builder.getTopologyEntityImpl().addCommoditySoldList(new CommoditySoldImpl().setCommodityType(ct));
        }

        addCommodityBoughtFromProviderMap(producers, builder.getTopologyEntityImpl());
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
     * @return A {@link TopologyEntityImpl} with the given properties.
     */
    public static TopologyEntity.Builder connectedTopologyEntity(long oid,
                                                             long discoveringTargetId,
                                                             long lastUpdatedTime,
                                                             EntityType entityType,
                                                             long... connectedToEntities) {
        return connectedTopologyEntity(oid, discoveringTargetId, lastUpdatedTime, "", entityType, connectedToEntities);
    }

    /**
     * Create a minimal topology entity builder.
     *
     * @param oid The OID of the topology entity.
     * @param discoveringTargetId The ID of the target that discovered the entity.
     * @param lastUpdatedTime last updated time of the topology entity
     * @param displayName topology entity display name.
     * @param entityType The entity type for the entity.
     * @param connectedToEntities The OIDs of the entities that the created entity should be
     *                            connected to in the topology
     * @return A {@link TopologyEntityDTO} with the given properties.
     */
    public static TopologyEntity.Builder connectedTopologyEntity(long oid,
                                                                 long discoveringTargetId,
                                                                 long lastUpdatedTime,
                                                                 String displayName,
                                                                 EntityType entityType,
                                                                 long... connectedToEntities) {
        final TopologyEntity.Builder builder = TopologyEntity.newBuilder(
            new TopologyEntityImpl()
                .setOid(oid)
                .setEntityType(entityType.getNumber())
                .setDisplayName(displayName)
                .setOrigin(new OriginImpl()
                        .setDiscoveryOrigin(
                                DiscoveryOriginImplBuilder.discoveredBy(discoveringTargetId, null,
                                        EntityOrigin.DISCOVERED)
                        .lastUpdatedAt(lastUpdatedTime))));

        for (long connectedTo : connectedToEntities) {
            builder.getTopologyEntityImpl()
                    .addConnectedEntityList(new ConnectedEntityImpl()
                .setConnectedEntityId(connectedTo)
                .setConnectionType(ConnectionType.NORMAL_CONNECTION));
        }
        return builder;
    }

    /**
     * Create a minimal topology entity.
     *
     * @param oid The OID of the topology entity.
     * @param discoveringTargetId The ID of the target that discovered the entity.
     * @param lastUpdatedTime last updated time of the topology entity
     * @param displayName topology entity display name.
     * @param entityType The entity type for the entity.
     * @param connectedToEntities The OIDs of the entities that the created entity should be
     *                            connected to in the topology together with the respective
     *                            connection types
     * @return A {@link TopologyEntityImpl} with the given properties.
     */
    public static TopologyEntity.Builder connectedTopologyEntity(
             long oid, long discoveringTargetId, long lastUpdatedTime, String displayName,
             EntityType entityType, Collection<ConnectedEntityView> connectedToEntities) {
        final TopologyEntity.Builder builder = TopologyEntity.newBuilder(
                new TopologyEntityImpl()
                        .setOid(oid)
                        .setEntityType(entityType.getNumber())
                        .setDisplayName(displayName)
                        .setOrigin(new OriginImpl()
                                .setDiscoveryOrigin(DiscoveryOriginImplBuilder.discoveredBy(discoveringTargetId)
                                        .lastUpdatedAt(lastUpdatedTime))));

        for (ConnectedEntityView connectedEntity : connectedToEntities) {
            builder.getTopologyEntityImpl().addConnectedEntityList(new ConnectedEntityImpl()
                    .setConnectedEntityId(connectedEntity.getConnectedEntityId())
                    .setConnectionType(connectedEntity.getConnectionType()));
        }
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
        final TopologyEntity.Builder builder = TopologyEntity.newBuilder(new TopologyEntityImpl()
                .setOid(oid)
                .setDisplayName(name)
                .setEntityType(entityType.getNumber()));

        addCommodityBoughtMap(builder.getTopologyEntityImpl(), producers);
        return builder;
    }

    /**
     * Add connected entity to given topology entity.
     *
     * @param entity            Given topology entity to which the connected enity is added to.
     * @param connectedEntityId Connected entity ID.
     * @param connectionType    Connection type.
     */
    public static void addConnectedEntity(@Nonnull final TopologyEntity.Builder entity, long connectedEntityId, ConnectionType connectionType) {
        entity.getTopologyEntityImpl()
                .addConnectedEntityList(new TopologyEntityImpl.ConnectedEntityImpl()
                        .setConnectedEntityId(connectedEntityId)
                        .setConnectionType(connectionType));
    }

    /**
     * Add each producer to builder commodity bought map.
     * @param entityImpl The builder of the topology entity
     * @param producers The is mapping of OIDs of the producers that the created entity should be consuming from.
     *                  associated with commodities bought from the provider.
     */
    private static void addCommodityBoughtFromProviderMap(TopologyEntityImpl entityImpl,
                                                          Map<Long, List<CommodityTypeView>> producers) {
        for (Map.Entry<Long, List<CommodityTypeView>> producer : producers.entrySet()) {
            entityImpl.addCommoditiesBoughtFromProviders(new TopologyEntityImpl.CommoditiesBoughtFromProviderImpl()
                    .addAllCommodityBought(producer.getValue().stream().map(c ->
                           new CommodityBoughtImpl().setCommodityType(c)).collect(Collectors.toList()))
                    .setProviderId(producer.getKey()));
        }
    }

    /**
     * Add each producer to builder commodity bought map.
     * @param producers The is mapping of OIDs of the producers that the created entity should be consuming from.
     *                  associated with commodities bought from the provider.
     * @param entity The topology entity
     */
    private static void addCommodityBoughtFromProviderMap(Map<Long, Pair<Integer, List<CommodityTypeView>>> producers,
                                                          TopologyEntityImpl entity) {
        for (Map.Entry<Long, Pair<Integer, List<CommodityTypeView>>> producer : producers.entrySet()) {
            entity.addCommoditiesBoughtFromProviders(new CommoditiesBoughtFromProviderImpl()
                .addAllCommodityBought(producer.getValue().getSecond().stream().map(c ->
                    new CommodityBoughtImpl().setCommodityType(c)).collect(Collectors.toList()))
                .setProviderId(producer.getKey())
                .setProviderEntityType(producer.getValue().getFirst()));
        }
    }

    /**
     * Add each producer to builder commodity bought map.
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

    /**
     * Add each producer to builder commodity bought map.
     * @param builder The builder of the topology entity
     * @param producers The OIDs of the producers that the created entity should be consuming from.
     *                  Does not actually associate any commodities with the producers.
     */
    private static void addCommodityBoughtMap(TopologyEntityImpl builder, long... producers) {
        for (long producer : producers) {
            builder.addCommoditiesBoughtFromProviders(new TopologyEntityImpl.CommoditiesBoughtFromProviderImpl()
                    .setProviderId(producer));
        }
    }

    static TopologyEntity.Builder buildTopologyEntityWithCommBought(
            long oid, int commType, int entityType, long provider) {
        return buildTopologyEntityWithCommBought(oid, commType, entityType, provider, 0, 0, null, null);
    }

    static TopologyEntity.Builder buildTopologyEntityWithCommBought(
        long oid, int commType, int entityType, long provider, double used, double peak) {
        return buildTopologyEntityWithCommBought(oid, commType, entityType, provider, used, peak, null, null);
    }

    static TopologyEntity.Builder buildTopologyEntityWithCommBought(
            long oid, int commType, int entityType, long provider,
            double used, double peak, @Nullable Double historicalUsed, @Nullable Double historicalPeak) {
        CommodityBoughtImpl commodityBought = new CommodityBoughtImpl().setCommodityType(
                new CommodityTypeImpl().setType(commType).setKey(""))
                .setActive(true)
                .setUsed(used)
                .setPeak(peak);
        if (historicalUsed != null) {
            commodityBought.setHistoricalUsed(new HistoricalValuesImpl().setHistUtilization(historicalUsed));
        }
        if (historicalPeak != null) {
            commodityBought.setHistoricalPeak(new HistoricalValuesImpl().setHistUtilization(historicalPeak));
        }
        CommoditiesBoughtFromProviderImpl commFromProvider =
            new CommoditiesBoughtFromProviderImpl().addCommodityBought(commodityBought)
                .setProviderId(provider);

        return TopologyEntityUtils.topologyEntityBuilder(new TopologyEntityImpl().setOid(oid)
            .addCommoditiesBoughtFromProviders(commFromProvider)
            .setEntityType(entityType));
    }

    static TopologyEntity.Builder buildTopologyEntityWithCommSold(
            long oid, int commType, int entityType) {
        return buildTopologyEntityWithCommSold(oid, commType, entityType, 0, 0, null, null);
    }

    static TopologyEntity.Builder buildTopologyEntityWithCommSold(
        long oid, int commType, int entityType, double used, double peak) {
        return buildTopologyEntityWithCommSold(oid, commType, entityType, used, peak, null, null);
    }

    static TopologyEntity.Builder buildTopologyEntityWithCommSold(
            long oid, int commType, int entityType,
            double used, double peak, @Nullable Double historicalUsed, @Nullable Double historicalPeak) {
        final ImmutableList.Builder<CommoditySoldView> commSoldList = ImmutableList.builder();
        CommoditySoldImpl commoditySoldBuilder = new CommoditySoldImpl().setCommodityType(
                new CommodityTypeImpl().setType(commType).setKey(""))
                .setActive(true)
                .setUsed(used)
                .setPeak(peak);
        if (historicalUsed != null) {
            commoditySoldBuilder.setHistoricalUsed(new HistoricalValuesImpl()
                    .setHistUtilization(historicalUsed));
        }
        if (historicalPeak != null) {
            commoditySoldBuilder.setHistoricalPeak(new HistoricalValuesImpl()
                    .setHistUtilization(historicalPeak));
        }
        commSoldList.add(commoditySoldBuilder);

        return TopologyEntityUtils.topologyEntityBuilder(new TopologyEntityImpl().setOid(oid)
            .addAllCommoditySoldList(commSoldList.build())
            .setEntityType(entityType));
    }

    static TopologyEntity.Builder buildTopologyEntityWithCommSoldCommBoughtWithHistoricalValues(
            long oid, HistoricalValuesView historicalUsedSold1, HistoricalValuesView historicalPeakSold1,
            HistoricalValuesView historicalUsedSold2, HistoricalValuesView historicalPeakSold2,
            long providerOid1, HistoricalValuesView historicalUsedBought1, HistoricalValuesView historicalPeakBought1,
            long providerOid2, HistoricalValuesView historicalUsedBought2, HistoricalValuesView historicalPeakBought2) {
        final ImmutableList.Builder<CommoditySoldImpl> commSoldList = ImmutableList.builder();
        commSoldList.add(new CommoditySoldImpl().setCommodityType(
            new CommodityTypeImpl().setType(CommodityDTO.CommodityType.VMEM.getNumber()).setKey(""))
            .setHistoricalUsed(historicalUsedSold1)
            .setHistoricalPeak(historicalPeakSold1));
        commSoldList.add(new CommoditySoldImpl().setCommodityType(
                        new CommodityTypeImpl().setType(CommodityDTO.CommodityType.VCPU.getNumber()).setKey(""))
            .setHistoricalUsed(historicalUsedSold2)
            .setHistoricalPeak(historicalPeakSold2));

        final ImmutableList.Builder<CommoditiesBoughtFromProviderImpl> commBoughtList = ImmutableList.builder();
        commBoughtList.add(new CommoditiesBoughtFromProviderImpl().setProviderId(providerOid1)
            .addCommodityBought(new CommodityBoughtImpl().setCommodityType(
                new CommodityTypeImpl().setType(CommodityDTO.CommodityType.MEM.getNumber()).setKey(""))
                .setHistoricalUsed(historicalUsedBought1)
                .setHistoricalPeak(historicalPeakBought1)));
        commBoughtList.add(new CommoditiesBoughtFromProviderImpl().setProviderId(providerOid2)
            .addCommodityBought(new CommodityBoughtImpl().setCommodityType(
                new CommodityTypeImpl().setType(CommodityDTO.CommodityType.STORAGE_AMOUNT.getNumber()).setKey(""))
                .setHistoricalUsed(historicalUsedBought2)
                .setHistoricalPeak(historicalPeakBought2)));

        return TopologyEntityUtils.topologyEntityBuilder(new TopologyEntityImpl().setOid(oid)
            .addAllCommoditySoldList(commSoldList.build())
            .addAllCommoditiesBoughtFromProviders(commBoughtList.build())
            .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE));
    }

    static TopologyGraph<TopologyEntity> createTopologyGraph(CommodityDTO.CommodityType commType,
                                                         double pmUsed, double pmPeak, double vmUsed, double vmPeak) {
        final Map<Long, TopologyEntity.Builder> topology = new HashMap<>();

        // Set physical machine with commodities sold.
        topology.put(1L, buildTopologyEntityWithCommSold(1L, commType.getNumber(),
                EntityType.PHYSICAL_MACHINE_VALUE, pmUsed, pmPeak));

        // Set virtual machine with commodities bought.
        topology.put(2L, buildTopologyEntityWithCommBought(2L, commType.getNumber(),
                EntityType.VIRTUAL_MACHINE_VALUE, 1L, vmUsed, vmPeak));

        return TopologyEntityTopologyGraphCreator.newGraph(topology);
    }

    /**
     * Load a json file into an Entity DTO.
     *
     * @param fileBasename Basename of file to load.
     * @return The entity DTO represented by the file
     * @throws IllegalArgumentException On file read error or missing file.
     */
    public static CommonDTO.EntityDTO loadEntityDTO(@Nonnull final String fileBasename) {
        CommonDTO.EntityDTO.Builder builder = CommonDTO.EntityDTO.newBuilder();
        try {
            JsonFormat.parser().merge(getInputReader(fileBasename), builder);
            JsonFormat.printer().appendTo(EntityDTO.newBuilder(), new StringBuilder());
        } catch (IOException ioe) {
            throw new IllegalArgumentException("Bad input JSON file " + fileBasename, ioe);
        }
        return builder.build();
    }

    /**
     * Load an SDK discovery txt file for a target and extract all entity DTOs.
     *
     * @param fileBasename basename of file to load
     * @return the list of entity DTO builders extracted from the discovery response
     * @throws IllegalArgumentException on read error or missing file
     */
    public static List<CommonDTO.EntityDTO.Builder> loadEntityDTOsFromSDKDiscoveryTextFile(
            final String fileBasename) {
        final DiscoveryResponse.Builder discoveryResponseBuilder = DiscoveryResponse.newBuilder();
        try {
            TextFormat.getParser().merge(getInputReader(fileBasename), discoveryResponseBuilder);
        } catch (IOException ioe) {
            throw new IllegalArgumentException("Bad input txt file: " + fileBasename, ioe);
        }
        return discoveryResponseBuilder.getEntityDTOBuilderList();
    }

    /**
     * Logs JSON file containing TopologyEntityDTO.Builder.
     *
     * @param fileBasename Base filename of JSON file.
     * @return TopologyEntityDTO.Builder read from file.
     * @throws IllegalArgumentException On file read error or missing file.
     */
    public static TopologyEntityDTO.Builder loadTopologyBuilderDTO(String fileBasename) {
        TopologyEntityDTO.Builder builder = TopologyEntityDTO.newBuilder();
        try {
            JsonFormat.parser().merge(getInputReader(fileBasename), builder);
        } catch (IOException ioe) {
            throw new IllegalArgumentException("Bad input JSON file " + fileBasename, ioe);
        }
        return builder;
    }

    /**
     * Logs JSON file containing TopologyInfo.
     *
     * @param fileBasename Base filename of JSON file.
     * @return TopologyInfo read from file.
     * @throws IllegalArgumentException On file read error or missing file.
     */
    public static TopologyInfo loadTopologyInfo(String fileBasename) {
        TopologyInfo.Builder builder = TopologyInfo.newBuilder();
        try {
            JsonFormat.parser().merge(getInputReader(fileBasename), builder);
        } catch (IOException ioe) {
            throw new IllegalArgumentException("Bad input JSON file " + fileBasename, ioe);
        }
        return builder.build();
    }

    /**
     * Protobuf message JSON file reader helper.
     *
     * @param fileBasename Base filename of JSON file.
     * @return Reader to pass to JsonFormat.
     * @throws IOException Thrown on file read error.
     */
    private static InputStreamReader getInputReader(@Nonnull final String fileBasename)
            throws IOException {
        String fileName = "protobuf/messages/" + fileBasename;
        URL fileUrl = TopologyEntityUtils.class.getClassLoader().getResource(fileName);
        if (fileUrl == null) {
            throw new IOException("Could not locate file: " + fileName);
        }
        return new InputStreamReader(fileUrl.openStream());
    }
}
