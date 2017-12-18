package com.vmturbo.topology.processor.topology;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CommodityBought;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;

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
        return topologyEntityBuilder(TopologyEntityDTO.newBuilder()
            .setOid(oid)
            .setEntityType(entityType.getNumber())
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
    public static TopologyGraph topologyGraphOf(@Nonnull final TopologyEntity.Builder... entityBuilders) {
        return TopologyGraph.newGraph(Stream.of(entityBuilders)
            .collect(Collectors.toMap(TopologyEntity.Builder::getOid, Function.identity())));
    }
}
