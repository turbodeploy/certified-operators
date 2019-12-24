package com.vmturbo.topology.processor.history;

import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.mockito.Mockito;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.UtilizationData;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;

/**
 * Base functionality for mocking topology graph for historical editing tests.
 */
public abstract class BaseGraphRelatedTest {
    /**
     * Mock the topology graph from the entities. Only the entity retrieval.
     *
     * @param entities collection of entities
     * @return mock of the graph
     */
    @Nonnull
    protected static TopologyGraph<TopologyEntity> mockGraph(@Nonnull Set<TopologyEntity> entities) {
        @SuppressWarnings("unchecked")
        TopologyGraph<TopologyEntity> graph = Mockito.mock(TopologyGraph.class);
        Mockito.when(graph.entities()).thenReturn(entities.stream());
        Mockito.when(graph.size()).thenReturn(entities.size());
        for (TopologyEntity entity : entities) {
            Mockito.when(graph.getEntity(entity.getOid())).thenReturn(Optional.of(entity));
        }
        return graph;
    }

    /**
     * Mock an entity with one sold and optional 1 bought commodity.
     *
     * @param type entity type
     * @param oid entity oid
     * @param ctSold sold commodity type
     * @param capacitySold sold capacity
     * @param usedSold sold usage
     * @param provider bought commodity provider oid
     * @param ctBought bought commodity type
     * @param usedBought bought usage
     * @param utilizationData sold commodity utilization data
     * @param resizable whether sold commodity is resizable
     * @return mocked entity
     */
    @Nonnull
    protected static TopologyEntity mockEntity(int type, long oid, @Nonnull CommodityType ctSold,
                    double capacitySold, double usedSold, @Nullable Long provider, @Nullable CommodityType ctBought,
                    @Nullable Double usedBought, @Nullable UtilizationData utilizationData,
                    boolean resizable) {
        TopologyEntity e = Mockito.mock(TopologyEntity.class);
        Mockito.when(e.getEntityType()).thenReturn(type);
        Mockito.when(e.getOid()).thenReturn(oid);
        TopologyEntityDTO.Builder entityBuilder = TopologyEntityDTO.newBuilder();
        entityBuilder.setOid(oid).setEntityType(type);
        if (ctSold != null) {
            CommoditySoldDTO.Builder commSold =
                            entityBuilder.addCommoditySoldListBuilder().setCommodityType(ctSold)
                                            .setUsed(usedSold).setCapacity(capacitySold)
                                            .setIsResizeable(resizable);
            if (utilizationData != null) {
                commSold.setUtilizationData(utilizationData);
            }
        }
        if (provider != null) {
            entityBuilder.addCommoditiesBoughtFromProvidersBuilder().setProviderId(provider)
                            .addCommodityBoughtBuilder().setCommodityType(ctBought)
                            .setUsed(usedBought);
        }
        Mockito.when(e.getTopologyEntityDtoBuilder()).thenReturn(entityBuilder);
        return e;
    }
}
