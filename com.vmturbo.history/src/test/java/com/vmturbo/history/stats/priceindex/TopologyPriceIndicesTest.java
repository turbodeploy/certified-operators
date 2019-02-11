package com.vmturbo.history.stats.priceindex;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopology.Start.SkippedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.history.db.VmtDbException;
import com.vmturbo.history.utils.HistoryStatsUtils;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

public class TopologyPriceIndicesTest {

    private static final TopologyInfo TOPOLOGY_INFO = TopologyInfo.newBuilder()
        .setTopologyContextId(777777)
        .setTopologyId(123)
        .setCreationTime(1000000)
        .build();

    @Test
    public void testVisitProjectedEntities() throws VmtDbException {
        final TopologyPriceIndices priceIndices = TopologyPriceIndices.builder(TOPOLOGY_INFO)
            .addEntity(ProjectedTopologyEntity.newBuilder()
                .setOriginalPriceIndex(8)
                .setEntity(TopologyEntityDTO.newBuilder()
                    .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                    .setOid(7L)
                    .setEnvironmentType(EnvironmentType.ON_PREM))
                .build())
            .addEntity(ProjectedTopologyEntity.newBuilder()
                .setOriginalPriceIndex(88)
                .setEntity(TopologyEntityDTO.newBuilder()
                    .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                    .setOid(77L)
                    .setEnvironmentType(EnvironmentType.CLOUD))
                .build())
            .addEntity(ProjectedTopologyEntity.newBuilder()
                .setOriginalPriceIndex(888)
                .setEntity(TopologyEntityDTO.newBuilder()
                    .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
                    .setOid(777L)
                    .setEnvironmentType(EnvironmentType.ON_PREM))
                .build())
            .build();

        final TopologyPriceIndexVisitor visitor = mock(TopologyPriceIndexVisitor.class);
        priceIndices.visit(visitor);

        // Verify that onComplete gets called after visits.
        final InOrder inOrder = Mockito.inOrder(visitor);
        inOrder.verify(visitor, times(3)).visit(any(), any(), any());
        inOrder.verify(visitor).onComplete();

        verify(visitor).visit(EntityType.VIRTUAL_MACHINE_VALUE,
            EnvironmentType.ON_PREM,
            ImmutableMap.of(7L, 8.0));
        verify(visitor).visit(EntityType.VIRTUAL_MACHINE_VALUE,
            EnvironmentType.CLOUD,
            ImmutableMap.of(77L, 88.0));
        verify(visitor).visit(EntityType.PHYSICAL_MACHINE_VALUE,
            EnvironmentType.ON_PREM,
            ImmutableMap.of(777L, 888.0));
    }

    @Test
    public void testVisitSkippedEntities() throws VmtDbException {
        final TopologyPriceIndices priceIndices = TopologyPriceIndices.builder(TOPOLOGY_INFO)
            .addSkippedEntity(SkippedEntity.newBuilder()
                .setOid(7)
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .setEnvironmentType(EnvironmentType.ON_PREM)
                .build())
            .addSkippedEntity(SkippedEntity.newBuilder()
                .setOid(77)
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .setEnvironmentType(EnvironmentType.CLOUD)
                .build())
            .addSkippedEntity(SkippedEntity.newBuilder()
                .setOid(777)
                .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
                .setEnvironmentType(EnvironmentType.ON_PREM)
                .build())
            .build();

        final TopologyPriceIndexVisitor visitor = mock(TopologyPriceIndexVisitor.class);
        priceIndices.visit(visitor);

        verify(visitor).visit(EntityType.VIRTUAL_MACHINE_VALUE,
            EnvironmentType.ON_PREM,
            ImmutableMap.of(7L, HistoryStatsUtils.DEFAULT_PRICE_IDX));
        verify(visitor).visit(EntityType.VIRTUAL_MACHINE_VALUE,
            EnvironmentType.CLOUD,
            ImmutableMap.of(77L, HistoryStatsUtils.DEFAULT_PRICE_IDX));
        verify(visitor).visit(EntityType.PHYSICAL_MACHINE_VALUE,
            EnvironmentType.ON_PREM,
            ImmutableMap.of(777L, HistoryStatsUtils.DEFAULT_PRICE_IDX));
    }

    @Test
    public void testVisitProjectedEntityUnsetPriceIdx() throws VmtDbException {
        final TopologyPriceIndices priceIndices = TopologyPriceIndices.builder(TOPOLOGY_INFO)
            .addEntity(ProjectedTopologyEntity.newBuilder()
                .setEntity(TopologyEntityDTO.newBuilder()
                    .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                    .setOid(7L)
                    .setEnvironmentType(EnvironmentType.ON_PREM))
                .build())
            .build();

        final TopologyPriceIndexVisitor visitor = mock(TopologyPriceIndexVisitor.class);
        priceIndices.visit(visitor);

        verify(visitor).visit(EntityType.VIRTUAL_MACHINE_VALUE,
            EnvironmentType.ON_PREM,
            ImmutableMap.of(7L, HistoryStatsUtils.DEFAULT_PRICE_IDX));
    }
}