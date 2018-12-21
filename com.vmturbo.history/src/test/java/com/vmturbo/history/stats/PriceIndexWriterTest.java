package com.vmturbo.history.stats;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;

import org.jooq.InsertSetMoreStep;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mockito;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;

import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopology.Start.SkippedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.history.db.BasedbIO.Style;
import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.db.VmtDbException;
import com.vmturbo.history.schema.RelationType;
import com.vmturbo.history.stats.MarketStatsAccumulator.MarketStatsData;
import com.vmturbo.history.utils.HistoryStatsUtils;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Unit tests for {@link PriceIndexWriter}.
 */
public class PriceIndexWriterTest {

    private HistorydbIO historydbIO = mock(HistorydbIO.class);

    private static final int WRITE_TOPOLOGY_CHUNK_SIZE = 1;

    private static final TopologyInfo TOPOLOGY_INFO = TopologyInfo.newBuilder()
        .setTopologyContextId(777777)
        .setTopologyId(123)
        .setCreationTime(1000000)
        .build();

    private org.jooq.Table<?> vmTable =
        com.vmturbo.history.db.EntityType.VIRTUAL_MACHINE.getLatestTable();

    @Before
    public void setup() {
        when(historydbIO.getEntityType(EntityType.VIRTUAL_MACHINE_VALUE))
            .thenReturn(Optional.of(com.vmturbo.history.db.EntityType.VIRTUAL_MACHINE));
    }

    @Test
    public void testPersistEntityPriceIndex() throws VmtDbException {
        final PriceIndexWriter priceIndexWriter =
            new PriceIndexWriter(historydbIO, WRITE_TOPOLOGY_CHUNK_SIZE);
        final long entityId = 7;
        final double priceIndex = 12345;
        final Table<Integer, Long, Double> priceIndices = HashBasedTable.create();
        priceIndices.put(EntityType.VIRTUAL_MACHINE_VALUE, entityId, priceIndex);

        final InsertSetMoreStep insertStatement = mock(InsertSetMoreStep.class);
        when(historydbIO.getCommodityInsertStatement(vmTable))
            .thenReturn(insertStatement);
        priceIndexWriter.persistPriceIndexInfo(TOPOLOGY_INFO, priceIndices, Collections.emptySet());

        final InOrder inOrder = Mockito.inOrder(historydbIO);
        inOrder.verify(historydbIO).initializeCommodityInsert(StringConstants.PRICE_INDEX,
            TOPOLOGY_INFO.getCreationTime(),
            entityId,
            RelationType.METRICS,
            null, null, null, null, insertStatement,
            vmTable);
        inOrder.verify(historydbIO).setCommodityValues(
            StringConstants.PRICE_INDEX, priceIndex, insertStatement, vmTable);
        inOrder.verify(historydbIO).execute(Style.FORCED, Collections.singletonList(insertStatement));
    }

    @Test
    public void testPersistEntityPriceIndexChunks() throws VmtDbException {
        final PriceIndexWriter priceIndexWriter =
            new PriceIndexWriter(historydbIO, 2);
        final Table<Integer, Long, Double> priceIndices = HashBasedTable.create();
        priceIndices.put(EntityType.VIRTUAL_MACHINE_VALUE, 1L, 1.0);
        priceIndices.put(EntityType.VIRTUAL_MACHINE_VALUE, 2L, 2.0);
        priceIndices.put(EntityType.VIRTUAL_MACHINE_VALUE, 3L, 3.0);

        final InsertSetMoreStep insertStatement1 = mock(InsertSetMoreStep.class);
        final InsertSetMoreStep insertStatement2 = mock(InsertSetMoreStep.class);
        final InsertSetMoreStep insertStatement3 = mock(InsertSetMoreStep.class);
        when(historydbIO.getCommodityInsertStatement(vmTable))
            .thenReturn(insertStatement1, insertStatement2, insertStatement3);

        priceIndexWriter.persistPriceIndexInfo(TOPOLOGY_INFO, priceIndices, Collections.emptySet());

        InOrder inOrder = Mockito.inOrder(historydbIO);
        inOrder.verify(historydbIO).execute(Style.FORCED, Arrays.asList(insertStatement1, insertStatement2));
        inOrder.verify(historydbIO).execute(Style.FORCED, Arrays.asList(insertStatement3));
    }

    @Test
    public void testPersistSkippedEntityPriceIndex() throws VmtDbException {
        final PriceIndexWriter priceIndexWriter =
            new PriceIndexWriter(historydbIO, WRITE_TOPOLOGY_CHUNK_SIZE);
        final long entityId = 7;

        final InsertSetMoreStep insertStatement = mock(InsertSetMoreStep.class);
        when(historydbIO.getCommodityInsertStatement(vmTable))
            .thenReturn(insertStatement);
        priceIndexWriter.persistPriceIndexInfo(TOPOLOGY_INFO, HashBasedTable.create(),
            Collections.singleton(SkippedEntity.newBuilder()
                .setOid(entityId)
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .build()));

        // Similar verification to regular price index test.
        final InOrder inOrder = Mockito.inOrder(historydbIO);
        // Make sure we initialize a commodity insert for the entity.
        inOrder.verify(historydbIO).initializeCommodityInsert(StringConstants.PRICE_INDEX,
            TOPOLOGY_INFO.getCreationTime(),
            entityId,
            RelationType.METRICS,
            null, null, null, null, insertStatement,
            vmTable);
        // Make sure that we set the price index commodity to the default price index.
        inOrder.verify(historydbIO).setCommodityValues(
            StringConstants.PRICE_INDEX, HistoryStatsUtils.DEFAULT_PRICE_IDX, insertStatement, vmTable);
        // Make sure we execute the insert statement.
        inOrder.verify(historydbIO).execute(Style.FORCED, Collections.singletonList(insertStatement));
    }

    @Test
    public void testPersistAggregatePriceIndex() throws VmtDbException {
        final PriceIndexWriter priceIndexWriter =
            new PriceIndexWriter(historydbIO, WRITE_TOPOLOGY_CHUNK_SIZE);
        final double priceIndex1 = 1;
        final double priceIndex2 = 3;
        final Table<Integer, Long, Double> priceIndices = HashBasedTable.create();
        priceIndices.put(EntityType.VIRTUAL_MACHINE_VALUE, 7L, priceIndex1);
        priceIndices.put(EntityType.VIRTUAL_MACHINE_VALUE, 77L, priceIndex2);

        final InsertSetMoreStep insertStatement = mock(InsertSetMoreStep.class);
        when(historydbIO.getMarketStatsInsertStmt(any(), any())).thenReturn(insertStatement);

        priceIndexWriter.persistPriceIndexInfo(TOPOLOGY_INFO, priceIndices, Collections.emptySet());

        final ArgumentCaptor<MarketStatsData> aggregateDataCaptor =
            ArgumentCaptor.forClass(MarketStatsData.class);

        verify(historydbIO).getMarketStatsInsertStmt(aggregateDataCaptor.capture(),
            eq(TOPOLOGY_INFO));
        verify(historydbIO).execute(Style.FORCED, Collections.singletonList(insertStatement));

        final MarketStatsData mktStatsData = aggregateDataCaptor.getValue();
        assertThat(mktStatsData.getPropertySubtype(), is(StringConstants.PRICE_INDEX));
        assertThat(mktStatsData.getPropertyType(), is(StringConstants.PRICE_INDEX));
        assertThat(mktStatsData.getEntityType(), is("VirtualMachine"));
        assertThat(mktStatsData.getCapacity(), is((priceIndex1 + priceIndex2) / 2));
        assertThat(mktStatsData.getMax(), is(priceIndex2));
        assertThat(mktStatsData.getMin(), is(priceIndex1));
        assertThat(mktStatsData.getUsed(), is((priceIndex1 + priceIndex2) / 2));
        assertThat(mktStatsData.getRelationType(), is(RelationType.METRICS));
    }

    @Test
    public void testPersistAggregatePriceIndexSkippedEntities() throws VmtDbException {
        final PriceIndexWriter priceIndexWriter =
            new PriceIndexWriter(historydbIO, WRITE_TOPOLOGY_CHUNK_SIZE);
        final double priceIndex1 = 3;
        final Table<Integer, Long, Double> priceIndices = HashBasedTable.create();
        priceIndices.put(EntityType.VIRTUAL_MACHINE_VALUE, 7L, priceIndex1);

        final InsertSetMoreStep insertStatement = mock(InsertSetMoreStep.class);
        when(historydbIO.getMarketStatsInsertStmt(any(), any())).thenReturn(insertStatement);

        priceIndexWriter.persistPriceIndexInfo(TOPOLOGY_INFO, priceIndices,
            // A single skipped entity, which should get the default price idx.
            Collections.singleton(SkippedEntity.newBuilder()
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .setOid(77L)
                .build()));

        final ArgumentCaptor<MarketStatsData> aggregateDataCaptor =
            ArgumentCaptor.forClass(MarketStatsData.class);

        verify(historydbIO).getMarketStatsInsertStmt(aggregateDataCaptor.capture(),
            eq(TOPOLOGY_INFO));
        verify(historydbIO).execute(Style.FORCED, Collections.singletonList(insertStatement));

        final MarketStatsData mktStatsData = aggregateDataCaptor.getValue();
        assertThat(mktStatsData.getPropertySubtype(), is(StringConstants.PRICE_INDEX));
        assertThat(mktStatsData.getPropertyType(), is(StringConstants.PRICE_INDEX));
        assertThat(mktStatsData.getEntityType(), is("VirtualMachine"));
        assertThat(mktStatsData.getCapacity(), is((HistoryStatsUtils.DEFAULT_PRICE_IDX + priceIndex1) / 2));
        assertThat(mktStatsData.getMax(), is(priceIndex1));
        assertThat(mktStatsData.getMin(), is(HistoryStatsUtils.DEFAULT_PRICE_IDX));
        assertThat(mktStatsData.getUsed(), is((HistoryStatsUtils.DEFAULT_PRICE_IDX + priceIndex1) / 2));
        assertThat(mktStatsData.getRelationType(), is(RelationType.METRICS));
    }
}
