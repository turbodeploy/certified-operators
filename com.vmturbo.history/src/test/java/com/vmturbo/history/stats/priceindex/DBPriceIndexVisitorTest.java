package com.vmturbo.history.stats.priceindex;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.jooq.InsertSetMoreStep;
import org.jooq.Table;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.history.db.BasedbIO.Style;
import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.db.VmtDbException;
import com.vmturbo.history.schema.RelationType;
import com.vmturbo.history.stats.MarketStatsAccumulator.MarketStatsData;
import com.vmturbo.history.stats.priceindex.DBPriceIndexVisitor.DBPriceIndexVisitorFactory;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

public class DBPriceIndexVisitorTest {

    private HistorydbIO historydbIO = mock(HistorydbIO.class);

    private static final TopologyInfo TOPOLOGY_INFO = TopologyInfo.newBuilder()
        .setTopologyContextId(777777)
        .setTopologyId(123)
        .setCreationTime(1000000)
        .build();

    private final int VM_TYPE = EntityType.VIRTUAL_MACHINE_VALUE;

    private final Table<?> VM_TABLE = com.vmturbo.history.db.EntityType.VIRTUAL_MACHINE.getLatestTable();

    @Before
    public void setup() {
        when(historydbIO.getEntityType(VM_TYPE))
            .thenReturn(Optional.of(com.vmturbo.history.db.EntityType.VIRTUAL_MACHINE));
    }

    @Test
    public void testPersistChunks() throws VmtDbException {
        final int chunkSize = 1;
        final DBPriceIndexVisitor visitor = new DBPriceIndexVisitorFactory(historydbIO, chunkSize)
            .newVisitor(TOPOLOGY_INFO);

        final long entityId = 121;
        final double priceIdx = 7;

        final InsertSetMoreStep insert = mock(InsertSetMoreStep.class);
        when(historydbIO.getCommodityInsertStatement(VM_TABLE)).thenReturn(insert);

        visitor.visit(VM_TYPE, EnvironmentType.ON_PREM, ImmutableMap.of(entityId, priceIdx));

        verify(historydbIO).initializeCommodityInsert(StringConstants.PRICE_INDEX,
            TOPOLOGY_INFO.getCreationTime(),
            entityId, RelationType.METRICS,
            null, null, null, null, insert,
            VM_TABLE);
        verify(historydbIO).setCommodityValues(StringConstants.PRICE_INDEX, priceIdx, 0,
            insert, VM_TABLE);
        verify(historydbIO).execute(Style.FORCED, Collections.singletonList(insert));

        visitor.onComplete();
        // No more inserts.
        verify(historydbIO, times(0)).execute(anyString(), any());
    }

    @Test
    public void testPersistChunksLeftOver() throws VmtDbException {
        // In this test, the chunk size is 2.
        final int chunkSize = 2;
        final DBPriceIndexVisitor visitor = new DBPriceIndexVisitorFactory(historydbIO, chunkSize)
            .newVisitor(TOPOLOGY_INFO);

        final long entityId = 121;
        final double priceIdx = 7;

        final InsertSetMoreStep insert = mock(InsertSetMoreStep.class);
        when(historydbIO.getCommodityInsertStatement(VM_TABLE)).thenReturn(insert);

        visitor.visit(VM_TYPE, EnvironmentType.ON_PREM, ImmutableMap.of(entityId, priceIdx));

        verify(historydbIO).initializeCommodityInsert(StringConstants.PRICE_INDEX,
            TOPOLOGY_INFO.getCreationTime(),
            entityId, RelationType.METRICS,
            null, null, null, null, insert,
            VM_TABLE);
        verify(historydbIO).setCommodityValues(StringConstants.PRICE_INDEX, priceIdx, 0,
            insert, VM_TABLE);
        // Chunk size is 2, and we only add one record. So it should only get inserted after
        // onComplete.
        verify(historydbIO, times(0)).execute(anyString(), any());

        visitor.onComplete();

        verify(historydbIO).execute(Style.FORCED, Collections.singletonList(insert));
    }

    @Test
    public void testPersistAggregateMixedEnvTypes() throws VmtDbException {
        final int chunkSize = 2;
        final DBPriceIndexVisitor visitor = new DBPriceIndexVisitorFactory(historydbIO, chunkSize)
            .newVisitor(TOPOLOGY_INFO);

        final long entityId1 = 121;
        final long entityId2 = 122;
        final long cloudEntityId = 123;
        final double priceIndex1 = 1;
        final double priceIndex2 = 3;
        final double cloudPriceIndex = 4;

        // Return a mock insert statement just to avoid NPEs.
        // We won't be verifying the commodity inserts here.
        final InsertSetMoreStep insert = mock(InsertSetMoreStep.class);
        when(historydbIO.getCommodityInsertStatement(VM_TABLE)).thenReturn(insert);

        visitor.visit(VM_TYPE, EnvironmentType.ON_PREM, ImmutableMap.of(
                entityId1, priceIndex1,
                entityId2, priceIndex2));
        visitor.visit(VM_TYPE, EnvironmentType.CLOUD, ImmutableMap.of(
            cloudEntityId, cloudPriceIndex));


        final InsertSetMoreStep insertStatement = mock(InsertSetMoreStep.class);
        when(historydbIO.getMarketStatsInsertStmt(any(), any())).thenReturn(insertStatement);

        visitor.onComplete();

        final ArgumentCaptor<MarketStatsData> aggregateDataCaptor =
            ArgumentCaptor.forClass(MarketStatsData.class);

        verify(historydbIO, times(2)).getMarketStatsInsertStmt(aggregateDataCaptor.capture(),
            eq(TOPOLOGY_INFO));
        // We always return the same insert statement from the mock,
        // so it should just be inserted twice.
        verify(historydbIO).execute(Style.FORCED, Arrays.asList(insertStatement, insertStatement));

        final Map<EnvironmentType, MarketStatsData> mktStatsDataByEnvType =
            aggregateDataCaptor.getAllValues().stream()
            .collect(Collectors.toMap(MarketStatsData::getEnvironmentType, Function.identity()));

        assertThat(mktStatsDataByEnvType.keySet(),
            containsInAnyOrder(EnvironmentType.ON_PREM, EnvironmentType.CLOUD));

        final MarketStatsData onPremMktStats = mktStatsDataByEnvType.get(EnvironmentType.ON_PREM);
        assertThat(onPremMktStats.getPropertySubtype(), is(StringConstants.PRICE_INDEX));
        assertThat(onPremMktStats.getPropertyType(), is(StringConstants.PRICE_INDEX));
        assertThat(onPremMktStats.getEntityType(), is("VirtualMachine"));
        assertThat(onPremMktStats.getEnvironmentType(), is(EnvironmentType.ON_PREM));
        assertThat(onPremMktStats.getCapacity(), is((priceIndex1 + priceIndex2) / 2));
        assertThat(onPremMktStats.getMax(), is(priceIndex2));
        assertThat(onPremMktStats.getMin(), is(priceIndex1));
        assertThat(onPremMktStats.getUsed(), is((priceIndex1 + priceIndex2) / 2));
        assertThat(onPremMktStats.getRelationType(), is(RelationType.METRICS));

        final MarketStatsData cloudMktStats = mktStatsDataByEnvType.get(EnvironmentType.CLOUD);
        assertThat(cloudMktStats.getPropertySubtype(), is(StringConstants.PRICE_INDEX));
        assertThat(cloudMktStats.getPropertyType(), is(StringConstants.PRICE_INDEX));
        assertThat(cloudMktStats.getEntityType(), is("VirtualMachine"));
        assertThat(cloudMktStats.getEnvironmentType(), is(EnvironmentType.CLOUD));
        assertThat(cloudMktStats.getCapacity(), is(cloudPriceIndex));
        assertThat(cloudMktStats.getMax(), is(cloudPriceIndex));
        assertThat(cloudMktStats.getMin(), is(cloudPriceIndex));
        assertThat(cloudMktStats.getUsed(), is(cloudPriceIndex));
        assertThat(cloudMktStats.getRelationType(), is(RelationType.METRICS));
    }

}