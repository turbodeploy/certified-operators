package com.vmturbo.history.stats.priceindex;

import static com.vmturbo.common.protobuf.utils.StringConstants.BUSINESS_ACCOUNT;
import static com.vmturbo.common.protobuf.utils.StringConstants.VIRTUAL_MACHINE;
import static com.vmturbo.history.schema.abstraction.Tables.VM_STATS_LATEST;
import static junit.framework.TestCase.assertEquals;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.MockitoAnnotations;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.history.db.EntityType;
import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.db.bulk.BulkLoaderMock;
import com.vmturbo.history.db.bulk.DbMock;
import com.vmturbo.history.db.bulk.SimpleBulkLoaderFactory;
import com.vmturbo.history.schema.RelationType;
import com.vmturbo.history.schema.abstraction.tables.records.VmStatsLatestRecord;
import com.vmturbo.history.stats.MarketStatsAccumulatorImpl.MarketStatsData;
import com.vmturbo.history.stats.priceindex.DBPriceIndexVisitor.DBPriceIndexVisitorFactory;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;

/**
 * Tests for {@link DBPriceIndexVisitor} class.
 */
public class DBPriceIndexVisitorTest {

    private static final EntityType VIRTUAL_MACHINE_ENTITY_TYPE = EntityType.named(VIRTUAL_MACHINE).get();
    private static final EntityType BUSINESS_ACCOUNT_ENTITY_TYPE = EntityType.named(BUSINESS_ACCOUNT).get();
    private HistorydbIO historydbIO = mock(HistorydbIO.class);
    private DbMock dbMock = new DbMock();
    private SimpleBulkLoaderFactory loaders = new BulkLoaderMock(dbMock).getFactory();

    private static final TopologyInfo TOPOLOGY_INFO = TopologyInfo.newBuilder()
            .setTopologyContextId(777777)
            .setTopologyId(123)
            .setCreationTime(1000000)
            .build();

    private static final int VM_TYPE = EntityDTO.EntityType.VIRTUAL_MACHINE_VALUE;
    private static final int BA_TYPE = EntityDTO.EntityType.BUSINESS_ACCOUNT_VALUE;

    @Captor
    private ArgumentCaptor<MarketStatsData> aggregateDataCaptor;

    /**
     * Create and configure mocks required for tests.
     */
    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        when(historydbIO.getEntityType(VM_TYPE))
                .thenReturn(Optional.of(VIRTUAL_MACHINE_ENTITY_TYPE));
        when(historydbIO.getEntityType(BA_TYPE))
                .thenReturn(Optional.of(BUSINESS_ACCOUNT_ENTITY_TYPE));
        when(historydbIO.getMarketStatsRecord(any(MarketStatsData.class), any(TopologyInfo.class)))
                .thenCallRealMethod();
    }

    /**
     * Test that data is properly persisted.
     *
     * @throws InterruptedException if interrupted
     */
    @Test
    public void testPersistChunks() throws InterruptedException {
        final DBPriceIndexVisitor visitor = new DBPriceIndexVisitorFactory(historydbIO)
                .newVisitor(TOPOLOGY_INFO, loaders);

        final long entityId = 121;
        final double priceIdx = 7;

        visitor.visit(VM_TYPE, EnvironmentType.ON_PREM, ImmutableMap.of(entityId, priceIdx));
        visitor.onComplete();
        final Collection<VmStatsLatestRecord> records = dbMock.getRecords(VM_STATS_LATEST);
        assertEquals(1, records.size());
    }

    /**
     * Test that records are properly persisted with mixed environment types.
     *
     * @throws InterruptedException if interrupted
     */
    @Test
    public void testPersistAggregateMixedEnvTypes() throws InterruptedException {
        final DBPriceIndexVisitor visitor = new DBPriceIndexVisitorFactory(historydbIO)
                .newVisitor(TOPOLOGY_INFO, loaders);

        final long entityId1 = 121;
        final long entityId2 = 122;
        final long cloudEntityId = 123;
        final double priceIndex1 = 1;
        final double priceIndex2 = 3;
        final double cloudPriceIndex = 4;

        visitor.visit(VM_TYPE, EnvironmentType.ON_PREM, ImmutableMap.of(
                entityId1, priceIndex1,
                entityId2, priceIndex2));
        visitor.visit(VM_TYPE, EnvironmentType.CLOUD, ImmutableMap.of(
                cloudEntityId, cloudPriceIndex));

        visitor.onComplete();

        final Collection<VmStatsLatestRecord> records = dbMock.getRecords(VM_STATS_LATEST);

        verify(historydbIO, times(2)).getMarketStatsRecord(aggregateDataCaptor.capture(), any());
        final Map<EnvironmentType, MarketStatsData> mktStatsDataByEnvType =
                Maps.uniqueIndex(aggregateDataCaptor.getAllValues(), MarketStatsData::getEnvironmentType);

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
