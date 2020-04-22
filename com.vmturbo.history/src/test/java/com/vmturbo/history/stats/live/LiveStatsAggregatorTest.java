package com.vmturbo.history.stats.live;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyDouble;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.base.Functions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.jooq.Record;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.db.VmtDbException;
import com.vmturbo.history.db.bulk.BulkLoaderMock;
import com.vmturbo.history.db.bulk.DbMock;
import com.vmturbo.history.db.bulk.SimpleBulkLoaderFactory;
import com.vmturbo.history.schema.abstraction.Tables;
import com.vmturbo.history.schema.abstraction.tables.records.PmStatsLatestRecord;
import com.vmturbo.history.schema.abstraction.tables.records.VmStatsLatestRecord;
import com.vmturbo.history.stats.MarketStatsAccumulatorImpl.MarketStatsData;
import com.vmturbo.history.stats.PropertySubType;
import com.vmturbo.history.stats.StatsTestUtils;

/**
 * Unit tests for {@link LiveStatsAggregator}.
 */
public class LiveStatsAggregatorTest {

    private static final String PRODUCES = PropertySubType.Produces.getApiParameterName();
    private static LiveStatsAggregator aggregator;
    private static Record record;
    private static HistorydbIO historydbIO;

    // mocks for DB and bulk loaders that keep track of inserted record objects for later verification
    private DbMock dbMock = new DbMock();
    private SimpleBulkLoaderFactory loaders = new BulkLoaderMock(dbMock).getFactory();

    /**
     * Set up a a HistorydbIO partial mock for use in tests.
     *
     * @throws VmtDbException       should not happen in these tests
     * @throws InterruptedException if interrupted
     */
    @Before
    public void before() throws VmtDbException, InterruptedException {
        historydbIO = Mockito.mock(HistorydbIO.class);
        // we need these methods to work normally in order to construct records to be inserted
        Mockito.doCallRealMethod().when(historydbIO).initializeCommodityRecord(any(), anyLong(), anyLong(), any(), any(), any(), any(), any(), any(), any(), any());
        Mockito.doCallRealMethod().when(historydbIO).setCommodityValues(any(), anyDouble(), anyDouble(), any(), any());
        Mockito.doCallRealMethod().when(historydbIO).getMarketStatsRecord(any(MarketStatsData.class), any(TopologyInfo.class));
        Mockito.doCallRealMethod().when(historydbIO).clipValue(anyDouble());
        Mockito.when(historydbIO.getEntityType(anyInt())).thenCallRealMethod();
        Mockito.when(historydbIO.getBaseEntityType(anyInt())).thenCallRealMethod();
    }

    /**
     * Create and process a small sample topology for use in tests.
     *
     * <p>This method also contains assertions that check that the expected number of pending bought
     * commodities are present after each chunk is processed.</p>
     *
     * @throws InterruptedException if interrupted
     * @throws VmtDbException       on db exceptionn
     */
    private void setupTopologyAndTestPendingBoughtCommodities() throws InterruptedException, VmtDbException {
        final TopologyEntityDTO vm1 = StatsTestUtils.vm(10, 30); // buys from pm1
        final TopologyEntityDTO vm2 = StatsTestUtils.vm(20, 40); // buys from pm2
        final TopologyEntityDTO vm3 = StatsTestUtils.vm(25, 50, 100, 100); // buys flow from pm3
        final TopologyEntityDTO pm1 = StatsTestUtils.pm(30, 10);
        final TopologyEntityDTO pm2 = StatsTestUtils.pm(40, 10);
        final TopologyEntityDTO pm3 = StatsTestUtils.pm(50, 1000, 1000);
        final TopologyEntityDTO pm4 = StatsTestUtils.pm(55, 10);

        ImmutableSet<String> exclude =
                ImmutableSet.copyOf(
                        Arrays.asList("Application CLUSTER DATACENTER DATASTORE DSPMAccess NETWORK"
                                .toLowerCase().split(" ")));

        final TopologyInfo topologyInfo = TopologyInfo.newBuilder()
                .setTopologyContextId(123456)
                .setTopologyId(55555)
                .build();

        // now simulate processing a topology
        aggregator = new LiveStatsAggregator(historydbIO, topologyInfo, exclude, loaders);

        // chunk #1 includes a forward reference: vm processed before the pm it is buying from
        Map<Long, TopologyEntityDTO> entityByOid = ImmutableMap.of(
                vm1.getOid(), vm1,
                pm1.getOid(), pm1
        );
        aggregator.aggregateEntity(vm1, entityByOid);
        aggregator.aggregateEntity(pm1, entityByOid);
        assertEquals(0, aggregator.numPendingBought());

        // chunk #2 includes an unsatisfied forward reference: vm2 buys from pm2
        entityByOid = ImmutableMap.of(
                vm2.getOid(), vm2,
                pm3.getOid(), pm3
        );
        aggregator.aggregateEntity(vm2, entityByOid);
        aggregator.aggregateEntity(pm3, entityByOid);
        // we should see one pending bought commodities entry for vm2 buying from pm2
        assertEquals(1, aggregator.numPendingBought());

        // chunk #3 satisfies our pending commodity and also throws in some
        // flow commodity trades that are differentiated by commodity key
        // (vm3 buying both flow-0 and flow-1 commodities from pm4)
        entityByOid = ImmutableMap.of(
                pm2.getOid(), pm2,
                pm4.getOid(), pm4,
                vm3.getOid(), vm3);
        aggregator.aggregateEntity(pm2, entityByOid);
        aggregator.aggregateEntity(pm4, entityByOid);
        aggregator.aggregateEntity(vm3, entityByOid);
        // no more pending commodities
        assertEquals(0, aggregator.numPendingBought());

        aggregator.writeFinalStats();

    }

    /**
     * Verify counts of various subsets of the records inserted during topology processing.
     *
     * @throws VmtDbException       should not happen
     * @throws InterruptedException if interrupted
     */
    @Test
    public void testRecords() throws InterruptedException, VmtDbException {
        setupTopologyAndTestPendingBoughtCommodities();
        // totoal records inserted
        // 4 PM attribute + 3 PM cpu sold + 2 PM flow sold = 9
        // 3 VM attribute + 2 VM cpu bought + 2 VM flow bought = 7
        // 2 entities * 3 commodities each = 6 market stats records
        assertEquals(20, (long)dbMock.getTables().stream()
                .map(dbMock::getRecords)
                .collect(Collectors.summingInt(Collection::size)));
        // "Produces" attribute records, recording # of sold commodities
        // 3 PMs sell one commodity each, 1 PM sells 2
        assertEquals(5.0, (double)dbMock.getRecords(Tables.PM_STATS_LATEST).stream()
                        .filter(r -> r.getPropertyType().equalsIgnoreCase(PRODUCES))
                        .collect(Collectors.summingDouble(PmStatsLatestRecord::getAvgValue)),
                0.0);
        // no VMs sell any commodities
        assertEquals(0, (double)dbMock.getRecords(Tables.VM_STATS_LATEST).stream()
                        .filter(r -> r.getPropertyType().equalsIgnoreCase(PRODUCES))
                        .collect(Collectors.summingDouble(VmStatsLatestRecord::getAvgValue)),
                0.0);
        // check flow capacities are correctly matched based on commodity key
        // flow-0 capacities should all be Float.MAX_VALUE
        dbMock.getRecords(Tables.PM_STATS_LATEST).stream()
                .filter(r -> r.getPropertySubtype().equalsIgnoreCase("Flow-1"))
                .forEach(r -> assertEquals((double)Float.MAX_VALUE, (double)r.getAvgValue(), 0.0));
        // flow-1 capacities should all be 100 million
        dbMock.getRecords(Tables.PM_STATS_LATEST).stream()
                .filter(r -> r.getPropertySubtype().equalsIgnoreCase("Flow-0"))
                .forEach(r -> assertEquals((double)100_000_000, (double)r.getAvgValue(), 0.0));
    }

    /**
     * Verify properties of the capacity cache.
     *
     * @throws VmtDbException       on db error
     * @throws InterruptedException if interrupted
     */
    @Test
    public void testCapacities() throws VmtDbException, InterruptedException {
        setupTopologyAndTestPendingBoughtCommodities();
        Object[] capacities = aggregator.capacities().getAllEntityCapacities().toArray();
        // 4 entities with commodities sold (the 4 PMs), each should have cached capacities
        assertEquals(4, capacities.length);
        // Verify that the sold commodities maps are being reused. The three cpu-selling PMs
        // should all share a single map, and the flow-selling PM should have its own.
        // We compute share counts and then make sure we have a 3 and a 1
        Set<Integer> useCounts = Stream.of(capacities)
                .collect(Collectors.groupingBy(Functions.identity()))
                .values().stream().map(Collection::size).collect(Collectors.toSet());
        assertEquals((ImmutableSet.of(1, 3)), useCounts);
    }
}
