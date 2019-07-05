package com.vmturbo.history.stats.live;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.when;

import java.util.Map;

import org.jooq.InsertSetMoreStep;
import org.jooq.InsertSetStep;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import com.vmturbo.auth.api.db.DBPasswordUtil;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.db.VmtDbException;
import com.vmturbo.history.stats.StatsTestUtils;

/**
 * Unit tests for {@link LiveStatsAggregator}.
 *
 */
public class LiveStatsAggregatorTest {

    private static LiveStatsAggregator aggregator;
    private static InsertSetMoreStep stmt;
    private static HistorydbIO historydbIO;

    /**
     * Set up a topology of a few VMs and a few PMs, and test the handling of
     * pending commodities bought.
     * @throws VmtDbException should not happen in these tests
     *
     */
    @BeforeClass
    public static void setupAndTestPending() throws VmtDbException {
        final TopologyEntityDTO vm1 = StatsTestUtils.vm(10, 30); // buys from pm1
        final TopologyEntityDTO vm2 = StatsTestUtils.vm(20, 40); // buys from pm2
        final TopologyEntityDTO pm1 = StatsTestUtils.pm(30, 10);
        final TopologyEntityDTO pm2 = StatsTestUtils.pm(40, 10);
        final TopologyEntityDTO pm3 = StatsTestUtils.pm(50, 10);
        ImmutableList<String> exclude =
            ImmutableList.copyOf("Application CLUSTER DATACENTER DATASTORE DSPMAccess NETWORK"
                .toLowerCase().split(" "));

        final TopologyInfo topologyInfo = TopologyInfo.newBuilder()
            .setTopologyContextId(123456)
            .setTopologyId(55555)
            .build();

        stmt = Mockito.mock(InsertSetMoreStep.class);
        historydbIO = mockdbIO();
        aggregator = new LiveStatsAggregator(historydbIO, topologyInfo, exclude, 3);

        // includes a forward reference: vm processed before the pm it is buying from
        Map<Long, TopologyEntityDTO> entityByOid = ImmutableMap.of(
                vm1.getOid(), vm1,
                pm1.getOid(), pm1
        );
        aggregator.aggregateEntity(vm1, entityByOid);
        aggregator.aggregateEntity(pm1, entityByOid);

        assertEquals(0, aggregator.numPendingBought());
        entityByOid = ImmutableMap.of(
                vm2.getOid(), vm2,
                pm3.getOid(), pm3
        );
        aggregator.aggregateEntity(vm2, entityByOid);
        aggregator.aggregateEntity(pm3, entityByOid);
        // vm2 is buying from pm2, but pm2 is not yet processed, so the commodities bought by vm2 are pending
        assertEquals(1, aggregator.numPendingBought());
        entityByOid = ImmutableMap.of(pm2.getOid(), pm2);
        aggregator.aggregateEntity(pm2, entityByOid);

        // pm2 added so vm2 can be processed so no pending commodities bought
        assertEquals(0, aggregator.numPendingBought());

        aggregator.writeFinalStats();
    }

    /**
     * Mostly a mock of {@link HistorydbIO} but some methods are invoked on a real instance.
     * @return an instance of HistorydbIO
     */
    private static HistorydbIO mockdbIO() {
        // always return the default DB password for this test
        DBPasswordUtil dbPasswordUtilMock = Mockito.mock(DBPasswordUtil.class);
        when(dbPasswordUtilMock.getSqlDbRootPassword()).thenReturn(DBPasswordUtil.obtainDefaultPW());
        HistorydbIO real = new HistorydbIO(dbPasswordUtilMock);
        HistorydbIO mock = Mockito.mock(HistorydbIO.class);
        for (int i = 0; i < 100; i++) {
            Mockito.when(mock.getEntityType(i)).thenReturn(real.getEntityType(i));
            Mockito.when(mock.getBaseEntityType(i)).thenReturn(real.getBaseEntityType(i));
        }
        Mockito.when(mock.getCommodityInsertStatement(Mockito.any())).thenReturn(stmt);
        return mock;
    }

    /**
     * Verify properties of the records to be written to the DB.
     * @throws VmtDbException should not happen
     */
    @Test
    public void testRecords() {
        // 10 records: 5 x Produces attributes + 2 x commodity bought + 3 x commodity sold
        Mockito.verify(stmt, Mockito.times(10)).newRecord();
        // 3 PMs produce 1 entity each
        Mockito.verify(historydbIO, Mockito.times(3)).setCommodityValues(Mockito.eq("Produces"),
            Mockito.eq(1.0), Mockito.eq(0.0), Mockito.eq(stmt), Mockito.any());
        // 2 VMs produce 0 entities
        Mockito.verify(historydbIO, Mockito.times(2)).setCommodityValues(Mockito.eq("Produces"),
            Mockito.eq(0.0), Mockito.eq(0.0), Mockito.eq(stmt), Mockito.any());
    }

    /**
     * Verify properties of the capacities sold map.
     *
     */
    @Test
    public void testCapacities() {
        Object[] capacities = aggregator.capacities().values().toArray();
        // 3 entities with commodities sold (the 3 PMs)
        assertEquals(3, capacities.length);
        // Verify that the sold commodities maps are being reused
        assertSame(capacities[0], capacities[1]);
        assertSame(capacities[0], capacities[2]);
    }

}
