package com.vmturbo.history.db;

import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.Collections;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.history.stats.DbTestConfig;
import com.vmturbo.history.stats.PlanStatsAggregator;
import com.vmturbo.history.utils.TopologyOrganizer;

/**
 * Edge unit test for {@link PlanStatsAggregator}.
 * Verify storing "Infinity" capacity by clipping.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {DbTestConfig.class})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
public class PlanStatsAggregatorInfinityCapacityTest {

    private static final long SNAPSHOT_TIME = 12345L;
    private static final long TOPOLOGY_CONTEXT_ID = 111;
    private static final long TOPOLOGY_ID = 222;

    @Autowired
    private DbTestConfig dbTestConfig;
    private String testDbName;
    private HistorydbIO historydbIO;

    private TopologyOrganizer topologyOrganizer = Mockito.mock(TopologyOrganizer.class);

    @Before
    public void setup() throws Exception {
        testDbName = dbTestConfig.testDbName();
        historydbIO = dbTestConfig.historydbIO();
        HistorydbIO.mappedSchemaForTests = testDbName;
        System.out.println("Initializing DB - " + testDbName);
        HistorydbIO.setSharedInstance(historydbIO);
        historydbIO.init(true, null, testDbName);

        when(topologyOrganizer.getSnapshotTime()).thenReturn(SNAPSHOT_TIME);
        when(topologyOrganizer.getTopologyContextId()).thenReturn(TOPOLOGY_CONTEXT_ID);
        when(topologyOrganizer.getTopologyId()).thenReturn(TOPOLOGY_ID);

        BasedbIO.setSharedInstance(historydbIO);

    }

    @After
    public void after() throws Throwable {
        DBConnectionPool.instance.getInternalPool().close();
        try {
            SchemaUtil.dropDb(testDbName);
            System.out.println("Dropped DB - " + testDbName);
        } catch (VmtDbException e) {
            System.out.println("Problem dropping db: " + testDbName);
        }
    }

    @Test
    public void testSettingCommodityCapacityToInfinity() throws VmtDbException {
        historydbIO.addMktSnapshotRecord(topologyOrganizer);
        final PlanStatsAggregator aggregator
                = new PlanStatsAggregator(historydbIO, topologyOrganizer, true);
        final TopologyEntityDTO topology2 =
                TopologyEntityDTO.newBuilder().setOid(2L).setEntityType(2)
                        .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                                .setCommodityType(CommodityType.newBuilder()
                                        .setType(0)
                                        .setKey("111").build())
                                .setCapacity(1 / Double.MIN_VALUE).build()) // setting the capacity to inifinity
                        .build();
        final Collection<TopologyEntityDTO> chunk = Collections.singleton(topology2);
        aggregator.handleChunk(chunk);
        aggregator.writeAggregates();
    }
}
