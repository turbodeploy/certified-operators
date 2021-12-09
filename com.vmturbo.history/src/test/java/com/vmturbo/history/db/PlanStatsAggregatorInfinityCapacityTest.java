package com.vmturbo.history.db;

import java.util.Collection;
import java.util.Collections;

import com.google.common.collect.ImmutableSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.exception.DataAccessException;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.history.schema.abstraction.Vmtdb;
import com.vmturbo.history.stats.PlanStatsAggregator;
import com.vmturbo.sql.utils.DbCleanupRule;
import com.vmturbo.sql.utils.DbConfigurationRule;

/**
 * Edge unit test for {@link PlanStatsAggregator}.
 * Verify storing "Infinity" capacity by clipping.
 */
public class PlanStatsAggregatorInfinityCapacityTest {
    // TODO unify: revive tests
    private static final Logger logger = LogManager.getLogger();

    private static final long SNAPSHOT_TIME = 12345L;
    private static final long TOPOLOGY_CONTEXT_ID = 111;
    private static final long TOPOLOGY_ID = 222;


    private static final TopologyInfo TOPOLOGY_INFO = TopologyInfo.newBuilder()
        .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
        .setTopologyId(TOPOLOGY_ID)
        .setCreationTime(SNAPSHOT_TIME)
        .build();

    /**
     * Provision and provide access to a test database.
     */
    @ClassRule
    public static DbConfigurationRule dbConfig = new DbConfigurationRule(Vmtdb.VMTDB);

    /**
     * Clean up tables in the test database before each test.
     */
    @Rule
    public DbCleanupRule dbCleanup = dbConfig.cleanupRule();

    private final DSLContext dsl = dbConfig.getDslContext();


    @Test
    public void testSettingCommodityCapacityToInfinity() throws DataAccessException {
//        historydbIO.addMktSnapshotRecord(TOPOLOGY_INFO);

        //        final PlanStatsAggregator aggregator
//                = new PlanStatsAggregator(historydbIO, TOPOLOGY_INFO, true);
        final TopologyEntityDTO topology2 =
                TopologyEntityDTO.newBuilder().setOid(2L).setEntityType(2)
                        .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                                .setCommodityType(CommodityType.newBuilder()
                                        .setType(0)
                                        .setKey("111").build())
                                .setCapacity(1 / Double.MIN_VALUE).build()) // setting the capacity to inifinity
                        .build();
        final Collection<TopologyEntityDTO> chunk = Collections.singleton(topology2);
//        aggregator.handleChunk(chunk);
//        aggregator.writeAggregates();
    }

    @Test
    public void testSettingCommodityCapacityToInfinityMultipleChunk() throws DataAccessException {
//        historydbIO.addMktSnapshotRecord(TOPOLOGY_INFO);
//        final PlanStatsAggregator aggregator
//                = new PlanStatsAggregator(historydbIO, TOPOLOGY_INFO, true);
        final TopologyEntityDTO topology1 =
                TopologyEntityDTO.newBuilder().setOid(2L).setEntityType(2)
                        .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                                .setCommodityType(CommodityType.newBuilder()
                                        .setType(0)
                                        .setKey("111").build())
                                .setCapacity(1 / Double.MIN_VALUE).build()) // setting the capacity to infinity
                        .build();
        final TopologyEntityDTO topology2 =
                TopologyEntityDTO.newBuilder().setOid(2L).setEntityType(2)
                        .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                                .setCommodityType(CommodityType.newBuilder()
                                        .setType(0)
                                        .setKey("111").build())
                                .setCapacity(Double.MAX_VALUE).build()) // setting the capacity to MAX_VALUE
                        .build();
        final TopologyEntityDTO topology3 =
                TopologyEntityDTO.newBuilder().setOid(2L).setEntityType(2)
                        .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                                .setCommodityType(CommodityType.newBuilder()
                                        .setType(0)
                                        .setKey("111").build())
                                .setCapacity(1 / Double.MIN_VALUE).build()) // setting the capacity to infinity
                        .build();
        final Collection<TopologyEntityDTO> chunk = ImmutableSet.of(topology1, topology2, topology3);
//        aggregator.handleChunk(chunk);
//        aggregator.handleChunk(chunk);
//        aggregator.writeAggregates();
    }
}
