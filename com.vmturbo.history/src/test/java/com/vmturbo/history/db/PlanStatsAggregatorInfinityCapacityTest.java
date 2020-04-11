package com.vmturbo.history.db;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;

import com.google.common.collect.ImmutableSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.annotation.DirtiesContext.ClassMode;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.history.schema.abstraction.Vmtdb;
import com.vmturbo.history.stats.DbTestConfig;
import com.vmturbo.history.stats.PlanStatsAggregator;
import com.vmturbo.sql.utils.DbCleanupRule;

/**
 * Edge unit test for {@link PlanStatsAggregator}.
 * Verify storing "Infinity" capacity by clipping.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {DbTestConfig.class})
@DirtiesContext(classMode = ClassMode.BEFORE_CLASS)
public class PlanStatsAggregatorInfinityCapacityTest {
    // TODO unify: revive tests
    private static final Logger logger = LogManager.getLogger();

    private static final long SNAPSHOT_TIME = 12345L;
    private static final long TOPOLOGY_CONTEXT_ID = 111;
    private static final long TOPOLOGY_ID = 222;

    @Autowired
    private DbTestConfig dbTestConfig;
    private static String testDbName;
    private HistorydbIO historydbIO;

    private static final TopologyInfo TOPOLOGY_INFO = TopologyInfo.newBuilder()
        .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
        .setTopologyId(TOPOLOGY_ID)
        .setCreationTime(SNAPSHOT_TIME)
        .build();

    /**
     * Common setup code.
     *
     * @throws Exception If anything goes wrong.
     */
    @Before
    public void setup() throws Exception {
        testDbName = dbTestConfig.testDbName();
        historydbIO = dbTestConfig.historydbIO();
        HistorydbIO.mappedSchemaForTests = testDbName;
        logger.info("Initializing DB - {}", testDbName);
        HistorydbIO.setSharedInstance(historydbIO);
        // Important to NOT clear the old DB, because that will re-run all migrations.
        // We rely on the "after" method to clean up the database after each test is done.
        historydbIO.init(false, null, testDbName, Optional.empty());

        BasedbIO.setSharedInstance(historydbIO);

    }

    /**
     * Delete all data in database after every test.
     *
     * @throws Exception If there is an error.
     */
    @After
    public void after() throws Exception {
        // Delete all data in any tables.
        try (Connection conn = historydbIO.connection()) {
            DbCleanupRule.cleanDb(historydbIO.using(conn), Vmtdb.VMTDB);
        } catch (SQLException e) {
            throw e;
        }
    }

    /**
     * Tear down the database after all tests execute.
     */
    @AfterClass
    public static void teardown() {
        DBConnectionPool.instance.getInternalPool().close();
        try {
            SchemaUtil.dropDb(testDbName);
            logger.info("Dropped DB - {}", testDbName);
        } catch (VmtDbException e) {
            logger.error("Problem dropping db: " + testDbName, e);
        }
    }

    @Test
    public void testSettingCommodityCapacityToInfinity() throws VmtDbException {
        historydbIO.addMktSnapshotRecord(TOPOLOGY_INFO);

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
    public void testSettingCommodityCapacityToInfinityMultipleChunk() throws VmtDbException {
        historydbIO.addMktSnapshotRecord(TOPOLOGY_INFO);
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
