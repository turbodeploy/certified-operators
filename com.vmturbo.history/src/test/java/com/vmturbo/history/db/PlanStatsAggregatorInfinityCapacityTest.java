package com.vmturbo.history.db;

import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;

import javax.sql.DataSource;

import com.google.common.collect.ImmutableSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.exception.DataAccessException;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.annotation.DirtiesContext.ClassMode;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.components.common.featureflags.FeatureFlags;
import com.vmturbo.history.db.HistoryDbEndpointConfig.TestHistoryDbEndpointConfig;
import com.vmturbo.history.schema.abstraction.Vmtdb;
import com.vmturbo.history.stats.PlanStatsAggregator;
import com.vmturbo.sql.utils.DbCleanupRule;
import com.vmturbo.sql.utils.DbConfigurationRule;
import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.sql.utils.DbEndpointTestRule;
import com.vmturbo.test.utils.FeatureFlagTestRule;

/**
 * Edge unit test for {@link PlanStatsAggregator}.
 * Verify storing "Infinity" capacity by clipping.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {TestHistoryDbEndpointConfig.class})
@DirtiesContext(classMode = ClassMode.BEFORE_CLASS)
@TestPropertySource(properties = {"sqlDialect=MARIADB"})
public class PlanStatsAggregatorInfinityCapacityTest {

    @Autowired(required = false)
    private TestHistoryDbEndpointConfig dbEndpointConfig;

    /**
     * Rule to set up the database before running the tests.
     */
    @ClassRule
    public static DbConfigurationRule dbConfig = new DbConfigurationRule(Vmtdb.VMTDB);

    /**
     * Rule to clean up the database after each test.
     */
    @Rule
    public DbCleanupRule dbCleanup = dbConfig.cleanupRule();

    /**
     * Test rule to use {@link DbEndpoint}s in test.
     */
    @ClassRule
    public static DbEndpointTestRule dbEndpointTestRule = new DbEndpointTestRule("history");

    /**
     * Rule to manage feature flag enablement to make sure FeatureFlagManager store is set up.
     */
    @Rule
    public FeatureFlagTestRule featureFlagTestRule = new FeatureFlagTestRule().testAllCombos(
            FeatureFlags.POSTGRES_PRIMARY_DB);

    private DSLContext dsl;
    private DataSource dataSource;

    /**
     * Set up and populate live database for tests, and create required mocks.
     *
     * @throws SQLException If a DB operation filas
     * @throws UnsupportedDialectException if the dialect is bogus
     * @throws InterruptedException if we're interrupted
     */
    @Before
    public void before() throws SQLException, UnsupportedDialectException, InterruptedException {
        if (FeatureFlags.POSTGRES_PRIMARY_DB.isEnabled()) {
            dbEndpointTestRule.addEndpoints(dbEndpointConfig.historyEndpoint());
            dsl = dbEndpointConfig.historyEndpoint().dslContext();
            dataSource = dbEndpointConfig.historyEndpoint().datasource();
        } else {
            dsl = dbConfig.getDslContext();
            dataSource = dbConfig.getDataSource();
        }
    }

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
