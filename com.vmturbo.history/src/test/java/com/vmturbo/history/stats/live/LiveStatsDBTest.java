package com.vmturbo.history.stats.live;

import static com.vmturbo.history.schema.StringConstants.NUM_CPUS;
import static com.vmturbo.history.schema.StringConstants.NUM_HOSTS;
import static com.vmturbo.history.schema.StringConstants.NUM_SOCKETS;
import static com.vmturbo.history.schema.StringConstants.NUM_STORAGES;
import static com.vmturbo.history.schema.StringConstants.NUM_VMS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Record;
import org.jooq.Record1;
import org.jooq.Result;
import org.jooq.SelectConditionStep;
import org.jooq.SelectJoinStep;
import org.jooq.Table;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.google.common.collect.ImmutableList;

import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.communication.chunking.RemoteIterator;
import com.vmturbo.components.common.ClassicEnumMapper.CommodityTypeUnits;
import com.vmturbo.history.db.BasedbIO;
import com.vmturbo.history.db.DBConnectionPool;
import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.db.SchemaUtil;
import com.vmturbo.history.db.VmtDbException;
import com.vmturbo.history.schema.abstraction.tables.AppStatsLatest;
import com.vmturbo.history.schema.abstraction.tables.ChStatsLatest;
import com.vmturbo.history.schema.abstraction.tables.CntStatsLatest;
import com.vmturbo.history.schema.abstraction.tables.DaStatsLatest;
import com.vmturbo.history.schema.abstraction.tables.DpodStatsLatest;
import com.vmturbo.history.schema.abstraction.tables.DsStatsLatest;
import com.vmturbo.history.schema.abstraction.tables.Entities;
import com.vmturbo.history.schema.abstraction.tables.MarketStatsLatest;
import com.vmturbo.history.schema.abstraction.tables.PmStatsLatest;
import com.vmturbo.history.schema.abstraction.tables.ScStatsLatest;
import com.vmturbo.history.schema.abstraction.tables.SwStatsLatest;
import com.vmturbo.history.schema.abstraction.tables.VdcStatsLatest;
import com.vmturbo.history.schema.abstraction.tables.VmStatsLatest;
import com.vmturbo.history.schema.abstraction.tables.VpodStatsLatest;
import com.vmturbo.history.schema.abstraction.tables.records.MarketStatsLatestRecord;
import com.vmturbo.history.stats.DbTestConfig;
import com.vmturbo.history.stats.StatsTestUtils;
import com.vmturbo.history.topology.TopologySnapshotRegistry;
import com.vmturbo.history.utils.SystemLoadHelper;
import com.vmturbo.history.utils.TopologyOrganizer;
import com.vmturbo.history.topology.TopologyListenerConfig;
//import com.vmturbo.sql.utils.TestSQLDatabaseConfig;

//import com.vmturbo.history.util.IDGen;

/**
 * Write live stats to real DB table.
 **/
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {DbTestConfig.class})
@DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_EACH_TEST_METHOD)
public class LiveStatsDBTest {

    private static final Logger logger = LogManager.getLogger();

    private static final long REALTIME_TOPOLOGY_CONTEXT_ID = 7777777;
    private static final int TEST_TOPOLOGY_ID = 5678;

    /**
     * This is the test topology, captured from a VCenter target in Vallhalla on 1/26/17.
     * There are 96 entities total, but 3 network entities which are not processed.
     */
    private static final String TEST_TOPOLOGY_PATH = "topology/test-topo-1.json.zip";
    private static final String TEST_TOPOLOGY_FILE_NAME = "test-topo-1.json";
    private static final int NUMBER_OF_ENTITIES = 93;

    @Autowired
    private DbTestConfig dbTestConfig;

    private String testDbName;

    private HistorydbIO historydbIO;

    @Before
    public void before() throws Throwable {
        IdentityGenerator.initPrefix(0);
        testDbName = dbTestConfig.testDbName();
        historydbIO = dbTestConfig.historydbIO();
        // map the 'vmtdb' database name used in the code into the test DB name
        historydbIO.setSchemaForTests(testDbName);
        logger.info("Initializing DB - " + testDbName);
        HistorydbIO.setSharedInstance(historydbIO);
        historydbIO.init(true, null, testDbName);
    }

    @After
    public void after() {
        DBConnectionPool.instance.getInternalPool().close();
        try {
            SchemaUtil.dropDb(testDbName);
            System.out.println("Dropped DB - " + testDbName);
        } catch (VmtDbException e) {
            logger.error("Problem dropping db: " + testDbName, e);
        }
    }

    /**
     * Persist a live topology and check the stats recorded.
     * @throws Exception too many exceptions to throw, so just throw this one.
     */
    @Test
    public void writeTopologyStatsTest() throws Exception {
        // Arrange
        TopologySnapshotRegistry topologySnapshotRegistry =
                Mockito.mock(TopologySnapshotRegistry.class);
        int writeTopologyChunkSize = 10;
        List<CommodityTypeUnits> excludedCommodities = Arrays.asList(
                CommodityTypeUnits.APPLICATION,
                CommodityTypeUnits.CLUSTER,
                CommodityTypeUnits.DATACENTER,
                CommodityTypeUnits.DATASTORE,
                CommodityTypeUnits.DSPM_ACCESS,
                CommodityTypeUnits.NETWORK);
        ImmutableList<String> commoditiesToExclude = ImmutableList.copyOf(
                excludedCommodities.stream()
                        .map(CommodityTypeUnits::getMixedCase)
                        .collect(Collectors.toList()));
        LiveStatsWriter writerUnderTest = new LiveStatsWriter(topologySnapshotRegistry,
                historydbIO, writeTopologyChunkSize, commoditiesToExclude);

        List<TopologyEntityDTO> allEntityDTOs
                = new ArrayList<>(StatsTestUtils.generateEntityDTOs(TEST_TOPOLOGY_PATH, TEST_TOPOLOGY_FILE_NAME));
        int listSize = allEntityDTOs.size();

        TopologyOrganizer topologyOrganizer = new TopologyOrganizer(REALTIME_TOPOLOGY_CONTEXT_ID,
                TEST_TOPOLOGY_ID);

        GroupServiceBlockingStub groupServiceClient = Mockito.mock(TopologyListenerConfig.class).groupServiceClient();
        SystemLoadHelper systemLoadHelper = Mockito.mock(SystemLoadHelper.class);

        RemoteIterator<TopologyEntityDTO> allDTOs = Mockito.mock(RemoteIterator.class);
        when(allDTOs.hasNext()).thenReturn(true).thenReturn(true).thenReturn(false);
        when(allDTOs.nextChunk())
            .thenReturn(allEntityDTOs.subList(0, listSize * 2 / 3))
            .thenReturn(allEntityDTOs.subList(listSize * 2 / 3, listSize));

        // Act
        writerUnderTest.processChunks(topologyOrganizer, allDTOs, groupServiceClient, systemLoadHelper);

        // Assert
        // expected row counts from the sample topology
        // 1 DC + 10 ST + 10 DA + 8 PM + 6 VDC + 29 VM + 29 APP = 93
        checkTableCount(Entities.ENTITIES, NUMBER_OF_ENTITIES);
        // Each APP has VCPU, VMem and Produces.
        // One APP has 2 VStorages and one has 3 VStorages.
        // Total 3 x 29 + 5 = 92.
        checkTableCount(AppStatsLatest.APP_STATS_LATEST, 92);
        checkTableCount(ChStatsLatest.CH_STATS_LATEST, 0);
        checkTableCount(CntStatsLatest.CNT_STATS_LATEST, 0);
        // (StorageAccess + StorageLatency + Extent + Produces) x 10
        checkTableCount(DaStatsLatest.DA_STATS_LATEST, 40);
        checkTableCount(DpodStatsLatest.DPOD_STATS_LATEST, 0);
        // StorageAccess, StorageLatency, Extent bought
        // StorageCluster, StorageAccess, StorageLatency, StorageProvisioned, StorageAmount sold
        // Produces. Total of 9 per ST.
        checkTableCount(DsStatsLatest.DS_STATS_LATEST, 90);
        // Cooling, Power, Space bought x 9 (1 bought by DC, 8 bought by PM)
        // Produces x 9
        // 3x PM connected to 3 ST, 1x PM connected to 4 DS, 4x PM connected to 5 DS
        // 33 such connections. Each such connection has StorageAcces, StorageLatency.
        // Q1_VCPU, Q2_VCPU x 8, Q4_VCPU x 3.
        // MemAllocation, StorageCluster, Mem, Swapping, Ballooning, CPUProvisioned,
        // CPU, CPUAllocation, MemProvisioned, NetThrloughput, IOThroughput x 8
        // numSockets, numCpus x 8
        // Total
        checkTableCount(PmStatsLatest.PM_STATS_LATEST, 225);
        checkTableCount(ScStatsLatest.SC_STATS_LATEST, 0);
        checkTableCount(SwStatsLatest.SW_STATS_LATEST, 0);
        // 1 VDC buys MemAllocation and CPUAllocation from 6 PMs
        // 1 VDC buys from 2 other PMs
        // 4 VDCs buy from the 1st VDC
        // Each of the 6 VDC also sells MemAllocation and CPUAllocation
        // Each VDC has a Produces metric.
        // Total = 6 x 2 + 2 x 2 + 4 x 2 + 6 x 2 + 6 = 42
        checkTableCount(VdcStatsLatest.VDC_STATS_LATEST, 42);
        // Most VMs buys/sell/Produce 18 metrics
        checkTableCount(VmStatsLatest.VM_STATS_LATEST, 531);
        checkTableCount(VpodStatsLatest.VPOD_STATS_LATEST, 0);

        // stats counts: application (4), DC (4), DA (3), PM (24), ST (8), VDC (2), VM (29) = 74
        checkTableCount(MarketStatsLatest.MARKET_STATS_LATEST, 61);
        checkPropertyValue(NUM_HOSTS, 8);
        checkPropertyValue(NUM_VMS, 29);
        checkPropertyValue(NUM_STORAGES, 10);
        checkPropertyValue(NUM_SOCKETS, 1);
        checkPropertyValue(NUM_CPUS, 2.75);
    }

    private void checkPropertyValue(String propertyName, double expected) throws VmtDbException {
        SelectConditionStep<MarketStatsLatestRecord> selectStmt = HistorydbIO.getJooqBuilder().selectFrom(MarketStatsLatest.MARKET_STATS_LATEST)
                .where(MarketStatsLatest.MARKET_STATS_LATEST.PROPERTY_TYPE.eq(propertyName));
        Result<MarketStatsLatestRecord> answer =
                (Result<MarketStatsLatestRecord>) historydbIO.execute(BasedbIO.Style.IMMEDIATE,
                        selectStmt);
        assertThat(answer.size(), is(1));
        Double count = answer.get(0).getAvgValue();
        assertThat(count, is(expected));
    }

    private void checkTableCount(Table tableToQuery, int numberOfEntities) throws VmtDbException {
        SelectJoinStep<Record1<Integer>> getRecordCount = HistorydbIO.getJooqBuilder().selectCount()
                .from(tableToQuery);
        Result<? extends Record> countResult = historydbIO.execute(BasedbIO.Style.IMMEDIATE,
                getRecordCount);
        Integer count = (Integer)countResult.getValue(0, 0, 0L);
        assertThat(count, is(numberOfEntities));
    }
}
