package com.vmturbo.history.stats.live;

import static com.vmturbo.common.protobuf.utils.StringConstants.NUM_CPUS;
import static com.vmturbo.common.protobuf.utils.StringConstants.NUM_HOSTS;
import static com.vmturbo.common.protobuf.utils.StringConstants.NUM_SOCKETS;
import static com.vmturbo.common.protobuf.utils.StringConstants.NUM_STORAGES;
import static com.vmturbo.common.protobuf.utils.StringConstants.NUM_VMS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;

import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Record1;
import org.jooq.Result;
import org.jooq.SelectConditionStep;
import org.jooq.SelectJoinStep;
import org.jooq.Table;
import org.jooq.exception.DataAccessException;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;
import org.mockito.Mockito;

import com.vmturbo.api.conversion.entity.CommodityTypeMapping;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.communication.chunking.RemoteIterator;
import com.vmturbo.history.ingesters.IngestersConfig;
import com.vmturbo.history.schema.abstraction.Vmtdb;
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
import com.vmturbo.history.stats.StatsTestUtils;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.sql.utils.DbCleanupRule;
import com.vmturbo.sql.utils.DbConfigurationRule;

/**
 * Write live stats to real DB table.
 **/
public class LiveStatsDBTest {
    // TODO unify: revive tests

    private static final long REALTIME_TOPOLOGY_CONTEXT_ID = 7777777;
    private static final int TEST_TOPOLOGY_ID = 5678;

    /**
     * This is the test topology, captured from a VCenter target in Vallhalla on 1/26/17.
     * There are 96 entities total, but 3 network entities which are not processed.
     */
    private static final String TEST_TOPOLOGY_PATH = "topology/test-topo-1.json.zip";
    private static final String TEST_TOPOLOGY_FILE_NAME = "test-topo-1.json";
    // With V1.9__insert_missing_reportdata.sql two new entries were added to entities table.
    // The two new entries were "MarketSettingsManager" and "PresentationManager".
    private static final int NUMBER_OF_ENTITIES = 93 + 2;

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

    /**
     * Report all assertion failures during test, not just the first one.
     */
    @Rule
    public ErrorCollector collector = new ErrorCollector();

    /**
     * Persist a live topology and check the stats recorded.
     *
     * @throws Exception too many exceptions to throw, so just throw this one.
     */
    @Ignore // TODO
    @Test
    public void writeTopologyStatsTest() throws Exception {
        // Arrange
        int writeTopologyChunkSize = 10;
        List<CommodityType> excludedCommodities = Arrays.asList(
                CommodityType.APPLICATION,
                CommodityType.CLUSTER,
                CommodityType.DATACENTER,
                CommodityType.DATASTORE,
                CommodityType.DSPM_ACCESS,
                CommodityType.NETWORK);
        ImmutableList<String> commoditiesToExclude = ImmutableList.copyOf(
                excludedCommodities.stream()
                        .map(CommodityTypeMapping::getMixedCaseFromCommodityType)
                        .collect(Collectors.toList()));
//        LiveStatsWriter writerUnderTest = new LiveStatsWriter(
//            historydbIO,
//            commoditiesToExclude,
//            RecordWriterUtils.getRecordWriterConfig(),
//            RecordWriterUtils.getRecordWritersThreadPool());

        List<TopologyEntityDTO> allEntityDTOs
                = new ArrayList<>(
                StatsTestUtils.generateEntityDTOs(TEST_TOPOLOGY_PATH, TEST_TOPOLOGY_FILE_NAME));
        int listSize = allEntityDTOs.size();

        final TopologyInfo topologyInfo = TopologyInfo.newBuilder()
                .setTopologyContextId(REALTIME_TOPOLOGY_CONTEXT_ID)
                .setTopologyId(TEST_TOPOLOGY_ID)
                .setCreationTime(1000)
                .build();

        GroupServiceBlockingStub groupServiceClient = Mockito.mock(IngestersConfig.class).groupServiceBlockingStub();

        RemoteIterator<TopologyEntityDTO> allDTOs = Mockito.mock(RemoteIterator.class);
        when(allDTOs.hasNext()).thenReturn(true).thenReturn(true).thenReturn(false);
        when(allDTOs.nextChunk())
                .thenReturn(allEntityDTOs.subList(0, listSize * 2 / 3))
                .thenReturn(allEntityDTOs.subList(listSize * 2 / 3, listSize));

        // Act
//        writerUnderTest.processChunks(topologyInfo, allDTOs, groupServiceClient, systemLoadHelper);

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
        // (StorageAccess + StorageLatency + Extent + Produces) x 10 - inactive counts (10)
        checkTableCount(DaStatsLatest.DA_STATS_LATEST, 20);
        checkTableCount(DpodStatsLatest.DPOD_STATS_LATEST, 0);
        // StorageAccess, StorageLatency, Extent bought
        // StorageCluster, StorageAccess, StorageLatency, StorageProvisioned, StorageAmount sold
        // Produces. Total of 9 per ST. So total is 90, of which 20 are inactive and excluded.
        checkTableCount(DsStatsLatest.DS_STATS_LATEST, 70);
        // Cooling, Power, Space bought x 9 (1 bought by DC, 8 bought by PM)
        // Produces x 9
        // 3x PM connected to 3 ST, 1x PM connected to 4 DS, 4x PM connected to 5 DS
        // 33 such connections. Each such connection has StorageAcces, StorageLatency.
        // Q1_VCPU, Q2_VCPU x 8, Q4_VCPU x 3.
        // MemAllocation, StorageCluster, Mem, Swapping, Ballooning, CPUProvisioned,
        // CPU, CPUAllocation, MemProvisioned, NetThrloughput, IOThroughput x 8
        // numSockets, numCpus x 8
        // Total is 225, of which 82 are inactive (16 of the inactive are Swapping and Ballooning).
        checkTableCount(PmStatsLatest.PM_STATS_LATEST, 159);
        checkTableCount(ScStatsLatest.SC_STATS_LATEST, 0);
        checkTableCount(SwStatsLatest.SW_STATS_LATEST, 0);
        // 1 VDC buys MemAllocation and CPUAllocation from 6 PMs
        // 1 VDC buys from 2 other PMs
        // 4 VDCs buy from the 1st VDC
        // Each of the 6 VDC also sells MemAllocation and CPUAllocation
        // Each VDC has a Produces metric.
        // Total = 6 x 2 + 2 x 2 + 4 x 2 + 6 x 2 + 6 = 42
        checkTableCount(VdcStatsLatest.VDC_STATS_LATEST, 42);
        // Most VMs buys/sell/Produce 18 metrics, total is 531, of which 118 are inactive
        // (58 of the inactive are Swapping and Ballooning)
        checkTableCount(VmStatsLatest.VM_STATS_LATEST, 471);
        checkTableCount(VpodStatsLatest.VPOD_STATS_LATEST, 0);

        // stats counts: application (3), DC (3), DA (1), PM (20), ST (7), VDC (2), VM (19) = 55
        checkTableCount(MarketStatsLatest.MARKET_STATS_LATEST, 55);
        checkPropertyValue(NUM_HOSTS, 8);
        checkPropertyValue(NUM_VMS, 29);
        checkPropertyValue(NUM_STORAGES, 10);
        checkPropertyValue(NUM_SOCKETS, 1);
        checkPropertyValue(NUM_CPUS, 2.75);
    }

    private void checkPropertyValue(String propertyName, double expected) throws
            DataAccessException {
        SelectConditionStep<MarketStatsLatestRecord> selectStmt = dsl.selectFrom(
                        MarketStatsLatest.MARKET_STATS_LATEST)
                .where(MarketStatsLatest.MARKET_STATS_LATEST.PROPERTY_TYPE.eq(propertyName));
        Result<MarketStatsLatestRecord> answer = dsl.fetch(selectStmt);
        assertThat(answer.size(), is(1));
        Double count = answer.get(0).getAvgValue();
        assertThat(count, is(expected));
    }

    private void checkTableCount(Table tableToQuery, int numberOfEntities) throws DataAccessException {
        SelectJoinStep<Record1<Integer>> getRecordCount = dsl.selectCount().from(tableToQuery);
        Result<? extends Record> countResult = dsl.fetch(getRecordCount);
        Integer count = (Integer)countResult.getValue(0, 0, 0L);
        collector.checkThat(String.format("Table %s: ", tableToQuery.getName()), count,
                is(numberOfEntities));
    }
}
