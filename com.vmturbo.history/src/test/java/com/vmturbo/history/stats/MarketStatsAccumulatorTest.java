package com.vmturbo.history.stats;

import static com.vmturbo.components.common.utils.StringConstants.NUM_HOSTS;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;

import org.jooq.InsertSetMoreStep;
import org.jooq.InsertSetStep;
import org.jooq.Query;
import org.jooq.Table;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.history.db.BasedbIO;
import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.schema.abstraction.tables.records.VmStatsLatestRecord;
import com.vmturbo.history.stats.MarketStatsAccumulator.DelayedCommodityBoughtWriter;
import com.vmturbo.history.stats.MarketStatsAccumulator.MarketStatsData;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Tests for the MarketStatsAccumulator.
 */
@RunWith(MockitoJUnitRunner.class)
public class MarketStatsAccumulatorTest {

    private static final long SNAPSHOT_TIME = 12345L;
    private static final long TOPOLOGY_CONTEXT_ID = 111;
    private static final long TOPOLOGY_ID = 222;
    private static final String PM_ENTITY_TYPE = "PhysicalMachine";
    private static final String APP_ENTITY_TYPE = "Application";
    private static final long ENTITY_ID = 999L;

    private static final TopologyInfo TOPOLOGY_INFO = TopologyInfo.newBuilder()
        .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
        .setTopologyId(TOPOLOGY_ID)
        .setCreationTime(SNAPSHOT_TIME)
        .build();

    private ImmutableList<String> commoditiesToExclude =
            ImmutableList.copyOf(("ApplicationCommodity CLUSTERCommodity DATACENTERCommodity " +
                    "DATASTORECommodity DSPMAccessCommodity NETWORKCommodity").toLowerCase()
                    .split(" "));

    private static final int FIELDS_PER_ROW = (new VmStatsLatestRecord()).fieldsRow().size();

    // collect stats rows in groups of 3 to be written (inserted) into the DB
    private final static long WRITE_TOPOLOGY_CHUNK_SIZE = 3;

    // DTO's to use for testing
    private TopologyEntityDTO testPm;
    private TopologyEntityDTO testVm;
    private TopologyEntityDTO testApp;
    private TopologyEntityDTO testAppWithoutProvider;

    @Mock
    private HistorydbIO historydbIO;


    @Captor
    private ArgumentCaptor<MarketStatsData> statsDataCaptor;

    @Captor
    private ArgumentCaptor<List<Query>> queryListCaptor;

    @Before
    public void setup() {
        BasedbIO.setSharedInstance(historydbIO);

        try {
            testVm = StatsTestUtils.generateEntityDTO(StatsTestUtils.TEST_VM_PATH);
            testApp = StatsTestUtils.generateEntityDTO(StatsTestUtils.TEST_APP_PATH);
            testAppWithoutProvider =
                StatsTestUtils.generateEntityDTO(StatsTestUtils.TEST_APP_WITHOUT_PROVIDER_PATH);
            testPm = StatsTestUtils.generateEntityDTO(StatsTestUtils.TEST_PM_PATH);
        } catch (Exception e) {
            throw new RuntimeException("Cannot load DTO's", e);
        }
    }


    @Test
    public void testPersistMarketStats() throws Exception {
        // arrange
        String entityType = "PhysicalMachine";
        MarketStatsAccumulator marketStatsAccumulator = new MarketStatsAccumulator(TOPOLOGY_INFO,
            entityType, EnvironmentType.ON_PREM, historydbIO, WRITE_TOPOLOGY_CHUNK_SIZE, commoditiesToExclude);

        // act
        marketStatsAccumulator.persistMarketStats();

        // assert
        assertThat(marketStatsAccumulator.values().size(), is(1));
        verify(historydbIO).getCommodityInsertStatement(StatsTestUtils.PM_LATEST_TABLE);
        verify(historydbIO).execute(Mockito.anyObject(), queryListCaptor.capture());
        verify(historydbIO).getMarketStatsInsertStmt(statsDataCaptor.capture(), eq(TOPOLOGY_INFO));
        verifyNoMoreInteractions(historydbIO);

        final MarketStatsData mktStatsData = statsDataCaptor.getValue();
        assertThat(mktStatsData.getPropertyType(), is(NUM_HOSTS));
        assertThat(mktStatsData.getEnvironmentType(), is(EnvironmentType.ON_PREM));
        assertThat(mktStatsData.getUsed(), is((double)0));

    }

    @Test
    public void testPersistCommoditiesSold() throws Exception {
        // arrange
        InsertSetMoreStep mockInsertStmt = createMockInsertQuery(StatsTestUtils.PM_LATEST_TABLE);

        // create the object under test
        String entityType = "PhysicalMachine";
        MarketStatsAccumulator marketStatsAccumulator = new MarketStatsAccumulator(TOPOLOGY_INFO,
                entityType, EnvironmentType.ON_PREM,
                historydbIO, WRITE_TOPOLOGY_CHUNK_SIZE, commoditiesToExclude);
        List<CommoditySoldDTO> commoditiesSold = new ArrayList<>();
        commoditiesSold.add(StatsTestUtils.cpu(3.14));

        // act
        // write 10 stats rows - doesn't matter that they are the same
        for (int i=0; i < 10; i++) {
            marketStatsAccumulator.persistCommoditiesSold(ENTITY_ID, commoditiesSold);
        }
        // flush the stats, triggering the final insert
        marketStatsAccumulator.writeQueuedRows();

        // assert 20 rows, buffering 3 at a time -> 7 insert statements total
        verify(historydbIO, times(7)).getCommodityInsertStatement(StatsTestUtils.PM_LATEST_TABLE);
        verify(historydbIO, times(7)).execute(eq(BasedbIO.Style.FORCED), eq(mockInsertStmt));
    }

    @Test
    public void testExclusion() throws Exception {
        // arrange
        InsertSetMoreStep mockInsertStmt = createMockInsertQuery(StatsTestUtils.PM_LATEST_TABLE);

        // create the object under test
        String entityType = "PhysicalMachine";
        MarketStatsAccumulator marketStatsAccumulator = new MarketStatsAccumulator(
                TOPOLOGY_INFO, entityType, EnvironmentType.ON_PREM,
                historydbIO, WRITE_TOPOLOGY_CHUNK_SIZE, commoditiesToExclude);
        List<CommoditySoldDTO> commoditiesSold = new ArrayList<>();
        commoditiesSold.add(StatsTestUtils.dspma(50000));

        // act
        marketStatsAccumulator.persistCommoditiesSold(ENTITY_ID, commoditiesSold);

        // flush the stats, triggering the final insert
        marketStatsAccumulator.writeQueuedRows();

        // assert - should be no writes, since the only commodity will be excluded
        verify(historydbIO, times(0)).execute(eq(BasedbIO.Style.FORCED), eq(mockInsertStmt));
        verify(historydbIO, times(1)).getCommodityInsertStatement(StatsTestUtils.PM_LATEST_TABLE);
    }

    @Test
    public void testPersistCommoditiesBoughtAvailable() throws Exception {

        // arrange
        InsertSetMoreStep mockInsertStmt = createMockInsertQuery(StatsTestUtils.APP_LATEST_TABLE);

        MarketStatsAccumulator marketStatsAccumulator = new MarketStatsAccumulator(
                TOPOLOGY_INFO, APP_ENTITY_TYPE, EnvironmentType.ON_PREM,
                historydbIO, WRITE_TOPOLOGY_CHUNK_SIZE, commoditiesToExclude);
        Map<Long, Map<Integer, Double>> capacities = Maps.newHashMap();
        Multimap<Long, DelayedCommodityBoughtWriter> delayedCommoditiesBought = HashMultimap.create();

        // calculate the commodities sold by the provider (vm)
        Map<Integer, Double> vmCommoditiesSoldMap = testVm.getCommoditySoldListList().stream().collect(Collectors.toMap(
                (CommoditySoldDTO commoditySoldDTO) ->
                        commoditySoldDTO.getCommodityType().getType(),
                CommoditySoldDTO::getCapacity
        ));
        capacities.put(testVm.getOid(), vmCommoditiesSoldMap);
        Map<Long, TopologyEntityDTO> entityByOid = ImmutableMap.of(testVm.getOid(), testVm);

        // act
        for (int i=0; i < 10; i++) {
            marketStatsAccumulator.persistCommoditiesBought(testApp,
                    capacities, delayedCommoditiesBought, entityByOid);
        }
        // flush the stats, triggering the final insert
        marketStatsAccumulator.writeQueuedRows();

        // assert
        // 10 * 2 commodities (1 commodity with percentile * 10) -> 30 rows; 3 at a time -> 10 inserts;
        verify(historydbIO, times(10)).execute(eq(BasedbIO.Style.FORCED), eq(mockInsertStmt));
        // no additional unused getCommodityInsertStatement fetched => 11
        verify(historydbIO, times(11)).getCommodityInsertStatement(StatsTestUtils.APP_LATEST_TABLE);
    }

    @Test
    public void testPersistCommoditiesBoughtWithoutProvider() throws Exception {
        // arrange
        InsertSetMoreStep mockInsertStmt = createMockInsertQuery(StatsTestUtils.APP_LATEST_TABLE);

        MarketStatsAccumulator marketStatsAccumulator = new MarketStatsAccumulator(
            TOPOLOGY_INFO, APP_ENTITY_TYPE, EnvironmentType.ON_PREM,
            historydbIO, WRITE_TOPOLOGY_CHUNK_SIZE, commoditiesToExclude);

        Map<Long, Map<Integer, Double>> capacities = Mockito.mock(Map.class);
        Multimap<Long, DelayedCommodityBoughtWriter> delayedCommoditiesBought = HashMultimap.create();
        Map<Long, TopologyEntityDTO> entityByOid = ImmutableMap.of(testAppWithoutProvider.getOid(),
                testAppWithoutProvider);

        // act
        for (int i=0; i < 10; i++) {
            marketStatsAccumulator.persistCommoditiesBought(testAppWithoutProvider,
                capacities, delayedCommoditiesBought, entityByOid);
        }
        // flush the stats, triggering the final insert
        marketStatsAccumulator.writeQueuedRows();

        // assert
        // 10 * 2 commodities -> 20 rows; 3 at a time -> 7 inserts;
        verify(historydbIO, times(7)).execute(eq(BasedbIO.Style.FORCED), eq(mockInsertStmt));
        // no additional unused getCommodityInsertStatement fetched => 7
        verify(historydbIO, times(7)).getCommodityInsertStatement(StatsTestUtils.APP_LATEST_TABLE);

    }

    /**
     * Test that the commodities bought from an entity not yet know are marked as pending.
     *
     */
    @Test
    public void testPersistCommoditiesBoughtUnavailable() throws Exception {
        // arrange
        InsertSetMoreStep mockInsertStmt = createMockInsertQuery(StatsTestUtils.APP_LATEST_TABLE);

        MarketStatsAccumulator marketStatsAccumulator = new MarketStatsAccumulator(
            TOPOLOGY_INFO, APP_ENTITY_TYPE, EnvironmentType.ON_PREM,
            historydbIO, WRITE_TOPOLOGY_CHUNK_SIZE, commoditiesToExclude);
        Map<Long, Map<Integer, Double>> capacities = Maps.newHashMap();
        Multimap<Long, DelayedCommodityBoughtWriter> delayedCommoditiesBought = HashMultimap.create();
        Map<Long, TopologyEntityDTO> entityByOid = ImmutableMap.of(testApp.getOid(), testApp);

        // act
        marketStatsAccumulator.persistCommoditiesBought(testApp,
                capacities, delayedCommoditiesBought, entityByOid);
        // flush the stats, triggering the final insert
        marketStatsAccumulator.writeQueuedRows();

        // assert
        // no stats will be inserted since the seller is unknown
        verify(historydbIO, times(0)).execute(eq(BasedbIO.Style.FORCED), eq(mockInsertStmt));
        // one unused getCommodityInsertStatement fetched
        verify(historydbIO, times(1)).getCommodityInsertStatement(StatsTestUtils.APP_LATEST_TABLE);
        // only one entity with delayed commodities
        assertThat(delayedCommoditiesBought.size(), is(1));
        // test that we've recorded the dependency from the app to the VM
        assertThat(delayedCommoditiesBought.containsKey(testVm.getOid()), is(true));
        // only one unknown entity that this app buys from
        assertThat(delayedCommoditiesBought.get(testVm.getOid()).size(), is(1));
    }

    @Test
    public void testPersistCommoditiesBoughtAvailableLater() throws Exception {
        // arrange
        InsertSetMoreStep mockInsertStmt = createMockInsertQuery(StatsTestUtils.APP_LATEST_TABLE);

        MarketStatsAccumulator marketStatsAccumulator = new MarketStatsAccumulator(
            TOPOLOGY_INFO, APP_ENTITY_TYPE, EnvironmentType.ON_PREM,
            historydbIO, WRITE_TOPOLOGY_CHUNK_SIZE, commoditiesToExclude);
        Map<Long, Map<Integer, Double>> capacities = Maps.newHashMap();
        Multimap<Long, DelayedCommodityBoughtWriter> delayedCommoditiesBought = HashMultimap.create();
        Map<Long, TopologyEntityDTO> entityByOid = ImmutableMap.of(testApp.getOid(), testApp);

        // accumulate commodities bought from a seller who is not yet known
        marketStatsAccumulator.persistCommoditiesBought(testApp,
                capacities, delayedCommoditiesBought, entityByOid);
        // save capacities from vm
        Map<Integer, Double> capacityMap = testVm.getCommoditySoldListList().stream().collect(Collectors.toMap(
                commodityDTO -> commodityDTO.getCommodityType().getType(),
                CommoditySoldDTO::getCapacity
        ));
        capacities.put(testVm.getOid(), capacityMap);

        // act - call the getters that were delayed
        for (DelayedCommodityBoughtWriter delayedWriter : delayedCommoditiesBought.get(
                testVm.getOid())) {
            delayedWriter.queCommoditiesNow();
        }

        // flush the stats, triggering the final insert
        marketStatsAccumulator.writeQueuedRows();

        // assert
        // vm - 2 commodities bought (1 commodity with percentile) -> 3 rows; 1 insert
        verify(historydbIO, times(1)).execute(eq(BasedbIO.Style.FORCED), eq(mockInsertStmt));
        // 2 unused getCommodityInsertStatement fetched
        verify(historydbIO, times(2)).getCommodityInsertStatement(StatsTestUtils.APP_LATEST_TABLE);
    }

    /**
     * Test that the max value is set correctly in the case that peak value is not present. It
     * should be the larger value of used and peak, rather than only considering peak alone.
     *
     * @throws Exception exception
     */
    @Test
    public void testPersistCommoditiesBoughtMaxValueSetCorrectly() throws Exception {
        // mock the db server, whose used is set, but peak is not set
        TopologyEntityDTO dbServer = TopologyEntityDTO.newBuilder()
                .setOid(72891)
                .setEntityType(EntityType.DATABASE_SERVER_VALUE)
                .addCommoditiesBoughtFromProviders(
                        CommoditiesBoughtFromProvider.newBuilder()
                                .setProviderId(testVm.getOid())
                                .addCommodityBought(CommodityBoughtDTO.newBuilder()
                                        .setCommodityType(CommodityType.newBuilder()
                                                .setType(CommodityDTO.CommodityType.DB_CACHE_HIT_RATE_VALUE))
                                        .setUsed(20)))
                .build();
        // arrange
        createMockInsertQuery(StatsTestUtils.APP_LATEST_TABLE);
        MarketStatsAccumulator marketStatsAccumulator = new MarketStatsAccumulator(
                TOPOLOGY_INFO, APP_ENTITY_TYPE, EnvironmentType.ON_PREM,
                historydbIO, WRITE_TOPOLOGY_CHUNK_SIZE, commoditiesToExclude);
        final Map<Long, Map<Integer, Double>> capacities = ImmutableMap.of(testVm.getOid(),
                ImmutableMap.of(CommodityDTO.CommodityType.DB_CACHE_HIT_RATE_VALUE, 100D));
        Map<Long, TopologyEntityDTO> entityByOid = ImmutableMap.of(testVm.getOid(), testVm);

        // act
        marketStatsAccumulator.persistCommoditiesBought(dbServer, capacities,
                HashMultimap.create(), entityByOid);

        Collection<MarketStatsData> statsData = marketStatsAccumulator.values();
        assertThat(statsData.size(), is(1));
        MarketStatsData mktStatsData = statsData.iterator().next();
        assertThat(mktStatsData.getUsed(), is(20D));
        // verify the max value is 20, not the peak value (0)
        assertThat(mktStatsData.getMax(), is(20D));
    }

    @Test
    public void testPersistEntityAttributes() throws Exception {
        // arrange
        InsertSetMoreStep mockInsertStmt = createMockInsertQuery(StatsTestUtils.PM_LATEST_TABLE);
        MarketStatsAccumulator marketStatsAccumulator = new MarketStatsAccumulator(TOPOLOGY_INFO,
            PM_ENTITY_TYPE, EnvironmentType.ON_PREM,
            historydbIO, WRITE_TOPOLOGY_CHUNK_SIZE, commoditiesToExclude);

        // act
        marketStatsAccumulator.persistEntityAttributes(testPm);
        marketStatsAccumulator.writeQueuedRows();

        // assert - 3 attribute rows (other attributes filtered) -> 1 write
        verify(historydbIO, times(1)).execute(eq(BasedbIO.Style.FORCED), eq(mockInsertStmt));
    }

    private InsertSetMoreStep createMockInsertQuery(Table table) {

        InsertSetMoreStep mockInsertStmt = Mockito.mock(InsertSetMoreStep.class);
        InsertSetStep mockInsertSetStep = Mockito.mock(InsertSetStep.class);

        // simulate the accumulation of DB values on the mockInsertStmt
        int[] count = {0};

        // reset the counter when we get a new insert stmt
        doAnswer(invocationOnMock -> {
            count[0] = 0;
            return mockInsertStmt;
        }).when(historydbIO).getCommodityInsertStatement(table);

        // increment the count when the method "newRecord()" is called, indicating end of row
        doAnswer(invocation -> {
            count[0] = count[0] + FIELDS_PER_ROW;
            return mockInsertSetStep;
        }).when(mockInsertStmt).newRecord();

        // return an array simulating the bind values with 'rows * FIELDS_PER_ROW' elements
        doAnswer(invocation -> {
                    // Return a list with 'n' integers. The values don't matter.
                    return IntStream.range(0, count[0]).boxed().collect(Collectors.toList());
                }
        ).when(mockInsertStmt).getBindValues();
        return mockInsertStmt;
    }

}
