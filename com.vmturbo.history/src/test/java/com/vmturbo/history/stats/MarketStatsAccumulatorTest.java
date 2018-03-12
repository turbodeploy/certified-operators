package com.vmturbo.history.stats;

import static com.vmturbo.history.schema.StringConstants.NUM_HOSTS;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.collections.map.HashedMap;
import org.jooq.DSLContext;
import org.jooq.InsertSetMoreStep;
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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.history.db.BasedbIO;
import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.schema.abstraction.tables.records.VmStatsLatestRecord;
import com.vmturbo.history.stats.MarketStatsAccumulator.DelayedCommodityBoughtWriter;
import com.vmturbo.history.stats.MarketStatsAccumulator.MarketStatsData;
import com.vmturbo.history.utils.TopologyOrganizer;


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

    @Mock
    private TopologyOrganizer topologyOrganizer;

    @Captor
    private ArgumentCaptor<MarketStatsData> statsDataCaptor;

    @Captor
    private ArgumentCaptor<List<Query>> queryListCaptor;

    @Before
    public void setup() {
        when(topologyOrganizer.getSnapshotTime()).thenReturn(SNAPSHOT_TIME);
        when(topologyOrganizer.getTopologyContextId()).thenReturn(TOPOLOGY_CONTEXT_ID);
        when(topologyOrganizer.getTopologyId()).thenReturn(TOPOLOGY_ID);

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
        MarketStatsAccumulator marketStatsAccumulator = new MarketStatsAccumulator(entityType,
                historydbIO, WRITE_TOPOLOGY_CHUNK_SIZE, commoditiesToExclude);

        // act
        final int hostCount = 10;
        marketStatsAccumulator.persistMarketStats(hostCount, topologyOrganizer);

        // assert
        assertThat(marketStatsAccumulator.values().size(), is(1));
        verify(historydbIO).getCommodityInsertStatement(StatsTestUtils.PM_LATEST_TABLE);
        verify(historydbIO).execute(Mockito.anyObject(), queryListCaptor.capture());
        verify(historydbIO).getMarketStatsInsertStmt(statsDataCaptor.capture(), eq(SNAPSHOT_TIME),
                eq(TOPOLOGY_CONTEXT_ID), eq(TOPOLOGY_ID));
        verifyNoMoreInteractions(historydbIO);
        assertThat(statsDataCaptor.getValue().getPropertyType(), is(NUM_HOSTS));
        assertThat(statsDataCaptor.getValue().getUsed(), is((double)hostCount));

    }

    @Test
    public void testPersistCommoditiesSold() throws Exception {
        // arrange
        InsertSetMoreStep mockInsertStmt = createMockInsertQuery(StatsTestUtils.PM_LATEST_TABLE);

        // create the object under test
        String entityType = "PhysicalMachine";
        MarketStatsAccumulator marketStatsAccumulator = new MarketStatsAccumulator(entityType,
                historydbIO, WRITE_TOPOLOGY_CHUNK_SIZE, commoditiesToExclude);
        List<CommoditySoldDTO> commoditiesSold = new ArrayList<>();
        commoditiesSold.add(StatsTestUtils.cpu(3.14));

        // act
        // write 10 stats rows - doesn't matter that they are the same
        for (int i=0; i < 10; i++) {
            marketStatsAccumulator.persistCommoditiesSold(SNAPSHOT_TIME, ENTITY_ID, commoditiesSold);
        }
        // flush the stats, triggering the final insert
        marketStatsAccumulator.writeQueuedRows();

        // assert 10 rows, buffering 3 at a time -> 4 insert statements total
        verify(historydbIO, times(4)).getCommodityInsertStatement(StatsTestUtils.PM_LATEST_TABLE);
        verify(historydbIO, times(4)).execute(eq(BasedbIO.Style.FORCED), eq(mockInsertStmt));
    }


    @Test
    public void testExclusion() throws Exception {
        // arrange
        InsertSetMoreStep mockInsertStmt = createMockInsertQuery(StatsTestUtils.PM_LATEST_TABLE);

        // create the object under test
        String entityType = "PhysicalMachine";
        MarketStatsAccumulator marketStatsAccumulator = new MarketStatsAccumulator(entityType,
                historydbIO, WRITE_TOPOLOGY_CHUNK_SIZE, commoditiesToExclude);
        List<CommoditySoldDTO> commoditiesSold = new ArrayList<>();
        commoditiesSold.add(StatsTestUtils.dspma(50000));

        // act
        marketStatsAccumulator.persistCommoditiesSold(SNAPSHOT_TIME, ENTITY_ID, commoditiesSold);

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

        MarketStatsAccumulator marketStatsAccumulator = new MarketStatsAccumulator(APP_ENTITY_TYPE,
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

        // act
        for (int i=0; i < 10; i++) {
            marketStatsAccumulator.persistCommoditiesBought(SNAPSHOT_TIME, testApp,
                    capacities, delayedCommoditiesBought);
        }
        // flush the stats, triggering the final insert
        marketStatsAccumulator.writeQueuedRows();

        // assert
        // 10 * 2 commodities -> 20 rows; 3 at a time -> 7 inserts;
        verify(historydbIO, times(7)).execute(eq(BasedbIO.Style.FORCED), eq(mockInsertStmt));
        // no additional unused getCommodityInsertStatement fetched => 7
        verify(historydbIO, times(7)).getCommodityInsertStatement(StatsTestUtils.APP_LATEST_TABLE);
    }

    @Test
    public void testPersistCommoditiesBoughtWithoutProvider() throws Exception {
        // arrange
        InsertSetMoreStep mockInsertStmt = createMockInsertQuery(StatsTestUtils.APP_LATEST_TABLE);

        MarketStatsAccumulator marketStatsAccumulator = new MarketStatsAccumulator(APP_ENTITY_TYPE,
            historydbIO, WRITE_TOPOLOGY_CHUNK_SIZE, commoditiesToExclude);

        Map<Long, Map<Integer, Double>> capacities = Mockito.mock(HashedMap.class);
        Multimap<Long, DelayedCommodityBoughtWriter> delayedCommoditiesBought = HashMultimap.create();

        // act
        for (int i=0; i < 10; i++) {
            marketStatsAccumulator.persistCommoditiesBought(SNAPSHOT_TIME, testAppWithoutProvider,
                capacities, delayedCommoditiesBought);
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

        MarketStatsAccumulator marketStatsAccumulator = new MarketStatsAccumulator(APP_ENTITY_TYPE,
                historydbIO, WRITE_TOPOLOGY_CHUNK_SIZE, commoditiesToExclude);
        Map<Long, Map<Integer, Double>> capacities = Maps.newHashMap();
        Multimap<Long, DelayedCommodityBoughtWriter> delayedCommoditiesBought = HashMultimap.create();

        // act
        marketStatsAccumulator.persistCommoditiesBought(SNAPSHOT_TIME, testApp,
                capacities, delayedCommoditiesBought);
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

        MarketStatsAccumulator marketStatsAccumulator = new MarketStatsAccumulator(APP_ENTITY_TYPE,
                historydbIO, WRITE_TOPOLOGY_CHUNK_SIZE, commoditiesToExclude);
        Map<Long, Map<Integer, Double>> capacities = Maps.newHashMap();
        Multimap<Long, DelayedCommodityBoughtWriter> delayedCommoditiesBought = HashMultimap.create();

        // accumulate commodities bought from a seller who is not yet known
        marketStatsAccumulator.persistCommoditiesBought(SNAPSHOT_TIME, testApp,
                capacities, delayedCommoditiesBought);
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
        // vm - 2 commodities bought -> 2 rows; 1 insert
        verify(historydbIO, times(1)).execute(eq(BasedbIO.Style.FORCED), eq(mockInsertStmt));
        // one unused getCommodityInsertStatement fetched
        verify(historydbIO, times(1)).getCommodityInsertStatement(StatsTestUtils.APP_LATEST_TABLE);
    }

    @Test
    public void testPersistEntityAttributes() throws Exception {
        // arrange
        InsertSetMoreStep mockInsertStmt = createMockInsertQuery(StatsTestUtils.PM_LATEST_TABLE);
        MarketStatsAccumulator marketStatsAccumulator = new MarketStatsAccumulator(PM_ENTITY_TYPE,
                historydbIO, WRITE_TOPOLOGY_CHUNK_SIZE, commoditiesToExclude);

        // act
        marketStatsAccumulator.persistEntityAttributes(SNAPSHOT_TIME, testPm);
        marketStatsAccumulator.writeQueuedRows();

        // assert - 3 attribute rows (other attributes filtered) -> 1 write
        verify(historydbIO, times(1)).execute(eq(BasedbIO.Style.FORCED), eq(mockInsertStmt));
    }

    private InsertSetMoreStep createMockInsertQuery(Table table) {

        InsertSetMoreStep mockInsertStmt = Mockito.mock(InsertSetMoreStep.class);
        DSLContext mockDSLContext = Mockito.mock(DSLContext.class);
        when(mockDSLContext.insertInto(table)).thenReturn(mockInsertStmt);

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
            return mockInsertStmt;
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