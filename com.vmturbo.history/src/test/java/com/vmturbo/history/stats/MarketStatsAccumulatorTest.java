package com.vmturbo.history.stats;

import static com.vmturbo.components.common.utils.StringConstants.NUM_HOSTS;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
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
import com.vmturbo.common.protobuf.topology.TopologyDTO.PerTargetEntityInformation;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Builder;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.DiscoveryOrigin;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Origin;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.history.db.BasedbIO;
import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.schema.abstraction.tables.records.VmStatsLatestRecord;
import com.vmturbo.history.stats.MarketStatsAccumulator.DelayedCommodityBoughtWriter;
import com.vmturbo.history.stats.MarketStatsAccumulator.MarketStatsData;
import com.vmturbo.history.stats.live.LiveStatsAggregator.CapacityCache;
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
    private static final String VM_ENTITY_TYPE = "VirtualMachine";
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
        CapacityCache capacityCache = new CapacityCache();
        Multimap<Long, DelayedCommodityBoughtWriter> delayedCommoditiesBought = HashMultimap.create();

        // calculate the commodities sold by the provider (vm)

        capacityCache.cacheCapacities(testVm);
        Map<Long, TopologyEntityDTO> entityByOid = ImmutableMap.of(testVm.getOid(), testVm);

        // act
        for (int i = 0; i < 10; i++) {
            marketStatsAccumulator.persistCommoditiesBought(testApp,
                    capacityCache, delayedCommoditiesBought, entityByOid);
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

        CapacityCache capacityCache = Mockito.mock(CapacityCache.class);
        Multimap<Long, DelayedCommodityBoughtWriter> delayedCommoditiesBought = HashMultimap.create();
        Map<Long, TopologyEntityDTO> entityByOid = ImmutableMap.of(testAppWithoutProvider.getOid(),
                testAppWithoutProvider);

        // act
        for (int i=0; i < 10; i++) {
            marketStatsAccumulator.persistCommoditiesBought(testAppWithoutProvider,
                    capacityCache, delayedCommoditiesBought, entityByOid);
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
        CapacityCache capacityCache = new CapacityCache();
        Multimap<Long, DelayedCommodityBoughtWriter> delayedCommoditiesBought = HashMultimap.create();
        Map<Long, TopologyEntityDTO> entityByOid = ImmutableMap.of(testApp.getOid(), testApp);

        // act
        marketStatsAccumulator.persistCommoditiesBought(testApp,
                capacityCache, delayedCommoditiesBought, entityByOid);
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
        CapacityCache capacityCache = new CapacityCache();
        Multimap<Long, DelayedCommodityBoughtWriter> delayedCommoditiesBought = HashMultimap.create();
        Map<Long, TopologyEntityDTO> entityByOid = ImmutableMap.of(testApp.getOid(), testApp);

        // accumulate commodities bought from a seller who is not yet known
        marketStatsAccumulator.persistCommoditiesBought(testApp,
                capacityCache, delayedCommoditiesBought, entityByOid);
        // save capacities from vm
        capacityCache.cacheCapacities(testVm);

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
        CapacityCache capacityCache = new CapacityCache();
        capacityCache.cacheCapacity(
                testVm.getOid(), CommodityDTO.CommodityType.DB_CACHE_HIT_RATE_VALUE, 100D);
        Map<Long, TopologyEntityDTO> entityByOid = ImmutableMap.of(testVm.getOid(), testVm);

        // act
        marketStatsAccumulator.persistCommoditiesBought(dbServer, capacityCache,
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

    /**
     * Test that a volume-specific commodity key is not extracted if inapplicable to the situation.
     */
    @Test
    public void testBoughtCommodityKeyNotExtracted() {
        final MarketStatsAccumulator accumulator =
            new MarketStatsAccumulator(TOPOLOGY_INFO, VM_ENTITY_TYPE, EnvironmentType.CLOUD,
                historydbIO, WRITE_TOPOLOGY_CHUNK_SIZE, commoditiesToExclude);

        // case when buyer entity is not in cloud

        final Optional<String> extractedKey1 = accumulator.extractVolumeKey(
            TopologyEntityDTO.getDefaultInstance(),
            CommoditiesBoughtFromProvider.getDefaultInstance(), Collections.emptyMap());
        assertFalse(extractedKey1.isPresent());


        final TopologyEntityDTO buyerEntity = TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
            .setOid(ENTITY_ID)
            .setEnvironmentType(EnvironmentType.CLOUD)
            .build();

        //case when bought commodity has no volume id

        final Optional<String> extractedKey2 = accumulator.extractVolumeKey(buyerEntity,
            CommoditiesBoughtFromProvider.getDefaultInstance(), Collections.emptyMap());
        assertFalse(extractedKey2.isPresent());

        final long volumeId = 1234567;
        final CommoditiesBoughtFromProvider commoditiesBought =
            CommoditiesBoughtFromProvider.newBuilder().setVolumeId(volumeId).build();

        // case when bought commodity's volume does not appear in map

        final Optional<String> extractedKey3 = accumulator.extractVolumeKey(buyerEntity,
            commoditiesBought, Collections.emptyMap());
        assertFalse(extractedKey3.isPresent());
    }

    /**
     * Test that a volume-specific commodity key is extracted if applicable to the situation.
     */
    @Test
    public void testBoughtCommodityKeyVolumeDisplayName() {
        final long volumeId = 1234567;
        final TopologyEntityDTO buyerEntity = TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
            .setOid(ENTITY_ID)
            .setEnvironmentType(EnvironmentType.CLOUD)
            .build();
        final CommoditiesBoughtFromProvider commoditiesBought =
            CommoditiesBoughtFromProvider.newBuilder().setVolumeId(volumeId).build();
        final Builder volumeEntityBuilder = TopologyEntityDTO.newBuilder()
            .setOid(volumeId)
            .setEntityType(EntityType.VIRTUAL_VOLUME_VALUE)
            .setDisplayName("volume-name");
        final Map<Long, TopologyEntityDTO> entityMap = new HashMap<>();
        final  MarketStatsAccumulator marketStatsAccumulator =
            new MarketStatsAccumulator(TOPOLOGY_INFO, VM_ENTITY_TYPE, EnvironmentType.CLOUD,
                historydbIO, WRITE_TOPOLOGY_CHUNK_SIZE, commoditiesToExclude);

        //case when volume entity has no origin

        entityMap.put(volumeId, volumeEntityBuilder.build());
        final Optional<String> extractedKey1 =
            marketStatsAccumulator.extractVolumeKey(buyerEntity, commoditiesBought, entityMap);
        assertTrue(extractedKey1.isPresent());
        assertThat(extractedKey1.get(), is("volume-name"));

        // case when volume's origin is not discovery

        final TopologyEntityDTO volumeEntity2 = volumeEntityBuilder
            .setOrigin(Origin.getDefaultInstance())
            .build();
        entityMap.put(volumeId, volumeEntity2);
        final Optional<String> extractedKey2 =
            marketStatsAccumulator.extractVolumeKey(buyerEntity, commoditiesBought, entityMap);
        assertTrue(extractedKey2.isPresent());
        assertThat(extractedKey2.get(), is("volume-name"));

        // case when volume has no vendor IDs

        final TopologyEntityDTO volumeEntity3 = volumeEntityBuilder
            .setOrigin(Origin.newBuilder()
                .setDiscoveryOrigin(DiscoveryOrigin.getDefaultInstance()))
            .build();
        entityMap.put(volumeId, volumeEntity3);
        final Optional<String> extractedKey3 =
            marketStatsAccumulator.extractVolumeKey(buyerEntity, commoditiesBought, entityMap);
        assertTrue(extractedKey3.isPresent());
        assertThat(extractedKey3.get(), is("volume-name"));

        // case when volume has a vendor ID

        final TopologyEntityDTO volumeEntity4 = volumeEntityBuilder
            .setOrigin(Origin.newBuilder()
                .setDiscoveryOrigin(DiscoveryOrigin.newBuilder()
                    .putAllDiscoveredTargetData(ImmutableMap.of(
                        1L, PerTargetEntityInformation.newBuilder()
                            .setVendorId("vendor-id").build()
                    ))))
            .build();
        entityMap.put(volumeId, volumeEntity4);
        final Optional<String> extractedKey4 =
            marketStatsAccumulator.extractVolumeKey(buyerEntity, commoditiesBought, entityMap);
        assertTrue(extractedKey4.isPresent());
        assertThat(extractedKey4.get(), is("volume-name - vendor-id"));

        // case when volume's vendor ID is the same as its display name

        final TopologyEntityDTO volumeEntity5 = volumeEntityBuilder
            .setOrigin(Origin.newBuilder()
                .setDiscoveryOrigin(DiscoveryOrigin.newBuilder()
                    .putAllDiscoveredTargetData(ImmutableMap.of(
                        1L, PerTargetEntityInformation.newBuilder()
                            .setVendorId("volume-name").build()
                    ))))
            .build();
        entityMap.put(volumeId, volumeEntity5);
        final Optional<String> extractedKey5 =
            marketStatsAccumulator.extractVolumeKey(buyerEntity, commoditiesBought, entityMap);
        assertTrue(extractedKey5.isPresent());
        assertThat(extractedKey5.get(), is("volume-name"));

        // case when volume has multiple vendor IDs

        final TopologyEntityDTO volumeEntity6 = volumeEntityBuilder
            .setOrigin(Origin.newBuilder()
                .setDiscoveryOrigin(DiscoveryOrigin.newBuilder()
                    .putAllDiscoveredTargetData(ImmutableMap.of(
                        1L, PerTargetEntityInformation.newBuilder()
                            .setVendorId("vendor-id-1").build(),
                        2L, PerTargetEntityInformation.newBuilder()
                            .setVendorId("vendor-id-2").build())
                    )))
            .build();
        entityMap.put(volumeId, volumeEntity6);
        final Optional<String> extractedKey6 =
            marketStatsAccumulator.extractVolumeKey(buyerEntity, commoditiesBought, entityMap);
        assertTrue(extractedKey6.isPresent());
        assertTrue(extractedKey6.get().equals("volume-name - vendor-id-1, vendor-id-2") ||
            extractedKey6.get().equals("volume-name - vendor-id-2, vendor-id-1"));

        // case when volume has multiple vendor IDs, one of which is its name

        final TopologyEntityDTO volumeEntity7 = volumeEntityBuilder
            .setOrigin(Origin.newBuilder()
                .setDiscoveryOrigin(DiscoveryOrigin.newBuilder()
                    .putAllDiscoveredTargetData(ImmutableMap.of(
                        1L, PerTargetEntityInformation.newBuilder()
                            .setVendorId("vendor-id-1").build(),
                        2L, PerTargetEntityInformation.newBuilder()
                            .setVendorId("volume-name").build())
                    )))
            .build();
        entityMap.put(volumeId, volumeEntity7);
        final Optional<String> extractedKey7 =
            marketStatsAccumulator.extractVolumeKey(buyerEntity, commoditiesBought, entityMap);
        assertTrue(extractedKey7.isPresent());
        assertThat(extractedKey7.get(), is("volume-name - vendor-id-1"));
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
