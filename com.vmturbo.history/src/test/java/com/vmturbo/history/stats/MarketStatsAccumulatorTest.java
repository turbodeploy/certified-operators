package com.vmturbo.history.stats;

import static com.vmturbo.common.protobuf.utils.StringConstants.NUM_HOSTS;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;

import org.hamcrest.Matchers;
import org.jooq.Insert;
import org.jooq.InsertSetMoreStep;
import org.jooq.InsertSetStep;
import org.jooq.Query;
import org.jooq.Record;
import org.jooq.Table;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.TObjectDoubleMap;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.HistoricalValues;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PerTargetEntityInformation;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Builder;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.DiscoveryOrigin;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Origin;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.history.db.BasedbIO;
import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.db.VmtDbException;
import com.vmturbo.history.db.bulk.BulkLoader;
import com.vmturbo.history.db.bulk.SimpleBulkLoaderFactory;
import com.vmturbo.history.schema.abstraction.tables.AppStatsLatest;
import com.vmturbo.history.schema.abstraction.tables.HistUtilization;
import com.vmturbo.history.schema.abstraction.tables.records.HistUtilizationRecord;
import com.vmturbo.history.schema.abstraction.tables.records.VmStatsLatestRecord;
import com.vmturbo.history.stats.MarketStatsAccumulatorImpl.DelayedCommodityBoughtWriter;
import com.vmturbo.history.stats.MarketStatsAccumulatorImpl.MarketStatsData;
import com.vmturbo.history.stats.live.LiveStatsAggregator.CapacityCache;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Tests for the MarketStatsAccumulator.
 */
@RunWith(MockitoJUnitRunner.class)
public class MarketStatsAccumulatorTest {
    // TODO unify: revive tests

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

    private Set<String> commoditiesToExclude =
        ImmutableSet.copyOf(("ApplicationCommodity CLUSTERCommodity DATACENTERCommodity " +
            "DATASTORECommodity DSPMAccessCommodity NETWORKCommodity").toLowerCase()
            .split(" "));

    private static final int FIELDS_PER_ROW = (new VmStatsLatestRecord()).fieldsRow().size();

    // collect stats rows in groups of 3 to be written (inserted) into the DB
    private static final int WRITE_TOPOLOGY_CHUNK_SIZE = 3;

    // DTO's to use for testing
    private TopologyEntityDTO testPm;
    private TopologyEntityDTO testVm;
    private TopologyEntityDTO testApp;
    private TopologyEntityDTO testAppWithoutProvider;

    @Mock
    private HistorydbIO historydbIO;


    @Captor
    private ArgumentCaptor<Collection<MarketStatsData>> statsDataCaptor;

    @Captor
    private ArgumentCaptor<List<Query>> queryListCaptor;
    private BulkLoader<HistUtilizationRecord> mockBulkLoader;
    private MarketStatsAccumulatorImpl marketStatsAccumulator;

    /**
     * Create test entities used in tests.
     */
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
        mockBulkLoader = Mockito.mock(BulkLoader.class);
        final BulkLoader<?> loaderLatestTable = Mockito.mock(BulkLoader.class);
        final SimpleBulkLoaderFactory loaderFactory = mock(SimpleBulkLoaderFactory.class);
        when(loaderFactory.getLoader(any())).thenReturn((BulkLoader<Record>)loaderLatestTable);
        when(loaderFactory.getLoader(HistUtilization.HIST_UTILIZATION)).thenReturn(mockBulkLoader);
        this.marketStatsAccumulator =
                new MarketStatsAccumulatorImpl(TOPOLOGY_INFO, APP_ENTITY_TYPE, EnvironmentType.ON_PREM,
                        historydbIO, commoditiesToExclude, loaderFactory, new HashSet<>());
    }


    /**
     * Test that market stats are properly persisted.
     *
     * @throws Exception if there's a problem
     */
    @Ignore // TODO
    @Test
    public void testPersistMarketStats() throws Exception {
        // arrange
        String entityType = "PhysicalMachine";
//        MarketStatsAccumulator marketStatsAccumulator = new MarketStatsAccumulator(TOPOLOGY_INFO,
//            entityType, EnvironmentType.ON_PREM, historydbIO, commoditiesToExclude,
//            RecordWriterUtils.getRecordWriterFactory(historydbIO));

        // act
//        marketStatsAccumulator.persistMarketStats();

        // assert
//        assertThat(marketStatsAccumulator.values().size(), is(1));
        verify(historydbIO).getCommodityInsertStatement(StatsTestUtils.PM_LATEST_TABLE);
        verify(historydbIO).execute(Mockito.anyObject(), any(Insert.class));
// TODO        verify(historydbIO).getMarketStatsRecord(statsDataCaptor.capture(), eq(TOPOLOGY_INFO));
        verifyNoMoreInteractions(historydbIO);

        final MarketStatsData mktStatsData = statsDataCaptor.getValue().iterator().next();
        assertThat(mktStatsData.getPropertyType(), is(NUM_HOSTS));
        assertThat(mktStatsData.getEnvironmentType(), is(EnvironmentType.ON_PREM));
        assertThat(mktStatsData.getUsed(), is((double)0));

    }

    /**
     * Currently ignored, will be revisited.
     * @throws Exception if something goes wrong
     */
    @Ignore
    @Test
    public void testPersistCommoditiesSold() throws Exception {
        try (SimpleBulkLoaderFactory writers = mock(SimpleBulkLoaderFactory.class)) {
            BulkLoader<Record> writer = mock(BulkLoader.class);
            when(writers.getLoader(any())).thenReturn((BulkLoader<Record>)writer);
            // create the object under test
            String entityType = "PhysicalMachine";
//            MarketStatsAccumulator marketStatsAccumulator = new MarketStatsAccumulator(TOPOLOGY_INFO,
//                entityType, EnvironmentType.ON_PREM,
//                historydbIO, commoditiesToExclude, writers);
            List<CommoditySoldDTO> commoditiesSold = new ArrayList<>();
            commoditiesSold.add(StatsTestUtils.cpu(3.14));

            // act
            // write 10 stats rows - doesn't matter that they are the same
//            for (int i = 0; i < 10; i++) {
//                marketStatsAccumulator.persistCommoditiesSold(ENTITY_ID, commoditiesSold);
//            }

            // assert 10 rows
            verify(writer, times(10)).insert(any(Record.class));
        }
    }

    /**
     * Tests the amount of generated records for inserting information about timeslots and
     * percentile utilization for all commodities.
     *
     * @throws InterruptedException if interrupted.
     */
    @Test
    public void testAmountGeneratedRecords() throws InterruptedException {
        final List<CommoditySoldDTO> commoditiesSold = new ArrayList<>();
        commoditiesSold.add(StatsTestUtils.q1_vcpu(3.14));
        marketStatsAccumulator.persistCommoditiesSold(ENTITY_ID, commoditiesSold);
        final ArgumentCaptor<HistUtilizationRecord> recordArgumentCaptor =
                ArgumentCaptor.forClass(HistUtilizationRecord.class);
        final CapacityCache capacityCache = new CapacityCache();
        final Multimap<Long, DelayedCommodityBoughtWriter> delayedCommoditiesBought =
                HashMultimap.create();
        capacityCache.cacheCapacities(testVm);
        final Map<Long, TopologyEntityDTO> entityByOid = ImmutableMap.of(testVm.getOid(), testVm);
        marketStatsAccumulator.persistCommoditiesBought(testApp, capacityCache,
                delayedCommoditiesBought, entityByOid);
        Mockito.verify(mockBulkLoader, times(5)).insert(recordArgumentCaptor.capture());
        Assert.assertEquals(5, recordArgumentCaptor.getAllValues().size());
    }

    /**
     * Tests generated records parameters for inserting information about timeslots and percentile
     * utilization for sold commodities.
     *
     * @throws VmtDbException       If there is an error interacting with the database.
     * @throws InterruptedException if interrupted.
     */
    @Test
    public void testRecordsParametersSoldCommodity() throws InterruptedException, VmtDbException {
        final List<CommoditySoldDTO> commoditiesSold = new ArrayList<>();
        final CommoditySoldDTO soldCpu = StatsTestUtils.q1_vcpu(3.14);
        commoditiesSold.add(soldCpu);
        marketStatsAccumulator.persistCommoditiesSold(ENTITY_ID, commoditiesSold);
        final ArgumentCaptor<HistUtilizationRecord> recordArgumentCaptor =
                ArgumentCaptor.forClass(HistUtilizationRecord.class);
        Mockito.verify(mockBulkLoader, Mockito.atLeastOnce())
                .insert(recordArgumentCaptor.capture());
        final List<HistUtilizationRecord> allValues = recordArgumentCaptor.getAllValues();
        for (CommoditySoldDTO commoditySoldDTO : commoditiesSold) {
            final List<HistUtilizationRecord> records = allValues.stream()
                    .filter(rec -> rec.getPropertyTypeId() ==
                            commoditySoldDTO.getCommodityType().getType())
                    .collect(Collectors.toList());
            checkParametersRecordsByCommodityValues(ENTITY_ID, null, records,
                    commoditySoldDTO.getCommodityType(), commoditySoldDTO.getCapacity(),
                    commoditySoldDTO.getHistoricalUsed());
        }
    }

    /**
     * Tests generated records parameters for inserting information about timeslots and percentile
     * utilization for bought commodities.
     *
     * @throws VmtDbException       If there is an error interacting with the database.
     * @throws InterruptedException if interrupted.
     */
    @Test
    public void testRecordsParametersForBoughtCommodity()
            throws InterruptedException, VmtDbException {
        final CapacityCache capacityCache = new CapacityCache();
        capacityCache.cacheCapacities(testVm);
        final Multimap<Long, DelayedCommodityBoughtWriter> delayedCommoditiesBought =
                HashMultimap.create();
        final Map<Long, TopologyEntityDTO> entityByOid = ImmutableMap.of(testVm.getOid(), testVm);
        marketStatsAccumulator.persistCommoditiesBought(testApp, capacityCache,
                delayedCommoditiesBought, entityByOid);
        final CommoditiesBoughtFromProvider commoditiesBoughtFromProviders =
                testApp.getCommoditiesBoughtFromProviders(0);
        final ArgumentCaptor<HistUtilizationRecord> recordArgumentCaptor =
                ArgumentCaptor.forClass(HistUtilizationRecord.class);
        Mockito.verify(mockBulkLoader, Mockito.atLeastOnce())
                .insert(recordArgumentCaptor.capture());
        final TIntObjectMap<TObjectDoubleMap<String>> entityCapacities =
                capacityCache.getEntityCapacities(commoditiesBoughtFromProviders.getProviderId());
        for (CommodityBoughtDTO commodityBoughtDTO : commoditiesBoughtFromProviders.getCommodityBoughtList()) {
            final double[] capacityValues =
                    entityCapacities.get(commodityBoughtDTO.getCommodityType().getType()).values();
            final double capacity = capacityValues[0];
            final List<HistUtilizationRecord> records = recordArgumentCaptor.getAllValues()
                    .stream()
                    .filter(rec -> rec.getPropertyTypeId() ==
                            commodityBoughtDTO.getCommodityType().getType())
                    .collect(Collectors.toList());
            checkParametersRecordsByCommodityValues(testApp.getOid(),
                    commoditiesBoughtFromProviders.getProviderId(), records,
                    commodityBoughtDTO.getCommodityType(), capacity,
                    commodityBoughtDTO.getHistoricalUsed());
        }
    }

    private void checkParametersRecordsByCommodityValues(long oid, Long providerId,
            Collection<HistUtilizationRecord> records, CommodityType commodityType, double capacity,
            HistoricalValues historicalUsed) throws VmtDbException {
        Long checkProviderId = providerId;
        if (providerId == null) {
            checkProviderId = 0L;
        }
        int propertySlot = 0;
        for (HistUtilizationRecord record : records) {
            final HistoryUtilizationType historyUtilizationType =
                    HistoryUtilizationType.forNumber(record.getValueType());
            final boolean isTimeslot = historyUtilizationType == HistoryUtilizationType.Timeslot;
            final int newPropertySlot = isTimeslot ? propertySlot++ : 0;
            final double value = isTimeslot ? historicalUsed.getTimeSlot(newPropertySlot) :
                    historicalUsed.getPercentile();
            checkParametersRecord(oid, checkProviderId, record, commodityType, capacity,
                    historyUtilizationType, newPropertySlot, value);
        }
    }

    private void checkParametersRecord(long oid, Long providerId, HistUtilizationRecord record,
            CommodityType commodityType, double capacity, HistoryUtilizationType percentile,
            int propertySlot, double utilization) {
        Assert.assertThat(record.getOid(), Matchers.is(oid));
        Assert.assertEquals(providerId, record.getProducerOid());
        Assert.assertThat(commodityType.getType(), Matchers.is(record.getPropertyTypeId()));
        Assert.assertThat(PropertySubType.Utilization.ordinal(),
                Matchers.is(record.getPropertySubtypeId()));
        Assert.assertEquals(commodityType.getKey(), record.getCommodityKey());
        Assert.assertThat(percentile.ordinal(), Matchers.is(record.getValueType()));
        Assert.assertThat(propertySlot, Matchers.is(record.getPropertySlot()));
        Assert.assertEquals(BigDecimal.valueOf(utilization), record.getUtilization());
        Assert.assertThat(capacity, Matchers.is(record.getCapacity()));
    }

    /**
     * Currently ignored, will be revisited.
     * @throws Exception if something goes wrong
     */
    @Ignore
    @Test
    public void testExclusion() throws Exception {
        // arrange
        SimpleBulkLoaderFactory writers = mock(SimpleBulkLoaderFactory.class);
        BulkLoader writer = mock(BulkLoader.class);
        when(writers.getLoader(any())).thenReturn(writer);

        // create the object under test
        String entityType = "PhysicalMachine";
//        MarketStatsAccumulator marketStatsAccumulator = new MarketStatsAccumulator(
//                TOPOLOGY_INFO, entityType, EnvironmentType.ON_PREM,
//                historydbIO, commoditiesToExclude, writers);
        List<CommoditySoldDTO> commoditiesSold = new ArrayList<>();
        commoditiesSold.add(StatsTestUtils.dspma(50000));

        // act
//        marketStatsAccumulator.persistCommoditiesSold(ENTITY_ID, commoditiesSold);

        // assert - should be no writes, since the only commodity will be excluded
//        verify(historydbIO, times(0)).execute(eq(BasedbIO.Style.FORCED), eq(mockInsertStmt));
        verify(historydbIO, times(1)).getCommodityInsertStatement(StatsTestUtils.PM_LATEST_TABLE);
    }

    /**
     * Test that bought commodities are properly persisted.
     *
     * @throws Exception if there's a problem
     */
    @Ignore // TODO
    @Test
    public void testPersistCommoditiesBoughtAvailable() throws Exception {

        // arrange
        SimpleBulkLoaderFactory writers = mock(SimpleBulkLoaderFactory.class);
        BulkLoader writer = mock(BulkLoader.class);
        when(writers.getLoader(any())).thenReturn(writer);

//        MarketStatsAccumulator marketStatsAccumulator = new MarketStatsAccumulator(
//                TOPOLOGY_INFO, APP_ENTITY_TYPE, EnvironmentType.ON_PREM,
//                historydbIO, commoditiesToExclude, writers);
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
//        for (int i=0; i < 10; i++) {
//            marketStatsAccumulator.persistCommoditiesBought(testApp,
//                    capacities, delayedCommoditiesBought, entityByOid);
//        }

        // assert
        // 10 * 2 commodities -> 20 rows
        verify(writers, times(7)).getLoader(AppStatsLatest.APP_STATS_LATEST);
        verify(writer, times(20)).insert(any());
    }

    /**
     * Test that commodities bought without a provider are properly persisted.
     *
     * @throws Exception if there's a problem
     */
    @Ignore // TODO
    @Test
    public void testPersistCommoditiesBoughtWithoutProvider() throws Exception {
        // arrange
        SimpleBulkLoaderFactory writers = mock(SimpleBulkLoaderFactory.class);
        BulkLoader writer = mock(BulkLoader.class);
        when(writers.getLoader(any())).thenReturn(writer);

//        MarketStatsAccumulator marketStatsAccumulator = new MarketStatsAccumulator(
//            TOPOLOGY_INFO, APP_ENTITY_TYPE, EnvironmentType.ON_PREM,
//            historydbIO, commoditiesToExclude, writers);

        Map<Long, Map<Integer, Double>> capacities = mock(Map.class);
        Multimap<Long, DelayedCommodityBoughtWriter> delayedCommoditiesBought = HashMultimap.create();
        Map<Long, TopologyEntityDTO> entityByOid = ImmutableMap.of(testAppWithoutProvider.getOid(),
                testAppWithoutProvider);

        // act
//        for (int i=0; i < 10; i++) {
//            marketStatsAccumulator.persistCommoditiesBought(testAppWithoutProvider,
//                capacities, delayedCommoditiesBought, entityByOid);
//        }

        // assert
        // 10 * 2 commodities -> 20 rows
        verify(writers, times(7)).getLoader(AppStatsLatest.APP_STATS_LATEST);
        verify(writer, times(20)).insert(any());

    }

    /**
     * Test that the commodities bought from an entity not yet know are marked as pending.
     *
     * @throws Exception if there's a problem
     */
    @Ignore
    @Test
    public void testPersistCommoditiesBoughtUnavailable() throws Exception {
        // arrange
        SimpleBulkLoaderFactory writers = mock(SimpleBulkLoaderFactory.class);
        BulkLoader writer = mock(BulkLoader.class);
        when(writers.getLoader(any())).thenReturn(writer);

//        MarketStatsAccumulator marketStatsAccumulator = new MarketStatsAccumulator(
//            TOPOLOGY_INFO, APP_ENTITY_TYPE, EnvironmentType.ON_PREM,
//            historydbIO, commoditiesToExclude, writers);
        Map<Long, Map<Integer, Double>> capacities = Maps.newHashMap();
        Multimap<Long, DelayedCommodityBoughtWriter> delayedCommoditiesBought = HashMultimap.create();
        Map<Long, TopologyEntityDTO> entityByOid = ImmutableMap.of(testApp.getOid(), testApp);

        // act
//        marketStatsAccumulator.persistCommoditiesBought(testApp,
//                capacities, delayedCommoditiesBought, entityByOid);

        // assert
        // no stats will be inserted since the seller is unknown
        verify(writers, times(1)).getLoader(AppStatsLatest.APP_STATS_LATEST);
        verify(writer, times(0)).insert(any());

        // one unused getCommodityInsertStatement fetched
        // only one entity with delayed commodities
        assertThat(delayedCommoditiesBought.size(), is(1));
        // test that we've recorded the dependency from the app to the VM
        assertThat(delayedCommoditiesBought.containsKey(testVm.getOid()), is(true));
        // only one unknown entity that this app buys from
        assertThat(delayedCommoditiesBought.get(testVm.getOid()).size(), is(1));
    }

    /**
     * Test that commodities are persisted properly when provider appears later in the topology.
     *
     * @throws Exception if there's a problem
     */
    @Ignore // TODO
    @Test
    public void testPersistCommoditiesBoughtAvailableLater() throws Exception {
        // arrange
        SimpleBulkLoaderFactory writers = mock(SimpleBulkLoaderFactory.class);
        BulkLoader writer = mock(BulkLoader.class);
        when(writers.getLoader(any())).thenReturn(writer);

//        MarketStatsAccumulator marketStatsAccumulator = new MarketStatsAccumulator(
//            TOPOLOGY_INFO, APP_ENTITY_TYPE, EnvironmentType.ON_PREM,
//            historydbIO, commoditiesToExclude, writers);
        Map<Long, Map<Integer, Double>> capacities = Maps.newHashMap();
        Multimap<Long, DelayedCommodityBoughtWriter> delayedCommoditiesBought = HashMultimap.create();
        Map<Long, TopologyEntityDTO> entityByOid = ImmutableMap.of(testApp.getOid(), testApp);

        // accumulate commodities bought from a seller who is not yet known
//        marketStatsAccumulator.persistCommoditiesBought(testApp,
//                capacities, delayedCommoditiesBought, entityByOid);
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

        // assert
        // vm - 2 commodities bought -> 2 rows
        verify(writers, times(1)).getLoader(AppStatsLatest.APP_STATS_LATEST);
        verify(writer, times(1)).insert(any());
    }

    /**
     * Currently ignored, will be revisited.
     * @throws Exception if something goes wrong
     */
    @Ignore
    @Test
    public void testPersistEntityAttributes() throws Exception {
        // arrange
        SimpleBulkLoaderFactory writers = mock(SimpleBulkLoaderFactory.class);
        BulkLoader writer = mock(BulkLoader.class);
        when(writers.getLoader(any())).thenReturn(writer);

//        MarketStatsAccumulator marketStatsAccumulator = new MarketStatsAccumulator(TOPOLOGY_INFO,
//            PM_ENTITY_TYPE, EnvironmentType.ON_PREM,
//            historydbIO, commoditiesToExclude, writers);

        // act
//        marketStatsAccumulator.persistEntityAttributes(testPm);

        // assert - 3 attribute rows (other attributes filtered)
        verify(writer, times(1)).insert(any());
    }

    /**
     * Test that a volume-specific commodity key is not extracted if inapplicable to the situation.
     */
    @Test
    public void testBoughtCommodityKeyNotExtracted() {
//        final MarketStatsAccumulator accumulator =
//            new MarketStatsAccumulator(TOPOLOGY_INFO, VM_ENTITY_TYPE, EnvironmentType.CLOUD,
//                historydbIO, WRITE_TOPOLOGY_CHUNK_SIZE, commoditiesToExclude);

        // case when buyer entity is not in cloud

//        final Optional<String> extractedKey1 = accumulator.extractVolumeKey(
//            TopologyEntityDTO.getDefaultInstance(),
//            CommoditiesBoughtFromProvider.getDefaultInstance(), Collections.emptyMap());
//        assertFalse(extractedKey1.isPresent());


        final TopologyEntityDTO buyerEntity = TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
            .setOid(ENTITY_ID)
            .setEnvironmentType(EnvironmentType.CLOUD)
            .build();

        //case when bought commodity has no volume id

//        final Optional<String> extractedKey2 = accumulator.extractVolumeKey(buyerEntity,
//            CommoditiesBoughtFromProvider.getDefaultInstance(), Collections.emptyMap());
//        assertFalse(extractedKey2.isPresent());

        final long volumeId = 1234567;
        final CommoditiesBoughtFromProvider commoditiesBought =
            CommoditiesBoughtFromProvider.newBuilder().setVolumeId(volumeId).build();

        // case when bought commodity's volume does not appear in map

//        final Optional<String> extractedKey3 = accumulator.extractVolumeKey(buyerEntity,
//            commoditiesBought, Collections.emptyMap());
//        assertFalse(extractedKey3.isPresent());
    }

    /**
     * Test that a volume-specific commodity key is extracted if applicable to the situation.
     */
    @Ignore
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
//        final  MarketStatsAccumulator marketStatsAccumulator =
//            new MarketStatsAccumulator(TOPOLOGY_INFO, VM_ENTITY_TYPE, EnvironmentType.CLOUD,
//                historydbIO, WRITE_TOPOLOGY_CHUNK_SIZE, commoditiesToExclude);

        //case when volume entity has no origin

        entityMap.put(volumeId, volumeEntityBuilder.build());
//        final Optional<String> extractedKey1 =
//            marketStatsAccumulator.extractVolumeKey(buyerEntity, commoditiesBought, entityMap);
//        assertTrue(extractedKey1.isPresent());
//        assertThat(extractedKey1.get(), is("volume-name"));

        // case when volume's origin is not discovery

        final TopologyEntityDTO volumeEntity2 = volumeEntityBuilder
            .setOrigin(Origin.getDefaultInstance())
            .build();
        entityMap.put(volumeId, volumeEntity2);
//        final Optional<String> extractedKey2 =
//            marketStatsAccumulator.extractVolumeKey(buyerEntity, commoditiesBought, entityMap);
//        assertTrue(extractedKey2.isPresent());
//        assertThat(extractedKey2.get(), is("volume-name"));

        // case when volume has no vendor IDs

        final TopologyEntityDTO volumeEntity3 = volumeEntityBuilder
            .setOrigin(Origin.newBuilder()
                .setDiscoveryOrigin(DiscoveryOrigin.getDefaultInstance()))
            .build();
        entityMap.put(volumeId, volumeEntity3);
//        final Optional<String> extractedKey3 =
//            marketStatsAccumulator.extractVolumeKey(buyerEntity, commoditiesBought, entityMap);
//        assertTrue(extractedKey3.isPresent());
//        assertThat(extractedKey3.get(), is("volume-name"));

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
//        final Optional<String> extractedKey4 =
//            marketStatsAccumulator.extractVolumeKey(buyerEntity, commoditiesBought, entityMap);
//        assertTrue(extractedKey4.isPresent());
//        assertThat(extractedKey4.get(), is("volume-name - vendor-id"));

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
//        final Optional<String> extractedKey5 =
//            marketStatsAccumulator.extractVolumeKey(buyerEntity, commoditiesBought, entityMap);
//        assertTrue(extractedKey5.isPresent());
//        assertThat(extractedKey5.get(), is("volume-name"));

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
//        final Optional<String> extractedKey6 =
//            marketStatsAccumulator.extractVolumeKey(buyerEntity, commoditiesBought, entityMap);
//        assertTrue(extractedKey6.isPresent());
//        assertTrue(extractedKey6.get().equals("volume-name - vendor-id-1, vendor-id-2") ||
//            extractedKey6.get().equals("volume-name - vendor-id-2, vendor-id-1"));

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
//        final Optional<String> extractedKey7 =
//            marketStatsAccumulator.extractVolumeKey(buyerEntity, commoditiesBought, entityMap);
//        assertTrue(extractedKey7.isPresent());
//        assertThat(extractedKey7.get(), is("volume-name - vendor-id-1"));
    }

    private InsertSetMoreStep createMockInsertQuery(Table table) {

        InsertSetMoreStep mockInsertStmt = mock(InsertSetMoreStep.class);
        InsertSetStep mockInsertSetStep = mock(InsertSetStep.class);

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
