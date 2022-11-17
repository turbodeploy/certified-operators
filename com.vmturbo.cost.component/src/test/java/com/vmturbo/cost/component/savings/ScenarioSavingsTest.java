package com.vmturbo.cost.component.savings;

import static com.vmturbo.cost.component.util.TestUtils.getTimeMillis;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.time.Clock;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.gson.stream.JsonReader;
import com.google.protobuf.Message.Builder;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import org.jooq.DSLContext;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.vmturbo.cloud.common.topology.TopologyEntityCloudTopologyFactory;
import com.vmturbo.common.protobuf.action.ActionDTO.ExecutedActionsChangeWindow;
import com.vmturbo.common.protobuf.cost.Cost.EntitySavingsStatsType;
import com.vmturbo.common.protobuf.search.Search.SearchEntitiesRequest;
import com.vmturbo.common.protobuf.search.Search.SearchEntitiesResponse;
import com.vmturbo.common.protobuf.search.SearchMoles.SearchServiceMole;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.cost.component.db.Cost;
import com.vmturbo.cost.component.pricing.BusinessAccountPriceTableKeyStore;
import com.vmturbo.cost.component.pricing.PriceTableStore;
import com.vmturbo.cost.component.savings.DataInjectionMonitor.ScriptEvent;
import com.vmturbo.cost.component.savings.bottomup.AggregatedSavingsStats;
import com.vmturbo.cost.component.savings.bottomup.SqlEntitySavingsStore;
import com.vmturbo.cost.component.savings.calculator.Calculator;
import com.vmturbo.cost.component.savings.calculator.StorageAmountResolver;
import com.vmturbo.cost.component.util.TestUtils;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.PricingDTO.StorageTierPriceList;
import com.vmturbo.sql.utils.DbConfigurationRule;

/**
 * Pass in files ending with
 * *-input.json from /savings/scenario folder as inputs
 * Compare the stats result with the matching file ending with *-output.csv
 * The prefix of the file names should match.
 */
@RunWith(Parameterized.class)
public class ScenarioSavingsTest {

    private static final Logger logger = LogManager.getLogger();

    /**
     *  DB config.
     */
    @ClassRule
    public static final DbConfigurationRule dbConfig = new DbConfigurationRule(Cost.COST);

    /**
     * Provide test parameters.
     *
     * @return test parameters
     */
    @Parameters
    public static Object[] parameters() {
        File folder = new File("src/test/resources/savings/scenario");
        return Arrays.stream(folder.listFiles()).filter(file -> file.getName().endsWith("-input.json")).toArray();
    }

    /**
     * Processor reference.
     */
    private static SavingsTracker savingsTracker;

    private static SqlEntitySavingsStore savingsStore;

    private final String filePath;

    private static final BusinessAccountPriceTableKeyStore priceTableKeyStore = mock(BusinessAccountPriceTableKeyStore.class);
    private static final PriceTableStore priceTableStore = mock(PriceTableStore.class);
    private static final StorageAmountResolver storageAmountResolver = spy(new StorageAmountResolver(priceTableKeyStore, priceTableStore));

    private static final long STANDARD_HDD_DISK_TIER_OID = 20000L;
    private static final long ULTRA_DISK_TIER_OID = 35000L;
    private static final long PREMIUM_DISK_TIER_OID = 30000L;

    private static final Set<EntityType> supportedEntityTypes = ImmutableSet.of(EntityType.VIRTUAL_VOLUME, EntityType.DATABASE);

    private static final Set<String> supportedCSPs = ImmutableSet.of("Azure");

    Set<Long> participatingUuids = new HashSet<>();
    Set<Long> expectedUuids = new HashSet<>();
    private static Map<Long, StorageTierPriceList> priceListMap = new HashMap<>();

    private static final SearchServiceMole searchServiceMole = spy(new SearchServiceMole());

    /**
     * GRPC server.
     */
    @ClassRule
    public static GrpcTestServer grpcServer = GrpcTestServer.newServer(searchServiceMole);

    @BeforeClass
    public static void setup() throws IOException {
        // Set this to TRACE and Change trax verbosity level manually to get see the trax output.
        Configurator.setAllLevels("com.vmturbo.cost.component.savings", Level.INFO);
        int chunkSize = 1000;
        DSLContext dsl = dbConfig.getDslContext();
        Clock clock =  Clock.systemUTC();
        savingsStore = new SqlEntitySavingsStore(dsl, clock, chunkSize, true);
        grpcServer.start();
        SearchEntitiesResponse response = SearchEntitiesResponse.newBuilder().addEntities(
                PartialEntity.newBuilder().setMinimal(
                        MinimalEntity.newBuilder().setOid(1234L).build()).build()).build();
        when(searchServiceMole.searchEntities(any(SearchEntitiesRequest.class)))
                .thenReturn(response);
        final SearchServiceBlockingStub searchService =
                SearchServiceGrpc.newBlockingStub(grpcServer.getChannel());
        long deleteActionRetentionMs = TimeUnit.DAYS.toMillis(365);
        savingsTracker = spy(new SavingsTracker(
                new SqlBillingRecordStore(dsl),
                mock(GrpcActionChainStore.class),
                savingsStore,
                supportedEntityTypes,
                supportedCSPs,
                deleteActionRetentionMs,
                clock, mock(TopologyEntityCloudTopologyFactory.class),
                null, dsl, priceTableKeyStore,
                priceTableStore, searchService, 0, 777777, chunkSize));
        savingsTracker.setCalculator(new Calculator(deleteActionRetentionMs, clock, storageAmountResolver));
        doReturn(priceListMap).when(storageAmountResolver).getStoragePriceTiers(anyLong(), anyLong());

        doReturn(true).when(savingsTracker).isSupportedCSP(any());

        // Create price list map.
        final Builder standHdd = StorageTierPriceList.newBuilder();
        assertTrue(TestUtils.loadProtobufBuilder("/savings/storageTierPriceStandardHDD.json",
                standHdd, null));

        priceListMap.put(ScenarioGenerator.providerTypeToProviderId.get("STANDARDHDD"), (StorageTierPriceList)(standHdd.build()));
        final Builder standardSsd = StorageTierPriceList.newBuilder();
        assertTrue(TestUtils.loadProtobufBuilder("/savings/storageTierPriceStandardSSD.json",
                standardSsd, null));
        priceListMap.put(ScenarioGenerator.providerTypeToProviderId.get("STANDARDSDD"), (StorageTierPriceList)(standardSsd.build()));

        final Builder ultra = StorageTierPriceList.newBuilder();
        assertTrue(TestUtils.loadProtobufBuilder("/savings/storageTierPriceUltra.json",
                ultra, null));
        priceListMap.put(ScenarioGenerator.providerTypeToProviderId.get("ULTRA"), (StorageTierPriceList)(ultra.build()));

        final Builder premium = StorageTierPriceList.newBuilder();
        final Builder premiumSsd = StorageTierPriceList.newBuilder();
        assertTrue(TestUtils.loadProtobufBuilder("/savings/storageTierPricePremiumSSD.json",
                premiumSsd, null));
        priceListMap.put(ScenarioGenerator.providerTypeToProviderId.get("PREMIUM"), (StorageTierPriceList)(premiumSsd.build()));
    }

    /**
     *
     * @param file All the *-input.json files from /savings/scenario
     */
    public ScenarioSavingsTest(File file) {
        this.filePath = "/savings/scenario/" + file.getName();
    }

    enum SavingsResultsHeader {
        entity_oid,
        /**
         * Date for the stats, e.g. '2022-01-02T00:00'
         */
        timestamp,

        /**
         * Realized savings value expected.
         */
        savings,

        /**
         * Realized investments value expected.
         */
        investments
    }

    @Test
    public void savingsScenarioTest() throws EntitySavingsException {
        logger.info("Test running for input file: " + filePath);
        // parse the input actions
        injectActions();
        // Map of date timestamp to StatsValue having realized savings/investment, for comparing
        // with actual test results.
        final List<AggregatedSavingsStats> expectedResults = loadExpectedResults();
        assertFalse(expectedResults.isEmpty());
        assertTrue(CollectionUtils.isEqualCollection(participatingUuids, expectedUuids));

        // Read savings results from stats DB table and verify with expected results.
        final List<AggregatedSavingsStats> dailyStats = savingsStore.getDailyStats(
                ImmutableSet.of(EntitySavingsStatsType.REALIZED_SAVINGS,
                        EntitySavingsStatsType.REALIZED_INVESTMENTS), 0L,
                System.currentTimeMillis(), participatingUuids,
                ImmutableList.of(EntityType.VIRTUAL_MACHINE_VALUE), Collections.emptyList());

        // Verify that all savings and investments match up with expected results.
        validateResults(dailyStats, expectedResults);
    }

    private void validateResults(List<AggregatedSavingsStats> results, List<AggregatedSavingsStats> expectedResults) {
        Assert.assertEquals(expectedResults.size(), results.size());
        Assert.assertEquals(expectedResults.stream().map(AggregatedSavingsStats::getTimestamp).collect(Collectors.toList()),
                results.stream().map(AggregatedSavingsStats::getTimestamp).collect(Collectors.toList()));
        Map<Long, List<AggregatedSavingsStats>> resultsMap =
                results.stream().collect(Collectors.groupingBy(AggregatedSavingsStats::getTimestamp));
        Map<Long, List<AggregatedSavingsStats>> expectedResultsMap =
                expectedResults.stream().collect(Collectors.groupingBy(AggregatedSavingsStats::getTimestamp));
        expectedResults.forEach(r -> {
            Map<EntitySavingsStatsType, Double> savingsValues =
                    resultsMap.get(r.getTimestamp()).stream().collect(Collectors.toMap(AggregatedSavingsStats::getType, AggregatedSavingsStats::getValue));
            Map<EntitySavingsStatsType, Double> expectedSavingsValues =
                    expectedResultsMap.get(r.getTimestamp()).stream().collect(Collectors.toMap(AggregatedSavingsStats::getType, AggregatedSavingsStats::getValue));
            expectedSavingsValues.forEach((type, value) -> Assert.assertEquals(savingsValues.get(type), expectedSavingsValues.get(type), 0.0001));
        });
    }

    /**
     * Inject the actions from the json file,
     * calculate the savings with the savings
     * calculator and update the db with the values
     */
    private void injectActions() {
        ScenarioDataInjector scenarioDataInjector = new BillingDataInjector();

        Gson gson = new Gson();
        JsonReader reader;
        List<ScriptEvent> scriptEvents;
        try {
            reader = TestUtils.readJsonFile(filePath, getClass());
            scriptEvents = Arrays.asList(gson.fromJson(reader,
                    scenarioDataInjector.getScriptEventClass()));
        } catch (IOException e) {
            fail("There is syntax error in the Json file.");
            return;
        } catch (JsonSyntaxException e) {
            fail("There is error injecting events");
            return;
        }
        if (scriptEvents.isEmpty()) {
            fail("There are no events in the input file");
            return;
        }

        // Find OIDs of entities referenced in the script events.
        Map<String, Long> uuidMap = makeDummyOid(scriptEvents.stream()
                .map(e -> e.uuid)
                // If the event is not associated with an entity, skip it.
                .filter(ScenarioSavingsTest::uuidIsValid)
                .collect(Collectors.toSet()));
        participatingUuids.clear();
        // Determine the scope of the scenario: participating UUIDs and the time period.
        long earliestEventTime = Long.MAX_VALUE;
        long latestEventTime = Long.MIN_VALUE;
        for (ScriptEvent event : scriptEvents) {
            earliestEventTime = Math.min(earliestEventTime, event.timestamp);
            latestEventTime = Math.max(latestEventTime, event.timestamp);
            if (uuidIsValid(event.uuid)) {
                participatingUuids.add(uuidMap.get(event.uuid));
            }
        }
        savingsTracker.purgeState(participatingUuids);
        if (earliestEventTime > latestEventTime) {
            fail("No events in script file - not running savings tracker");
            return;
        }
        LocalDateTime startTime = DataInjectionMonitor.makeLocalDateTime(earliestEventTime, false);
        LocalDateTime endTime = DataInjectionMonitor.makeLocalDateTime(latestEventTime, true);

        doReturn(priceListMap).when(storageAmountResolver).getStoragePriceTiers(anyLong(), anyLong());

        // Handle the script events.
        final AtomicBoolean purgePreviousTestState = new AtomicBoolean(false);
        final Map<Long, NavigableSet<ExecutedActionsChangeWindow>> actionChains = new HashMap<>();
        final Map<Long, Set<BillingRecord>> billRecordsByEntity = new HashMap<>();
        scenarioDataInjector.handleScriptEvents(scriptEvents, uuidMap, purgePreviousTestState,
                actionChains, billRecordsByEntity);

        // Invoke the savings calculations for the specified entities and time period.
        try {
            savingsTracker.processSavings(participatingUuids, startTime, endTime, actionChains,
                    billRecordsByEntity);
        } catch (EntitySavingsException e) {
            fail("Error occurred when writing savings stats" + e);
        }
    }

    /**
     * To resolve the entity oids from the input json file
     * @param entityNames entityName
     * @return a map of resolved oids
     */
    private static Map<String, Long> makeDummyOid(Set<String> entityNames) {
        Map<String, Long> result = new Hashtable<>();
        for (String entityName : entityNames) {
            try {
                result.put(entityName, Long.valueOf(entityName));
            } catch (NumberFormatException e) {
                result.put(entityName, Long.valueOf(entityName.hashCode()));
            }
        }
        return result;
    }

    private static boolean uuidIsValid(@Nullable String uuid) {
        return uuid != null && !uuid.isEmpty() && !uuid.equals("0");
    }

    @Nonnull
    private List<AggregatedSavingsStats> loadExpectedResults() {
        CSVParser parser = null;
        expectedUuids.clear();

        //the prefix of the output file should match the input file
        final String csvFilePath = String.format(filePath.replace("-input.json",
                "-output.csv"));
        try {
            parser = TestUtils.readCsvFile(csvFilePath, getClass());
        } catch (IOException ioe) {
            fail("Unable to read {}: " + csvFilePath + ioe);
        }
        final List<AggregatedSavingsStats> expectedResults = new ArrayList<>();
        if (parser == null) {
            return expectedResults;
        }
        for (CSVRecord record : parser) {
            long timestamp = getTimeMillis(record.get(
                    SavingsResultsHeader.timestamp.ordinal()).trim());
            String entity_oid = record.get(SavingsResultsHeader.entity_oid);
            expectedUuids.add(Long.valueOf(entity_oid));
            expectedResults.add(new AggregatedSavingsStats(timestamp,
                    EntitySavingsStatsType.REALIZED_SAVINGS,
                    Double.parseDouble(record.get(
                            SavingsResultsHeader.savings.ordinal()).trim())));

            expectedResults.add(new AggregatedSavingsStats(timestamp,
                    EntitySavingsStatsType.REALIZED_INVESTMENTS,
                    Double.parseDouble(record.get(
                            SavingsResultsHeader.investments.ordinal()).trim())));
        }
        return expectedResults;
    }
}
