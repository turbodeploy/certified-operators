package com.vmturbo.cost.component.savings;

import static com.vmturbo.cost.component.savings.GrpcActionChainStore.changeWindowComparator;
import static com.vmturbo.cost.component.util.TestUtils.getTimeMillis;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.Message.Builder;
import com.google.protobuf.MessageLite;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.IOUtils;
import org.apache.commons.text.StringSubstitutor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.vmturbo.cloud.common.topology.TopologyEntityCloudTopologyFactory;
import com.vmturbo.common.protobuf.action.ActionDTO.ExecutedActionsChangeWindow;
import com.vmturbo.common.protobuf.action.ActionDTO.ExecutedActionsChangeWindow.LivenessState;
import com.vmturbo.common.protobuf.cost.Cost.EntitySavingsStatsType;
import com.vmturbo.common.protobuf.cost.Cost.UploadBilledCostRequest.BillingDataPoint;
import com.vmturbo.common.protobuf.search.Search.SearchEntitiesRequest;
import com.vmturbo.common.protobuf.search.Search.SearchEntitiesResponse;
import com.vmturbo.common.protobuf.search.SearchMoles.SearchServiceMole;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.common.utils.TimeFrameCalculator;
import com.vmturbo.cost.component.billedcosts.BatchInserter;
import com.vmturbo.cost.component.billedcosts.BilledCostStore;
import com.vmturbo.cost.component.billedcosts.SqlBilledCostStore;
import com.vmturbo.cost.component.db.Cost;
import com.vmturbo.cost.component.db.TestCostDbEndpointConfig;
import com.vmturbo.cost.component.db.tables.EntitySavingsByDay;
import com.vmturbo.cost.component.pricing.BusinessAccountPriceTableKeyStore;
import com.vmturbo.cost.component.pricing.PriceTableStore;
import com.vmturbo.cost.component.rollup.LastRollupTimes;
import com.vmturbo.cost.component.rollup.RollupTimesStore;
import com.vmturbo.cost.component.savings.bottomup.AggregatedSavingsStats;
import com.vmturbo.cost.component.savings.bottomup.EntitySavingsRetentionConfig;
import com.vmturbo.cost.component.savings.bottomup.SqlEntitySavingsStore;
import com.vmturbo.cost.component.util.TestUtils;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CostBilling.CloudBillingData.CloudBillingBucket.Granularity;
import com.vmturbo.sql.utils.DbCleanupRule.CleanupOverrides;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.sql.utils.MultiDbTestBase;

/**
 * Test for top-level savings processor execute calls. Pass in external inputs and verify final
 * stats results generated are matching the expected results.
 * This test is trying to test the scenario mentioned in the wiki:
 * https://vmturbo.atlassian.net/wiki/spaces/PMTES/pages/3200843803/Design+Billing+Based+Savings#Building-The-Watermark-Timeline
 */
@RunWith(Parameterized.class)
@CleanupOverrides(truncate = {EntitySavingsByDay.class})
public class SavingsProcessorTest extends MultiDbTestBase {
    /**
     * For logging some warnings if needed.
     */
    private static final Logger logger = LogManager.getLogger();

    /**
     * Provide test parameters.
     *
     * @return test parameters
     */
    @Parameters
    public static Object[][] parameters() {
        return MultiDbTestBase.POSTGRES_CONVERTED_PARAMS;
    }

    /**
     * Context for DB ops.
     */
    private final DSLContext dsl;

    /**
     * DB read/write chunk size.
     */
    private final int chunkSize = 2;

    /** Rule chain to manage db provisioning and lifecycle. */
    @Rule
    public TestRule multiDbRules = super.ruleChain;

    /**
     * Clock time: 07 Jan 2022 10:00:00 GMT
     */
    private final Clock clock =  Clock.fixed(Instant.ofEpochMilli(1641549600000L),
            ZoneId.from(ZoneOffset.UTC));

    /**
     * Processor reference.
     */
    private final SavingsProcessor savingsProcessor;

    /**
     * Mocked chain store.
     */
    private final GrpcActionChainStore actionChainStore;

    /**
     * Real billing DB reference to help insert billing records.
     */
    private final BilledCostStore billedCostStore;

    /**
     * Savings action store.
     */
    private final SavingsActionStore savingsActionStore;

    private final SqlEntitySavingsStore savingsStore;

    /**
     * For last rollup time fetch.
     */
    private final RollupTimesStore rollupTimesStore;

    /**
     * VM ID 1 for test.
     */
    private static final long vm1Id = 74413626193600L;

    /**
     * Name of entity.
     */
    private static final String vm1Name = "vm1";

    private final Set<EntityType> supportedEntityTypes = ImmutableSet.of(EntityType.VIRTUAL_VOLUME, EntityType.DATABASE);

    private final Set<String> supportedCSPs = ImmutableSet.of("Azure");

    private final SearchServiceMole searchServiceMole = spy(new SearchServiceMole());

    /**
     * GRPC server.
     */
    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(searchServiceMole);

    /**
     * Headers for settings related to action spec template file.
     */
    enum ActionSettingsHeader {
        /**
         * Display timestamp when action executed.
         */
        displayTime,

        /**
         * Action source tier id.
         */
        sourceTierId,

        /**
         * Action destination tier id.
         */
        destinationTierId,

        /**
         * On-demand rate before action.
         */
        sourceOnDemandRate,

        /**
         * Execution time of action.
         */
        completionTime
    }

    /**
     * Header fields for expected results.
     */
    enum SavingsResultsHeader {
        /**
         * Date for the stats, e.g. '2022-01-02T00:00'
         */
        stats_date,

        /**
         * Realized savings value expected.
         */
        realized_savings,

        /**
         * Realized investments value expected.
         */
        realized_investments
    }

    /**
     * Headers for settings related to action spec template file.
     */
    enum BillSettingsHeader {
        /**
         * Display timestamp when action executed.
         */
        displayTime,

        /**
         * Daily bill usage time.
         */
        sampleTime,

        /**
         * Hours in a day used.
         */
        usageAmount,

        /**
         * Cost for the hours used.
         */
        cost,

        /**
         * Provider id.
         */
        providerId
    }

    /**
     * Create a new instance with given parameters.
     *
     * @param configurableDbDialect true to enable POSTGRES_PRIMARY_DB feature flag
     * @param dialect         DB dialect to use
     * @throws SQLException                if a DB operation fails
     * @throws UnsupportedDialectException if dialect is bogus
     * @throws InterruptedException        if we're interrupted
     */
    public SavingsProcessorTest(boolean configurableDbDialect, SQLDialect dialect)
            throws SQLException, UnsupportedDialectException, InterruptedException, IOException {
        super(Cost.COST, configurableDbDialect, dialect, "cost",
                TestCostDbEndpointConfig::costEndpoint);
        this.dsl = super.getDslContext();
        this.actionChainStore = mock(GrpcActionChainStore.class);
        this.rollupTimesStore = mock(RollupTimesStore.class);
        this.savingsStore = new SqlEntitySavingsStore(dsl, clock, chunkSize, true);
        this.savingsActionStore = mock(CachedSavingsActionStore.class);

        this.billedCostStore = new SqlBilledCostStore(dsl,
                new BatchInserter(chunkSize, 1, rollupTimesStore),
                mock(TimeFrameCalculator.class));
        grpcServer.start();
        SearchEntitiesResponse response = SearchEntitiesResponse.newBuilder().addEntities(
                PartialEntity.newBuilder().setMinimal(
                        MinimalEntity.newBuilder().setOid(1234L).build()).build()).build();
        when(searchServiceMole.searchEntities(any(SearchEntitiesRequest.class)))
                .thenReturn(response);
        final SearchServiceBlockingStub searchService =
                SearchServiceGrpc.newBlockingStub(grpcServer.getChannel());

        SavingsTracker tracker = spy(new SavingsTracker(
                new SqlBillingRecordStore(dsl),
                actionChainStore,
                savingsStore,
                supportedEntityTypes,
                supportedCSPs,
                TimeUnit.DAYS.toMillis(365),
                clock, mock(TopologyEntityCloudTopologyFactory.class),
                null, dsl, mock(BusinessAccountPriceTableKeyStore.class),
                mock(PriceTableStore.class), searchService, 0, 777777, chunkSize));
        doReturn(true).when(tracker).isSupportedCSP(any());

        this.savingsProcessor = new SavingsProcessor(clock,
                chunkSize,
                rollupTimesStore,
                savingsActionStore,
                tracker,
                mock(DataRetentionProcessor.class),
                mock(EntitySavingsRetentionConfig.class));
    }

    /**
     * Testing scaling scenario for VMs 1 and 2, and verifying expected results.
     *
     * @throws EntitySavingsException Thrown on DB errors.
     */
    @Test
    public void vm1Scaling() throws EntitySavingsException, SavingsException {
        // Map of date timestamp to StatsValue having realized savings/investment, for comparing
        // with actual test results.
        final List<AggregatedSavingsStats> expectedResults = loadExpectedResults(vm1Name);
        assertFalse(expectedResults.isEmpty());

        // Fix up dummy response to action chain requests.
        setupActionStore(vm1Name, vm1Id);

        // Current time being 10 AM, this billing update happened '1 hour back'.
        // This is the time we use for last_updated field of the billing records.
        long currentUpdateTime = getTimeMillis("2021-01-07T09:00:00");

        insertBillingRecords(currentUpdateTime, vm1Name);

        // Setup last rollup time, 1 day and 5 hours back.
        long previousUpdateTime = getTimeMillis("2021-01-06T05:00:00");
        final LastRollupTimes lastRollupTime = new LastRollupTimes(previousUpdateTime, 0, 0, 0);
        when(rollupTimesStore.getLastRollupTimes())
                .thenReturn(lastRollupTime);

        ExecutedActionsChangeWindow actionsChangeWindow = ExecutedActionsChangeWindow.newBuilder()
                .setEntityOid(vm1Id)
                .setActionOid(1L)
                .build();
        Set<ExecutedActionsChangeWindow> actions = Collections.singleton(actionsChangeWindow);
        when(savingsActionStore.getActions(LivenessState.LIVE)).thenReturn(actions);

        // Run the processor with the above inputs.
        savingsProcessor.execute();

        // Read savings results from stats DB table and verify with expected results.
        final List<AggregatedSavingsStats> dailyStats = savingsStore.getDailyStats(
                ImmutableSet.of(EntitySavingsStatsType.REALIZED_SAVINGS,
                        EntitySavingsStatsType.REALIZED_INVESTMENTS), 0L,
                System.currentTimeMillis(), ImmutableList.of(vm1Id),
                ImmutableList.of(EntityType.VIRTUAL_VOLUME_VALUE), Collections.emptyList());
        assertFalse(dailyStats.isEmpty());

        // Verify that all savings and investments match up with expected results.
        // This is for 6 days, so there should be 12 entries in the stats list.
        assertTrue(CollectionUtils.isEqualCollection(dailyStats, expectedResults));
    }

    /**
     * Sets up mock AO response to action chain request.
     */
    private void setupActionStore(@Nonnull final String entityName, long entityId) {
        // Read action settings csv and the template file.
        final String actionCsvFile = String.format("/savings/%s-scale-action-settings.csv",
                entityName);
        final String templateFile = String.format("/savings/%s-scale-action-template.json",
                entityName);
        final CSVParser parser;
        final String templateContents;
        final Map<Long, NavigableSet<ExecutedActionsChangeWindow>> specsByEntity = ImmutableMap.of(
                entityId, new TreeSet<>(changeWindowComparator)
        );
        try {
            parser = TestUtils.readCsvFile(actionCsvFile, getClass());
            templateContents = TestUtils.readTxtFile(templateFile, getClass());

            // For each settings record, populate the template file and load the protobuf spec.
            if (parser != null && templateContents != null) {
                parser.forEach(csvRecord -> {
                    final ExecutedActionsChangeWindow.Builder builder = ExecutedActionsChangeWindow.newBuilder();
                    loadProtobufBuilder(entityName, templateContents, csvRecord,
                            EnumSet.allOf(ActionSettingsHeader.class), builder);
                    specsByEntity.get(entityId).add(builder.build());
                });
            }
        } catch (IOException ioe) {
            logger.warn("Unable to read {} and/or {}: ", actionCsvFile, templateFile, ioe);
        }

        when(actionChainStore.getActionChains(any()))
                .thenReturn(specsByEntity);
    }

    /**
     * Helper to load protobuf from template file.
     *
     * @param entityName Name of entity.
     * @param templateContents Contents of the template file.
     * @param csvRecord CSV records with data to fill in the template.
     * @param enumSet Header enums.
     * @param builder Protobuf builder that will get filled in.
     */
    private void loadProtobufBuilder(@Nonnull final String entityName,
            @Nonnull final String templateContents,
            @Nonnull final CSVRecord csvRecord,
            EnumSet<?> enumSet,
            @Nonnull final MessageLite.Builder builder) {
        // Figure out what values to sub, based on the settings row from input.
        final Map<String, String> valuesMap = new HashMap<>();

        enumSet.forEach(header -> valuesMap.put(header.name(), csvRecord.get(header.ordinal()).trim()));
        StringSubstitutor subs = new StringSubstitutor(valuesMap);
        String resolvedString = subs.replace(templateContents);

        // Load the ActionSpec protobuf from the resolved file text.
        assertTrue(TestUtils.loadProtobufBuilder(String.format("%s-spec", entityName),
                (Builder)builder,
                IOUtils.toInputStream(resolvedString, StandardCharsets.UTF_8)));
    }

    /**
     * Inserts test billing records into DB.
     *
     * @param currentUpdateTime Last update time to use.
     */
    private void insertBillingRecords(long currentUpdateTime, @Nonnull final String entityName) {
        // Insert billing records, current time being 10 AM.
        // Don't do any rollup for billing data.
        when(rollupTimesStore.getLastRollupTimes())
                .thenReturn(new LastRollupTimes());

        final String billCsvFile = String.format("/savings/%s-bill-settings.csv", entityName);
        final String templateFile = String.format("/savings/%s-bill-template.json", entityName);
        final CSVParser parser;
        final String templateContents;
        final List<BillingDataPoint> dataPoints = new ArrayList<>();
        try {
            parser = TestUtils.readCsvFile(billCsvFile, getClass());
            templateContents = TestUtils.readTxtFile(templateFile, getClass());

            // For each settings record, populate the template file and load the protobuf spec.
            if (parser != null && templateContents != null) {
                parser.forEach(csvRecord -> {
                    final BillingDataPoint.Builder builder = BillingDataPoint.newBuilder();
                    loadProtobufBuilder(entityName, templateContents, csvRecord,
                            EnumSet.allOf(BillSettingsHeader.class), builder);
                    dataPoints.add(builder.build());
                });
            }
        } catch (IOException ioe) {
            logger.warn("Unable to read {} and/or {}: ", billCsvFile, templateFile, ioe);
        }

        final List<Future<Integer>> billingBatches = new ArrayList<>(
                billedCostStore.insertBillingDataPoints(dataPoints, ImmutableMap.of(),
                        Granularity.DAILY, currentUpdateTime));
        try {
            for (final Future<Integer> batch : billingBatches) {
                batch.get();
            }
        } catch (InterruptedException ie) {
            logger.warn("Interrupted for billing insertion requests.", ie);
            Thread.currentThread().interrupt();
        } catch (ExecutionException ee) {
            logger.warn("Unable to submit billing insertion requests.", ee);
        }
    }

    /**
     * Loads expected results from csv file.
     *
     * @return Map of key (scenario,entity,timestamp), to the StatsValue that is expected.
     */
    @Nonnull
    private List<AggregatedSavingsStats> loadExpectedResults(@Nonnull final String entityName) {
        CSVParser parser = null;
        final String csvFilePath = String.format("/savings/%s-expected-results.csv", entityName);
        try {
            parser = TestUtils.readCsvFile(csvFilePath, getClass());
        } catch (IOException ioe) {
            logger.warn("Unable to read {}: ", csvFilePath, ioe);
        }
        final List<AggregatedSavingsStats> expectedResults = new ArrayList<>();
        if (parser == null) {
            return expectedResults;
        }
        for (CSVRecord record : parser) {
            long timestamp = getTimeMillis(record.get(
                    SavingsResultsHeader.stats_date.ordinal()).trim());
            expectedResults.add(new AggregatedSavingsStats(timestamp,
                    EntitySavingsStatsType.REALIZED_SAVINGS,
                    Double.parseDouble(record.get(
                            SavingsResultsHeader.realized_savings.ordinal()).trim())));

            expectedResults.add(new AggregatedSavingsStats(timestamp,
                    EntitySavingsStatsType.REALIZED_INVESTMENTS,
                    Double.parseDouble(record.get(
                            SavingsResultsHeader.realized_investments.ordinal()).trim())));
        }
        return expectedResults;
    }
}
