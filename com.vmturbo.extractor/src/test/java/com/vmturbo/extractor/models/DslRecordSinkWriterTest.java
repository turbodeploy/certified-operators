package com.vmturbo.extractor.models;

import static com.vmturbo.extractor.models.ModelDefinitions.ENTITY_OID;
import static com.vmturbo.extractor.models.ModelDefinitions.METRIC_TABLE;
import static com.vmturbo.extractor.models.ModelDefinitions.TIME;
import static com.vmturbo.extractor.util.RecordTestUtil.MapMatchesLaxly.mapMatchesLaxly;
import static com.vmturbo.extractor.util.RecordTestUtil.createMetricRecordMap;
import static com.vmturbo.extractor.util.RecordTestUtil.createRecordByName;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.jooq.DSLContext;
import org.jooq.TransactionalRunnable;
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

import com.vmturbo.components.common.featureflags.FeatureFlags;
import com.vmturbo.extractor.ExtractorDbConfig;
import com.vmturbo.extractor.schema.ExtractorDbBaseConfig;
import com.vmturbo.extractor.schema.enums.EntityType;
import com.vmturbo.extractor.schema.enums.MetricType;
import com.vmturbo.extractor.topology.ImmutableWriterConfig;
import com.vmturbo.extractor.topology.WriterConfig;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.sql.utils.DbEndpointTestRule;
import com.vmturbo.test.utils.FeatureFlagTestRule;

/**
 * Live DB tests that record sinks can store data into a database.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {ExtractorDbConfig.class, ExtractorDbBaseConfig.class})
@DirtiesContext(classMode = ClassMode.BEFORE_CLASS)
@TestPropertySource(properties = {"enableReporting=true", "sqlDialect=POSTGRES"})
public class DslRecordSinkWriterTest {

    private static final WriterConfig config = ImmutableWriterConfig.builder()
            .addAllReportingCommodityWhitelist(
                    Constants.REPORTING_DEFAULT_COMMODITY_TYPES_WHITELIST.stream()
                            .map(CommodityType::getNumber)
                            .collect(Collectors.toList()))
            .insertTimeoutSeconds(60)
            .unaggregatedCommodities(Constants.UNAGGREGATED_KEYED_COMMODITY_TYPES)
            .searchBatchSize(10)
            .build();

    private DslRecordSink metricSink;
    private DSLContext dsl;

    @Autowired
    private ExtractorDbConfig dbConfig;

    /**
     * Manage the live DB endpoint we're using for our tests.
     */
    @Rule
    @ClassRule
    public static DbEndpointTestRule endpointRule = new DbEndpointTestRule("extractor");

    /**
     * Manage feature flags.
     */
    @Rule
    public FeatureFlagTestRule featureFlagTestRule = new FeatureFlagTestRule()
            .testAllCombos(FeatureFlags.POSTGRES_PRIMARY_DB);

    private final Map<String, Object> metricData1 = createMetricRecordMap(
            OffsetDateTime.now(), 1L, MetricType.CPU, null, null, null, null, 1.0, 2L, null, null,
            EntityType.VIRTUAL_MACHINE);
    private final Map<String, Object> metricData2 = createMetricRecordMap(
            OffsetDateTime.now(), 2L, MetricType.MEM, null, 1.0, 1.0, null, null, null, null, null,
            EntityType.VIRTUAL_MACHINE);

    /**
     * Set up for tests.
     *
     * @throws UnsupportedDialectException if the endpoint is mis-configured
     * @throws SQLException                if there's a problem
     * @throws InterruptedException        if interrupted
     */
    @Before
    public void before() throws UnsupportedDialectException, SQLException, InterruptedException {
        final DbEndpoint endpoint = dbConfig.ingesterEndpoint();
        endpointRule.addEndpoints(endpoint);
        this.dsl = spy(endpoint.dslContext());
        final ScheduledExecutorService pool = Executors.newSingleThreadScheduledExecutor();
        this.metricSink = new DslRecordSink(dsl, METRIC_TABLE, config, pool);
    }

    /**
     * Test that sinks can write data to a database table.
     * @throws InterruptedException when interrupted
     * @throws SQLException should not happen
     */
    @Test
    public void testMetricInserts() throws SQLException, InterruptedException {
        metricSink.accept(createRecordByName(METRIC_TABLE, metricData1));
        metricSink.accept(createRecordByName(METRIC_TABLE, metricData2));
        metricSink.accept(null);
        assertThat(dsl.fetchValue("SELECT count(*) FROM metric"), is(2L));
        checkRecord(METRIC_TABLE, metricData1, TIME, ENTITY_OID);
        checkRecord(METRIC_TABLE, metricData2, TIME, ENTITY_OID);
    }

    private void checkRecord(final Table table, final Map<String, Object> data, Column<?>... keys) {
        String conditions = Arrays.stream(keys)
                .map(c -> String.format("\"%s\" = '%s'", c.getName(), data.get(c.getName())))
                .collect(Collectors.joining(" AND "));
        Map<String, Object> fromDb = dsl.fetchOne(String.format("SELECT * FROM \"%s\" WHERE %s",
                table.getName(), conditions)).intoMap();
        assertThat(fromDb, mapMatchesLaxly(data));
    }

    /**
     * Verify that the main ingestion thread is not blocked if postgres reading thread is
     * terminated due to db issues.
     *
     * @throws InterruptedException if current thread is interrupted
     * @throws TimeoutException if the test can not finish within given time
     * @throws ExecutionException if exception when getting the result of the task
     */
    @Test
    public void testMainIngestionThreadNotBlocked()
            throws InterruptedException, TimeoutException, ExecutionException {
        final ScheduledExecutorService pool = Executors.newSingleThreadScheduledExecutor();
        // mock the main thread task
        Future<?> future = pool.submit(() -> {
            // mock that reading thread throws exception
            doThrow(new DataAccessException("foo")).when(dsl).transaction(any(TransactionalRunnable.class));
            // ensure it exceeds the default buffer size (1024) in PipedInputStream
            IntStream.range(0, 50).forEach(id -> {
                try {
                    metricSink.accept(createRecordByName(METRIC_TABLE, metricData1));
                } catch (SQLException | InterruptedException e) {
                    // do not close
                    return;
                }
            });
            try {
                metricSink.accept(null);
            } catch (SQLException | InterruptedException e) {
                // noop - should not happen
            }
        });
        // it should not wait forever (finish within 1 minute)
        future.get(1, TimeUnit.MINUTES);
    }
}
