package com.vmturbo.extractor.models;

import static com.vmturbo.extractor.models.ModelDefinitions.COMMODITY_CAPACITY;
import static com.vmturbo.extractor.models.ModelDefinitions.COMMODITY_CONSUMED;
import static com.vmturbo.extractor.models.ModelDefinitions.COMMODITY_CURRENT;
import static com.vmturbo.extractor.models.ModelDefinitions.COMMODITY_PROVIDER;
import static com.vmturbo.extractor.models.ModelDefinitions.COMMODITY_TYPE;
import static com.vmturbo.extractor.models.ModelDefinitions.COMMODITY_UTILIZATION;
import static com.vmturbo.extractor.models.ModelDefinitions.ENTITY_HASH;
import static com.vmturbo.extractor.models.ModelDefinitions.ENTITY_OID;
import static com.vmturbo.extractor.models.ModelDefinitions.METRIC_TABLE;
import static com.vmturbo.extractor.models.ModelDefinitions.TIME;
import static com.vmturbo.extractor.util.RecordTestUtil.MapMatchesLaxly.mapMatchesLaxly;
import static com.vmturbo.extractor.util.RecordTestUtil.createRecordByName;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;

import org.apache.commons.lang3.tuple.Pair;
import org.jooq.DSLContext;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import com.vmturbo.extractor.schema.ExtractorDbConfig;
import com.vmturbo.extractor.topology.ImmutableWriterConfig;
import com.vmturbo.extractor.topology.WriterConfig;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.sql.utils.DbEndpointTestRule;

/**
 * Live DB tests that record sinks can store data into a database.
 */
public class DslRecordSinkWriterTest {

    private static final WriterConfig config = ImmutableWriterConfig.builder()
            .addAllReportingCommodityWhitelist(
                    ModelDefinitions.REPORTING_DEFAULT_COMMODITY_TYPES_WHITELIST.stream()
                            .map(CommodityType::getNumber)
                            .collect(Collectors.toList()))
            .insertTimeoutSeconds(60)
            .lastSeenAdditionalFuzzMinutes(10)
            .lastSeenUpdateIntervalMinutes(10)
            .build();

    private static final DbEndpoint endpoint = new ExtractorDbConfig().ingesterEndpoint();
    private DslRecordSink metricSink;
    private DSLContext dsl;


    /**
     * Manage the live DB endpoint we're using for our tests.
     */
    @Rule
    @ClassRule
    public static DbEndpointTestRule endpointRule = new DbEndpointTestRule(
            "extractor", Collections.emptyMap(), endpoint);

    private final Map<String, Object> metricData1 = createRecordMap(
            OffsetDateTime.now(), 1L, 100L, "VM", null, null, 1.0, 1.0, 2L);
    private final Map<String, Object> metricData2 = createRecordMap(
            OffsetDateTime.now(), 2L, 200L, "PM", 1.0, 1.0, null, null, null);


    /**
     * Set up for tests.
     *
     * @throws UnsupportedDialectException if the endpoint is mis-configured
     * @throws SQLException                if there's a problem
     */
    @Before
    public void before() throws UnsupportedDialectException, SQLException {
        this.dsl = endpoint.dslContext();
        final ScheduledExecutorService pool = Executors.newSingleThreadScheduledExecutor();
        this.metricSink = new DslRecordSink(dsl, METRIC_TABLE, config, pool);
    }

    /**
     * Test that sinks can write data to a database table.
     */
    @Test
    public void testMetricInserts() {
        metricSink.accept(createRecordByName(METRIC_TABLE, metricData1));
        metricSink.accept(createRecordByName(METRIC_TABLE, metricData2));
        metricSink.accept(null);
        assertThat(dsl.fetchValue("SELECT count(*) FROM metric"), is(2L));
        checkRecord(METRIC_TABLE, metricData1, TIME, ENTITY_HASH);
        checkRecord(METRIC_TABLE, metricData2, TIME, ENTITY_HASH);
    }

    private void checkRecord(final Table table, final Map<String, Object> data, Column<?>... keys) {
        String conditions = Arrays.stream(keys)
                .map(c -> String.format("\"%s\" = '%s'", c.getName(), data.get(c.getName())))
                .collect(Collectors.joining(" AND "));
        Map<String, Object> fromDb = dsl.fetchOne(String.format("SELECT * FROM \"%s\" WHERE %s",
                table.getName(), conditions)).intoMap();
        assertThat(fromDb, mapMatchesLaxly(data));
    }

    private static Map<String, Object> createRecordMap(
            final OffsetDateTime time, final long oid, final long hash, final String type,
            final Double current, final Double capacity, final Double utilization,
            final Double consumed, final Long provider) {
        return ImmutableList.<Pair<String, Object>>of(
                Pair.of(TIME.getName(), time),
                Pair.of(ENTITY_OID.getName(), oid),
                Pair.of(ENTITY_HASH.getName(), hash),
                Pair.of(COMMODITY_TYPE.getName(), type),
                Pair.of(COMMODITY_CURRENT.getName(), current),
                Pair.of(COMMODITY_CAPACITY.getName(), capacity),
                Pair.of(COMMODITY_UTILIZATION.getName(), utilization),
                Pair.of(COMMODITY_CONSUMED.getName(), consumed),
                Pair.of(COMMODITY_PROVIDER.getName(), provider))
                .stream()
                .filter(pair -> pair.getRight() != null)
                .collect(Collectors.toMap(Pair::getLeft, Pair::getRight));
    }
}
