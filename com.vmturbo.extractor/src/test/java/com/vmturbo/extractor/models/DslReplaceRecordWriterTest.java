package com.vmturbo.extractor.models;

import static com.vmturbo.extractor.models.ModelDefinitions.ATTRS;
import static com.vmturbo.extractor.models.ModelDefinitions.ENTITY_NAME;
import static com.vmturbo.extractor.models.ModelDefinitions.ENTITY_OID_AS_OID;
import static com.vmturbo.extractor.models.ModelDefinitions.ENTITY_STATE_ENUM;
import static com.vmturbo.extractor.models.ModelDefinitions.ENTITY_TYPE_ENUM;
import static com.vmturbo.extractor.models.ModelDefinitions.ENVIRONMENT_TYPE_ENUM;
import static com.vmturbo.extractor.models.ModelDefinitions.SEARCH_ENTITY_TABLE;
import static com.vmturbo.extractor.util.RecordTestUtil.createRecordByName;

import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.annotation.DirtiesContext.ClassMode;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.vmturbo.extractor.ExtractorDbConfig;
import com.vmturbo.extractor.schema.ExtractorDbBaseConfig;
import com.vmturbo.extractor.schema.enums.EntityState;
import com.vmturbo.extractor.schema.enums.EntityType;
import com.vmturbo.extractor.schema.enums.EnvironmentType;
import com.vmturbo.extractor.topology.ImmutableWriterConfig;
import com.vmturbo.extractor.topology.WriterConfig;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.sql.utils.DbEndpointTestRule;

/**
 * Live DB tests for {@link DslReplaceRecordSink}.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {ExtractorDbConfig.class, ExtractorDbBaseConfig.class})
@DirtiesContext(classMode = ClassMode.BEFORE_CLASS)
public class DslReplaceRecordWriterTest {

    private static final WriterConfig config = ImmutableWriterConfig.builder()
            .addAllReportingCommodityWhitelist(
                    Constants.REPORTING_DEFAULT_COMMODITY_TYPES_WHITELIST.stream()
                            .map(CommodityType::getNumber)
                            .collect(Collectors.toList()))
            .insertTimeoutSeconds(60)
            .lastSeenAdditionalFuzzMinutes(10)
            .lastSeenUpdateIntervalMinutes(10)
            .build();

    private DslReplaceRecordSink replaceRecordSink;
    private DbEndpoint ingesterEndpoint;
    private DbEndpoint queryEndpoint;
    private DbEndpoint grafanaQueryEndpoint;

    @Autowired
    private ExtractorDbConfig dbConfig;

    /**
     * Manage the live DB endpoint we're using for our tests.
     */
    @Rule
    @ClassRule
    public static DbEndpointTestRule endpointRule = new DbEndpointTestRule("extractor");

    private final Map<String, Object> vmData1 = createRecordMap(1L, EntityType.VIRTUAL_MACHINE,
            "", EnvironmentType.ON_PREM, EntityState.POWERED_ON, Collections.emptyMap());

    /**
     * Set up for tests.
     *
     * @throws UnsupportedDialectException if the endpoint is mis-configured
     * @throws SQLException                if there's a problem
     * @throws InterruptedException        if interrupted
     */
    @Before
    public void before() throws UnsupportedDialectException, SQLException, InterruptedException {
        this.ingesterEndpoint = dbConfig.ingesterEndpoint();
        this.queryEndpoint = dbConfig.queryEndpoint();
        this.grafanaQueryEndpoint = dbConfig.grafanaQueryEndpoint();
        endpointRule.addEndpoints(ingesterEndpoint, queryEndpoint, grafanaQueryEndpoint);
        final ScheduledExecutorService pool = Executors.newSingleThreadScheduledExecutor();
        this.replaceRecordSink = new DslReplaceRecordSink(ingesterEndpoint.dslContext(),
                SEARCH_ENTITY_TABLE, config, pool, "replace");
    }

    /**
     * Test that after replacing all records in search_entity table, read-only users can still
     * read data from the new table.
     *
     * @throws Exception any error happens
     */
    @Test
    public void testReadOnlyUserPermissionPreservedAfterReplacingRecords() throws Exception {
        // before replace
        checkCanReadTables(ingesterEndpoint, 0L);
        checkCanReadTables(queryEndpoint, 0L);
        checkCanReadTables(grafanaQueryEndpoint, 0L);
        // replace
        replaceRecordSink.accept(createRecordByName(SEARCH_ENTITY_TABLE, vmData1));
        replaceRecordSink.accept(null);
        // after replace
        checkCanReadTables(ingesterEndpoint, 1L);
        checkCanReadTables(queryEndpoint, 1L);
        checkCanReadTables(grafanaQueryEndpoint, 1L);
    }

    /**
     * Check that the db endpoint has the permission to read.
     *
     * @param endpoint db endpoint
     * @param recordsCount number of records
     * @throws UnsupportedDialectException if the endpoint is mis-configured
     * @throws SQLException                if there's a problem
     * @throws InterruptedException        if interrupted
     */
    private void checkCanReadTables(DbEndpoint endpoint, Long recordsCount)
            throws UnsupportedDialectException, SQLException, InterruptedException {
        Assert.assertEquals(recordsCount,
                endpoint.dslContext().fetchValue("SELECT count(*) FROM search_entity"));
    }

    /**
     * Create record map for an entity.
     *
     * @param oid oid
     * @param entityType entityType
     * @param name name
     * @param environmentType environmentType
     * @param entityState entityState
     * @param attrs attrs
     * @return map
     */
    private static Map<String, Object> createRecordMap(
            final long oid, final EntityType entityType, final String name,
            final EnvironmentType environmentType, final EntityState entityState,
            final Map<String, Object> attrs) {
        return ImmutableList.<Pair<String, Object>>of(
                Pair.of(ENTITY_OID_AS_OID.getName(), oid),
                Pair.of(ENTITY_TYPE_ENUM.getName(), entityType),
                Pair.of(ENTITY_NAME.getName(), name),
                Pair.of(ENVIRONMENT_TYPE_ENUM.getName(), environmentType),
                Pair.of(ENTITY_STATE_ENUM.getName(), entityState),
                Pair.of(ATTRS.getName(), attrs))
                .stream()
                .filter(pair -> pair.getRight() != null)
                .collect(Collectors.toMap(Pair::getLeft, Pair::getRight));
    }
}
