package com.vmturbo.cost.component.scope;

import static com.vmturbo.cost.component.db.Cost.COST;

import java.sql.SQLException;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.vmturbo.cost.component.db.Tables;
import com.vmturbo.cost.component.db.TestCostDbEndpointConfig;
import com.vmturbo.cost.component.db.tables.records.OidMappingRecord;
import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.MultiDbTestBase;

/**
 * Unit tests for {@link SqlOidMappingStore}.
 */
@RunWith(Parameterized.class)
public class SqlOidMappingStoreTest extends MultiDbTestBase {

    private static final long ALIAS_OID = 111111L;
    private static final long REAL_OID = 7777777L;
    private static final long REAL_OID_2 = 8888888L;
    private static final long BROADCAST_TIME_UTC_MS = 1668046796000L;
    private static final long BROADCAST_TIME_UTC_MS_NEXT_DAY = 1668133196000L;

    private final DSLContext dslContext;
    private SqlOidMappingStore sqlOidMappingStore;

    /**
     * Create a new instance with given parameters.
     *
     * @param configurableDbDialect true to enable POSTGRES_PRIMARY_DB feature flag.
     * @param dialect to be used for tests.
     * @throws SQLException if DB operation fails.
     * @throws DbEndpoint.UnsupportedDialectException if dialect not supported.
     * @throws InterruptedException if creation is interrupted
     */
    public SqlOidMappingStoreTest(final boolean configurableDbDialect, final SQLDialect dialect) throws SQLException,
        DbEndpoint.UnsupportedDialectException, InterruptedException {
        super(COST, configurableDbDialect, dialect, "cost", TestCostDbEndpointConfig::costEndpoint);
        this.dslContext = super.getDslContext();
    }

    /**
     * Provide test parameters.
     *
     * @return test parameters
     */
    @Parameterized.Parameters
    public static Object[][] parameters() {
        return MultiDbTestBase.POSTGRES_CONVERTED_PARAMS;
    }

    /**
     * Initialize test resources.
     */
    @Before
    public void setup() {
        sqlOidMappingStore = new SqlOidMappingStore(dslContext);
    }

    /**
     * Test that {@link SqlOidMappingStore#registerOidMappings(Collection)} called with an empty Collection results in
     * no records being written into {@link Tables#OID_MAPPING}.
     */
    @Test
    public void testRegisterOidMappingsEmptyInput() {
        sqlOidMappingStore.registerOidMappings(Collections.emptyList());
        Assert.assertTrue(dslContext.selectFrom(Tables.OID_MAPPING).fetch().isEmpty());
    }

    /**
     * Test that {@link SqlOidMappingStore#registerOidMappings(Collection)} called with oid mappings not previously seen
     * results in corresponding records being written into {@link Tables#OID_MAPPING}.
     */
    @Test
    public void testRegisterOidMappingAliasNotPreviouslySeen() {
        final OidMapping oidMapping = createOidMapping(ALIAS_OID, REAL_OID, BROADCAST_TIME_UTC_MS);
        sqlOidMappingStore.registerOidMappings(Collections.singleton(oidMapping));
        final Map<Long, OidMappingRecord> resultMap = dslContext.selectFrom(Tables.OID_MAPPING).fetch().stream()
            .collect(Collectors.toMap(OidMappingRecord::getAliasId, Function.identity()));
        Assert.assertEquals(1, resultMap.size());
        verifyOidMappingRecord(oidMapping, resultMap.get(ALIAS_OID));
    }

    /**
     * Test that {@link SqlOidMappingStore#registerOidMappings(Collection)} called with oid mappings previously seen
     * (i.e. with the same {@link OidMappingKey}), then the earlier persisted
     * {@link OidMapping#firstDiscoveredTimeMsUtc()} is retained.
     */
    @Test
    public void testRegisterOidMappingAliasSeenBefore() {
        final OidMapping oidMapping = createOidMapping(ALIAS_OID, REAL_OID, BROADCAST_TIME_UTC_MS);
        sqlOidMappingStore.registerOidMappings(Collections.singleton(oidMapping));
        final OidMapping oidMappingNextBroadcast = createOidMapping(ALIAS_OID, REAL_OID,
            BROADCAST_TIME_UTC_MS_NEXT_DAY);
        sqlOidMappingStore.registerOidMappings(Collections.singleton(oidMappingNextBroadcast));
        final Map<Long, OidMappingRecord> resultMap = dslContext.selectFrom(Tables.OID_MAPPING).fetch().stream()
            .collect(Collectors.toMap(OidMappingRecord::getAliasId, Function.identity()));
        Assert.assertEquals(1, resultMap.size());
        verifyOidMappingRecord(oidMapping, resultMap.get(ALIAS_OID));
    }

    /**
     * Test that {@link SqlOidMappingStore#registerOidMappings(Collection)} called with oid mappings containing a
     * previously seen alias oid but a new real results in a new record being written into {@link Tables#OID_MAPPING}.
     */
    @Test
    public void testRegisterOidMappingNewRealOidForSeenAliasOid() {
        final OidMapping oidMapping = createOidMapping(ALIAS_OID, REAL_OID, BROADCAST_TIME_UTC_MS);
        sqlOidMappingStore.registerOidMappings(Collections.singleton(oidMapping));
        final OidMapping oidMappingNextBroadcast = createOidMapping(ALIAS_OID, REAL_OID_2,
            BROADCAST_TIME_UTC_MS_NEXT_DAY);
        sqlOidMappingStore.registerOidMappings(Collections.singleton(oidMappingNextBroadcast));
        final Map<Long, OidMappingRecord> resultMap = dslContext.selectFrom(Tables.OID_MAPPING).fetch().stream()
            .collect(Collectors.toMap(OidMappingRecord::getRealId, Function.identity()));
        Assert.assertEquals(2, resultMap.size());
        verifyOidMappingRecord(oidMapping, resultMap.get(REAL_OID));
        verifyOidMappingRecord(oidMappingNextBroadcast, resultMap.get(REAL_OID_2));
    }

    private void verifyOidMappingRecord(final OidMapping expectedMapping, final OidMappingRecord actualRecord) {
        Assert.assertEquals(expectedMapping.oidMappingKey().realOid(), (long)actualRecord.getRealId());
        Assert.assertEquals(expectedMapping.oidMappingKey().aliasOid(), (long)actualRecord.getAliasId());
        Assert.assertEquals(expectedMapping.firstDiscoveredTimeMsUtc(), actualRecord.getFirstDiscoveredTimeMsUtc());
    }

    private OidMapping createOidMapping(final long aliasOid, final long realOid, final long firstDiscoveredTime) {
        return ImmutableOidMapping.builder()
            .oidMappingKey(ImmutableOidMappingKey.builder()
                .aliasOid(aliasOid)
                .realOid(realOid)
                .build())
            .firstDiscoveredTimeMsUtc(Instant.ofEpochMilli(firstDiscoveredTime))
            .build();
    }
}