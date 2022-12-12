package com.vmturbo.cost.component.scope;

import static com.vmturbo.cost.component.db.Cost.COST;
import static com.vmturbo.cost.component.scope.ScopeIdReplacementTestUtils.ALIAS_OID;
import static com.vmturbo.cost.component.scope.ScopeIdReplacementTestUtils.BROADCAST_TIME_UTC_MS;
import static com.vmturbo.cost.component.scope.ScopeIdReplacementTestUtils.BROADCAST_TIME_UTC_MS_NEXT_DAY;
import static com.vmturbo.cost.component.scope.ScopeIdReplacementTestUtils.REAL_OID;
import static com.vmturbo.cost.component.scope.ScopeIdReplacementTestUtils.REAL_OID_2;
import static com.vmturbo.cost.component.scope.ScopeIdReplacementTestUtils.REAL_OID_3;
import static com.vmturbo.cost.component.scope.ScopeIdReplacementTestUtils.createOidMapping;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.jooq.DSLContext;
import org.jooq.Result;
import org.jooq.SQLDialect;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.vmturbo.cloud.common.persistence.DataQueueConfiguration;
import com.vmturbo.cloud.common.scope.CloudScopeIdentity;
import com.vmturbo.cloud.common.scope.CloudScopeIdentityStore;
import com.vmturbo.common.protobuf.cost.Cost;
import com.vmturbo.components.common.RequiresDataInitialization;
import com.vmturbo.cost.component.db.Tables;
import com.vmturbo.cost.component.db.TestCostDbEndpointConfig;
import com.vmturbo.cost.component.db.tables.records.CloudCostDailyRecord;
import com.vmturbo.cost.component.db.tables.records.CloudScopeRecord;
import com.vmturbo.cost.component.db.tables.records.ScopeIdReplacementLogRecord;
import com.vmturbo.platform.sdk.common.CommonCost;
import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.MultiDbTestBase;

/**
 * Unit tests for {@link SqlCloudCostScopeIdReplacementLog}.
 */
@RunWith(Parameterized.class)
public class SqlCloudCostScopeIdReplacementLogTest extends MultiDbTestBase {

    private static final long SERVICE_PROVIDER_ID = 12345L;
    private static final long BROADCAST_TIME_UTC_MS_PREVIOUS_DAY = BROADCAST_TIME_UTC_MS - TimeUnit.DAYS.toMillis(1);

    private final DSLContext dslContext;
    private SqlCloudCostScopeIdReplacementLog sqlCloudCostScopeIdReplacementLog;
    private CloudScopeIdentityStore cloudScopeIdentityStore;
    private SqlOidMappingStore sqlOidMappingStore;

    /**
     * Create a new instance with given parameters.
     *
     * @param configurableDbDialect true to enable POSTGRES_PRIMARY_DB feature flag
     * @param dialect         DB dialect to use
     * @throws SQLException                if a DB operation fails
     * @throws DbEndpoint.UnsupportedDialectException if dialect is bogus
     * @throws InterruptedException        if we're interrupted
     */
    public SqlCloudCostScopeIdReplacementLogTest(boolean configurableDbDialect, SQLDialect dialect) throws SQLException,
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
        return MultiDbTestBase.DBENDPOINT_CONVERTED_PARAMS;
    }

    @Before
    public void setup() throws RequiresDataInitialization.InitializationException {
        final DataQueueConfiguration dataQueueConfiguration = DataQueueConfiguration.builder()
            .queueName("test-data-queue").concurrency(1).build();
        sqlOidMappingStore = new SqlOidMappingStore(dslContext);
        cloudScopeIdentityStore = new SqlCloudScopeIdentityStore(dslContext,
            CloudScopeIdentityStore.PersistenceRetryPolicy.DEFAULT_POLICY, true, 10);
        sqlCloudCostScopeIdReplacementLog = new SqlCloudCostScopeIdReplacementLog(dslContext, sqlOidMappingStore,
            cloudScopeIdentityStore, dataQueueConfiguration, 100, Duration.ofMinutes(5));
    }

    /**
     * Test that when there are no replaced oid mappings, {@link ScopeIdReplacementLog
     * #getReplacedScopeId(long, Instant)} returns the input scopeId itself.
     */
    @Test
    public void testGetReplacedScopeIdNoReplaceMappings() {
        Assert.assertEquals(ALIAS_OID, sqlCloudCostScopeIdReplacementLog
            .getReplacedScopeId(ALIAS_OID, Instant.ofEpochMilli(BROADCAST_TIME_UTC_MS)));
    }

    @Test
    public void testGetReplacedScopeIdSingleReplacement() throws Exception {
        // insert oid mappings into OidMappingStore
        sqlOidMappingStore.registerOidMappings(Collections.singleton(createOidMapping(ALIAS_OID, REAL_OID,
            BROADCAST_TIME_UTC_MS)));
        // insert scope for alias oid
        cloudScopeIdentityStore.saveScopeIdentities(Collections.singletonList(CloudScopeIdentity.builder()
            .scopeId(ALIAS_OID)
            .scopeType(CloudScopeIdentity.CloudScopeType.RESOURCE)
            .serviceProviderId(SERVICE_PROVIDER_ID)
            .build()));
        sqlCloudCostScopeIdReplacementLog.persistScopeIdReplacements();

        // sample time before mapping first discovered time
        Assert.assertEquals(ALIAS_OID, sqlCloudCostScopeIdReplacementLog
            .getReplacedScopeId(ALIAS_OID, Instant.ofEpochMilli(BROADCAST_TIME_UTC_MS).minus(1, ChronoUnit.DAYS)));

        // sample time after mapping first discovered time
        Assert.assertEquals(REAL_OID, sqlCloudCostScopeIdReplacementLog
            .getReplacedScopeId(ALIAS_OID, Instant.ofEpochMilli(BROADCAST_TIME_UTC_MS)));
    }

    /**
     * Test that {@link ScopeIdReplacementLog#getReplacedScopeId(long, Instant)} returns the correct scopeIds when
     * there are multiple replaced oid mappings for different sample times.
     *
     * @throws Exception if {@link ScopeIdReplacementLog#persistScopeIdReplacements()} throws an Exception.
     */
    @Test
    public void testGetReplacedScopeIdMultipleReplacements() throws Exception {
        // insert oid mappings into OidMappingStore
        sqlOidMappingStore.registerOidMappings(Collections.singleton(createOidMapping(ALIAS_OID, REAL_OID,
            BROADCAST_TIME_UTC_MS)));
        sqlOidMappingStore.registerOidMappings(Collections.singleton(createOidMapping(ALIAS_OID, REAL_OID_2,
            BROADCAST_TIME_UTC_MS_NEXT_DAY)));
        // insert scope for alias oid
        cloudScopeIdentityStore.saveScopeIdentities(Collections.singletonList(CloudScopeIdentity.builder()
            .scopeId(ALIAS_OID)
            .scopeType(CloudScopeIdentity.CloudScopeType.RESOURCE)
            .serviceProviderId(SERVICE_PROVIDER_ID)
            .build()));
        sqlCloudCostScopeIdReplacementLog.persistScopeIdReplacements();

        // sample time before first mapping time
        Assert.assertEquals(ALIAS_OID, sqlCloudCostScopeIdReplacementLog
            .getReplacedScopeId(ALIAS_OID, Instant.ofEpochMilli(BROADCAST_TIME_UTC_MS).minus(1, ChronoUnit.DAYS)));

        // sample time exactly equal to first mapping time
        Assert.assertEquals(REAL_OID, sqlCloudCostScopeIdReplacementLog
            .getReplacedScopeId(ALIAS_OID, Instant.ofEpochMilli(BROADCAST_TIME_UTC_MS)));

        // sample time after first mapping time but before the second mapping
        Assert.assertEquals(REAL_OID, sqlCloudCostScopeIdReplacementLog
            .getReplacedScopeId(ALIAS_OID, Instant.ofEpochMilli(BROADCAST_TIME_UTC_MS).plus(1, ChronoUnit.HOURS)));

        // sample time exactly equal to second mapping time
        Assert.assertEquals(REAL_OID_2, sqlCloudCostScopeIdReplacementLog
            .getReplacedScopeId(ALIAS_OID, Instant.ofEpochMilli(BROADCAST_TIME_UTC_MS_NEXT_DAY)));

        // sample time after second mapping time
        Assert.assertEquals(REAL_OID_2, sqlCloudCostScopeIdReplacementLog
            .getReplacedScopeId(ALIAS_OID, Instant.ofEpochMilli(BROADCAST_TIME_UTC_MS_NEXT_DAY)
                .plus(1, ChronoUnit.DAYS)));
    }

    /**
     * Test that when an oid mapping is replaced and no new oid mappings are found, then the DataQueue returns with an
     * empty summary.
     */
    @Test
    public void testPersistScopeIdReplacementsNoNewOidMappingsFound() throws Exception {
        // insert oid mappings into OidMappingStore
        sqlOidMappingStore.registerOidMappings(Collections.singleton(createOidMapping(ALIAS_OID, REAL_OID,
            BROADCAST_TIME_UTC_MS)));

        // insert scope for alias oid
        cloudScopeIdentityStore.saveScopeIdentities(Collections.singletonList(CloudScopeIdentity.builder()
            .scopeId(ALIAS_OID)
            .scopeType(CloudScopeIdentity.CloudScopeType.RESOURCE)
            .serviceProviderId(SERVICE_PROVIDER_ID)
            .build()));

        // insert into scope_id_replacement_log and call initialize to load the replaced mapping
        dslContext.insertInto(Tables.SCOPE_ID_REPLACEMENT_LOG)
            .columns(Tables.SCOPE_ID_REPLACEMENT_LOG.REPLACEMENT_LOG_ID, Tables.SCOPE_ID_REPLACEMENT_LOG.REAL_ID)
            .values((short)1, REAL_OID).execute();
        sqlCloudCostScopeIdReplacementLog.initialize();

        // re-try persistence with no new mappings registered
        final Optional<ScopeIdReplacementPersistenceSummary> summary = sqlCloudCostScopeIdReplacementLog
            .persistScopeIdReplacements();
        Assert.assertFalse(summary.isPresent());
    }

    /**
     * Test that Cloud Cost Daily table records are updated correctly with the following replacements:
     * previous replacement: {}
     * new replacement: {ALIAS_OID, REAL_OID, BROADCAST_TIME_UTC_MS}
     * next replacement: {}
     */
    @Test
    public void testPersistScopeIdReplacementFirstTimeOidMappingCloudCostDailyUpdate() throws Exception {
        // insert oid mappings into OidMappingStore
        sqlOidMappingStore.registerOidMappings(Collections.singleton(createOidMapping(ALIAS_OID, REAL_OID,
            BROADCAST_TIME_UTC_MS)));
        // insert scope for alias oid
        cloudScopeIdentityStore.saveScopeIdentities(Collections.singletonList(CloudScopeIdentity.builder()
            .scopeId(ALIAS_OID)
            .scopeType(CloudScopeIdentity.CloudScopeType.RESOURCE)
            .serviceProviderId(SERVICE_PROVIDER_ID)
            .build()));
        // insert data into cloud cost daily
        insertIntoBilledCostDailyTable(ALIAS_OID, BROADCAST_TIME_UTC_MS_PREVIOUS_DAY);
        insertIntoBilledCostDailyTable(ALIAS_OID, BROADCAST_TIME_UTC_MS);
        // trigger persist
        sqlCloudCostScopeIdReplacementLog.persistScopeIdReplacements();
        // assert
        verifyPersistence(ALIAS_OID, Collections.singletonList(BROADCAST_TIME_UTC_MS_PREVIOUS_DAY), false);
        verifyPersistence(REAL_OID, Collections.singletonList(BROADCAST_TIME_UTC_MS), true);
    }

    /**
     * Test that Cloud Cost Daily table records are updated correctly with the following replacements:
     * previous replacement: {}
     * new replacement: {ALIAS_OID, REAL_OID, BROADCAST_TIME_UTC_MS}
     * next replacement: {ALIAS_OID, REAL_OID_2, BROADCAST_TIME_UTC_MS + 1 day}
     */
    @Test
    public void testPersistScopeIdReplacementWithNextReplacementDifferentDay() throws Exception {
        // insert oid mappings into OidMappingStore
        sqlOidMappingStore.registerOidMappings(Collections.singleton(createOidMapping(ALIAS_OID, REAL_OID,
            BROADCAST_TIME_UTC_MS)));
        sqlOidMappingStore.registerOidMappings(Collections.singleton(createOidMapping(ALIAS_OID, REAL_OID_2,
            BROADCAST_TIME_UTC_MS_NEXT_DAY)));
        // insert scope for alias oid
        cloudScopeIdentityStore.saveScopeIdentities(Collections.singletonList(CloudScopeIdentity.builder()
            .scopeId(ALIAS_OID)
            .scopeType(CloudScopeIdentity.CloudScopeType.RESOURCE)
            .serviceProviderId(SERVICE_PROVIDER_ID)
            .build()));
        // insert data into cloud cost daily
        insertIntoBilledCostDailyTable(ALIAS_OID, BROADCAST_TIME_UTC_MS_PREVIOUS_DAY);
        insertIntoBilledCostDailyTable(ALIAS_OID, BROADCAST_TIME_UTC_MS);
        insertIntoBilledCostDailyTable(ALIAS_OID, BROADCAST_TIME_UTC_MS_NEXT_DAY);
        // trigger persist
        sqlCloudCostScopeIdReplacementLog.persistScopeIdReplacements();

        verifyPersistence(ALIAS_OID, Collections.singletonList(BROADCAST_TIME_UTC_MS_PREVIOUS_DAY), false);
        verifyPersistence(REAL_OID, Collections.singletonList(BROADCAST_TIME_UTC_MS), true);
        verifyPersistence(REAL_OID_2, Collections.singletonList(BROADCAST_TIME_UTC_MS_NEXT_DAY), true);
    }

    /**
     * Test that Cloud Cost Daily table records are updated correctly with the following replacements:
     * previous replacement: {}
     * new replacement: {ALIAS_OID, REAL_OID, BROADCAST_TIME_UTC_MS}
     * next replacement: {ALIAS_OID, REAL_OID_2, BROADCAST_TIME_UTC_MS + 2 hours}
     */
    @Test
    public void testPersistScopeIdReplacementWithNextReplacementSameDay() throws Exception {
        // insert oid mappings into OidMappingStore
        sqlOidMappingStore.registerOidMappings(Collections.singleton(createOidMapping(ALIAS_OID, REAL_OID,
            BROADCAST_TIME_UTC_MS)));
        sqlOidMappingStore.registerOidMappings(Collections.singleton(createOidMapping(ALIAS_OID, REAL_OID_2,
            BROADCAST_TIME_UTC_MS + TimeUnit.HOURS.toMillis(2))));
        // insert scope for alias oid
        cloudScopeIdentityStore.saveScopeIdentities(Collections.singletonList(CloudScopeIdentity.builder()
            .scopeId(ALIAS_OID)
            .scopeType(CloudScopeIdentity.CloudScopeType.RESOURCE)
            .serviceProviderId(SERVICE_PROVIDER_ID)
            .build()));
        // insert data into cloud cost daily
        insertIntoBilledCostDailyTable(ALIAS_OID, BROADCAST_TIME_UTC_MS_PREVIOUS_DAY);
        insertIntoBilledCostDailyTable(ALIAS_OID, BROADCAST_TIME_UTC_MS);
        insertIntoBilledCostDailyTable(ALIAS_OID, BROADCAST_TIME_UTC_MS_NEXT_DAY);
        // trigger persist
        sqlCloudCostScopeIdReplacementLog.persistScopeIdReplacements();

        verifyPersistence(ALIAS_OID, Collections.singletonList(BROADCAST_TIME_UTC_MS_PREVIOUS_DAY), false);
        verifyPersistence(REAL_OID_2, Arrays.asList(BROADCAST_TIME_UTC_MS, BROADCAST_TIME_UTC_MS_NEXT_DAY), true);
        verifyNoPersistenceToCloudCostDaily(REAL_OID);
    }

    /**
     * Test that Cloud Cost Daily table records are updated correctly with the following replacements:
     * previous replacement: {ALIAS_OID, REAL_OID, BROADCAST_TIME_UTC_MS}
     * new replacement: {ALIAS_OID, REAL_OID_2, BROADCAST_TIME_UTC_MS + 1 day}
     * next replacement: {ALIAS_OID, REAL_OID_3, BROADCAST_TIME_UTC_MS + 2 day}
     */
    @Test
    public void testPersistScopeIdReplacementWithPreviousReplacementAndNextReplacement() throws Exception {
        // insert oid mappings into OidMappingStore
        sqlOidMappingStore.registerOidMappings(Collections.singleton(createOidMapping(ALIAS_OID, REAL_OID,
            BROADCAST_TIME_UTC_MS)));
        // insert scope for alias oid
        cloudScopeIdentityStore.saveScopeIdentities(Collections.singletonList(CloudScopeIdentity.builder()
            .scopeId(ALIAS_OID)
            .scopeType(CloudScopeIdentity.CloudScopeType.RESOURCE)
            .serviceProviderId(SERVICE_PROVIDER_ID)
            .build()));
        // insert data into cloud cost daily
        final long dayAfterNextBroadcast = BROADCAST_TIME_UTC_MS_NEXT_DAY + TimeUnit.DAYS.toMillis(1);
        insertIntoBilledCostDailyTable(ALIAS_OID, BROADCAST_TIME_UTC_MS_PREVIOUS_DAY);
        insertIntoBilledCostDailyTable(ALIAS_OID, BROADCAST_TIME_UTC_MS);
        insertIntoBilledCostDailyTable(ALIAS_OID, BROADCAST_TIME_UTC_MS_NEXT_DAY);
        insertIntoBilledCostDailyTable(ALIAS_OID, dayAfterNextBroadcast);

        // trigger persist
        sqlCloudCostScopeIdReplacementLog.persistScopeIdReplacements();

        verifyPersistence(REAL_OID, Arrays.asList(BROADCAST_TIME_UTC_MS, BROADCAST_TIME_UTC_MS_NEXT_DAY,
            dayAfterNextBroadcast), true);

        // register 2 new mappings
        sqlOidMappingStore.registerOidMappings(Collections.singleton(createOidMapping(ALIAS_OID, REAL_OID_2,
            BROADCAST_TIME_UTC_MS_NEXT_DAY)));
        sqlOidMappingStore.registerOidMappings(Collections.singleton(createOidMapping(ALIAS_OID, REAL_OID_3,
            dayAfterNextBroadcast)));

        // trigger persist
        sqlCloudCostScopeIdReplacementLog.persistScopeIdReplacements();

        verifyPersistence(ALIAS_OID, Collections.singletonList(BROADCAST_TIME_UTC_MS_PREVIOUS_DAY), false);
        verifyPersistence(REAL_OID_2, Collections.singletonList(BROADCAST_TIME_UTC_MS_NEXT_DAY), true);
        verifyPersistence(REAL_OID_3, Collections.singletonList(dayAfterNextBroadcast), true);
    }

    private void insertIntoBilledCostDailyTable(final long scopeId, final long sampleTime) {
        dslContext.insertInto(Tables.CLOUD_COST_DAILY).set(createCloudCostDailyRecord(scopeId,
                Instant.ofEpochMilli(sampleTime).truncatedTo(ChronoUnit.DAYS)))
            .execute();
    }

    private CloudCostDailyRecord createCloudCostDailyRecord(final long scopeId, final Instant sampleTime) {
        final CloudCostDailyRecord record = new CloudCostDailyRecord();
        record.setCost(40D);
        record.setScopeId(scopeId);
        record.setSampleMsUtc(sampleTime);
        record.setCostCategory((short)Cost.CostCategory.ON_DEMAND_COMPUTE_VALUE);
        record.setPriceModel((short)CommonCost.PriceModel.ON_DEMAND.getNumber());
        record.setCurrency((short)CommonCost.CurrencyAmount.getDefaultInstance().getCurrency());
        record.setUsageAmount(100D);
        return record;
    }

    private void verifyNoPersistenceToCloudCostDaily(final long expectedAbsentScopeId) {
        final Result<CloudCostDailyRecord> result = dslContext.selectFrom(Tables.CLOUD_COST_DAILY)
            .where(Tables.CLOUD_COST_DAILY.SCOPE_ID.eq(expectedAbsentScopeId))
            .orderBy(Tables.CLOUD_COST_DAILY.SAMPLE_MS_UTC)
            .fetch();
        Assert.assertTrue(result.isEmpty());
    }

    private void verifyPersistence(final long expectedScopeId, final List<Long> orderedExpectedSampleTimes,
                                   boolean isRealOid) {
        // check that expectedScopeId is stored in cloud_scope table
        final Result<CloudScopeRecord> scopeResult =  dslContext.selectFrom(Tables.CLOUD_SCOPE)
            .where(Tables.CLOUD_SCOPE.SCOPE_ID.eq(expectedScopeId)).fetch();
        Assert.assertTrue(scopeResult.isNotEmpty());

        // check that expectedScopeId is present in the cloud_cost_daily table at the expected time stamps
        final Result<CloudCostDailyRecord> result = dslContext.selectFrom(Tables.CLOUD_COST_DAILY)
            .where(Tables.CLOUD_COST_DAILY.SCOPE_ID.eq(expectedScopeId))
            .orderBy(Tables.CLOUD_COST_DAILY.SAMPLE_MS_UTC)
            .fetch();
        Assert.assertEquals(orderedExpectedSampleTimes.size(), result.size());
        for (int i = 0; i < orderedExpectedSampleTimes.size(); i++) {
            Assert.assertEquals(Instant.ofEpochMilli(orderedExpectedSampleTimes.get(i))
                    .truncatedTo(ChronoUnit.DAYS), result.get(i).getSampleMsUtc());
        }

        // check that expectedScopeId is present in the scope_id_replacement_log table only if real oid is being
        // verified
        if (isRealOid) {
            final Result<ScopeIdReplacementLogRecord> scopeIdReplacementLogResult = dslContext
                .selectFrom(Tables.SCOPE_ID_REPLACEMENT_LOG)
                .where(Tables.SCOPE_ID_REPLACEMENT_LOG.REAL_ID.eq(expectedScopeId))
                .fetch();
            Assert.assertTrue(scopeIdReplacementLogResult.isNotEmpty());
        }
    }
}