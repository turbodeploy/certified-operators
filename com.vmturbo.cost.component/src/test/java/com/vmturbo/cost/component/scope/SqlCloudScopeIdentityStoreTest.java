package com.vmturbo.cost.component.scope;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertTrue;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;

import com.google.common.collect.ImmutableList;

import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.vmturbo.cloud.common.scope.CloudScopeIdentity;
import com.vmturbo.cloud.common.scope.CloudScopeIdentity.CloudScopeType;
import com.vmturbo.cloud.common.scope.CloudScopeIdentityStore.CloudScopeIdentityFilter;
import com.vmturbo.cost.component.db.Cost;
import com.vmturbo.cost.component.db.Tables;
import com.vmturbo.cost.component.db.TestCostDbEndpointConfig;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.sql.utils.MultiDbTestBase;

/**
 * Test class for {@link SqlCloudScopeIdentityStore}.
 */
@RunWith(Parameterized.class)
public class SqlCloudScopeIdentityStoreTest extends MultiDbTestBase {

    private static final CloudScopeIdentity TEST_CLOUD_SCOPE = CloudScopeIdentity.builder()
            .scopeId(123)
            .scopeType(CloudScopeType.RESOURCE)
            .resourceId(111L)
            .resourceType(EntityType.COMPUTE_RESOURCE)
            .accountId(222L)
            .regionId(333L)
            .zoneId(444L)
            .cloudServiceId(555L)
            .resourceGroupId(666L)
            .serviceProviderId(777L)
            .build();

    /**
     * Provide test parameters.
     * @return test parameters.
     */
    @Parameters
    public static Object[][] parameters() {
        return MultiDbTestBase.POSTGRES_CONVERTED_PARAMS;
    }

    private final DSLContext dsl;

    private SqlCloudScopeIdentityStore cloudScopeIdentityStore;


    /**
     * Create a new instance with given parameters.
     *
     * @param configurableDbDialect true to enable POSTGRES_PRIMARY_DB feature flag
     * @param dialect         DB dialect to use
     * @throws SQLException                if a DB operation fails
     * @throws UnsupportedDialectException if dialect is bogus
     * @throws InterruptedException        if we're interrupted
     */
    public SqlCloudScopeIdentityStoreTest(boolean configurableDbDialect, SQLDialect dialect)
            throws SQLException, UnsupportedDialectException, InterruptedException {
        super(Cost.COST, configurableDbDialect, dialect, "cost",
                TestCostDbEndpointConfig::costEndpoint);
        this.dsl = super.getDslContext();
    }

    /**
     * Setup.
     */
    @Before
    public void setup() {
        cloudScopeIdentityStore = new SqlCloudScopeIdentityStore(dsl, 1000);
    }

    /**
     * Test scope identity persistence, including updating the update_ts.
     * @throws InterruptedException Unexpected exception.
     */
    @Test
    public void testPersistence() throws InterruptedException {

        // invoke the store
        cloudScopeIdentityStore.saveScopeIdentities(ImmutableList.of(TEST_CLOUD_SCOPE));

        // check the record exists
        final List<CloudScopeIdentity> storedScopeIdentities = cloudScopeIdentityStore.getIdentitiesByFilter(
                CloudScopeIdentityFilter.ALL_SCOPE_IDENTITIES);

        assertThat(storedScopeIdentities, hasSize(1));
        assertThat(storedScopeIdentities, containsInAnyOrder(TEST_CLOUD_SCOPE));

        // Check the update TS
        final Timestamp firstUpdateTs = dsl.select(Tables.CLOUD_SCOPE.UPDATE_TS)
                .from(Tables.CLOUD_SCOPE)
                .where(Tables.CLOUD_SCOPE.SCOPE_ID.eq(TEST_CLOUD_SCOPE.scopeId()))
                .fetchOne().value1();

        // The update TS has second granularity - make sure at least 1 second has passed before updating
        // the scope
        Thread.sleep(1000);

        // Persist the record again to update the update TS
        cloudScopeIdentityStore.saveScopeIdentities(ImmutableList.of(TEST_CLOUD_SCOPE));

        final Timestamp secondUpdateTs = dsl.select(Tables.CLOUD_SCOPE.UPDATE_TS)
                .from(Tables.CLOUD_SCOPE)
                .where(Tables.CLOUD_SCOPE.SCOPE_ID.eq(TEST_CLOUD_SCOPE.scopeId()))
                .fetchOne().value1();

        // Make sure the second update timestamp is after the first
        assertThat(firstUpdateTs.getTime(), lessThan(secondUpdateTs.getTime()));
    }

    @Test
    public void testFilterScopeId() {

        // store the cloud scope
        cloudScopeIdentityStore.saveScopeIdentities(ImmutableList.of(TEST_CLOUD_SCOPE));

        // Check the entry is found
        final CloudScopeIdentityFilter inclusiveFilter = CloudScopeIdentityFilter.builder()
                .addScopeId(123L)
                .build();
        final List<CloudScopeIdentity> inclusiveIdentities = cloudScopeIdentityStore.getIdentitiesByFilter(inclusiveFilter);

        // verify it is found
        assertThat(inclusiveIdentities, hasSize(1));
        assertThat(inclusiveIdentities, containsInAnyOrder(TEST_CLOUD_SCOPE));

        // Make sure it's excluded when it should be
        final CloudScopeIdentityFilter exclusiveFilter = CloudScopeIdentityFilter.builder()
                .addScopeId(333333L)
                .build();
        final List<CloudScopeIdentity> exclusiveIdentities = cloudScopeIdentityStore.getIdentitiesByFilter(exclusiveFilter);

        // verify the list is empty
        assertTrue(exclusiveIdentities.isEmpty());
    }

    @Test
    public void testFilterScopeType() {

        // store the cloud scope
        cloudScopeIdentityStore.saveScopeIdentities(ImmutableList.of(TEST_CLOUD_SCOPE));

        // Check the entry is found
        final CloudScopeIdentityFilter inclusiveFilter = CloudScopeIdentityFilter.builder()
                .addScopeType(CloudScopeType.RESOURCE)
                .build();
        final List<CloudScopeIdentity> inclusiveIdentities = cloudScopeIdentityStore.getIdentitiesByFilter(inclusiveFilter);

        // verify it is found
        assertThat(inclusiveIdentities, hasSize(1));
        assertThat(inclusiveIdentities, containsInAnyOrder(TEST_CLOUD_SCOPE));

        // Make sure it's excluded when it should be
        final CloudScopeIdentityFilter exclusiveFilter = CloudScopeIdentityFilter.builder()
                .addScopeType(CloudScopeType.AGGREGATE)
                .build();
        final List<CloudScopeIdentity> exclusiveIdentities = cloudScopeIdentityStore.getIdentitiesByFilter(exclusiveFilter);

        // verify the list is empty
        assertTrue(exclusiveIdentities.isEmpty());
    }

    @Test
    public void testFilterAccountId() {

        // store the cloud scope
        cloudScopeIdentityStore.saveScopeIdentities(ImmutableList.of(TEST_CLOUD_SCOPE));

        // Check the entry is found
        final CloudScopeIdentityFilter inclusiveFilter = CloudScopeIdentityFilter.builder()
                .addAccountId(222L)
                .build();
        final List<CloudScopeIdentity> inclusiveIdentities = cloudScopeIdentityStore.getIdentitiesByFilter(inclusiveFilter);

        // verify it is found
        assertThat(inclusiveIdentities, hasSize(1));
        assertThat(inclusiveIdentities, containsInAnyOrder(TEST_CLOUD_SCOPE));

        // Make sure it's excluded when it should be
        final CloudScopeIdentityFilter exclusiveFilter = CloudScopeIdentityFilter.builder()
                .addAccountId(333333L)
                .build();
        final List<CloudScopeIdentity> exclusiveIdentities = cloudScopeIdentityStore.getIdentitiesByFilter(exclusiveFilter);

        // verify the list is empty
        assertTrue(exclusiveIdentities.isEmpty());
    }
}
