package com.vmturbo.action.orchestrator.stats.groups;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

import org.jooq.DSLContext;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import com.vmturbo.action.orchestrator.db.Action;
import com.vmturbo.action.orchestrator.stats.ManagementUnitType;
import com.vmturbo.action.orchestrator.stats.aggregator.GlobalActionAggregator;
import com.vmturbo.action.orchestrator.stats.groups.MgmtUnitSubgroup.MgmtUnitSubgroupKey;
import com.vmturbo.action.orchestrator.stats.groups.MgmtUnitSubgroupStore.QueryResult;
import com.vmturbo.common.protobuf.action.ActionDTO.HistoricalActionStatsQuery.GroupBy;
import com.vmturbo.common.protobuf.action.ActionDTO.HistoricalActionStatsQuery.MgmtUnitSubgroupFilter;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.sql.utils.DbCleanupRule;
import com.vmturbo.sql.utils.DbConfigurationRule;

public class MgmtUnitSubgroupStoreTest {
    /**
     * Rule to create the DB schema and migrate it.
     */
    @ClassRule
    public static DbConfigurationRule dbConfig = new DbConfigurationRule(Action.ACTION);

    /**
     * Rule to automatically cleanup DB data before each test.
     */
    @Rule
    public DbCleanupRule dbCleanup = dbConfig.cleanupRule();

    private DSLContext dsl = dbConfig.getDslContext();

    private MgmtUnitSubgroupStore mgmtUnitSubgroupStore = new MgmtUnitSubgroupStore(dsl);

    @Test
    public void testUpsert() {
        final MgmtUnitSubgroupKey key = ImmutableMgmtUnitSubgroupKey.builder()
            .entityType(1)
            .environmentType(EnvironmentType.CLOUD)
            .mgmtUnitType(ManagementUnitType.CLUSTER)
            .mgmtUnitId(123)
            .build();
        final Map<MgmtUnitSubgroupKey, MgmtUnitSubgroup> subgroups =
                mgmtUnitSubgroupStore.ensureExist(Collections.singleton(key));
        assertThat(subgroups.get(key).key(), is(key));
    }

    @Test
    public void testUpsertUnsetOptionals() {
        final MgmtUnitSubgroupKey key = ImmutableMgmtUnitSubgroupKey.builder()
            .mgmtUnitId(123)
            .mgmtUnitType(ManagementUnitType.CLUSTER)
            .environmentType(EnvironmentType.ON_PREM)
            .build();
        final Map<MgmtUnitSubgroupKey, MgmtUnitSubgroup> subgroups =
                mgmtUnitSubgroupStore.ensureExist(Collections.singleton(key));
        assertThat(subgroups.get(key).key(), is(key));
    }

    @Test
    public void testUpsertRetainExisting() {
        final MgmtUnitSubgroupKey key1 = ImmutableMgmtUnitSubgroupKey.builder()
            .entityType(1)
            .environmentType(EnvironmentType.CLOUD)
            .mgmtUnitId(123)
            .mgmtUnitType(ManagementUnitType.CLUSTER)
            .build();
        final Map<MgmtUnitSubgroupKey, MgmtUnitSubgroup> initialSubgroups =
            mgmtUnitSubgroupStore.ensureExist(Collections.singleton(key1));
        assertThat(initialSubgroups.get(key1).key(), is(key1));
        final int key1Id = initialSubgroups.get(key1).id();

        final MgmtUnitSubgroupKey key2 = ImmutableMgmtUnitSubgroupKey.builder()
            .entityType(2)
            .environmentType(EnvironmentType.ON_PREM)
            .mgmtUnitId(432)
            .mgmtUnitType(ManagementUnitType.CLUSTER)
            .build();
        // Insert the same one again.
        final Map<MgmtUnitSubgroupKey, MgmtUnitSubgroup> subgroups =
                mgmtUnitSubgroupStore.ensureExist(Sets.newHashSet(key1, key2));
        assertThat(subgroups.get(key2).key(), is(key2));
        assertThat(subgroups.get(key1).key(), is(key1));
        // ID should be the original ID.
        assertThat(subgroups.get(key1).id(), is(key1Id));
    }

    @Test
    public void testUpsertDuplicate() {
        final MgmtUnitSubgroupKey key = ImmutableMgmtUnitSubgroupKey.builder()
            .entityType(1)
            .environmentType(EnvironmentType.CLOUD)
            .mgmtUnitType(ManagementUnitType.CLUSTER)
            .mgmtUnitId(123)
            .build();
        final Map<MgmtUnitSubgroupKey, MgmtUnitSubgroup> initialSubgroups =
            mgmtUnitSubgroupStore.ensureExist(Collections.singleton(key));
        final int keyId = initialSubgroups.get(key).id();

        // Insert the same one again.
        final Map<MgmtUnitSubgroupKey, MgmtUnitSubgroup> subgroups =
                mgmtUnitSubgroupStore.ensureExist(Collections.singleton(key));
        assertThat(subgroups.get(key).key(), is(key));
        // ID should be the original ID.
        assertThat(subgroups.get(key).id(), is(keyId));
    }

    @Test
    public void testQueryMgmtUnit() {
        final long mu1Id = 123;
        final MgmtUnitSubgroupKey mu1Key1 = ImmutableMgmtUnitSubgroupKey.builder()
            .environmentType(EnvironmentType.CLOUD)
            .mgmtUnitType(ManagementUnitType.CLUSTER)
            .mgmtUnitId(mu1Id)
            .build();
        final MgmtUnitSubgroupKey mu2 = ImmutableMgmtUnitSubgroupKey.builder()
            // Different entity type.
            .entityType(2)
            .environmentType(EnvironmentType.CLOUD)
            .mgmtUnitType(ManagementUnitType.CLUSTER)
            .mgmtUnitId(mu1Id + 1)
            .build();
        final Map<MgmtUnitSubgroupKey, MgmtUnitSubgroup> subgroups =
            mgmtUnitSubgroupStore.ensureExist(Sets.newHashSet(mu1Key1, mu2));

        final Optional<QueryResult> result =
            mgmtUnitSubgroupStore.query(MgmtUnitSubgroupFilter.newBuilder()
                .setMgmtUnitId(mu1Id)
                .build(), GroupBy.ACTION_CATEGORY);
        assertTrue(result.isPresent());
        assertThat(result.get().mgmtUnit().get(), is(123L));
        assertThat(result.get().mgmtUnitSubgroups(),
            is(Collections.singletonMap(subgroups.get(mu1Key1).id(), subgroups.get(mu1Key1))));
    }

    @Test
    public void testQueryFilterByEntityType() {
        final long mu1Id = 123;
        final MgmtUnitSubgroupKey mu1Key1 = ImmutableMgmtUnitSubgroupKey.builder()
            .entityType(1)
            .environmentType(EnvironmentType.CLOUD)
            .mgmtUnitType(ManagementUnitType.CLUSTER)
            .mgmtUnitId(mu1Id)
            .build();
        final MgmtUnitSubgroupKey mu1Key2 = ImmutableMgmtUnitSubgroupKey.builder()
            // Different entity type.
            .entityType(2)
            .environmentType(EnvironmentType.CLOUD)
            .mgmtUnitType(ManagementUnitType.CLUSTER)
            .mgmtUnitId(mu1Id)
            .build();
        final Map<MgmtUnitSubgroupKey, MgmtUnitSubgroup> subgroups =
            mgmtUnitSubgroupStore.ensureExist(Sets.newHashSet(mu1Key1, mu1Key2));

        final Optional<QueryResult> result =
            mgmtUnitSubgroupStore.query(MgmtUnitSubgroupFilter.newBuilder()
                .setMgmtUnitId(mu1Id)
                .addEntityType(1)
                .build(), GroupBy.ACTION_CATEGORY);
        assertTrue(result.isPresent());
        assertThat(result.get().mgmtUnit().get(), is(mu1Id));
        assertThat(result.get().mgmtUnitSubgroups(),
            is(Collections.singletonMap(subgroups.get(mu1Key1).id(), subgroups.get(mu1Key1))));
    }

    @Test
    public void testQueryFilterUnsetEntityType() {
        final long mu1Id = 123;
        final MgmtUnitSubgroupKey mu1Key1 = ImmutableMgmtUnitSubgroupKey.builder()
            .environmentType(EnvironmentType.CLOUD)
            .mgmtUnitType(ManagementUnitType.CLUSTER)
            .mgmtUnitId(mu1Id)
            .build();
        final MgmtUnitSubgroupKey mu1Key2 = ImmutableMgmtUnitSubgroupKey.builder()
            // Different entity type.
            .entityType(2)
            .environmentType(EnvironmentType.CLOUD)
            .mgmtUnitType(ManagementUnitType.CLUSTER)
            .mgmtUnitId(mu1Id)
            .build();
        final Map<MgmtUnitSubgroupKey, MgmtUnitSubgroup> subgroups =
            mgmtUnitSubgroupStore.ensureExist(Sets.newHashSet(mu1Key1, mu1Key2));

        final Optional<QueryResult> result =
            mgmtUnitSubgroupStore.query(MgmtUnitSubgroupFilter.newBuilder()
                .setMgmtUnitId(mu1Id)
                // Unset entity type in the request.
                .build(), GroupBy.ACTION_CATEGORY);
        assertTrue(result.isPresent());
        assertThat(result.get().mgmtUnit().get(), is(mu1Id));
        // Should only return the ID of the mgmt unit subgroup with no entity type set.
        assertThat(result.get().mgmtUnitSubgroups(),
            is(Collections.singletonMap(subgroups.get(mu1Key1).id(), subgroups.get(mu1Key1))));
    }

    @Test
    public void testQueryFilterByEnvironmentType() {
        final long mu1Id = 123;
        final MgmtUnitSubgroupKey mu1Key1 = ImmutableMgmtUnitSubgroupKey.builder()
            .entityType(1)
            .environmentType(EnvironmentType.CLOUD)
            .mgmtUnitType(ManagementUnitType.GLOBAL)
            .mgmtUnitId(mu1Id)
            .build();
        final MgmtUnitSubgroupKey mu1Key2 = ImmutableMgmtUnitSubgroupKey.builder()
            .entityType(1)
            // Different environment type type.
            .environmentType(EnvironmentType.ON_PREM)
            .mgmtUnitType(ManagementUnitType.GLOBAL)
            .mgmtUnitId(mu1Id)
            .build();
        final Map<MgmtUnitSubgroupKey, MgmtUnitSubgroup> subgroups =
            mgmtUnitSubgroupStore.ensureExist(Sets.newHashSet(mu1Key1, mu1Key2));

        final Optional<QueryResult> result =
            mgmtUnitSubgroupStore.query(MgmtUnitSubgroupFilter.newBuilder()
                .setMgmtUnitId(mu1Id)
                .setEnvironmentType(EnvironmentType.CLOUD)
                .addEntityType(1)
                .build(), GroupBy.ACTION_CATEGORY);
        assertTrue(result.isPresent());
        assertThat(result.get().mgmtUnit().get(), is(mu1Id));
        assertThat(result.get().mgmtUnitSubgroups(),
            is(Collections.singletonMap(subgroups.get(mu1Key1).id(), subgroups.get(mu1Key1))));
    }

    @Test
    public void testQueryFilterClusterByEnvironmentType() {
        final long mu1Id = 123;
        final MgmtUnitSubgroupKey mu1Key1 = ImmutableMgmtUnitSubgroupKey.builder()
            .entityType(1)
            .mgmtUnitType(ManagementUnitType.CLUSTER)
            .mgmtUnitId(mu1Id)
            .environmentType(EnvironmentType.ON_PREM)
            .build();
        final Map<MgmtUnitSubgroupKey, MgmtUnitSubgroup> subgroups =
            mgmtUnitSubgroupStore.ensureExist(Sets.newHashSet(mu1Key1));

        final Optional<QueryResult> result =
            mgmtUnitSubgroupStore.query(MgmtUnitSubgroupFilter.newBuilder()
                .setMgmtUnitId(mu1Id)
                // Check if we can find it by explicitly specifying the on-prem env type.
                .setEnvironmentType(EnvironmentType.ON_PREM)
                .addEntityType(1)
                .build(), GroupBy.ACTION_CATEGORY);
        assertTrue(result.isPresent());
        assertThat(result.get().mgmtUnit().get(), is(mu1Id));
        assertThat(result.get().mgmtUnitSubgroups(),
            is(Collections.singletonMap(subgroups.get(mu1Key1).id(), subgroups.get(mu1Key1))));

        final Optional<QueryResult> result2 =
            mgmtUnitSubgroupStore.query(MgmtUnitSubgroupFilter.newBuilder()
                .setMgmtUnitId(mu1Id)
                // Check if we can find it without explicitly specifying an env type.
                .addEntityType(1)
                .build(), GroupBy.ACTION_CATEGORY);
        // Should be the same result.
        assertThat(result2, is(result));
    }

    @Test
    public void testQueryMarket() {
        final long mu1Id = GlobalActionAggregator.GLOBAL_MGMT_UNIT_ID;
        final MgmtUnitSubgroupKey mu1Key1 = ImmutableMgmtUnitSubgroupKey.builder()
            .environmentType(EnvironmentType.CLOUD)
            .mgmtUnitType(ManagementUnitType.GLOBAL)
            .mgmtUnitId(mu1Id)
            .build();
        final MgmtUnitSubgroupKey mu1Key2 = ImmutableMgmtUnitSubgroupKey.builder()
            // Different env type.
            .environmentType(EnvironmentType.ON_PREM)
            .mgmtUnitType(ManagementUnitType.GLOBAL)
            .mgmtUnitId(mu1Id)
            .build();
        final MgmtUnitSubgroupKey mu2 = ImmutableMgmtUnitSubgroupKey.builder()
            .environmentType(EnvironmentType.CLOUD)
            .mgmtUnitType(ManagementUnitType.GLOBAL)
            .mgmtUnitId(123)
            .build();
        final Map<MgmtUnitSubgroupKey, MgmtUnitSubgroup> subgroups =
            mgmtUnitSubgroupStore.ensureExist(Sets.newHashSet(mu1Key1, mu1Key2, mu2));

        final Optional<QueryResult> result =
            mgmtUnitSubgroupStore.query(MgmtUnitSubgroupFilter.newBuilder()
                .setMarket(true)
                .build(), GroupBy.ACTION_CATEGORY);
        assertTrue(result.isPresent());

        // Mgmt unit should be unset.
        assertFalse(result.get().mgmtUnit().isPresent());

        assertThat(result.get().mgmtUnitSubgroups(),
            is(ImmutableMap.of(subgroups.get(mu1Key1).id(), subgroups.get(mu1Key1),
                subgroups.get(mu1Key2).id(), subgroups.get(mu1Key2))));
    }

    @Test
    public void testQueryNoResult() {
        final Optional<QueryResult> result =
            mgmtUnitSubgroupStore.query(MgmtUnitSubgroupFilter.newBuilder()
                .setMgmtUnitId(7)
                .build(), GroupBy.ACTION_CATEGORY);
        assertFalse(result.isPresent());
    }

    @Test
    public void testQueryBadFilter() {
        final Optional<QueryResult> result =
            mgmtUnitSubgroupStore.query(MgmtUnitSubgroupFilter.newBuilder()
                // No market, and no specific mgmt unit.
                .build(), GroupBy.ACTION_CATEGORY);
        assertFalse(result.isPresent());
    }
}
