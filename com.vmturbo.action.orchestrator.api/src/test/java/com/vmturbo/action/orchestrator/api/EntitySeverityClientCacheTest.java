package com.vmturbo.action.orchestrator.api;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import com.google.common.collect.ImmutableMap;

import it.unimi.dsi.fastutil.longs.LongList;

import org.hamcrest.Matchers;
import org.junit.Test;

import com.vmturbo.common.protobuf.action.ActionDTO.Severity;
import com.vmturbo.common.protobuf.action.EntitySeverityNotificationOuterClass.EntitySeverityNotification.EntitiesWithSeverity;
import com.vmturbo.common.protobuf.action.EntitySeverityNotificationOuterClass.EntitySeverityNotification.SeverityBreakdown;
import com.vmturbo.common.protobuf.action.EntitySeverityNotificationOuterClass.EntitySeverityNotification.SeverityBreakdown.SingleSeverityCount;

/**
 * Unit tests for {@link EntitySeverityClientCache}.
 */
public class EntitySeverityClientCacheTest {

    /**
     * Test that ordering by severity is stable (OID as secondary sort).
     */
    @Test
    public void testOrderBySeverityStable() {
        EntitySeverityClientCache clientCache = new EntitySeverityClientCache();
        EntitiesWithSeverity entitiesWithSeverity = EntitiesWithSeverity.newBuilder().setSeverity(
                Severity.MAJOR).addOids(1L).addOids(2L).build();
        clientCache.entitySeveritiesRefresh(Collections.singletonList(entitiesWithSeverity), Collections.emptyMap());

        LongList l = clientCache.sortBySeverity(entitiesWithSeverity.getOidsList(), true);
        assertThat(l, Matchers.contains(1L, 2L));
    }

    /**
     * Ascending vs. descending flags should work.
     */
    @Test
    public void testAscendingVsDescending() {
        EntitySeverityClientCache clientCache = new EntitySeverityClientCache();
        EntitiesWithSeverity entitiesWithSeverity = EntitiesWithSeverity.newBuilder().setSeverity(
                Severity.MAJOR).addOids(1L).addOids(2L).build();
        clientCache.entitySeveritiesRefresh(Collections.singletonList(entitiesWithSeverity), Collections.emptyMap());

        LongList l = clientCache.sortBySeverity(entitiesWithSeverity.getOidsList(), true);
        assertThat(l, Matchers.contains(1L, 2L));
        LongList l2 = clientCache.sortBySeverity(entitiesWithSeverity.getOidsList(), false);
        assertThat(l2, Matchers.contains(2L, 1L));
    }

    /**
     * Test that ordering by breakdown is stable (OID as secondary sort).
     */
    @Test
    public void testOrderByBreakdownStable() {
        EntitySeverityClientCache clientCache = new EntitySeverityClientCache();
        // Same
        clientCache.entitySeveritiesRefresh(Collections.emptyList(),
                ImmutableMap.of(1L, newBreakdown(Severity.MAJOR, 1),
                2L, newBreakdown(Severity.MAJOR, 1)));

        LongList l = clientCache.sortBySeverity(Arrays.asList(2L, 1L), true);
        assertThat(l, Matchers.contains(1L, 2L));
    }

    /**
     * Empty breakdown is less than breakdown with some counts.
     */
    @Test
    public void testOrderByEmptyBreakdown() {
        EntitySeverityClientCache clientCache = new EntitySeverityClientCache();
        // Same
        clientCache.entitySeveritiesRefresh(Collections.emptyList(),
                ImmutableMap.of(2L, newBreakdown(Collections.emptyMap()),
                        1L, newBreakdown(Severity.MINOR, 1)));

        LongList l = clientCache.sortBySeverity(Arrays.asList(1L, 2L), true);
        assertThat(l, Matchers.contains(2L, 1L));

        LongList l2 = clientCache.sortBySeverity(Arrays.asList(2L, 1L), true);
        assertThat(l, Matchers.contains(2L, 1L));
    }

    /**
     * When comparing breakdown to severity, use highest severity within breakdown.
     */
    @Test
    public void testOrderByBreakdownVsSeverity() {
        // Look at highest severity within breakdown.
        EntitySeverityClientCache clientCache = new EntitySeverityClientCache();
        EntitiesWithSeverity entitiesWithSeverity = EntitiesWithSeverity.newBuilder().setSeverity(
                Severity.MINOR).addOids(2L).build();

        clientCache.entitySeveritiesRefresh(Collections.singleton(entitiesWithSeverity),
            ImmutableMap.of(1L, newBreakdown(ImmutableMap.of(Severity.MAJOR, 1, Severity.MINOR, 1))));

        LongList l = clientCache.sortBySeverity(Arrays.asList(1L, 2L), true);
        // 1 has a lower severity than the highest severity under 2.
        assertThat(l, Matchers.contains(2L, 1L));
    }

    /**
     * Empty breakdown is less "severe" than an entity with a non-NORMAL severity.
     */
    @Test
    public void testOrderByEmptyBreakdownVsSeverity() {
        EntitySeverityClientCache clientCache = new EntitySeverityClientCache();
        EntitiesWithSeverity entitiesWithSeverity = EntitiesWithSeverity.newBuilder().setSeverity(
                Severity.MINOR).addOids(1L).build();

        clientCache.entitySeveritiesRefresh(Collections.singleton(entitiesWithSeverity),
                ImmutableMap.of(2L, newBreakdown(Collections.emptyMap())));

        LongList l = clientCache.sortBySeverity(Arrays.asList(1L, 2L), true);
        assertThat(l, Matchers.contains(2L, 1L));

    }

    /**
     * Bigger ratio of higher-severity actions takes precedence.
     */
    @Test
    public void testOrderByBreakdownBiggerRatio() {
        EntitySeverityClientCache clientCache = new EntitySeverityClientCache();

        // 1 - 33% major 66% minor
        // 2 - 50% major 50% minor
        clientCache.entitySeveritiesRefresh(Collections.emptyList(),
                ImmutableMap.of(2L, newBreakdown(ImmutableMap.of(Severity.MAJOR, 2, Severity.MINOR, 3)),
                        1L, newBreakdown(ImmutableMap.of(Severity.MINOR, 3, Severity.MAJOR, 3))));

        LongList l = clientCache.sortBySeverity(Arrays.asList(1L, 2L), true);
        assertThat(l, Matchers.contains(2L, 1L));
    }

    /**
     * Same ratios, but one entity has more total actions.
     */
    @Test
    public void testOrderByBreakdownSameRatioBiggerTotal() {
        EntitySeverityClientCache clientCache = new EntitySeverityClientCache();

        // 1 - 50% major 50% minor, 6 actions
        // 2 - 50% major 50% minor, 8 actions
        clientCache.entitySeveritiesRefresh(Collections.emptyList(),
                ImmutableMap.of(2L, newBreakdown(ImmutableMap.of(Severity.MAJOR, 3, Severity.MINOR, 3)),
                        1L, newBreakdown(ImmutableMap.of(Severity.MINOR, 4, Severity.MAJOR, 4))));

        LongList l = clientCache.sortBySeverity(Arrays.asList(1L, 2L), true);
        assertThat(l, Matchers.contains(2L, 1L));

    }

    /**
     * Same higher severity ratio, but different lower severity ratio.
     */
    @Test
    public void testOrderByBreakdownOrder() {
        // Same higher severity.
        // Difference is in a lower severity.
        EntitySeverityClientCache clientCache = new EntitySeverityClientCache();

        // 1 - 10% critical, 40% major, 50% minor.
        // 2 - 10% critical, 50% major, 40% minor.
        clientCache.entitySeveritiesRefresh(Collections.emptyList(),
                ImmutableMap.of(2L, newBreakdown(ImmutableMap.of(Severity.CRITICAL, 1, Severity.MAJOR, 4, Severity.MINOR, 5)),
                        1L, newBreakdown(ImmutableMap.of(Severity.CRITICAL, 1, Severity.MINOR, 4, Severity.MAJOR, 5))));

        LongList l = clientCache.sortBySeverity(Arrays.asList(1L, 2L), true);
        assertThat(l, Matchers.contains(2L, 1L));
    }

    /**
     * Test that a missing entity is considered to be smaller (i.e. normal severity).
     */
    @Test
    public void testOrderByMissingEntity() {
        EntitySeverityClientCache clientCache = new EntitySeverityClientCache();
        EntitiesWithSeverity entitiesWithSeverity = EntitiesWithSeverity.newBuilder().setSeverity(
                Severity.MAJOR).addOids(1L).build();
        clientCache.entitySeveritiesRefresh(Collections.singletonList(entitiesWithSeverity), Collections.emptyMap());

        LongList l = clientCache.sortBySeverity(Arrays.asList(1L, 2L), true);
        // Missing entity is smaller.
        assertThat(l, Matchers.contains(2L, 1L));
    }

    /**
     * Sorting - test that severity is ignored if both entities have severity breakdowns.
     */
    @Test
    public void testOrderByBreakdownIgnoresSeverity() {
        EntitySeverityClientCache clientCache = new EntitySeverityClientCache();
        EntitiesWithSeverity entitiesWithSeverity = EntitiesWithSeverity.newBuilder().setSeverity(
                Severity.MINOR).addOids(1L).build();
        EntitiesWithSeverity criticalEntitiesWithSeverity = EntitiesWithSeverity.newBuilder().setSeverity(
                Severity.CRITICAL).addOids(2L).build();

        // Even though 2 has a higher individual severity, we only look at the breakdown.
        clientCache.entitySeveritiesRefresh(Arrays.asList(entitiesWithSeverity, criticalEntitiesWithSeverity),
                ImmutableMap.of(2L, newBreakdown(ImmutableMap.of(Severity.MINOR, 1)),
                        1L, newBreakdown(ImmutableMap.of(Severity.CRITICAL, 2))));

        LongList l = clientCache.sortBySeverity(Arrays.asList(2L, 1L), true);
        assertThat(l, Matchers.contains(2L, 1L));
    }

    /**
     * Test getting severity counts.
     */
    @Test
    public void testGetSeverityCounts() {
        EntitySeverityClientCache clientCache = new EntitySeverityClientCache();
        EntitiesWithSeverity entitiesWithSeverity = EntitiesWithSeverity.newBuilder().setSeverity(
                Severity.MAJOR).addOids(1L).build();

        clientCache.entitySeveritiesRefresh(Collections.singleton(entitiesWithSeverity),
                ImmutableMap.of(2L, newBreakdown(ImmutableMap.of(Severity.MAJOR, 1, Severity.MINOR, 1)),
                        3L, newBreakdown(ImmutableMap.of(Severity.CRITICAL, 1))));

        Map<Severity, Long> counts = clientCache.getSeverityCounts(Arrays.asList(1L, 2L, 3L));
        assertThat(counts.get(Severity.CRITICAL), is(1L));
        assertThat(counts.get(Severity.MINOR), is(1L));
        assertThat(counts.get(Severity.MAJOR), is(2L));
    }

    /**
     * Test getting the numbers of breakdowns/severities.
     */
    @Test
    public void testGetNums() {
        EntitySeverityClientCache clientCache = new EntitySeverityClientCache();
        EntitiesWithSeverity entitiesWithSeverity = EntitiesWithSeverity.newBuilder().setSeverity(
                Severity.MAJOR).addOids(1L).build();

        clientCache.entitySeveritiesRefresh(Collections.singleton(entitiesWithSeverity),
                ImmutableMap.of(2L, newBreakdown(ImmutableMap.of(Severity.MAJOR, 1, Severity.MINOR, 1)),
                        3L, newBreakdown(ImmutableMap.of(Severity.CRITICAL, 1))));

        assertThat(clientCache.numBreakdowns(), is(2));
        assertThat(clientCache.numSeverities(), is(1));
    }

    /**
     * Test getting severities.
     */
    @Test
    public void testGetSeverity() {
        EntitySeverityClientCache clientCache = new EntitySeverityClientCache();
        EntitiesWithSeverity entitiesWithSeverity = EntitiesWithSeverity.newBuilder().setSeverity(
                Severity.MAJOR).addOids(1L).build();
        clientCache.entitySeveritiesRefresh(Collections.singleton(entitiesWithSeverity), Collections.emptyMap());

        assertThat(clientCache.getEntitySeverity(1L), is(Severity.MAJOR));
        // Non-existing entity is normal.
        assertThat(clientCache.getEntitySeverity(2L), is(Severity.NORMAL));
    }

    /**
     * Test that an update takes effect, and does not affect non-updated entries.
     */
    @Test
    public void testUpdate() {
        EntitySeverityClientCache clientCache = new EntitySeverityClientCache();
        EntitiesWithSeverity entitiesWithSeverity = EntitiesWithSeverity.newBuilder().setSeverity(
                Severity.MAJOR).addOids(1L).build();
        EntitiesWithSeverity entitiesWithCriticalSeverity = EntitiesWithSeverity.newBuilder().setSeverity(
                Severity.CRITICAL).addOids(4L).build();

        clientCache.entitySeveritiesRefresh(Arrays.asList(entitiesWithSeverity, entitiesWithCriticalSeverity),
                ImmutableMap.of(2L, newBreakdown(ImmutableMap.of(Severity.MAJOR, 1, Severity.MINOR, 1)),
                        3L, newBreakdown(ImmutableMap.of(Severity.CRITICAL, 1))));

        EntitiesWithSeverity updatedEntitiesWithSeverity = EntitiesWithSeverity.newBuilder().setSeverity(
                Severity.MINOR).addOids(1L).build();

        clientCache.entitySeveritiesUpdate(Collections.singleton(updatedEntitiesWithSeverity),
                ImmutableMap.of(2L, newBreakdown(ImmutableMap.of(Severity.MINOR, 1))));

        // Updated entity 1's severity.
        assertThat(clientCache.getEntitySeverity(1L), is(Severity.MINOR));
        // Updated entity 2's counts
        assertThat(clientCache.getSeverityCounts(Collections.singletonList(2L)), is(Collections.singletonMap(Severity.MINOR, 1L)));
        // Entity 3 and 4's counts persisted from before.
        assertThat(clientCache.getSeverityCounts(Collections.singletonList(3L)), is(Collections.singletonMap(Severity.CRITICAL, 1L)));
        assertThat(clientCache.getEntitySeverity(4L), is(Severity.CRITICAL));

    }

    /**
     * Test that a refresh overwrites the data completely.
     */
    @Test
    public void testRefreshOverwritesData() {
        EntitySeverityClientCache clientCache = new EntitySeverityClientCache();
        EntitiesWithSeverity entitiesWithSeverity = EntitiesWithSeverity.newBuilder().setSeverity(
                Severity.MAJOR).addOids(1L).build();
        EntitiesWithSeverity entitiesWithCriticalSeverity = EntitiesWithSeverity.newBuilder().setSeverity(
                Severity.CRITICAL).addOids(4L).build();

        clientCache.entitySeveritiesRefresh(Arrays.asList(entitiesWithSeverity, entitiesWithCriticalSeverity),
                ImmutableMap.of(2L, newBreakdown(ImmutableMap.of(Severity.MAJOR, 1, Severity.MINOR, 1)),
                        3L, newBreakdown(ImmutableMap.of(Severity.CRITICAL, 1))));

        EntitiesWithSeverity updatedEntitiesWithSeverity = EntitiesWithSeverity.newBuilder().setSeverity(
                Severity.MINOR).addOids(1L).build();

        clientCache.entitySeveritiesRefresh(Collections.singleton(updatedEntitiesWithSeverity),
                ImmutableMap.of(2L, newBreakdown(ImmutableMap.of(Severity.MINOR, 1))));

        // Entity 1 and 2 have new severities and counts
        assertThat(clientCache.getEntitySeverity(1L), is(Severity.MINOR));
        assertThat(clientCache.getSeverityCounts(Collections.singletonList(2L)), is(Collections.singletonMap(Severity.MINOR, 1L)));
        // Entity 3 and 4's counts cleared.
        assertThat(clientCache.getSeverityCounts(Collections.singletonList(3L)), is(Collections.singletonMap(Severity.NORMAL, 1L)));
        assertThat(clientCache.getEntitySeverity(4L), is(Severity.NORMAL));

    }

    private SeverityBreakdown newBreakdown(Map<Severity, Integer> cnt) {
        SeverityBreakdown.Builder b = SeverityBreakdown.newBuilder();
        cnt.forEach((s, c) -> b.addCounts(SingleSeverityCount.newBuilder()
                .setSeverity(s)
                .setCount(c)
                .build()));
        return b.build();
    }

    private SeverityBreakdown newBreakdown(Severity s, Integer c) {
        return newBreakdown(Collections.singletonMap(s, c));
    }
}
