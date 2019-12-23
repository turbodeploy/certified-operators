package com.vmturbo.api.component.external.api.util.stats;

import static com.vmturbo.api.component.external.api.util.stats.StatsTestUtil.snapshotWithStats;
import static com.vmturbo.api.component.external.api.util.stats.StatsTestUtil.stat;
import static com.vmturbo.api.component.external.api.util.stats.StatsTestUtil.statInput;
import static com.vmturbo.api.component.external.api.util.stats.StatsTestUtil.statWithKey;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryContextFactory.StatsQueryContext;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryScopeExpander.StatsQueryScope;
import com.vmturbo.api.component.external.api.util.stats.query.StatsSubQuery;
import com.vmturbo.api.component.external.api.util.stats.query.SubQuerySupportedStats;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.statistic.StatApiInputDTO;
import com.vmturbo.api.dto.statistic.StatPeriodApiInputDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.topology.processor.api.util.ImmutableThinProbeInfo;
import com.vmturbo.topology.processor.api.util.ImmutableThinTargetInfo;
import com.vmturbo.topology.processor.api.util.ThinTargetCache.ThinTargetInfo;

public class StatsQueryExecutorTest {

    private StatsQueryContextFactory contextFactory = mock(StatsQueryContextFactory.class);

    private StatsQueryScopeExpander scopeExpander = mock(StatsQueryScopeExpander.class);

    private StatsSubQuery statsSubQuery1 = mock(StatsSubQuery.class);

    private StatsSubQuery statsSubQuery2 = mock(StatsSubQuery.class);

    private StatsQueryExecutor executor = new StatsQueryExecutor(contextFactory,
        scopeExpander);

    private ApiId scope = mock(ApiId.class);

    private StatsQueryScope expandedScope = mock(StatsQueryScope.class);

    final StatsQueryContext statsQueryContext = mock(StatsQueryContext.class);

    private static final long MILLIS = 1_000_000;

    @Before
    public void setup() throws OperationFailedException {
        when(scopeExpander.expandScope(eq(scope), any())).thenReturn(expandedScope);
        when(contextFactory.newContext(eq(scope), any(), any())).thenReturn(statsQueryContext);

        executor.addSubquery(statsSubQuery1);
        executor.addSubquery(statsSubQuery2);
    }

    @Test
    public void testGetStatsEmptyScope() throws OperationFailedException {
        StatPeriodApiInputDTO period = new StatPeriodApiInputDTO();

        when(expandedScope.getGlobalScope()).thenReturn(Optional.empty());
        when(expandedScope.getEntities()).thenReturn(Collections.emptySet());

        assertThat(executor.getAggregateStats(scope, period), is(Collections.emptyList()));
    }

    @Test
    public void testGetStatsRequestAll() throws OperationFailedException {
        StatPeriodApiInputDTO period = new StatPeriodApiInputDTO();

        when(expandedScope.getGlobalScope()).thenReturn(Optional.empty());
        when(expandedScope.getEntities()).thenReturn(Collections.singleton(1L));

        // One of the queries is applicable.
        when(statsSubQuery1.applicableInContext(statsQueryContext)).thenReturn(true);
        when(statsSubQuery2.applicableInContext(statsQueryContext)).thenReturn(false);

        final StatApiDTO stat = stat("foo");
        StatSnapshotApiDTO snapshot = snapshotWithStats(MILLIS, stat);
        when(statsSubQuery1.getAggregateStats(any(), any()))
            .thenReturn(Collections.singletonList(snapshot));

        // ACT
        List<StatSnapshotApiDTO> results = executor.getAggregateStats(scope, period);

        // ASSERT
        verify(contextFactory).newContext(scope, expandedScope, period);

        assertThat(results.size(), is(1));

        final StatSnapshotApiDTO resultSnapshot = results.get(0);
        assertThat(resultSnapshot.getDate(), is(DateTimeUtil.toString(MILLIS)));
        assertThat(resultSnapshot.getStatistics(), containsInAnyOrder(stat));
    }

    @Test
    public void testGetStatsRequestMergeQueryStats() throws OperationFailedException {
        StatPeriodApiInputDTO period = new StatPeriodApiInputDTO();

        when(expandedScope.getGlobalScope()).thenReturn(Optional.empty());
        when(expandedScope.getEntities()).thenReturn(Collections.singleton(1L));

        // Both queries applicable
        when(statsSubQuery1.applicableInContext(statsQueryContext)).thenReturn(true);
        when(statsSubQuery2.applicableInContext(statsQueryContext)).thenReturn(true);


        final StatApiDTO stat1 = stat("foo");
        final StatApiDTO stat2 = stat("bar");
        StatSnapshotApiDTO snapshot1 = snapshotWithStats(MILLIS, stat1);
        StatSnapshotApiDTO snapshot2 = snapshotWithStats(MILLIS, stat2);
        when(statsSubQuery1.getAggregateStats(any(), any()))
            .thenReturn(Collections.singletonList(snapshot1));
        when(statsSubQuery2.getAggregateStats(any(), any()))
            .thenReturn(Collections.singletonList(snapshot2));

        // ACT
        List<StatSnapshotApiDTO> stats = executor.getAggregateStats(scope, period);

        // ASSERT
        verify(contextFactory).newContext(scope, expandedScope, period);

        assertThat(stats.size(), is(1));

        final StatSnapshotApiDTO snapshotApiDTO = stats.get(0);
        assertThat(snapshotApiDTO.getDate(), is(DateTimeUtil.toString(MILLIS)));
        assertThat(snapshotApiDTO.getStatistics(), containsInAnyOrder(stat1, stat2));
    }

    @Test
    public void testGetStatsRunSubqueries() throws OperationFailedException {
        final StatApiInputDTO fooInput = statInput("foo");
        final StatApiInputDTO barInput = statInput("bar");
        StatPeriodApiInputDTO period = new StatPeriodApiInputDTO();
        period.setStatistics(Lists.newArrayList(fooInput, barInput));
        when(statsQueryContext.getRequestedStats()).thenReturn(new HashSet<>(period.getStatistics()));

        when(expandedScope.getGlobalScope()).thenReturn(Optional.empty());
        when(expandedScope.getEntities()).thenReturn(Collections.singleton(1L));

        // Both queries applicable
        when(statsSubQuery1.applicableInContext(statsQueryContext)).thenReturn(true);
        when(statsSubQuery2.applicableInContext(statsQueryContext)).thenReturn(true);

        // One query handles foo, one query handles bar.
        when(statsSubQuery1.getHandledStats(statsQueryContext))
            .thenReturn(SubQuerySupportedStats.some(Collections.singleton(fooInput)));
        when(statsSubQuery2.getHandledStats(statsQueryContext))
            .thenReturn(SubQuerySupportedStats.some(Collections.singleton(barInput)));


        final StatApiDTO stat1 = stat("foo");
        final StatApiDTO stat2 = stat("bar");
        StatSnapshotApiDTO snapshot1 = snapshotWithStats(MILLIS, stat1);
        StatSnapshotApiDTO snapshot2 = snapshotWithStats(MILLIS, stat2);
        when(statsSubQuery1.getAggregateStats(any(), any()))
            .thenReturn(Collections.singletonList(snapshot1));
        when(statsSubQuery2.getAggregateStats(any(), any()))
            .thenReturn(Collections.singletonList(snapshot2));

        // ACT
        List<StatSnapshotApiDTO> stats = executor.getAggregateStats(scope, period);

        // ASSERT
        verify(contextFactory).newContext(scope, expandedScope, period);
        verify(statsSubQuery1).getAggregateStats(Collections.singleton(fooInput), statsQueryContext);
        verify(statsSubQuery2).getAggregateStats(Collections.singleton(barInput), statsQueryContext);

        assertThat(stats.size(), is(1));

        final StatSnapshotApiDTO snapshotApiDTO = stats.get(0);
        assertThat(snapshotApiDTO.getDate(), is(DateTimeUtil.toString(MILLIS)));
        assertThat(snapshotApiDTO.getStatistics(), containsInAnyOrder(stat1, stat2));
    }

    @Test
    public void testGetStatsSubqueryForLeftoverStats() throws OperationFailedException {
        final StatApiInputDTO fooInput = statInput("foo");
        final StatApiInputDTO barInput = statInput("bar");
        StatPeriodApiInputDTO period = new StatPeriodApiInputDTO();
        period.setStatistics(Lists.newArrayList(fooInput, barInput));
        when(statsQueryContext.getRequestedStats()).thenReturn(new HashSet<>(period.getStatistics()));

        when(expandedScope.getGlobalScope()).thenReturn(Optional.empty());
        when(expandedScope.getEntities()).thenReturn(Collections.singleton(1L));

        // Both queries applicable
        when(statsSubQuery1.applicableInContext(statsQueryContext)).thenReturn(true);
        when(statsSubQuery2.applicableInContext(statsQueryContext)).thenReturn(true);

        // One query handles foo, one query handles everything not explicitly handled by another.
        when(statsSubQuery1.getHandledStats(statsQueryContext))
            .thenReturn(SubQuerySupportedStats.some(Collections.singleton(fooInput)));
        when(statsSubQuery2.getHandledStats(statsQueryContext))
            .thenReturn(SubQuerySupportedStats.leftovers());

        final StatApiDTO stat1 = stat("foo");
        final StatApiDTO stat2 = stat("bar");
        StatSnapshotApiDTO snapshot1 = snapshotWithStats(MILLIS, stat1);
        StatSnapshotApiDTO snapshot2 = snapshotWithStats(MILLIS, stat2);
        when(statsSubQuery1.getAggregateStats(any(), any()))
            .thenReturn(Collections.singletonList(snapshot1));
        when(statsSubQuery2.getAggregateStats(any(), any()))
            .thenReturn(Collections.singletonList(snapshot2));

        // ACT
        List<StatSnapshotApiDTO> stats = executor.getAggregateStats(scope, period);

        // ASSERT
        verify(contextFactory).newContext(scope, expandedScope, period);
        verify(statsSubQuery1).getAggregateStats(Collections.singleton(fooInput), statsQueryContext);
        verify(statsSubQuery2).getAggregateStats(Collections.singleton(barInput), statsQueryContext);

        assertThat(stats.size(), is(1));

        final StatSnapshotApiDTO snapshotApiDTO = stats.get(0);
        assertThat(snapshotApiDTO.getDate(), is(DateTimeUtil.toString(MILLIS)));
        assertThat(snapshotApiDTO.getStatistics(), containsInAnyOrder(stat1, stat2));
    }

    @Test
    public void testGetStatsRequestSameStatNameDifferentRelatedEntity() throws OperationFailedException {
        StatPeriodApiInputDTO period = new StatPeriodApiInputDTO();

        when(expandedScope.getGlobalScope()).thenReturn(Optional.empty());
        when(expandedScope.getEntities()).thenReturn(Collections.singleton(1L));

        // Both queries applicable
        when(statsSubQuery1.applicableInContext(statsQueryContext)).thenReturn(true);
        when(statsSubQuery2.applicableInContext(statsQueryContext)).thenReturn(true);

        final StatApiDTO stat1 = stat("foo", "11");
        final StatApiDTO stat2 = stat("foo", "12");
        StatSnapshotApiDTO snapshot1 = snapshotWithStats(MILLIS, stat1);
        StatSnapshotApiDTO snapshot2 = snapshotWithStats(MILLIS, stat2);
        when(statsSubQuery1.getAggregateStats(any(), any()))
            .thenReturn(Collections.singletonList(snapshot1));
        when(statsSubQuery2.getAggregateStats(any(), any()))
            .thenReturn(Collections.singletonList(snapshot2));

        // ACT
        List<StatSnapshotApiDTO> stats = executor.getAggregateStats(scope, period);
        assertThat(stats.size(), is(1));

        final StatSnapshotApiDTO snapshotApiDTO = stats.get(0);
        // verify that there are 2 stats with same name, but different relatedEntity
        assertThat(snapshotApiDTO.getStatistics().size(), is(2));
        assertThat(snapshotApiDTO.getStatistics(), containsInAnyOrder(stat1, stat2));
    }

    @Test
    public void testGetStatsRequestSameStatNameDifferentKey() throws OperationFailedException {
        StatPeriodApiInputDTO period = new StatPeriodApiInputDTO();

        when(expandedScope.getGlobalScope()).thenReturn(Optional.empty());
        when(expandedScope.getEntities()).thenReturn(Collections.singleton(1L));

        // Both queries applicable
        when(statsSubQuery1.applicableInContext(statsQueryContext)).thenReturn(true);
        when(statsSubQuery2.applicableInContext(statsQueryContext)).thenReturn(true);

        final StatApiDTO stat1 = statWithKey("foo", "key1");
        final StatApiDTO stat2 = statWithKey("foo", "key2");
        StatSnapshotApiDTO snapshot1 = snapshotWithStats(MILLIS, stat1);
        StatSnapshotApiDTO snapshot2 = snapshotWithStats(MILLIS, stat2);
        when(statsSubQuery1.getAggregateStats(any(), any()))
            .thenReturn(Collections.singletonList(snapshot1));
        when(statsSubQuery2.getAggregateStats(any(), any()))
            .thenReturn(Collections.singletonList(snapshot2));

        // ACT
        List<StatSnapshotApiDTO> stats = executor.getAggregateStats(scope, period);
        assertThat(stats.size(), is(1));

        final StatSnapshotApiDTO snapshotApiDTO = stats.get(0);
        // verify that there are 2 stats with same name, but different key
        assertThat(snapshotApiDTO.getStatistics().size(), is(2));
        assertThat(snapshotApiDTO.getStatistics(), containsInAnyOrder(stat1, stat2));
    }

    @Test
    public void testCoolingPowerStatsRequestAll() throws OperationFailedException {
        StatPeriodApiInputDTO period = new StatPeriodApiInputDTO();

        when(expandedScope.getGlobalScope()).thenReturn(Optional.empty());
        when(expandedScope.getEntities()).thenReturn(Collections.singleton(1L));

        // One of the queries is applicable.
        when(statsSubQuery1.applicableInContext(statsQueryContext)).thenReturn(true);
        when(statsSubQuery2.applicableInContext(statsQueryContext)).thenReturn(true);

        final StatApiDTO stat1 = stat("Cooling");
        final StatApiDTO stat2 = stat("foo");
        StatSnapshotApiDTO snapshot1 = snapshotWithStats(MILLIS, stat1);
        StatSnapshotApiDTO snapshot2 = snapshotWithStats(MILLIS, stat2);
        when(statsSubQuery1.getAggregateStats(any(), any()))
            .thenReturn(Collections.singletonList(snapshot1));
        when(statsSubQuery2.getAggregateStats(any(), any()))
            .thenReturn(Collections.singletonList(snapshot2));

        // Create a list of targets.
        List<ThinTargetInfo> thinTargetInfos = Lists.newArrayList(
            ImmutableThinTargetInfo.builder().oid(1L).displayName("target1").isHidden(false).probeInfo(
                ImmutableThinProbeInfo.builder().oid(3L).type("probe1").category("hypervisor").build()).build(),
            ImmutableThinTargetInfo.builder().oid(2L).displayName("target2").isHidden(false).probeInfo(
                ImmutableThinProbeInfo.builder().oid(4L).type("probe2").category("fabric").build()).build());
        when(statsQueryContext.getTargets()).thenReturn(thinTargetInfos);

        List<StatSnapshotApiDTO> stats;
        StatSnapshotApiDTO snapshotApiDTO;

        // If not all entities were discovered by fabric, then don't show cooling and power.
        when(scope.getDiscoveringTargetIds()).thenReturn(Sets.newHashSet(1L, 2L));
        stats = executor.getAggregateStats(scope, period);
        snapshotApiDTO = stats.get(0);
        assertTrue(snapshotApiDTO.getStatistics().stream()
            .noneMatch(stat -> "Cooling".equalsIgnoreCase(stat.getName())));

        // If not all entities were discovered by fabric, then don't show cooling and power.
        when(scope.getDiscoveringTargetIds()).thenReturn(Sets.newHashSet(1L));
        stats = executor.getAggregateStats(scope, period);
        snapshotApiDTO = stats.get(0);
        assertTrue(snapshotApiDTO.getStatistics().stream()
            .noneMatch(stat -> "Cooling".equalsIgnoreCase(stat.getName())));

        // If all entities were discovered by fabric, then show cooling and power.
        when(scope.getDiscoveringTargetIds()).thenReturn(Sets.newHashSet(2L));
        stats = executor.getAggregateStats(scope, period);
        snapshotApiDTO = stats.get(0);
        assertTrue(snapshotApiDTO.getStatistics().stream()
            .anyMatch(stat -> "Cooling".equalsIgnoreCase(stat.getName())));
    }

}