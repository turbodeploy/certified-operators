package com.vmturbo.api.component.external.api.util.stats;

import static com.vmturbo.api.component.external.api.util.stats.StatsTestUtil.stat;
import static com.vmturbo.api.component.external.api.util.stats.StatsTestUtil.statInput;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

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

        when(expandedScope.isAll()).thenReturn(false);
        when(expandedScope.getEntities()).thenReturn(Collections.emptySet());

        assertThat(executor.getAggregateStats(scope, period), is(Collections.emptyList()));
    }

    @Test
    public void testGetStatsRequestAll() throws OperationFailedException {
        StatPeriodApiInputDTO period = new StatPeriodApiInputDTO();

        when(expandedScope.isAll()).thenReturn(false);
        when(expandedScope.getEntities()).thenReturn(Collections.singleton(1L));

        // One of the queries is applicable.
        when(statsSubQuery1.applicableInContext(statsQueryContext)).thenReturn(true);
        when(statsSubQuery2.applicableInContext(statsQueryContext)).thenReturn(false);


        final StatApiDTO stat = stat("foo");
        when(statsSubQuery1.getAggregateStats(any(), any()))
            .thenReturn(ImmutableMap.of(MILLIS, Collections.singletonList(stat)));

        // ACT
        List<StatSnapshotApiDTO> stats = executor.getAggregateStats(scope, period);

        // ASSERT
        verify(contextFactory).newContext(scope, expandedScope, period);

        assertThat(stats.size(), is(1));

        final StatSnapshotApiDTO snapshotApiDTO = stats.get(0);
        assertThat(snapshotApiDTO.getDate(), is(DateTimeUtil.toString(MILLIS)));
        assertThat(snapshotApiDTO.getStatistics(), containsInAnyOrder(stat));
    }

    @Test
    public void testGetStatsRequestMergeQueryStats() throws OperationFailedException {
        StatPeriodApiInputDTO period = new StatPeriodApiInputDTO();

        when(expandedScope.isAll()).thenReturn(false);
        when(expandedScope.getEntities()).thenReturn(Collections.singleton(1L));

        // Both queries applicable
        when(statsSubQuery1.applicableInContext(statsQueryContext)).thenReturn(true);
        when(statsSubQuery2.applicableInContext(statsQueryContext)).thenReturn(true);


        final StatApiDTO stat1 = stat("foo");
        final StatApiDTO stat2 = stat("bar");
        when(statsSubQuery1.getAggregateStats(any(), any()))
            .thenReturn(ImmutableMap.of(MILLIS, Collections.singletonList(stat1)));
        when(statsSubQuery2.getAggregateStats(any(), any()))
            .thenReturn(ImmutableMap.of(MILLIS, Collections.singletonList(stat2)));

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

        when(expandedScope.isAll()).thenReturn(false);
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
        when(statsSubQuery1.getAggregateStats(any(), any()))
            .thenReturn(ImmutableMap.of(MILLIS, Collections.singletonList(stat1)));
        when(statsSubQuery2.getAggregateStats(any(), any()))
            .thenReturn(ImmutableMap.of(MILLIS, Collections.singletonList(stat2)));

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

        when(expandedScope.isAll()).thenReturn(false);
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
        when(statsSubQuery1.getAggregateStats(any(), any()))
            .thenReturn(ImmutableMap.of(MILLIS, Collections.singletonList(stat1)));
        when(statsSubQuery2.getAggregateStats(any(), any()))
            .thenReturn(ImmutableMap.of(MILLIS, Collections.singletonList(stat2)));

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
}