package com.vmturbo.api.component.external.api.util.stats;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;

import com.google.common.collect.Sets;

import org.junit.Test;

import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.CachedPlanInfo;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryContextFactory.StatsQueryContext;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryScopeExpander.StatsQueryScope;
import com.vmturbo.api.dto.statistic.StatApiInputDTO;
import com.vmturbo.api.dto.statistic.StatPeriodApiInputDTO;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance.PlanStatus;
import com.vmturbo.components.api.test.MutableFixedClock;
import com.vmturbo.topology.processor.api.util.ThinTargetCache;
import com.vmturbo.topology.processor.api.util.ThinTargetCache.ThinTargetInfo;

public class StatsQueryContextFactoryTest {

    private final Duration LIVE_STATS_WINDOW = Duration.ofSeconds(2);

    private final UserSessionContext userSessionContext = mock(UserSessionContext.class);

    private final Clock clock = new MutableFixedClock(1_000_000);

    private final ThinTargetCache targetCache = mock(ThinTargetCache.class);

    private StatsQueryContextFactory factory =
        new StatsQueryContextFactory(LIVE_STATS_WINDOW, userSessionContext, clock, targetCache);

    private final StatsQueryScope expandedScope = mock(StatsQueryScope.class);

    @Test
    public void testNewContext() {
        final ThinTargetInfo targetInfo = mock(ThinTargetInfo.class);
        when(targetCache.getAllTargets())
            .thenReturn(Collections.singletonList(targetInfo));

        ApiId scope = mock(ApiId.class);
        when(expandedScope.getExpandedOids()).thenReturn(Collections.singleton(1L));
        StatPeriodApiInputDTO inputDTO = new StatPeriodApiInputDTO();
        inputDTO.setStartDate(DateTimeUtil.toString(clock.millis() - 5000));
        inputDTO.setEndDate(DateTimeUtil.toString(clock.millis() + 5000));

        final List<StatApiInputDTO> statApiDTOList = Collections.singletonList(new StatApiInputDTO());
        inputDTO.setStatistics(statApiDTOList);

        final StatsQueryContext context = factory.newContext(scope, expandedScope, inputDTO);

        assertThat(context.getInputScope(), is(scope));
        assertThat(context.getQueryScope().getExpandedOids(), is(Collections.singleton(1L)));
        assertThat(context.getTimeWindow(), is(Optional.of(ImmutableTimeWindow.builder()
            .startTime(clock.millis() - 5000)
            .endTime(clock.millis() + 5000)
            .includeHistorical(true)
            .includeCurrent(true)
            .includeProjected(true)
            .build())));
        assertThat(context.includeCurrent(), is(true));
        assertThat(context.getCurTime(), is(clock.millis()));
        assertThat(context.getTargets(), is(Collections.singletonList(targetInfo)));
        assertThat(context.getRequestedStats(), is(new HashSet<>(statApiDTOList)));
        assertThat(context.getSessionContext(), is(userSessionContext));
    }

    @Test
    public void testNewContextNoTime() {
        when(targetCache.getAllTargets())
            .thenReturn(Collections.emptyList());
        StatPeriodApiInputDTO inputDTO = new StatPeriodApiInputDTO();

        final StatsQueryContext context = factory.newContext(mock(ApiId.class), expandedScope, inputDTO);

        assertThat(context.getTimeWindow(), is(Optional.empty()));
        assertThat(context.includeCurrent(), is(true));
        assertThat(context.requestProjected(), is(false));
    }

    @Test
    public void testNewContextNoIncludeCurrent() {
        when(targetCache.getAllTargets())
            .thenReturn(Collections.emptyList());
        StatPeriodApiInputDTO inputDTO = new StatPeriodApiInputDTO();
        inputDTO.setStartDate(DateTimeUtil.toString(clock.millis() - 10000));
        inputDTO.setEndDate(DateTimeUtil.toString(clock.millis() - 5000));

        final StatsQueryContext context = factory.newContext(mock(ApiId.class), expandedScope, inputDTO);

        assertThat(context.getTimeWindow(), is(Optional.of(ImmutableTimeWindow.builder()
            .startTime(clock.millis() - 10000)
            .endTime(clock.millis() - 5000)
            .includeHistorical(true)
            .includeCurrent(false)
            .includeProjected(false)
            .build())));
        assertThat(context.includeCurrent(), is(false));
        assertThat(context.requestProjected(), is(false));
    }

    @Test
    public void testNewContextLatestRetrievalWindowStart() {
        when(targetCache.getAllTargets())
            .thenReturn(Collections.emptyList());
        StatPeriodApiInputDTO inputDTO = new StatPeriodApiInputDTO();
        final long startTime = clock.millis() - LIVE_STATS_WINDOW.toMillis();
        final long endTime = clock.millis() + 5000;
        inputDTO.setStartDate(DateTimeUtil.toString(startTime));
        inputDTO.setEndDate(DateTimeUtil.toString(endTime));

        final List<StatApiInputDTO> statApiDTOList = new ArrayList<>();

        inputDTO.setStatistics(statApiDTOList);

        final StatsQueryContext context = factory.newContext(mock(ApiId.class), expandedScope, inputDTO);

        assertThat(context.getTimeWindow(), is(Optional.of(ImmutableTimeWindow.builder()
                .startTime(startTime)
                .endTime(endTime)
                .includeHistorical(false)
                .includeCurrent(true)
                .includeProjected(true)
                .build())));
        assertThat(context.includeCurrent(), is(true));
        assertThat(context.requestProjected(), is(true));
    }

    @Test
    public void testContextFindStats() {
        when(targetCache.getAllTargets())
            .thenReturn(Collections.emptyList());
        final StatPeriodApiInputDTO inputDTO = new StatPeriodApiInputDTO();
        final StatApiInputDTO fooStat = new StatApiInputDTO();
        fooStat.setName("foo");
        final StatApiInputDTO barStat = new StatApiInputDTO();
        barStat.setName("bar");
        inputDTO.setStatistics(Arrays.asList(fooStat, barStat));

        final StatsQueryContext context = factory.newContext(mock(ApiId.class), expandedScope, inputDTO);

        assertThat(context.findStats(Collections.singleton("foo")), containsInAnyOrder(fooStat));
        assertThat(context.findStats(Collections.singleton("bar")), containsInAnyOrder(barStat));
        assertThat(context.findStats(Sets.newHashSet("foo", "bar")), containsInAnyOrder(fooStat, barStat));
        assertThat(context.findStats(Collections.emptySet()), is(Collections.emptySet()));
    }

    @Test
    public void testContextNewPeriodInputDto() {
        final ThinTargetInfo targetInfo = mock(ThinTargetInfo.class);
        when(targetCache.getAllTargets())
            .thenReturn(Collections.singletonList(targetInfo));

        StatPeriodApiInputDTO inputDTO = new StatPeriodApiInputDTO();
        final String startTime = DateTimeUtil.toString(clock.millis() - 5000);
        final String endTime = DateTimeUtil.toString(clock.millis() + 5000);
        inputDTO.setStartDate(startTime);
        inputDTO.setEndDate(endTime);

        final StatsQueryContext context = factory.newContext(mock(ApiId.class), expandedScope, inputDTO);

        final StatApiInputDTO fooStat = new StatApiInputDTO();
        fooStat.setName("foo");

        final StatPeriodApiInputDTO input = context.newPeriodInputDto(Collections.singleton(fooStat));
        assertThat(input.getStartDate(), is(startTime));
        assertThat(input.getEndDate(), is(endTime));
        assertThat(input.getStatistics(), containsInAnyOrder(fooStat));
    }

    @Test
    public void testContextGetPlanInstance() {
        when(targetCache.getAllTargets())
            .thenReturn(Collections.emptyList());
        final StatPeriodApiInputDTO inputDTO = new StatPeriodApiInputDTO();

        PlanInstance planInstance = PlanInstance.newBuilder()
            .setPlanId(1)
            .setStatus(PlanStatus.READY)
            .build();

        final ApiId apiId = mock(ApiId.class);
        final CachedPlanInfo planInfo = mock(CachedPlanInfo.class);
        when(planInfo.getPlanInstance()).thenReturn(planInstance);
        when(apiId.getCachedPlanInfo()).thenReturn(Optional.of(planInfo));

        final StatsQueryContext context = factory.newContext(apiId, expandedScope, inputDTO);

        assertThat(context.getPlanInstance(), is(Optional.of(planInstance)));
        verify(apiId, times(1)).getCachedPlanInfo();
        verify(planInfo, times(1)).getPlanInstance();

        // Test caching
        assertThat(context.getPlanInstance(), is(Optional.of(planInstance)));
        verify(apiId, times(1)).getCachedPlanInfo();
        verify(planInfo, times(1)).getPlanInstance();
    }

    @Test
    public void testContextIsGlobalScopeRealtime() {
        when(targetCache.getAllTargets())
            .thenReturn(Collections.emptyList());
        final StatPeriodApiInputDTO inputDTO = new StatPeriodApiInputDTO();

        final ApiId apiId = mock(ApiId.class);
        when(apiId.isRealtimeMarket()).thenReturn(true);


        final StatsQueryContext context = factory.newContext(apiId, expandedScope, inputDTO);

        assertThat(context.isGlobalScope(), is(true));
    }

    @Test
    public void testContextIsGlobalScopeTmpGroup() {
        when(targetCache.getAllTargets())
            .thenReturn(Collections.emptyList());
        final StatPeriodApiInputDTO inputDTO = new StatPeriodApiInputDTO();

        final ApiId apiId = mock(ApiId.class);
        when(apiId.isRealtimeMarket()).thenReturn(false);
        when(apiId.isGlobalTempGroup()).thenReturn(true);


        final StatsQueryContext context = factory.newContext(apiId, expandedScope, inputDTO);

        assertThat(context.isGlobalScope(), is(true));
    }

}