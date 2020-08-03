package com.vmturbo.api.component.external.api.util.stats.query.impl;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.api.component.ApiTestUtils;
import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.communication.RepositoryApi.MultiEntityRequest;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.component.external.api.util.stats.ImmutableTimeWindow;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryContextFactory.StatsQueryContext;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryContextFactory.StatsQueryContext.TimeWindow;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryScopeExpander.StatsQueryScope;
import com.vmturbo.api.component.external.api.util.stats.StatsTestUtil;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.platform.common.dto.CommonDTOREST.EntityDTO.EntityType;

public class ScopedUserCountStatsSubQueryTest {

    private RepositoryApi repositoryApi = mock(RepositoryApi.class);

    private static final Duration LIVE_STATS_WINDOW = Duration.ofSeconds(1);

    private UserSessionContext userSessionContext = mock(UserSessionContext.class);

    private ScopedUserCountStatsSubQuery query;

    @Before
    public void setup() {
        query = new ScopedUserCountStatsSubQuery(LIVE_STATS_WINDOW, userSessionContext,
            repositoryApi);
    }

    @Test
    public void testNotApplicableToPlan() {
        final ApiId scope = mock(ApiId.class);
        when(scope.isPlan()).thenReturn(true);
        final StatsQueryContext context = mock(StatsQueryContext.class);
        when(context.getInputScope()).thenReturn(scope);
        when(userSessionContext.isUserScoped()).thenReturn(true);

        assertThat(query.applicableInContext(context), is(false));
    }

    @Test
    public void testNotApplicableWhenUserIsNotScoped() {
        final ApiId scope = mock(ApiId.class);
        final StatsQueryContext context = mock(StatsQueryContext.class);
        when(scope.isPlan()).thenReturn(false);

        when(userSessionContext.isUserScoped()).thenReturn(false);
        when(context.getInputScope()).thenReturn(scope);

        assertThat(query.applicableInContext(context), is(false));
    }

    @Test
    public void testGetAggregateStats() throws OperationFailedException {
        final TimeWindow timeWindow = ImmutableTimeWindow.builder()
            .startTime(1_000)
            .endTime(3_000)
            .build();
        final StatsQueryContext context = mock(StatsQueryContext.class);
        final HashSet<Long> entities = new HashSet<>(Arrays.asList(1L));
        long millis=System.currentTimeMillis();
        when(context.getTimeWindow()).thenReturn(Optional.of(timeWindow));
        when(userSessionContext.isUserScoped()).thenReturn(true);
        final StatsQueryScope queryScope = mock(StatsQueryScope.class);
        when(queryScope.getGlobalScope()).thenReturn(Optional.empty());
        when(queryScope.getExpandedOids()).thenReturn(entities);
        when(context.getQueryScope()).thenReturn(queryScope);
        when(context.getSessionContext()).thenReturn(userSessionContext);
        when(context.requestProjected()).thenReturn(true);
        when(context.getCurTime()).thenReturn(millis);

        final StatSnapshotApiDTO mappedSnapshot = new StatSnapshotApiDTO();
        final StatApiDTO mappedStat = StatsTestUtil.stat(StringConstants.NUM_VMS);
        mappedSnapshot.setStatistics(Collections.singletonList(mappedStat));
        MultiEntityRequest request =
            ApiTestUtils.mockMultiMinEntityReq(Arrays.asList(MinimalEntity.newBuilder().setOid(1L).setEntityType(EntityType.VIRTUAL_MACHINE.getValue()).build()));
        when(repositoryApi.entitiesRequest(Collections.singleton(1L))).thenReturn(request);
        when(request.restrictTypes(anyList())).thenReturn(request);

        final List<StatSnapshotApiDTO> results = query.getAggregateStats(
            Collections.singleton(StatsTestUtil.statInput(StringConstants.NUM_VMS)), context);

        // ASSERT
        assertEquals(2, results.size());
        assertThat(results.stream()
                .map(StatSnapshotApiDTO::getDate)
                .map(DateTimeUtil::parseTime)
                .collect(Collectors.toList()),
            containsInAnyOrder(millis, timeWindow.endTime()));

        final StatSnapshotApiDTO endSnapshot = results.stream()
            .filter(snapshot -> timeWindow.endTime() == DateTimeUtil.parseTime(snapshot.getDate()))
            .findFirst()
            .get();
        assertThat(endSnapshot.getStatistics().get(0).getValue(), is(1F));
    }
}
