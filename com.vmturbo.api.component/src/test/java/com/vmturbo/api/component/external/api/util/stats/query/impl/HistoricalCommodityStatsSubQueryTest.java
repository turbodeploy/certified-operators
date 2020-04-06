package com.vmturbo.api.component.external.api.util.stats.query.impl;


import static com.vmturbo.api.component.external.api.util.stats.query.impl.HistoricalCommodityStatsSubQuery.storageCommodities;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.google.common.collect.Sets;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.communication.RepositoryApi.SearchRequest;
import com.vmturbo.api.component.external.api.mapper.StatsMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.component.external.api.util.StatsUtils;
import com.vmturbo.api.component.external.api.util.stats.ImmutableGlobalScope;
import com.vmturbo.api.component.external.api.util.stats.ImmutableTimeWindow;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryContextFactory.StatsQueryContext;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryScopeExpander.GlobalScope;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryScopeExpander.StatsQueryScope;
import com.vmturbo.api.component.external.api.util.stats.StatsTestUtil;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.statistic.StatApiInputDTO;
import com.vmturbo.api.dto.statistic.StatPeriodApiInputDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.enums.Epoch;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.stats.Stats.GetAveragedEntityStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats.GlobalFilter;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc;
import com.vmturbo.common.protobuf.stats.StatsMoles.StatsHistoryServiceMole;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

public class HistoricalCommodityStatsSubQueryTest {
    private static final long MILLIS = 1_000_000;
    private static final StatsFilter FILTER = StatsFilter.newBuilder()
        // For uniqueness/equality comparison
        .setStartDate(1L)
        .build();

    private static final StatSnapshot HISTORY_STAT_SNAPSHOT = StatSnapshot.newBuilder()
        // For uniqueness/equality comparison.
        .setSnapshotDate(MILLIS)
        .build();

    private static final StatSnapshotApiDTO MAPPED_STAT_SNAPSHOT;

    static {
        MAPPED_STAT_SNAPSHOT = new StatSnapshotApiDTO();
        MAPPED_STAT_SNAPSHOT.setDate(DateTimeUtil.toString(MILLIS));
        MAPPED_STAT_SNAPSHOT.setEpoch(Epoch.HISTORICAL);
        MAPPED_STAT_SNAPSHOT.setStatistics(Collections.singletonList(StatsTestUtil.stat("foo")));
    }

    private static final Set<StatApiInputDTO> REQ_STATS =
        Collections.singleton(StatsTestUtil.statInput("foo"));

    private final StatsMapper statsMapper = mock(StatsMapper.class);

    private final StatsHistoryServiceMole backend = spy(StatsHistoryServiceMole.class);

    @Captor
    public ArgumentCaptor<GetAveragedEntityStatsRequest> reqCaptor;

    @Rule
    public GrpcTestServer grpcTestServer = GrpcTestServer.newServer(backend);

    private HistoricalCommodityStatsSubQuery query;

    private UserSessionContext userSessionContext = mock(UserSessionContext.class);

    private final ApiId vmGroupScope = mock(ApiId.class);

    private final StatsQueryContext context = mock(StatsQueryContext.class);

    @Mock
    private RepositoryApi repositoryApi;

    private static final StatPeriodApiInputDTO NEW_PERIOD_INPUT_DTO = new StatPeriodApiInputDTO();

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        query = new HistoricalCommodityStatsSubQuery(statsMapper,
            StatsHistoryServiceGrpc.newBlockingStub(grpcTestServer.getChannel()),
            userSessionContext,
            repositoryApi);

        when(context.getInputScope()).thenReturn(vmGroupScope);

        when(statsMapper.newPeriodStatsFilter(any())).thenReturn(FILTER);

        when(statsMapper.toStatSnapshotApiDTO(HISTORY_STAT_SNAPSHOT)).thenReturn(MAPPED_STAT_SNAPSHOT);

        doReturn(Collections.singletonList(HISTORY_STAT_SNAPSHOT))
            .when(backend).getAveragedEntityStats(any());

        when(context.newPeriodInputDto(any())).thenReturn(NEW_PERIOD_INPUT_DTO);

        when(vmGroupScope.getScopeTypes()).thenReturn(Optional.of(Collections.singleton(ApiEntityType.VIRTUAL_MACHINE)));
    }

    @Test
    public void testNotApplicableInPlan() {
        final ApiId scope = mock(ApiId.class);
        when(scope.isPlan()).thenReturn(true);

        final StatsQueryContext context = mock(StatsQueryContext.class);
        when(context.getInputScope()).thenReturn(scope);

        assertThat(query.applicableInContext(context), is(false));
    }

    @Test
    public void testApplicableInNotPlan() {
        final ApiId scope = mock(ApiId.class);
        when(scope.isPlan()).thenReturn(false);

        final StatsQueryContext context = mock(StatsQueryContext.class);
        when(context.getInputScope()).thenReturn(scope);

        assertThat(query.applicableInContext(context), is(true));
    }

    @Test
    public void testStatsRequest() throws OperationFailedException {
        // Non-scoped user.
        when(userSessionContext.isUserScoped()).thenReturn(false);

        // Not a global temp group.
        when(vmGroupScope.isGlobalTempGroup()).thenReturn(false);

        // Don't include current stats.
        when(context.includeCurrent()).thenReturn(false);

        // These entities in the scope.
        final StatsQueryScope queryScope = mock(StatsQueryScope.class);
        when(queryScope.getGlobalScope()).thenReturn(Optional.empty());
        when(queryScope.getExpandedOids()).thenReturn(Collections.singleton(1L));
        when(context.getQueryScope()).thenReturn(queryScope);

        // ACT
        final List<StatSnapshotApiDTO> results = query.getAggregateStats(REQ_STATS, context);

        verify(context).newPeriodInputDto(REQ_STATS);
        verify(statsMapper).newPeriodStatsFilter(NEW_PERIOD_INPUT_DTO);

        verify(backend).getAveragedEntityStats(reqCaptor.capture());
        final GetAveragedEntityStatsRequest req = reqCaptor.getValue();
        assertThat(req.getFilter(), is(FILTER));
        assertThat(req.getEntitiesList(), containsInAnyOrder(1L));
        assertThat(req.getGlobalFilter(), is(GlobalFilter.getDefaultInstance()));

        assertEquals(1, results.size());
        final StatSnapshotApiDTO resultSnapshot = results.get(0);
        assertEquals(MILLIS, DateTimeUtil.parseTime(resultSnapshot.getDate()).longValue());
        assertThat(resultSnapshot.getStatistics(), is(MAPPED_STAT_SNAPSHOT.getStatistics()));
        assertEquals(MAPPED_STAT_SNAPSHOT, resultSnapshot);
    }

    @Test
    public void testGlobalGroupStatsRequest() throws OperationFailedException {
        // Non-scoped user.
        when(userSessionContext.isUserScoped()).thenReturn(false);

        // A global temp group.
        when(vmGroupScope.isGlobalTempGroup()).thenReturn(true);

        // Don't include current stats.
        when(context.includeCurrent()).thenReturn(false);

        // These entities in the scope.
        final StatsQueryScope queryScope = mock(StatsQueryScope.class);
        when(queryScope.getGlobalScope()).thenReturn(Optional.of(ImmutableGlobalScope.builder()
            .addEntityTypes(ApiEntityType.VIRTUAL_MACHINE)
            .environmentType(EnvironmentType.CLOUD)
            .build()));
        when(queryScope.getExpandedOids()).thenReturn(Collections.emptySet());
        when(context.getQueryScope()).thenReturn(queryScope);

        // normalize vm to vm
        when(statsMapper.normalizeRelatedType(ApiEntityType.VIRTUAL_MACHINE.apiStr())).thenReturn(
            ApiEntityType.VIRTUAL_MACHINE.apiStr());

        // ACT
        final List<StatSnapshotApiDTO> results = query.getAggregateStats(REQ_STATS, context);

        verify(context).newPeriodInputDto(REQ_STATS);
        // We should pass the global type to the stats mapper.
        verify(statsMapper).newPeriodStatsFilter(NEW_PERIOD_INPUT_DTO);

        verify(backend).getAveragedEntityStats(reqCaptor.capture());
        final GetAveragedEntityStatsRequest req = reqCaptor.getValue();
        assertThat(req.getFilter(), is(FILTER));
        // No entities, because it's a global temp group.
        assertThat(req.getEntitiesList(), is(Collections.emptyList()));
        // The type of the scope group.
        assertThat(req.getGlobalFilter().getRelatedEntityTypeList(), containsInAnyOrder(ApiEntityType.VIRTUAL_MACHINE.apiStr()));
        assertThat(req.getGlobalFilter().getEnvironmentType(), is(EnvironmentType.CLOUD));

        assertEquals(1, results.size());
        final StatSnapshotApiDTO resultSnapshot = results.get(0);
        assertEquals(MILLIS, DateTimeUtil.parseTime(resultSnapshot.getDate()).longValue());
        assertThat(resultSnapshot.getStatistics(), is(MAPPED_STAT_SNAPSHOT.getStatistics()));
        assertEquals(MAPPED_STAT_SNAPSHOT, resultSnapshot);
    }

    @Test
    public void testScopedGlobalGroupStatsRequest() throws OperationFailedException {
        // Scoped user
        when(userSessionContext.isUserScoped()).thenReturn(true);

        // A global temp group (for the user scope).
        when(vmGroupScope.isGlobalTempGroup()).thenReturn(true);

        // Don't include current stats.
        when(context.includeCurrent()).thenReturn(false);

        // These entities in the scope.
        final StatsQueryScope queryScope = mock(StatsQueryScope.class);
        when(queryScope.getGlobalScope()).thenReturn(Optional.empty());
        when(queryScope.getExpandedOids()).thenReturn(Collections.singleton(1L));
        when(context.getQueryScope()).thenReturn(queryScope);

        // ACT
        final List<StatSnapshotApiDTO> results = query.getAggregateStats(REQ_STATS, context);

        verify(context).newPeriodInputDto(REQ_STATS);
        // Shouldn't treat it as a GLOBAL temp group.
        verify(statsMapper).newPeriodStatsFilter(NEW_PERIOD_INPUT_DTO);

        verify(backend).getAveragedEntityStats(reqCaptor.capture());
        final GetAveragedEntityStatsRequest req = reqCaptor.getValue();
        assertThat(req.getFilter(), is(FILTER));
        // Shouldn't treat it as a GLOBAL temp group.
        assertThat(req.getEntitiesList(), containsInAnyOrder(1L));
        assertThat(req.getGlobalFilter(), is(GlobalFilter.getDefaultInstance()));

        assertEquals(1, results.size());
        final StatSnapshotApiDTO resultSnapshot = results.get(0);
        assertEquals(MILLIS, DateTimeUtil.parseTime(resultSnapshot.getDate()).longValue());
        assertThat(resultSnapshot.getStatistics(), is(MAPPED_STAT_SNAPSHOT.getStatistics()));
        assertEquals(MAPPED_STAT_SNAPSHOT, resultSnapshot);
    }

    @Test
    public void testIncludeCurrentCopyLast() throws OperationFailedException {
        // Non-scoped user.
        when(userSessionContext.isUserScoped()).thenReturn(false);

        // Not a global temp group.
        when(vmGroupScope.isGlobalTempGroup()).thenReturn(false);

        final long startTime = MILLIS;
        final long currentTime = MILLIS * 2;

        // Include current stats.
        when(context.includeCurrent()).thenReturn(true);
        when(context.getCurTime()).thenReturn(currentTime);
        when(context.getTimeWindow()).thenReturn(Optional.of(ImmutableTimeWindow.builder()
            .startTime(startTime)
            .endTime(startTime + 1_000)
            .build()));

        // These entities in the scope.
        final StatsQueryScope queryScope = mock(StatsQueryScope.class);
        when(queryScope.getGlobalScope()).thenReturn(Optional.empty());
        when(queryScope.getExpandedOids()).thenReturn(Collections.singleton(1L));
        when(context.getQueryScope()).thenReturn(queryScope);

        // ACT
        final List<StatSnapshotApiDTO> results = query.getAggregateStats(REQ_STATS, context);

        verify(context).newPeriodInputDto(REQ_STATS);
        verify(statsMapper).newPeriodStatsFilter(NEW_PERIOD_INPUT_DTO);

        verify(backend).getAveragedEntityStats(reqCaptor.capture());
        final GetAveragedEntityStatsRequest req = reqCaptor.getValue();
        assertThat(req.getFilter(), is(FILTER));
        assertThat(req.getEntitiesList(), containsInAnyOrder(1L));
        assertThat(req.getGlobalFilter(), is(GlobalFilter.getDefaultInstance()));

        // The same stat snapshot will be included in the results twice--once at the requested time,
        // and once at the "current" time.
        assertEquals(2, results.size());
        final List<StatSnapshotApiDTO> resultsAtStartTime = results.stream()
            .filter(statSnapshotApiDTO -> startTime == DateTimeUtil.parseTime(statSnapshotApiDTO.getDate()))
            .collect(Collectors.toList());
        assertEquals(1, resultsAtStartTime.size());
        final StatSnapshotApiDTO startTimeSnapshot = resultsAtStartTime.get(0);
        assertEquals(MAPPED_STAT_SNAPSHOT.getStatistics(), startTimeSnapshot.getStatistics());
        assertEquals(Epoch.HISTORICAL, startTimeSnapshot.getEpoch());
        final List<StatSnapshotApiDTO> resultsAtCurrentTime = results.stream()
            .filter(statSnapshotApiDTO -> currentTime == DateTimeUtil.parseTime(statSnapshotApiDTO.getDate()))
            .collect(Collectors.toList());
        assertEquals(1, resultsAtCurrentTime.size());
        final StatSnapshotApiDTO currentTimeSnapshot = resultsAtCurrentTime.get(0);
        assertEquals(MAPPED_STAT_SNAPSHOT.getStatistics(), currentTimeSnapshot.getStatistics());
        assertEquals(Epoch.CURRENT, currentTimeSnapshot.getEpoch());
    }

    @Test
    public void testlGroupStatsRequestWithNoOid() throws OperationFailedException {
        // These entities in the scope.
        final StatsQueryScope queryScope = mock(StatsQueryScope.class);
        when(queryScope.getGlobalScope()).thenReturn(Optional.empty());
        //setting expandedOids to empty.
        when(queryScope.getExpandedOids()).thenReturn(Collections.emptySet());
        when(context.getQueryScope()).thenReturn(queryScope);

        // ACT
        final List<StatSnapshotApiDTO> results = query.getAggregateStats(REQ_STATS, context);

        verify(backend, never()).getAveragedEntityStats(any());
        assertThat(results.size(), is(0));
    }

    @Test
    public void testlGroupStatsRequestWithNoOidGlobal() throws OperationFailedException {
        // These entities in the scope.
        final StatsQueryScope queryScope = mock(StatsQueryScope.class);
        //Setting to global scope.
        GlobalScope globalScope =  ImmutableGlobalScope.builder().build();
        when(queryScope.getGlobalScope()).thenReturn(Optional.of(globalScope));
        //setting expandedOids to empty.
        when(queryScope.getExpandedOids()).thenReturn(Collections.emptySet());
        when(context.getQueryScope()).thenReturn(queryScope);

        // ACT
        final List<StatSnapshotApiDTO> results = query.getAggregateStats(REQ_STATS, context);

        verify(backend, atLeastOnce()).getAveragedEntityStats(any());
        assertThat(results.size(), greaterThan(0));
    }

    /**
     * Test updateStorageCommodityName() when VM has no VVs, scope is VM.
     */
    @Test
    public void testUpdateStorageCommodityNameWhenNoVVOid() {
        // Given
        final Long vmOid = 1L;
        final List<StatSnapshotApiDTO> statsList = createVMStatSnapshotApiDto();
        final Set<Long> scopedOid = Collections.singleton(vmOid);
        final Set<Long> commodityOids = Collections.emptySet();

        // When
        query.updateStorageCommodityName(statsList, scopedOid, commodityOids);

        assertNotNull(statsList);
        assertEquals(1, statsList.size());
        final StatSnapshotApiDTO statSnapshotApiDTO = statsList.get(0);
        assertNotNull(statSnapshotApiDTO.getStatistics());
        assertEquals(4, statSnapshotApiDTO.getStatistics().size());
    }

    /**
     * Test updateStorageCommodityName() when VM has two connected VVs, scope is VM.
     */
    @Test
    public void testUpdateStorageCommodityNameWhenVMHasTwoVV() {
        // Given
        final Long vmOid = 1L;
        final Long vvOid1 = 21L;
        final String vv1DisplayName = "VV-21";
        final Long vvOid2 = 22L;
        final String vv2DisplayName = "VV-22";
        final String stDispalyName = "GP2";
        final List<StatSnapshotApiDTO> statsList = createVMStatSnapshotApiDto(vvOid1, vvOid2, stDispalyName);
        final Set<Long> scopedOid = Collections.singleton(vmOid);
        final Set<Long> commodityOids = Sets.newHashSet(vvOid1, vvOid2);

        SearchRequest searchRequest = mock(SearchRequest.class);
        when(repositoryApi.newSearchRequest(any())).thenReturn(searchRequest);

        final List<TopologyEntityDTO> vvTopologyEntityDtos = Arrays.asList(
            TopologyEntityDTO.newBuilder()
                .setOid(vvOid1)
                .setDisplayName(vv1DisplayName)
                .setEntityType(EntityType.VIRTUAL_VOLUME_VALUE)
                .build(),
            TopologyEntityDTO.newBuilder()
                .setOid(vvOid2)
                .setDisplayName(vv2DisplayName)
                .setEntityType(EntityType.VIRTUAL_VOLUME_VALUE)
                .build()
        );
        when(searchRequest.getFullEntities()).thenReturn(vvTopologyEntityDtos.stream());

        // When
        query.updateStorageCommodityName(statsList, scopedOid, commodityOids);

        assertNotNull(statsList);
        assertEquals(1, statsList.size());
        final StatSnapshotApiDTO statSnapshotApiDTO = statsList.get(0);
        List<StatApiDTO> statApiDtos = statSnapshotApiDTO.getStatistics();
        assertNotNull(statApiDtos);
        assertEquals(8, statApiDtos.size());

        // check displayName has been replaced
        final List<StatApiDTO> storageStatApiDTOs = statApiDtos.stream().filter(statApiDTO -> storageCommodities.contains(statApiDTO.getName()))
            .collect(Collectors.toList());

        assertNotNull(storageStatApiDTOs);
        assertEquals(4, storageStatApiDTOs.size());
        storageStatApiDTOs.forEach(storageStatApiDto -> {
            final String displayName = storageStatApiDto.getDisplayName();
            final String fakeUnit = storageStatApiDto.getUnits();

            if (fakeUnit.equals(vvOid1 + "")) {
                assertTrue(displayName.endsWith(vv1DisplayName));
            } else if (fakeUnit.equals(vvOid2 + "")) {
                assertTrue(displayName.endsWith(vv2DisplayName));
            } else {
                fail("unknown storage commodity");
            }
        });
    }

    /**
     * Test updateStorageCommodityName() when VM has two connected VVs, scope is VV.
     */
    @Test
    public void testUpdateStorageCommodityNameWhenVMHasTwoVVAndScopeIsVV() {
        // Given
        final Long vmOid = 1L;
        final Long vvOid1 = 21L;
        final String vv1DisplayName = "VV-21";
        final Long vvOid2 = 22L;
        final String vv2DisplayName = "VV-22";
        final String stDispalyName = "GP2";
        final List<StatSnapshotApiDTO> statsList = createVMStatSnapshotApiDto(vvOid1, vvOid2, stDispalyName);
        final Set<Long> scopedOid = Collections.singleton(vvOid1);
        final Set<Long> commodityOids = Sets.newHashSet(vvOid1, vvOid2);

        SearchRequest searchRequest = mock(SearchRequest.class);
        when(repositoryApi.newSearchRequest(any())).thenReturn(searchRequest);

        final List<TopologyEntityDTO> vvTopologyEntityDtos = Arrays.asList(
            TopologyEntityDTO.newBuilder()
                .setOid(vvOid1)
                .setDisplayName(vv1DisplayName)
                .setEntityType(EntityType.VIRTUAL_VOLUME_VALUE)
                .build(),
            TopologyEntityDTO.newBuilder()
                .setOid(vvOid2)
                .setDisplayName(vv2DisplayName)
                .setEntityType(EntityType.VIRTUAL_VOLUME_VALUE)
                .build()
        );
        when(searchRequest.getFullEntities()).thenReturn(vvTopologyEntityDtos.stream());

        // When
        query.updateStorageCommodityName(statsList, scopedOid, commodityOids);

        assertNotNull(statsList);
        assertEquals(1, statsList.size());
        final StatSnapshotApiDTO statSnapshotApiDTO = statsList.get(0);
        List<StatApiDTO> statApiDtos = statSnapshotApiDTO.getStatistics();
        assertNotNull(statApiDtos);
        assertEquals(2, statApiDtos.size());

        // check displayName has been replaced
        final List<StatApiDTO> storageStatApiDTOs = statApiDtos.stream().filter(statApiDTO -> storageCommodities.contains(statApiDTO.getName()))
            .collect(Collectors.toList());

        assertNotNull(storageStatApiDTOs);
        assertEquals(2, storageStatApiDTOs.size());
        storageStatApiDTOs.forEach(storageStatApiDto -> {
            final String displayName = storageStatApiDto.getDisplayName();
            final String fakeUnit = storageStatApiDto.getUnits();

            assertTrue(fakeUnit.equals(vvOid1 + ""));
            assertTrue(displayName.endsWith(vv1DisplayName));
        });
    }

    /**
     * Helper to create {@link StatSnapshotApiDTO} for VM.
     *
     * @return List of {@link StatSnapshotApiDTO}
     */
    @Nonnull
    public List<StatSnapshotApiDTO> createVMStatSnapshotApiDto() {
        final StatSnapshotApiDTO vmStatSnapshotApiDTO = new StatSnapshotApiDTO();

        final List<StatApiDTO> statApiDtos = new ArrayList<>();
        statApiDtos.add(createStatApiDto("VMem"));
        statApiDtos.add(createStatApiDto("VCPU"));
        statApiDtos.add(createStatApiDto("IOThroughput"));
        statApiDtos.add(createStatApiDto("NetThroughput"));

        vmStatSnapshotApiDTO.setStatistics(statApiDtos);

        return Collections.singletonList(vmStatSnapshotApiDTO);
    }

    /**
     *  Helper to create {@link StatSnapshotApiDTO} for VM with two VVs' commodities.
     *
     * @param vvOid1 first VV oid
     * @param vvOid2 second VV oid
     * @param stDisplayName storage tier name
     * @return List of {@link StatSnapshotApiDTO}
     */
    @Nonnull
    public List<StatSnapshotApiDTO> createVMStatSnapshotApiDto(final long vvOid1,
                                                               final long vvOid2,
                                                               @Nonnull final String stDisplayName) {
        final StatSnapshotApiDTO vmStatSnapshotApiDTO = new StatSnapshotApiDTO();

        final List<StatApiDTO> statApiDtos = new ArrayList<>();
        statApiDtos.add(createStatApiDto("VMem"));
        statApiDtos.add(createStatApiDto("VCPU"));
        statApiDtos.add(createStatApiDto("IOThroughput"));
        statApiDtos.add(createStatApiDto("NetThroughput"));

        statApiDtos.add(createStorageStatApiDto("StorageAccess", vvOid1, stDisplayName));
        statApiDtos.add(createStorageStatApiDto("StorageAccess", vvOid2, stDisplayName));
        statApiDtos.add(createStorageStatApiDto("StorageAmount", vvOid1, stDisplayName));
        statApiDtos.add(createStorageStatApiDto("StorageAmount", vvOid2, stDisplayName));

        vmStatSnapshotApiDTO.setStatistics(statApiDtos);

        return Collections.singletonList(vmStatSnapshotApiDTO);
    }

    /**
     * Create {@link StatApiDTO} for Virtual Volume.
     * Display Name is in form of "FROM: "Storage Tier Name." KEY: "VV Oid".
     * Unit field stored the oid in String type for verification later.
     *
     * @param name name of commodity
     * @param vvOid vv's Oid
     * @param stDisplayName storage Tier Display name
     * @return {@link StatApiDTO}
     */
    @Nonnull
    private StatApiDTO createStorageStatApiDto(@Nonnull final String name,
                                               final long vvOid,
                                               @Nonnull final String stDisplayName) {
        StatApiDTO stat = createStatApiDto(name);
        stat.setDisplayName("FROM: " + stDisplayName + " " + StatsUtils.COMMODITY_KEY_PREFIX + vvOid);
        stat.setUnits(vvOid + ""); // Use to check the corresponding stat after name conversion
        return stat;
    }

    /**
     * Create {@link StatApiDTO}.
     *
     * @param name name of commodity
     * @return {@link StatApiDTO}
     */
    @Nonnull
    private StatApiDTO createStatApiDto(@Nonnull final String name) {
        StatApiDTO stat = new StatApiDTO();
        stat.setName(name);
        return stat;
    }
}
