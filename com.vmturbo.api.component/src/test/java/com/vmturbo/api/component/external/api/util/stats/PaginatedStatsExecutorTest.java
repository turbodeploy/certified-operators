package com.vmturbo.api.component.external.api.util.stats;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.internal.verification.VerificationModeFactory.times;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import com.google.common.collect.Lists;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import com.vmturbo.api.component.ApiTestUtils;
import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.communication.RepositoryApi.MultiEntityRequest;
import com.vmturbo.api.component.communication.RepositoryApi.SearchRequest;
import com.vmturbo.api.component.external.api.mapper.StatsMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.component.external.api.util.GroupExpander;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory;
import com.vmturbo.api.component.external.api.util.stats.PaginatedStatsExecutor.PaginatedStatsGather;
import com.vmturbo.api.dto.statistic.EntityStatsApiDTO;
import com.vmturbo.api.dto.statistic.StatApiInputDTO;
import com.vmturbo.api.dto.statistic.StatPeriodApiInputDTO;
import com.vmturbo.api.dto.statistic.StatScopesApiInputDTO;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.pagination.EntityStatsPaginationRequest;
import com.vmturbo.api.pagination.EntityStatsPaginationRequest.EntityStatsPaginationResponse;
import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.common.protobuf.common.Pagination.PaginationResponse;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.stats.Stats.EntityStats;
import com.vmturbo.common.protobuf.stats.Stats.EntityStatsScope;
import com.vmturbo.common.protobuf.stats.Stats.GetEntityStatsResponse;
import com.vmturbo.common.protobuf.stats.Stats.ProjectedEntityStatsResponse;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.common.protobuf.stats.StatsMoles.StatsHistoryServiceMole;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.components.api.test.GrpcTestServer;

/**
 *  Test get paginated historical and projected stats.
 */
@RunWith(MockitoJUnitRunner.class)
public class PaginatedStatsExecutorTest {

    private PaginatedStatsExecutor paginatedStatsExecutor;

    private StatsMapper mockStatsMapper = Mockito.mock(StatsMapper.class);

    private UuidMapper mockUuidMapper = Mockito.mock(UuidMapper.class);

    private Clock mockClock = Mockito.mock(Clock.class);

    private RepositoryApi mockRepositoryApi = Mockito.mock(RepositoryApi.class);

    private SupplyChainFetcherFactory mockSupplyChainFetcherFactory = Mockito.mock(SupplyChainFetcherFactory.class);

    private UserSessionContext mockUserSessionContext = Mockito.mock(UserSessionContext.class);

    private GroupExpander mockGroupExpander = Mockito.mock(GroupExpander.class);

    private StatsHistoryServiceMole statsHistoryServiceSpy = spy(new StatsHistoryServiceMole());

    private static final StatSnapshot STAT_SNAPSHOT = StatSnapshot.newBuilder()
            .setSnapshotDate(Clock.systemUTC().millis())
            .build();

    private static final EntityStats ENTITY_STATS = EntityStats.newBuilder()
            .setOid(1L)
            .addStatSnapshots(STAT_SNAPSHOT)
            .build();

    private static final MinimalEntity ENTITY_DESCRIPTOR = MinimalEntity.newBuilder()
            .setOid(1)
            .setDisplayName("hello japan")
            .setEntityType(UIEntityType.PHYSICAL_MACHINE.typeNumber())
            .build();

    @Rule
    public GrpcTestServer testServer = GrpcTestServer.newServer(statsHistoryServiceSpy);

    @Before
    public void setUp() {
        final StatsHistoryServiceBlockingStub statsHistoryServiceSpy =
                StatsHistoryServiceGrpc.newBlockingStub(testServer.getChannel());

        paginatedStatsExecutor = new PaginatedStatsExecutor(mockStatsMapper, mockUuidMapper, mockClock,
                mockRepositoryApi, statsHistoryServiceSpy, mockSupplyChainFetcherFactory,
                mockUserSessionContext, mockGroupExpander);

        MultiEntityRequest req = ApiTestUtils.mockMultiEntityReqEmpty();
        when(mockRepositoryApi.entitiesRequest(any())).thenReturn(req);

        final SearchRequest dcReq = ApiTestUtils.mockSearchMinReq(Collections.emptyList());
        when(mockRepositoryApi.newSearchRequest(any(SearchParameters.class))).thenReturn(dcReq);

    }


    public StatPeriodApiInputDTO buildStatPeriodApiInputDTO(long currentDate, String startDate, String endDate, String statName) {
        StatPeriodApiInputDTO inputDto = new StatPeriodApiInputDTO();
        inputDto.setStartDate(startDate);
        inputDto.setEndDate(endDate);
        when(mockClock.millis()).thenReturn(currentDate);
        List<StatApiInputDTO> statisticsRequested = new ArrayList<>();
        StatApiInputDTO statApiInputDTO = new StatApiInputDTO();
        statApiInputDTO.setName(statName);
        statisticsRequested.add(statApiInputDTO);
        inputDto.setStatistics(statisticsRequested);
        return inputDto;
    }

    private PaginatedStatsGather getPaginatedStatsGatherSpy(StatScopesApiInputDTO inputDto,
            EntityStatsPaginationRequest paginationRequest) {
        return spy(paginatedStatsExecutor.new PaginatedStatsGather(inputDto, paginationRequest));
    }

    private StatScopesApiInputDTO getInputDtoWithHistoricPeriod() {
        StatScopesApiInputDTO inputDto = new StatScopesApiInputDTO();
        StatPeriodApiInputDTO historicPeriod  = buildStatPeriodApiInputDTO(2000L, "1000",
                "1000", "a");
        inputDto.setPeriod(historicPeriod);
        return inputDto;
    }

    private StatScopesApiInputDTO getInputDtoWithProjectionPeriod() {
        StatScopesApiInputDTO inputDto = new StatScopesApiInputDTO();
        StatPeriodApiInputDTO historicPeriod  = buildStatPeriodApiInputDTO(5L, "1000",
                "1000", "a");
        inputDto.setPeriod(historicPeriod);
        return inputDto;
    }

    /**
     * Tests is historical returns true when startDate in the past.
     */
    @Test
    public void testIsHistoricalStatsRequestTrue() {
        //GIVEN
        EntityStatsPaginationRequest paginationRequest = mock(EntityStatsPaginationRequest.class);
        PaginatedStatsGather paginatedStatsGatherSpy = getPaginatedStatsGatherSpy(
                getInputDtoWithHistoricPeriod(), paginationRequest);

        //THEN
        assertTrue(paginatedStatsGatherSpy.isHistoricalStatsRequest());
    }

    /**
     * Tests isHistorical returns false when startDate in the future.
     */
    @Test
    public void testIsHistoricalStatsRequestFalse() {
        //GIVEN
        EntityStatsPaginationRequest paginationRequest = mock(EntityStatsPaginationRequest.class);
        PaginatedStatsGather paginatedStatsGatherSpy = getPaginatedStatsGatherSpy(
                getInputDtoWithProjectionPeriod(), paginationRequest);

        //THEN
        assertFalse(paginatedStatsGatherSpy.isHistoricalStatsRequest());
    }

    /**
     * Tests isProjectionStats returns true when endDate in the future.
     */
    @Test
    public void testIsProjectionStatsRequestTrue() {
        //GIVEN
        EntityStatsPaginationRequest paginationRequest = mock(EntityStatsPaginationRequest.class);
        PaginatedStatsGather paginatedStatsGatherSpy = getPaginatedStatsGatherSpy(
                getInputDtoWithProjectionPeriod(), paginationRequest);

        //THEN
        assertTrue(paginatedStatsGatherSpy.isProjectedStatsRequest());
    }

    /**
     * Tests isProjectionStats returns true when endDate in the past.
     */
    @Test
    public void testIsProjectionStatsRequestFalse() {
        //GIVEN
        EntityStatsPaginationRequest paginationRequest = mock(EntityStatsPaginationRequest.class);
        PaginatedStatsGather paginatedStatsGatherSpy = getPaginatedStatsGatherSpy(
                getInputDtoWithHistoricPeriod(), paginationRequest);

        //THEN
        assertFalse(paginatedStatsGatherSpy.isProjectedStatsRequest());
    }

    /**
     * Tests statsRequest kicks of runHistoricalStatsRequest.
     *
     * @throws OperationFailedException exception thrown if the scope can not be recognized
     */
    @Test
    public void testPaginatedStatsGatherProcessRequestHistorical() throws OperationFailedException {
        //GIVEN
        StatScopesApiInputDTO inputDto = getInputDtoWithHistoricPeriod();
        EntityStatsPaginationRequest paginationRequest = mock(EntityStatsPaginationRequest.class);
        PaginatedStatsGather paginatedStatsGatherSpy = getPaginatedStatsGatherSpy(inputDto, paginationRequest);

        doNothing().when(paginatedStatsGatherSpy).runHistoricalStatsRequest();

        //WHEN
        paginatedStatsGatherSpy.processRequest();

        //THEN
        verify(paginatedStatsGatherSpy, times(1)).runHistoricalStatsRequest();
    }

    /**
     * Tests statsRequest kicks of runProjectedStatsRequest.
     *
     * @throws OperationFailedException exception thrown if the scope can not be recognized
     */
    @Test
    public void testPaginatedStatsGatherProcessRequestProjection() throws OperationFailedException{
        //GIVEN
        StatScopesApiInputDTO inputDto = getInputDtoWithProjectionPeriod();
        EntityStatsPaginationRequest paginationRequest = mock(EntityStatsPaginationRequest.class);
        PaginatedStatsGather paginatedStatsGatherSpy = getPaginatedStatsGatherSpy(inputDto, paginationRequest);

        doNothing().when(paginatedStatsGatherSpy).runProjectedStatsRequest();

        //WHEN
        paginatedStatsGatherSpy.processRequest();

        //THEN
        verify(paginatedStatsGatherSpy, times(1)).runProjectedStatsRequest();
    }

    /**
     * Tests runHistoricalStatsRequest, {@link EntityStatsScope} created that requires scope expansion.
     *
     * @throws OperationFailedException exception thrown if the scope can not be recognized
     */
    @Test
    public void testGetHistoricalStatsWithContainsEntityGroupScope()
            throws OperationFailedException {
        //GIVEN
        StatScopesApiInputDTO inputDto = getInputDtoWithHistoricPeriod();
        EntityStatsPaginationRequest paginationRequest = mock(EntityStatsPaginationRequest.class);
        PaginatedStatsGather paginatedStatsGatherSpy = getPaginatedStatsGatherSpy(inputDto, paginationRequest);

        doReturn(true).when(paginatedStatsGatherSpy).getContainsEntityGroupScope();
        doReturn(EntityStatsScope.getDefaultInstance()).when(paginatedStatsGatherSpy).createEntityGroupsStatsScope();

        doReturn(GetEntityStatsResponse.getDefaultInstance()).when(statsHistoryServiceSpy).getEntityStats(any());

        //WHEN
        paginatedStatsGatherSpy.runHistoricalStatsRequest();

        //THEN
        verify(paginatedStatsGatherSpy, times(1)).createEntityGroupsStatsScope();
        verify(paginatedStatsGatherSpy, times(0)).createEntityStatsScope();
        verify(paginatedStatsGatherSpy, times(1)).getAdditionalDisplayInfoForAllEntities(any(), any());
    }

    /**
     * Tests runHistoricalStatsRequest, {@link EntityStatsScope} created with  no scope expansion.
     *
     * @throws OperationFailedException exception thrown if the scope can not be recognized
     */

    @Test
    public void testGetHistoricalStatsWithoutContainsEntityGroupScope() throws OperationFailedException {
        //GIVEN
        StatScopesApiInputDTO inputDto = getInputDtoWithHistoricPeriod();
        EntityStatsPaginationRequest paginationRequest = mock(EntityStatsPaginationRequest.class);
        PaginatedStatsGather paginatedStatsGatherSpy = getPaginatedStatsGatherSpy(inputDto, paginationRequest);

        doReturn(false).when(paginatedStatsGatherSpy).getContainsEntityGroupScope();
        doReturn(EntityStatsScope.getDefaultInstance()).when(paginatedStatsGatherSpy).createEntityStatsScope();

        doReturn(GetEntityStatsResponse.getDefaultInstance()).when(statsHistoryServiceSpy).getEntityStats(any());

        //WHEN
        paginatedStatsGatherSpy.runHistoricalStatsRequest();

        //THEN
        verify(paginatedStatsGatherSpy, times(0)).createEntityGroupsStatsScope();
        verify(paginatedStatsGatherSpy, times(1)).createEntityStatsScope();
        verify(paginatedStatsGatherSpy, times(1)).getAdditionalDisplayInfoForAllEntities(any(), any());
    }

    /**
     * Tests runProjectedStatsRequest, {@link EntityStatsScope} created that requires scope expansion.
     *
     * @throws OperationFailedException exception thrown if the scope can not be recognized
     */
    @Test
    public void testRunProjectedStatsWithContainsEntityGroupScope()
            throws OperationFailedException {
        //GIVEN
        StatScopesApiInputDTO inputDto = getInputDtoWithProjectionPeriod();
        EntityStatsPaginationRequest paginationRequest = mock(EntityStatsPaginationRequest.class);
        PaginatedStatsGather paginatedStatsGatherSpy = getPaginatedStatsGatherSpy(inputDto, paginationRequest);

        doReturn(true).when(paginatedStatsGatherSpy).getContainsEntityGroupScope();
        doReturn(EntityStatsScope.getDefaultInstance()).when(paginatedStatsGatherSpy).createEntityGroupsStatsScope();

        doReturn(ProjectedEntityStatsResponse.getDefaultInstance()).when(statsHistoryServiceSpy).getProjectedEntityStats(any());

        //WHEN
        paginatedStatsGatherSpy.runProjectedStatsRequest();

        //THEN
        verify(paginatedStatsGatherSpy, times(1)).createEntityGroupsStatsScope();
        verify(paginatedStatsGatherSpy, times(1)).getAdditionalDisplayInfoForAllEntities(any(), any());
    }

    /**
     * Tests runProjectedStatsRequest, {@link EntityStatsScope} created with no scope expansion.
     *
     * @throws OperationFailedException exception thrown if the scope can not be recognized
     */
    @Test
    public void testRunProjectedStatsWithoutContainsEntityGroupScope()
            throws OperationFailedException {
        //GIVEN
        StatScopesApiInputDTO inputDto = getInputDtoWithProjectionPeriod();
        inputDto.setScopes(Lists.newArrayList("1"));
        EntityStatsPaginationRequest paginationRequest = mock(EntityStatsPaginationRequest.class);
        PaginatedStatsGather paginatedStatsGatherSpy = getPaginatedStatsGatherSpy(inputDto, paginationRequest);

        doReturn(false).when(paginatedStatsGatherSpy).getContainsEntityGroupScope();
        doReturn(ProjectedEntityStatsResponse.getDefaultInstance()).when(statsHistoryServiceSpy).getProjectedEntityStats(any());

        //WHEN
        paginatedStatsGatherSpy.runProjectedStatsRequest();

        //THEN
        verify(paginatedStatsGatherSpy, times(0)).createEntityGroupsStatsScope();
        verify(paginatedStatsGatherSpy, times(1)).getAdditionalDisplayInfoForAllEntities(any(), any());
    }

    /**
     * Tests paginationRequest calls nextPageResponse when nextCursor present.
     */
    @Test
    public void testsGetAdditionalDisplayInfoForAllEntitiesWithNextCursor() {
        //GIVEN
        EntityStats entityStats = ENTITY_STATS;
        List<EntityStats> nextStatsPage = Arrays.asList(entityStats);


        MultiEntityRequest req = ApiTestUtils.mockMultiMinEntityReq(Lists.newArrayList(ENTITY_DESCRIPTOR));
        doReturn(req).when(mockRepositoryApi).entitiesRequest(any());

        StatScopesApiInputDTO inputDto = getInputDtoWithProjectionPeriod();
        EntityStatsPaginationRequest mockPaginationRequest = mock(EntityStatsPaginationRequest.class);
        PaginatedStatsGather paginatedStatsGatherSpy = getPaginatedStatsGatherSpy(inputDto, mockPaginationRequest);

        EntityStatsPaginationResponse mockPaginationResponse = mock(EntityStatsPaginationResponse.class);
        doReturn(mockPaginationResponse).when(mockPaginationRequest).nextPageResponse(any(), any(), any());

        final String nextCursor = "you're next!";
        PaginationResponse paginationResponse = PaginationResponse.newBuilder().setNextCursor(nextCursor).build();
        //WHEN
        paginatedStatsGatherSpy.getAdditionalDisplayInfoForAllEntities(nextStatsPage, Optional.of(paginationResponse));

        //THEN
        assertEquals(paginatedStatsGatherSpy.getEntityStatsPaginationResponse(), mockPaginationResponse);
        verify(mockPaginationRequest, times(1)).nextPageResponse(any(), any(), any());
        verify(mockPaginationRequest, times(0)).finalPageResponse(any(), any());
        verify(mockPaginationRequest, times(0)).allResultsResponse(any());
    }

    /**
     * Tests paginationRequest calls finalPageResponse when nextCursor NOT present.
     */
    @Test
    public void testsGetAdditionalDisplayInfoForAllEntitiesWithoutNextCursor() {
        //GIVEN
        EntityStats entityStats = ENTITY_STATS;
        List<EntityStats> nextStatsPage = Arrays.asList(entityStats);


        MultiEntityRequest req = ApiTestUtils.mockMultiMinEntityReq(Lists.newArrayList(ENTITY_DESCRIPTOR));
        doReturn(req).when(mockRepositoryApi).entitiesRequest(any());

        StatScopesApiInputDTO inputDto = getInputDtoWithProjectionPeriod();
        EntityStatsPaginationRequest mockPaginationRequest = mock(EntityStatsPaginationRequest.class);
        PaginatedStatsGather paginatedStatsGatherSpy = getPaginatedStatsGatherSpy(inputDto, mockPaginationRequest);

        EntityStatsPaginationResponse mockPaginationResponse = mock(EntityStatsPaginationResponse.class);
        doReturn(mockPaginationResponse).when(mockPaginationRequest).finalPageResponse(any(), any());

        final String nextCursor = "you're next!";
        PaginationResponse paginationResponse = PaginationResponse.getDefaultInstance();
        //WHEN
        paginatedStatsGatherSpy.getAdditionalDisplayInfoForAllEntities(nextStatsPage, Optional.of(paginationResponse));

        //THEN
        assertEquals(paginatedStatsGatherSpy.getEntityStatsPaginationResponse(), mockPaginationResponse);
        verify(mockPaginationRequest, times(0)).nextPageResponse(any(), any(), any());
        verify(mockPaginationRequest, times(1)).finalPageResponse(any(), any());
        verify(mockPaginationRequest, times(0)).allResultsResponse(any());
    }

    /**
     * Tests paginationRequest calls allResultsResponse when paginationResponse is empty.
     */
    @Test
    public void testsGetAdditionalDisplayInfoForAllEntitiesWithEmptyPaginationResponse() {
        //GIVEN
        EntityStats entityStats = ENTITY_STATS;
        List<EntityStats> nextStatsPage = Arrays.asList(entityStats);


        MultiEntityRequest req = ApiTestUtils.mockMultiMinEntityReq(Lists.newArrayList(ENTITY_DESCRIPTOR));
        doReturn(req).when(mockRepositoryApi).entitiesRequest(any());

        StatScopesApiInputDTO inputDto = getInputDtoWithProjectionPeriod();
        EntityStatsPaginationRequest mockPaginationRequest = mock(EntityStatsPaginationRequest.class);
        PaginatedStatsGather paginatedStatsGatherSpy = getPaginatedStatsGatherSpy(inputDto, mockPaginationRequest);

        EntityStatsPaginationResponse mockPaginationResponse = mock(EntityStatsPaginationResponse.class);
        doReturn(mockPaginationResponse).when(mockPaginationRequest).allResultsResponse(any());

        //WHEN
        paginatedStatsGatherSpy.getAdditionalDisplayInfoForAllEntities(nextStatsPage, Optional.empty());

        //THEN
        assertEquals(paginatedStatsGatherSpy.getEntityStatsPaginationResponse(), mockPaginationResponse);
        verify(mockPaginationRequest, times(0)).nextPageResponse(any(), any(), any());
        verify(mockPaginationRequest, times(0)).finalPageResponse(any(), any());
        verify(mockPaginationRequest, times(1)).allResultsResponse(any());
    }

    /**
     * Tests paginationRequest calls allResultsResponse when paginationResponse is empty.
     */
    @Test
    public void testsGetAdditionalDisplayInfoForAllEntitiesCheckResponse() {
        //GIVEN
        final EntityStats entityStats = ENTITY_STATS;
        final List<EntityStats> nextStatsPage = Arrays.asList(entityStats);

        final MultiEntityRequest req = ApiTestUtils.mockMultiMinEntityReq(Lists.newArrayList(ENTITY_DESCRIPTOR));
        doReturn(req).when(mockRepositoryApi).entitiesRequest(any());

        final StatScopesApiInputDTO inputDto = getInputDtoWithProjectionPeriod();
        final EntityStatsPaginationRequest paginationRequest = new EntityStatsPaginationRequest("foo", 1, true, "order");
        PaginatedStatsGather paginatedStatsGatherSpy = getPaginatedStatsGatherSpy(inputDto, paginationRequest);

        //WHEN
        paginatedStatsGatherSpy.getAdditionalDisplayInfoForAllEntities(nextStatsPage, Optional.empty());

        //THEN
        final EntityStatsApiDTO entityStatDto = paginatedStatsGatherSpy.getEntityStatsPaginationResponse().getRawResults().get(0);
        assertThat(entityStatDto.getUuid(), is(Long.toString(ENTITY_DESCRIPTOR.getOid())));
        assertThat(entityStatDto.getDisplayName(), is(ENTITY_DESCRIPTOR.getDisplayName()));
        assertThat(entityStatDto.getClassName(), is(UIEntityType.fromType(ENTITY_DESCRIPTOR.getEntityType()).apiStr()));
    }

    /**
     * Tests getContainsEntityGroupScope returns false if scope empty.
     *
     * @throws OperationFailedException exception thrown if the scope can not be recognized
     */
    @Test
    public void testContainsEntityGroupScopeWhenAlreadySet() throws OperationFailedException {
        //GIVEN
        final StatScopesApiInputDTO inputDto = getInputDtoWithProjectionPeriod();
        final EntityStatsPaginationRequest paginationRequest = new EntityStatsPaginationRequest("foo", 1, true, "order");
        PaginatedStatsGather paginatedStatsGatherSpy = getPaginatedStatsGatherSpy(inputDto, paginationRequest);

        //THEN
        assertFalse(paginatedStatsGatherSpy.getContainsEntityGroupScope());
    }

    /**
     * Test the case that relatedType is set.
     *
     * <p>With relatedType in inputDto should call statsMapper.shouldNormalize.
     * Method identifies relatedTypes that should be expand scope further.
     * Example DC should expand to PMs</p>
     *
     * @throws OperationFailedException exception thrown if the scope can not be recognized
     */
    @Test
    public void testContainsEntityGroupScopeWithDCScope() throws OperationFailedException {
        //GIVEN
        StatScopesApiInputDTO inputDto = getInputDtoWithProjectionPeriod();
        inputDto.setScopes(Arrays.asList("123"));
        inputDto.setRelatedType(UIEntityType.DATACENTER.apiStr());
        final EntityStatsPaginationRequest paginationRequest = new EntityStatsPaginationRequest("foo", 1, true, "order");
        PaginatedStatsGather paginatedStatsGatherSpy = getPaginatedStatsGatherSpy(inputDto, paginationRequest);

        doReturn(true).when(mockStatsMapper).shouldNormalize(any());

        //THEN
        assertTrue(paginatedStatsGatherSpy.getContainsEntityGroupScope());
        verify(mockStatsMapper, times(1)).shouldNormalize(any());
    }

    /**
     * Tests containsEntityGroupScope return true when scope contains group.
     *
     * @throws OperationFailedException exception thrown if the scope can not be recognized
     */
    @Test
    public void testContainsEntityGroupScopeWhenGroupInScope() throws OperationFailedException {
        //GIVEN
        StatScopesApiInputDTO inputDto = getInputDtoWithProjectionPeriod();
        inputDto.setScopes(Arrays.asList("123"));
        final EntityStatsPaginationRequest paginationRequest = new EntityStatsPaginationRequest("foo", 1, true, "order");
        PaginatedStatsGather paginatedStatsGatherSpy = getPaginatedStatsGatherSpy(inputDto, paginationRequest);

        ApiId mockApiId = mock(ApiId.class);
        doReturn(mockApiId).when(mockUuidMapper).fromUuid(any());
        doReturn(true).when(mockApiId).isGroup();

        //THEN
        assertTrue(paginatedStatsGatherSpy.getContainsEntityGroupScope());
        verify(mockUuidMapper, times(1)).fromUuid(any());
        verify(mockApiId, times(1)).isGroup();
    }

    /**
     * Test that the 'relatedType' argument is required if the scope is "Market".
     *
     * @throws Exception as expected, with IllegalArgumentException since no 'relatedType'
     */
    @Test(expected = IllegalArgumentException.class)
    public void testFullMarketStatsNoRelatedTypeWithNullPeriod() throws Exception {
        //GIVEN
        StatScopesApiInputDTO inputDto = getInputDtoWithProjectionPeriod();
        inputDto.setScopes(Lists.newArrayList(UuidMapper.UI_REAL_TIME_MARKET_STR));
        final EntityStatsPaginationRequest paginationRequest = new EntityStatsPaginationRequest("foo", 1, true, "order");
        PaginatedStatsGather paginatedStatsGatherSpy = getPaginatedStatsGatherSpy(inputDto, paginationRequest);

        //WHEN
        paginatedStatsGatherSpy.createEntityStatsScope();
    }
}

