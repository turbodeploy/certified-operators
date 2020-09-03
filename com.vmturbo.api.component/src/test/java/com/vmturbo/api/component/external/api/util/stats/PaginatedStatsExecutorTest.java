package com.vmturbo.api.component.external.api.util.stats;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.joda.time.DateTime;
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
import com.vmturbo.api.component.external.api.mapper.PaginationMapper;
import com.vmturbo.api.component.external.api.mapper.StatsMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.component.external.api.util.GroupExpander;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory;
import com.vmturbo.api.component.external.api.util.stats.PaginatedStatsExecutor.PaginatedStatsGather;
import com.vmturbo.api.dto.statistic.EntityStatsApiDTO;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.statistic.StatApiInputDTO;
import com.vmturbo.api.dto.statistic.StatPeriodApiInputDTO;
import com.vmturbo.api.dto.statistic.StatScopesApiInputDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.pagination.EntityStatsPaginationRequest;
import com.vmturbo.api.pagination.EntityStatsPaginationRequest.EntityStatsPaginationResponse;
import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.common.Pagination.PaginationParameters;
import com.vmturbo.common.protobuf.common.Pagination.PaginationResponse;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatRecord;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatRecord.StatRecord;
import com.vmturbo.common.protobuf.cost.Cost.GetCloudCostStatsResponse;
import com.vmturbo.common.protobuf.cost.CostMoles.CostServiceMole;
import com.vmturbo.common.protobuf.cost.CostServiceGrpc;
import com.vmturbo.common.protobuf.cost.CostServiceGrpc.CostServiceBlockingStub;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.stats.Stats.EntityStats;
import com.vmturbo.common.protobuf.stats.Stats.EntityStatsScope;
import com.vmturbo.common.protobuf.stats.Stats.GetEntityStatsResponse;
import com.vmturbo.common.protobuf.stats.Stats.ProjectedEntityStatsResponse;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.common.protobuf.stats.StatsMoles.StatsHistoryServiceMole;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.common.pagination.EntityStatsPaginationParamsFactory;
import com.vmturbo.components.common.pagination.EntityStatsPaginator;
import com.vmturbo.components.common.pagination.EntityStatsPaginator.PaginatedStats;

/**
 *  Test get paginated historical and projected stats.
 */
@RunWith(MockitoJUnitRunner.class)
public class PaginatedStatsExecutorTest {

    private PaginatedStatsExecutor paginatedStatsExecutor;

    private PaginationMapper mockPaginationMapper = mock(PaginationMapper.class);

    private StatsMapper statsMapper = spy(new StatsMapper(mockPaginationMapper));

    private UuidMapper mockUuidMapper = Mockito.mock(UuidMapper.class);

    private Clock mockClock = Mockito.mock(Clock.class);

    private RepositoryApi mockRepositoryApi = Mockito.mock(RepositoryApi.class);

    private SupplyChainFetcherFactory mockSupplyChainFetcherFactory = Mockito.mock(SupplyChainFetcherFactory.class);

    private UserSessionContext mockUserSessionContext = Mockito.mock(UserSessionContext.class);

    private GroupExpander mockGroupExpander = Mockito.mock(GroupExpander.class);

    private StatsQueryExecutor mockStatsQueryExecutor = Mockito.mock(StatsQueryExecutor.class);

    private StatsHistoryServiceMole statsHistoryServiceSpy = spy(new StatsHistoryServiceMole());

    private CostServiceMole costServiceMole = spy(new CostServiceMole());

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
            .setEntityType(ApiEntityType.PHYSICAL_MACHINE.typeNumber())
            .build();

    private final String costCommodity = StringConstants.COST_PRICE;
    private final String historyCommodity = StringConstants.VCPU;
    private final long snapShotDate = DateTime.now().getMillis();

    @Rule
    public GrpcTestServer testServer = GrpcTestServer.newServer(statsHistoryServiceSpy, costServiceMole);

    @Before
    public void setUp() throws Exception {
        final StatsHistoryServiceBlockingStub statsHistoryServiceSpy =
                StatsHistoryServiceGrpc.newBlockingStub(testServer.getChannel());
        final CostServiceBlockingStub costServiceRpcSpy = CostServiceGrpc.newBlockingStub(testServer.getChannel());


        paginatedStatsExecutor = new PaginatedStatsExecutor(statsMapper, mockUuidMapper, mockClock,
                mockRepositoryApi, statsHistoryServiceSpy, mockSupplyChainFetcherFactory,
                mockUserSessionContext, mockGroupExpander, new EntityStatsPaginator(),
                mock(EntityStatsPaginationParamsFactory.class), new PaginationMapper(),
                costServiceRpcSpy, mockStatsQueryExecutor);

        doReturn(PaginationParameters.getDefaultInstance()).when(mockPaginationMapper).toProtoParams(any());
        //TODO: Can I get ride of all this?
        MultiEntityRequest req = ApiTestUtils.mockMultiEntityReqEmpty();
        when(mockRepositoryApi.entitiesRequest(any())).thenReturn(req);

        //TODO: Can I get ride of all this?
        final SearchRequest dcReq = ApiTestUtils.mockSearchMinReq(Collections.emptyList());
        when(mockRepositoryApi.newSearchRequest(any(SearchParameters.class))).thenReturn(dcReq);

    }

    public StatPeriodApiInputDTO buildStatPeriodApiInputDTO(long currentDate, String startDate, String endDate, String statName) {
        StatPeriodApiInputDTO inputDto = new StatPeriodApiInputDTO();
        inputDto.setStartDate(startDate);
        inputDto.setEndDate(endDate);
        when(mockClock.millis()).thenReturn(currentDate);
        StatApiInputDTO statApiInputDTO = new StatApiInputDTO();
        statApiInputDTO.setName(statName);
        List<StatApiInputDTO> statisticsRequested = Collections.singletonList(statApiInputDTO);
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

    private StatScopesApiInputDTO getInputDtoWithCustomPeriod(
            long currentDate,
            String startDate,
            String endDate,
            String statName) {
        StatScopesApiInputDTO inputDto = new StatScopesApiInputDTO();
        StatPeriodApiInputDTO historicPeriod  = buildStatPeriodApiInputDTO(currentDate, startDate,
                endDate, statName);
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
     * Tests is historical returns true when startDate is null.
     */
    @Test
    public void testIsHistoricalStatsRequestTrueWhenStartDateNull() {
        //GIVEN
        EntityStatsPaginationRequest paginationRequest = mock(EntityStatsPaginationRequest.class);
        StatPeriodApiInputDTO periodDto = new StatPeriodApiInputDTO();
        StatScopesApiInputDTO statScopesApiInputDTO = getInputDtoWithHistoricPeriod();
        getInputDtoWithHistoricPeriod().setPeriod(periodDto);
        PaginatedStatsGather paginatedStatsGatherSpy = getPaginatedStatsGatherSpy(
                statScopesApiInputDTO, paginationRequest);

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
     * Tests isProjectionStats returns false when endDate is null.
     */
    @Test
    public void testIsProjectionStatsRequestTrueWhenEndDateNull() {
        //GIVEN
        EntityStatsPaginationRequest paginationRequest = mock(EntityStatsPaginationRequest.class);
        StatPeriodApiInputDTO periodDto = new StatPeriodApiInputDTO();
        StatScopesApiInputDTO statScopesApiInputDTO = getInputDtoWithHistoricPeriod();
        getInputDtoWithHistoricPeriod().setPeriod(periodDto);
        PaginatedStatsGather paginatedStatsGatherSpy = getPaginatedStatsGatherSpy(
                statScopesApiInputDTO, paginationRequest);

        //THEN
        assertFalse(paginatedStatsGatherSpy.isProjectedStatsRequest());
    }

    /**
     * Test that if only the start date is provided and it is in the past, then the end date is set
     * to current time.
     */
    @Test
    public void testSanitizeStartDateOrEndDateWithOnlyStartDateInThePast() {
        long currentTime = new Date().getTime();
        long startTime = currentTime - 1000L * 60L * 60L * 24L;      // set start date to 1 day ago
        StatScopesApiInputDTO inputDto =
                getInputDtoAfterSanitizeStartDateOrEndDateWithCustomDates(
                        currentTime, Long.toString(startTime), null);

        // Assert that end date exists and is set to current time
        assertNotNull(inputDto.getPeriod().getEndDate());
        assertEquals(currentTime, Long.parseLong(inputDto.getPeriod().getEndDate()));
    }

    /**
     * Test that if only the start date is provided and it is set to now, then both are set
     * to null.
     */
    @Test
    public void testSanitizeStartDateOrEndDateWithOnlyStartDateNow() {
        long currentTime = new Date().getTime();
        StatScopesApiInputDTO inputDto =
                getInputDtoAfterSanitizeStartDateOrEndDateWithCustomDates(
                        currentTime, Long.toString(currentTime), null);

        // Assert that both dates are null
        assertNull(inputDto.getPeriod().getEndDate());
        assertNull(inputDto.getPeriod().getStartDate());
    }

    /**
     * Test that if only the start date is provided and it is in the future, the end date is set to
     * start date.
     */
    @Test
    public void testSanitizeStartDateOrEndDateWithOnlyStartDateInTheFuture() {
        long currentTime = new Date().getTime();
        // set start date to 1 day in the future
        long startTime = currentTime + 1000L * 60L * 60L * 24L;
        StatScopesApiInputDTO inputDto =
                getInputDtoAfterSanitizeStartDateOrEndDateWithCustomDates(
                        currentTime, Long.toString(startTime), null);

        // Assert that end date exists and is set to current time
        assertNotNull(inputDto.getPeriod().getEndDate());
        assertEquals(startTime, Long.parseLong(inputDto.getPeriod().getEndDate()));
    }

    /**
     * Test that if only the end date is provided and it is in the past, then an exception is thrown
     * since it is not a valid input.
     *
     * @throws IllegalArgumentException as expected.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testSanitizeStartDateOrEndDateWithOnlyEndDateInThePast() {
        long currentTime = new Date().getTime();
        long endDate = currentTime - 1000L * 60L * 60L * 24L;      // set end date to 1 day ago
        StatScopesApiInputDTO inputDto =
                getInputDtoAfterSanitizeStartDateOrEndDateWithCustomDates(
                        currentTime, null, Long.toString(endDate));
    }

    /**
     * Test that if only the end date is provided and it is set to now, then both are set
     * to null.
     */
    @Test
    public void testSanitizeStartDateOrEndDateWithOnlyEndDateNow() {
        long currentTime = new Date().getTime();
        StatScopesApiInputDTO inputDto =
                getInputDtoAfterSanitizeStartDateOrEndDateWithCustomDates(
                        currentTime, null, Long.toString(currentTime));

        // Assert that both dates are null
        assertNull(inputDto.getPeriod().getEndDate());
        assertNull(inputDto.getPeriod().getStartDate());
    }

    /**
     * Test that if only the end date is provided and it is in the future, then the start date is
     * set to now.
     */
    @Test
    public void testSanitizeStartDateOrEndDateWithOnlyEndDateInTheFuture() {
        long currentTime = new Date().getTime();
        // set end date to 1 day in the future
        long endTime = currentTime + 1000L * 60L * 60L * 24L;
        StatScopesApiInputDTO inputDto =
                getInputDtoAfterSanitizeStartDateOrEndDateWithCustomDates(
                        currentTime, null, Long.toString(endTime));
        // Assert that end date exists and is set to current time
        assertNotNull(inputDto.getPeriod().getStartDate());
        assertEquals(currentTime, Long.parseLong(inputDto.getPeriod().getStartDate()));
    }

    /**
     * Creates a StatScopesApiInputDTO with the times provided, executes processRequest() verifying
     * how many times runHistoricalStatsRequest() and runProjectedStatsRequest() were invoked, and
     * returns the dto in order to provide a way to the caller function to check if the dates
     * were altered correctly.
     *
     * @param currentTime a unix timestamp (milliseconds) to be considered 'now' for the execution.
     * @param startTime a String to initialize the dto's start date with. Can be null.
     * @param endTime a String to initialize the dto's end date with. Can be null.
     * @return the StatScopesApiInputDTO (with the dates possibly altered by processRequest()).
     */
    private StatScopesApiInputDTO getInputDtoAfterSanitizeStartDateOrEndDateWithCustomDates(
            long currentTime,
            String startTime,
            String endTime) {
        EntityStatsPaginationRequest paginationRequest = mock(EntityStatsPaginationRequest.class);
        StatScopesApiInputDTO inputDto =
                getInputDtoWithCustomPeriod(currentTime, startTime, endTime, "a");
        PaginatedStatsGather paginatedStatsGatherSpy =
                getPaginatedStatsGatherSpy(inputDto, paginationRequest);

        paginatedStatsGatherSpy.sanitizeStartDateOrEndDate();

        return inputDto;
    }

    /**
     * Tests statsRequest kicks of runProjectedStatsRequest.
     *
     * @throws OperationFailedException exception thrown if the scope can not be recognized
     */
    @Test
    public void testPaginatedStatsGatherProcessRequestProjection() throws OperationFailedException {
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

        doReturn(true).when(paginatedStatsGatherSpy).containsEntityGroupScope();
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
        final String entityId = "1";
        StatScopesApiInputDTO inputDto = getInputDtoWithProjectionPeriod();
        inputDto.setScopes(Lists.newArrayList(entityId));
        EntityStatsPaginationRequest paginationRequest = mock(EntityStatsPaginationRequest.class);
        PaginatedStatsGather paginatedStatsGatherSpy = getPaginatedStatsGatherSpy(inputDto, paginationRequest);

        final ApiId mockApiId = mock(ApiId.class);
        when(mockApiId.isResourceGroupOrGroupOfResourceGroups()).thenReturn(false);
        when(mockUuidMapper.fromOid(Long.parseLong(entityId))).thenReturn(mockApiId);

        doReturn(false).when(paginatedStatsGatherSpy).containsEntityGroupScope();
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
        List<EntityStats> nextStatsPage = Arrays.asList(ENTITY_STATS);

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
        List<EntityStats> nextStatsPage = Arrays.asList(ENTITY_STATS);

        MultiEntityRequest req = ApiTestUtils.mockMultiMinEntityReq(Lists.newArrayList(ENTITY_DESCRIPTOR));
        doReturn(req).when(mockRepositoryApi).entitiesRequest(any());

        StatScopesApiInputDTO inputDto = getInputDtoWithProjectionPeriod();
        EntityStatsPaginationRequest mockPaginationRequest = mock(EntityStatsPaginationRequest.class);
        PaginatedStatsGather paginatedStatsGatherSpy = getPaginatedStatsGatherSpy(inputDto, mockPaginationRequest);

        EntityStatsPaginationResponse mockPaginationResponse = mock(EntityStatsPaginationResponse.class);
        doReturn(mockPaginationResponse).when(mockPaginationRequest).finalPageResponse(any(), any());

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
        List<EntityStats> nextStatsPage = Arrays.asList(ENTITY_STATS);


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
        final List<EntityStats> nextStatsPage = Arrays.asList(ENTITY_STATS);

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
        assertThat(entityStatDto.getClassName(), is(ApiEntityType.fromType(ENTITY_DESCRIPTOR.getEntityType()).apiStr()));
    }

    /**
     * Tests that getAdditionalDisplayInfoForAllEntities includes cost stats to response
     * when queried entities are in cloud or hybrid environment.
     */
    @Test
    public void testsGetAdditionalDisplayInfoForAllEntitiesWithCostStats() {
        final long entityOid = 1L;
        final float statValue = 5;
        final EntityStats entityStats = EntityStats.newBuilder()
                .setOid(entityOid)
                .addStatSnapshots(STAT_SNAPSHOT)
                .build();
        final MinimalEntity cloudMinimalEntity = MinimalEntity.newBuilder()
                .setOid(entityOid)
                .setDisplayName("cloudMinimalEntity")
                .setEntityType(ApiEntityType.VIRTUAL_VOLUME.typeNumber())
                .setEnvironmentType(EnvironmentType.CLOUD)
                .build();

        final List<EntityStats> nextStatsPage = Arrays.asList(entityStats);
        final MultiEntityRequest multiEntityRequest = ApiTestUtils.mockMultiMinEntityReq(Lists.newArrayList(cloudMinimalEntity));
        doReturn(multiEntityRequest).when(mockRepositoryApi).entitiesRequest(any());
        doReturn(Collections.singletonList(costPriceResponse(costCommodity, entityOid, statValue, snapShotDate)))
                .when(costServiceMole).getCloudCostStats(any());
        final StatScopesApiInputDTO inputDto = getInputDtoWithProjectionPeriod();
        final EntityStatsPaginationRequest paginationRequest = new EntityStatsPaginationRequest("foo", 1, true, "order");
        PaginatedStatsGather paginatedStatsGatherSpy = getPaginatedStatsGatherSpy(inputDto, paginationRequest);

        //WHEN
        paginatedStatsGatherSpy.getAdditionalDisplayInfoForAllEntities(nextStatsPage, Optional.empty());
        //THEN
        EntityStatsPaginationResponse response =
                paginatedStatsGatherSpy.getEntityStatsPaginationResponse();
        assertNotNull(response);
        List<EntityStatsApiDTO> results = response.getRawResults();
        assertTrue(results.size() == 1);
        EntityStatsApiDTO result = results.get(0);
        assertTrue(result.getUuid().equals(String.valueOf(entityOid)));
        final List<StatSnapshotApiDTO> stats = result.getStats();
        assertTrue(stats.size() == 1);
        List<StatApiDTO> statistics = stats.get(0).getStatistics();
        assertTrue(statistics.size() == 1);
        final StatApiDTO costPriceStat =  statistics.get(0);
        assertEquals(costCommodity, costPriceStat.getName());
        assertTrue(costPriceStat.getValue() == statValue);
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
        assertFalse(paginatedStatsGatherSpy.containsEntityGroupScope());
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
        inputDto.setScopes(Collections.singletonList("123"));
        inputDto.setRelatedType(ApiEntityType.DATACENTER.apiStr());
        final EntityStatsPaginationRequest paginationRequest = new EntityStatsPaginationRequest("foo", 1, true, "order");
        PaginatedStatsGather paginatedStatsGatherSpy = getPaginatedStatsGatherSpy(inputDto, paginationRequest);

        doReturn(true).when(statsMapper).shouldNormalize(any());

        //THEN
        assertTrue(paginatedStatsGatherSpy.containsEntityGroupScope());
        verify(statsMapper, times(1)).shouldNormalize(any());
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
        inputDto.setScopes(Collections.singletonList("123"));
        final EntityStatsPaginationRequest paginationRequest = new EntityStatsPaginationRequest("foo", 1, true, "order");
        PaginatedStatsGather paginatedStatsGatherSpy = getPaginatedStatsGatherSpy(inputDto, paginationRequest);

        ApiId mockApiId = mock(ApiId.class);
        doReturn(mockApiId).when(mockUuidMapper).fromUuid(any());
        doReturn(true).when(mockApiId).isGroup();

        //THEN
        assertTrue(paginatedStatsGatherSpy.containsEntityGroupScope());
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

    /**
     * True if request is sort by a cloud cost stat.
     */
    @Test
    public void testRequestIsSortByCloudCostStats() {
        //GIVEN
        final StatScopesApiInputDTO inputDto =
                createStatScopesApiInputDTOWithCostAndHistoryStatsSortedBy(costCommodity, null);
        final EntityStatsPaginationRequest paginationRequest = new EntityStatsPaginationRequest(null,
                1, true, costCommodity);
        final PaginatedStatsGather paginatedStatsGatherSpy = getPaginatedStatsGatherSpy(
                inputDto, paginationRequest);

        //THEN
        assertTrue(paginatedStatsGatherSpy.requestIsSortByCloudCostStats());
    }

    /**
     * False if request is sort by a none cloud cost stat.
     */
    @Test
    public void testRequestIsSortByCloudCostStatsReturnFalse() {
        //GIVEN
        final StatScopesApiInputDTO inputDto =
                createStatScopesApiInputDTOWithCostAndHistoryStatsSortedBy(null, historyCommodity);
        final EntityStatsPaginationRequest paginationRequest = new EntityStatsPaginationRequest(null,
                1, true, historyCommodity);
        final PaginatedStatsGather paginatedStatsGatherSpy = getPaginatedStatsGatherSpy(
                inputDto, paginationRequest);

        //THEN
        assertFalse(paginatedStatsGatherSpy.requestIsSortByCloudCostStats());
    }

    /**
     * Test runRequestThroughCostComponent, only historical stats returned, no cost.
     *
     * @throws OperationFailedException exception thrown from unexpected behavior
     */
    @Test
    public void testRunRequestThroughCostComponentForCloudAndHistoryStatsNoCostStatsReturned()
            throws OperationFailedException {
        final StatScopesApiInputDTO inputDto =
                createStatScopesApiInputDTOWithCostAndHistoryStatsSortedBy(costCommodity, historyCommodity);
        final EntityStatsPaginationRequest paginationRequest = new EntityStatsPaginationRequest(null,
                1, true, costCommodity);
        final PaginatedStatsGather paginatedStatsGatherSpy = getPaginatedStatsGatherSpy(
                inputDto, paginationRequest);

        final long entityUuid = 5L;
        final float statValue = 5;
        final String nextCursor = "you're next!";
        PaginationResponse paginationResponse = PaginationResponse.newBuilder().setNextCursor(nextCursor).build();
        PaginatedStats mockPaginatedStats = mock(PaginatedStats.class);

        doReturn(Collections.singleton(entityUuid)).when(paginatedStatsGatherSpy).getExpandedScope(any());
        //Cost Stats
        doReturn(Collections.singletonList(GetCloudCostStatsResponse.getDefaultInstance())).when(costServiceMole).getCloudCostStats(any());

        //Pagination and sorting process
        doReturn(Collections.singletonList(entityUuid)).when(mockPaginatedStats).getNextPageIds();
        doReturn(paginationResponse).when(mockPaginatedStats).getPaginationResponse();
        doReturn(mockPaginatedStats).when(paginatedStatsGatherSpy).paginateAndSortCostResponse(any(), any());

        //History and MinimalEntity Responses
        doReturn(historyVCPUResponse(historyCommodity, entityUuid, statValue, snapShotDate)).when(statsHistoryServiceSpy).getEntityStats(any());
        doReturn(getMinimalEntityResponse(entityUuid)).when(paginatedStatsGatherSpy).getMinimalEntitiesForEntityList(any());

        //WHEN
        paginatedStatsGatherSpy.runRequestThroughCostComponent();

        //THEN
        EntityStatsPaginationResponse response =
                paginatedStatsGatherSpy.getEntityStatsPaginationResponse();
        assertNotNull(response);
        List<EntityStatsApiDTO> results = response.getRawResults();
        assertTrue(results.size() == 1);
        EntityStatsApiDTO result = results.get(0);
        assertTrue(result.getUuid().equals(String.valueOf(entityUuid)));
        final List<StatSnapshotApiDTO> stats = result.getStats();
        assertTrue(stats.size() == 1);
        List<StatApiDTO> statistics = stats.get(0).getStatistics();
        assertTrue(statistics.size() == 1);

        //Check combined stat values
        Map<String, StatApiDTO> statNames = statistics.stream()
                .collect(Collectors.toMap(StatApiDTO::getName, Function.identity()));
        assertTrue(statNames.containsKey(historyCommodity));
        assertTrue(statNames.get(historyCommodity).getValue() == statValue);
    }

    /**
     * Test runRequestThroughCostComponent, cost and historical stats returned.
     *
     * @throws OperationFailedException exception thrown from unexpected behavior
     */
    @Test
    public void testRunRequestThroughCostComponentForCloudAndHistoryStats() throws OperationFailedException {
        final StatScopesApiInputDTO inputDto =
                createStatScopesApiInputDTOWithCostAndHistoryStatsSortedBy(costCommodity, historyCommodity);
        final EntityStatsPaginationRequest paginationRequest = new EntityStatsPaginationRequest(null,
                1, true, costCommodity);
        final PaginatedStatsGather paginatedStatsGatherSpy = getPaginatedStatsGatherSpy(
                inputDto, paginationRequest);

        final long entityUuid = 5L;
        final float statValue = 5;
        final String nextCursor = "you're next!";
        PaginationResponse paginationResponse = PaginationResponse.newBuilder().setNextCursor(nextCursor).build();
        PaginatedStats mockPaginatedStats = mock(PaginatedStats.class);

        doReturn(Collections.singleton(entityUuid)).when(paginatedStatsGatherSpy).getExpandedScope(any());

        //Cost Response
        doReturn(Collections.singletonList(costPriceResponse(costCommodity, entityUuid, statValue, snapShotDate))).when(costServiceMole).getCloudCostStats(any());

        //Pagination and sorting process
        doReturn(Collections.singletonList(entityUuid)).when(mockPaginatedStats).getNextPageIds();
        doReturn(paginationResponse).when(mockPaginatedStats).getPaginationResponse();
        doReturn(mockPaginatedStats).when(paginatedStatsGatherSpy).paginateAndSortCostResponse(any(), any());

        //History and MinimalEntity Responses
        doReturn(historyVCPUResponse(historyCommodity, entityUuid, statValue, snapShotDate)).when(statsHistoryServiceSpy).getEntityStats(any());
        doReturn(getMinimalEntityResponse(entityUuid)).when(paginatedStatsGatherSpy).getMinimalEntitiesForEntityList(any());

        //WHEN
        paginatedStatsGatherSpy.runRequestThroughCostComponent();

        //THEN
        EntityStatsPaginationResponse response =
                paginatedStatsGatherSpy.getEntityStatsPaginationResponse();
        assertNotNull(response);
        List<EntityStatsApiDTO> results = response.getRawResults();
        assertTrue(results.size() == 1);
        EntityStatsApiDTO result = results.get(0);
        assertTrue(result.getUuid().equals(String.valueOf(entityUuid)));
        final List<StatSnapshotApiDTO> stats = result.getStats();
        assertTrue(stats.size() == 1);
        List<StatApiDTO> statistics = stats.get(0).getStatistics();
        assertTrue(statistics.size() == 2);

        //Check combined stat values
        Map<String, StatApiDTO> statNames = statistics.stream()
                .collect(Collectors.toMap(StatApiDTO::getName, Function.identity()));
        assertTrue(statNames.containsKey(historyCommodity));
        assertTrue(statNames.get(historyCommodity).getValue() == statValue);
        assertTrue(statNames.containsKey(costCommodity));
        assertTrue(statNames.get(costCommodity).getValue() == statValue);
    }

    /**
     * Test runRequestThroughHistoricalComponent with no cost stats returned.
     *
     * @throws OperationFailedException exception thrown from unexpected behavior
     */
    @Test
    public void testRunRequestThroughHistoricalComponentNoCostStatsRequested() throws OperationFailedException {
        final StatScopesApiInputDTO inputDto =
                createStatScopesApiInputDTOWithCostAndHistoryStatsSortedBy(null, historyCommodity);
        final EntityStatsPaginationRequest paginationRequest = new EntityStatsPaginationRequest(null,
        1, true, historyCommodity);
        final PaginatedStatsGather paginatedStatsGatherSpy = getPaginatedStatsGatherSpy(
                inputDto, paginationRequest);

        final long entityUuid = 5L;
        final float statValue = 5;

        doReturn(historyVCPUResponse(historyCommodity, entityUuid, statValue, snapShotDate)).when(statsHistoryServiceSpy).getEntityStats(any());
        doReturn(getMinimalEntityResponse(entityUuid)).when(paginatedStatsGatherSpy).getMinimalEntitiesForEntityList(any());

        //WHEN
        paginatedStatsGatherSpy.runRequestThroughHistoricalComponent();

        //THEN
        EntityStatsPaginationResponse response =
                paginatedStatsGatherSpy.getEntityStatsPaginationResponse();
        assertNotNull(response);
        List<EntityStatsApiDTO> results = response.getRawResults();
        assertTrue(results.size() == 1);
        EntityStatsApiDTO result = results.get(0);
        assertTrue(result.getUuid().equals(String.valueOf(entityUuid)));
        final List<StatSnapshotApiDTO> stats = result.getStats();
        assertTrue(stats.size() == 1);
        List<StatApiDTO> statistics = stats.get(0).getStatistics();
        assertTrue(statistics.size() == 1);

        //Check combined stat values
        Map<String, StatApiDTO> statNames = statistics.stream()
                .collect(Collectors.toMap(StatApiDTO::getName, Function.identity()));
        assertTrue(statNames.containsKey(historyCommodity));
        assertTrue(statNames.get(historyCommodity).getValue() == statValue);
    }

    /**
     * Tests runRequestThroughHistoricalComponent with no cost or historical stats returned.
     *
     * @throws OperationFailedException exception thrown from unexpected behavior
     */
    @Test
    public void testRunRequestThroughHistoricalComponentNoCostStatsExist() throws OperationFailedException {
        final StatScopesApiInputDTO inputDto =
                createStatScopesApiInputDTOWithCostAndHistoryStatsSortedBy(costCommodity, historyCommodity);
        final EntityStatsPaginationRequest paginationRequest = new EntityStatsPaginationRequest(null,
                1, true, historyCommodity);
        final PaginatedStatsGather paginatedStatsGatherSpy = getPaginatedStatsGatherSpy(
                inputDto, paginationRequest);

        final long entityUuid = 5L;
        final float statValue = 5;

        doReturn(historyVCPUResponse(historyCommodity, entityUuid, statValue, snapShotDate)).when(statsHistoryServiceSpy).getEntityStats(any());
        doReturn(Collections.singletonList(GetCloudCostStatsResponse.getDefaultInstance())).when(costServiceMole).getCloudCostStats(any());
        doReturn(getMinimalEntityResponse(entityUuid)).when(paginatedStatsGatherSpy).getMinimalEntitiesForEntityList(any());

        //WHEN
        paginatedStatsGatherSpy.runRequestThroughHistoricalComponent();

        //THEN
        EntityStatsPaginationResponse response =
                paginatedStatsGatherSpy.getEntityStatsPaginationResponse();
        assertNotNull(response);
        List<EntityStatsApiDTO> results = response.getRawResults();
        assertTrue(results.size() == 1);
        EntityStatsApiDTO result = results.get(0);
        assertTrue(result.getUuid().equals(String.valueOf(entityUuid)));
        final List<StatSnapshotApiDTO> stats = result.getStats();
        assertTrue(stats.size() == 1);
        List<StatApiDTO> statistics = stats.get(0).getStatistics();
        assertTrue(statistics.size() == 1);

        //Check combined stat values
        Map<String, StatApiDTO> statNames = statistics.stream()
                .collect(Collectors.toMap(StatApiDTO::getName, Function.identity()));
        assertTrue(statNames.containsKey(historyCommodity));
        assertTrue(statNames.get(historyCommodity).getValue() == statValue);
    }

    /**
     * Tests runRequestThroughHistoricalComponent with cost and history stats present.
     *
     * @throws OperationFailedException exception thrown from unexpected behavior
     */
    @Test
    public void testRunRequestThroughHistoricalComponent() throws OperationFailedException {
        final StatScopesApiInputDTO inputDto =
                createStatScopesApiInputDTOWithCostAndHistoryStatsSortedBy(costCommodity, historyCommodity);
        final EntityStatsPaginationRequest paginationRequest = new EntityStatsPaginationRequest(null,
                1, true, historyCommodity);
        final PaginatedStatsGather paginatedStatsGatherSpy = getPaginatedStatsGatherSpy(
                inputDto, paginationRequest);

        final long entityUuid = 5L;
        final float statValue = 5;

        doReturn(historyVCPUResponse(historyCommodity, entityUuid, statValue, snapShotDate)).when(statsHistoryServiceSpy).getEntityStats(any());
        doReturn(Collections.singletonList(costPriceResponse(costCommodity, entityUuid, statValue, snapShotDate))).when(costServiceMole).getCloudCostStats(any());
        doReturn(getMinimalEntityResponse(entityUuid)).when(paginatedStatsGatherSpy).getMinimalEntitiesForEntityList(any());

        //WHEN
        paginatedStatsGatherSpy.runRequestThroughHistoricalComponent();

        //THEN
        EntityStatsPaginationResponse response =
                paginatedStatsGatherSpy.getEntityStatsPaginationResponse();
        assertNotNull(response);
        List<EntityStatsApiDTO> results = response.getRawResults();
        assertTrue(results.size() == 1);
        EntityStatsApiDTO result = results.get(0);
        assertTrue(result.getUuid().equals(String.valueOf(entityUuid)));
        final List<StatSnapshotApiDTO> stats = result.getStats();
        assertTrue(stats.size() == 1);
        List<StatApiDTO> statistics = stats.get(0).getStatistics();
        assertTrue(statistics.size() == 2);

        //Check combined stat values
        Map<String, StatApiDTO> statNames = statistics.stream()
                .collect(Collectors.toMap(StatApiDTO::getName, Function.identity()));
        assertTrue(statNames.containsKey(historyCommodity));
        assertTrue(statNames.get(historyCommodity).getValue() == statValue);
        assertTrue(statNames.containsKey(costCommodity));
        assertTrue(statNames.get(costCommodity).getValue() == statValue);
    }


    /**
     * Creates a {@link StatApiInputDTO} with name.
     *
     * @param name used to set {@link StatApiInputDTO}
     * @return {@link StatApiInputDTO}
     */
    public StatApiInputDTO getStatApiInputDTOForName(String name) {
        StatApiInputDTO statApiInputDTO = new StatApiInputDTO();
        statApiInputDTO.setName(name);
        return statApiInputDTO;
    }

    /**
     * Test constructEntityStatsApiDTOFromResults maintains expected sort order of results.
     */
    @Test
    public void testConstructEntityStatsApiDTOFromResultsMaintainsOrder() {
        //GIVEN
        final List<Long> sortedNextPageEntityIds = Lists.newLinkedList();
        sortedNextPageEntityIds.add(1L);
        sortedNextPageEntityIds.add(2L);
        sortedNextPageEntityIds.add(3L);

        final Map<Long, MinimalEntity> minimalEntityMap = new HashMap<>();
        minimalEntityMap.put(2L, buildMinimalEntity(2L, ApiEntityType.VIRTUAL_MACHINE));
        minimalEntityMap.put(1L, buildMinimalEntity(1L, ApiEntityType.VIRTUAL_MACHINE));
        minimalEntityMap.put(3L, buildMinimalEntity(3L, ApiEntityType.VIRTUAL_MACHINE));

        final PaginatedStatsGather paginatedStatsGatherSpy = getPaginatedStatsGatherSpy(
                getInputDtoWithHistoricPeriod(), mock(EntityStatsPaginationRequest.class));

        //THEN
        List<EntityStatsApiDTO> entityStatsApiDTOS =
                paginatedStatsGatherSpy.constructEntityStatsApiDTOFromResults(
                        sortedNextPageEntityIds, minimalEntityMap,
                        Collections.emptyList(),
                        GetEntityStatsResponse.getDefaultInstance());

        //WHEN
        assertTrue(entityStatsApiDTOS.size() == 3);
        assertTrue(entityStatsApiDTOS.get(0).getUuid().equals("1"));
        assertTrue(entityStatsApiDTOS.get(1).getUuid().equals("2"));
        assertTrue(entityStatsApiDTOS.get(2).getUuid().equals("3"));
    }

    /**
     * Builds a {@link MinimalEntity} with uuids and entityType.
     *
     * @param uuid the entityUuid to use
     * @param entityType the {@link ApiEntityType} to set
     * @return throws OperationFailedException constructed from param data
     */
    public MinimalEntity buildMinimalEntity(Long uuid, ApiEntityType entityType) {
        return MinimalEntity.newBuilder()
                .setOid(uuid)
                .setEntityType(entityType.typeNumber())
                .build();
    }

    /**
     * Test constructEntityStatsApiDTOFromResults combines all gathered response.
     *
     * <p>Will combine {@link MinimalEntity}, cost {@link StatRecord}, history {@link EntityStats}
     *     into 1 {@link EntityStatsApiDTO} response</p>
     */
    @Test
    public void testConstructEntityStatsApiDTOFromResultsAddsCostAndHistoryStats() {
        //GIVEN
        final Long entityUuid = 5L;
        final float statValue = 5;

        final MinimalEntity minimalEntity =
                buildMinimalEntity(entityUuid, ApiEntityType.VIRTUAL_MACHINE);
        final Map<Long, MinimalEntity> minimalEntityMap = Maps.newHashMap();
        minimalEntityMap.put(entityUuid, minimalEntity);


        final PaginatedStatsGather paginatedStatsGatherSpy = getPaginatedStatsGatherSpy(
                getInputDtoWithHistoricPeriod(), mock(EntityStatsPaginationRequest.class));

        final List<Long> sortedNextPageEntityIds = Collections.singletonList(entityUuid);

        //WHEN
        List<EntityStatsApiDTO> entityStatsApiDTOS =
                paginatedStatsGatherSpy.constructEntityStatsApiDTOFromResults(
                        sortedNextPageEntityIds, minimalEntityMap,
                        costPriceRecords(costCommodity, entityUuid, statValue, snapShotDate),
                        historyVCPUResponse(historyCommodity, entityUuid, statValue, snapShotDate));
        //THEN
        assertTrue(entityStatsApiDTOS.size() == 1);
        EntityStatsApiDTO entityStatsApiDTO = entityStatsApiDTOS.get(0);
        assertTrue(entityStatsApiDTO.getStats().size() == 1);
        StatSnapshotApiDTO statSnapshotApiDTO = entityStatsApiDTO.getStats().get(0);
        assertTrue(statSnapshotApiDTO.getDate() != null);
        assertTrue(statSnapshotApiDTO.getStatistics().size() == 2);
    }

    /**
     * If the entityUuid passed in constructEntityStatsApiDTOFromMinimalEntity does not exist in
     * minimalEntityMap passed, Optional.empty() is expected to be returned.
     */
    @Test
    public void testConstructEntityStatsApiDTOFromNullMinimalEntity() {
        final Map<Long, MinimalEntity> minimalEntityMap = new HashMap<>();
        final PaginatedStatsGather paginatedStatsGatherSpy = getPaginatedStatsGatherSpy(
                getInputDtoWithHistoricPeriod(), mock(EntityStatsPaginationRequest.class));
        Optional<EntityStatsApiDTO> entityStatsApiDTO =
                paginatedStatsGatherSpy.constructEntityStatsApiDTOFromMinimalEntity(
                        7L, minimalEntityMap);
        assertEquals(Optional.empty(), entityStatsApiDTO);
    }

    /**
     * Test that we expand scope (in our case group of VMs) before supplyChain traverse. Because
     * if we don't request certain related entity types we return previously expanded scope
     * entities without supplyChain traverse.
     *
     * @throws OperationFailedException exception thrown if the scope can not be recognized
     */
    @Test
    public void testGetExpandedScopeForGroupOfVMs() throws OperationFailedException {
        final String groupVmsId = "123";
        final List<String> scopeSeedIds = Collections.singletonList(groupVmsId);
        final StatScopesApiInputDTO inputDto = new StatScopesApiInputDTO();
        inputDto.setScopes(scopeSeedIds);
        final EntityStatsPaginationRequest paginationRequest =
                new EntityStatsPaginationRequest(null, 1, true, historyCommodity);
        final PaginatedStatsGather paginatedStatsGatherSpy =
                getPaginatedStatsGatherSpy(inputDto, paginationRequest);

        final ApiId mockApiId = mock(ApiId.class);
        when(mockApiId.isResourceGroupOrGroupOfResourceGroups()).thenReturn(false);
        when(mockUuidMapper.fromOid(Long.parseLong(groupVmsId))).thenReturn(mockApiId);

        paginatedStatsGatherSpy.getExpandedScope(inputDto);
        Mockito.verify(mockGroupExpander, Mockito.times(1))
                .expandUuids(Collections.singleton(groupVmsId));
    }

    /**
     * Test that we shouldn't expand resource group or group of resource groups scope before
     * supplyChain traverse otherwise we miss special resource group logic.
     *
     * @throws OperationFailedException exception thrown if the scope can not be recognized
     */
    @Test
    public void testGetExpandedScopeForResourceGroup() throws OperationFailedException {
        final String resourceGroupId = "123";
        final List<String> scopeSeedIds = Collections.singletonList(resourceGroupId);
        final StatScopesApiInputDTO inputDto =
                createStatsScopeApiInputDTOForResourceGroupScope(scopeSeedIds);
        final EntityStatsPaginationRequest paginationRequest =
                new EntityStatsPaginationRequest(null, 1, true, historyCommodity);
        final PaginatedStatsGather paginatedStatsGatherSpy =
                getPaginatedStatsGatherSpy(inputDto, paginationRequest);

        final ApiId mockApiId = mock(ApiId.class);
        when(mockApiId.isResourceGroupOrGroupOfResourceGroups()).thenReturn(true);
        when(mockUuidMapper.fromOid(Long.parseLong(resourceGroupId))).thenReturn(mockApiId);

        paginatedStatsGatherSpy.getExpandedScope(inputDto);
        Mockito.verify(mockGroupExpander, Mockito.never())
                .expandUuids(Collections.singleton(resourceGroupId));
    }

    private StatScopesApiInputDTO createStatsScopeApiInputDTOForResourceGroupScope(
            List<String> resourceGroupId) {
        final StatScopesApiInputDTO inputDto = new StatScopesApiInputDTO();
        inputDto.setScopes(resourceGroupId);
        inputDto.setRelatedType(ApiEntityType.VIRTUAL_MACHINE.apiStr());
        return inputDto;
    }

    /**
     * Creates a {@link StatScopesApiInputDTO} requesting costCommodity and historyCommodity.
     *
     * @param costCommodity the costCommodity to add to request
     * @param historyCommodity the historyCommodity to add to request
     * @return {@link StatScopesApiInputDTO} configured to request cost/history commodities
     */
    public StatScopesApiInputDTO createStatScopesApiInputDTOWithCostAndHistoryStatsSortedBy(
            @Nullable String costCommodity, @Nullable String historyCommodity) {
        List<StatApiInputDTO> statApiInputDTOS = Lists.newLinkedList();
        if (costCommodity != null) {
            statApiInputDTOS.add(getStatApiInputDTOForName(costCommodity));
        }
        if (historyCommodity != null) {
            statApiInputDTOS.add(getStatApiInputDTOForName(historyCommodity));
        }
        StatPeriodApiInputDTO statPeriodApiInputDTO = new StatPeriodApiInputDTO();
        statPeriodApiInputDTO.setStatistics(statApiInputDTOS);

        final StatScopesApiInputDTO inputDto = new StatScopesApiInputDTO();
        inputDto.setScopes(Collections.singletonList("Market"));
        inputDto.setPeriod(statPeriodApiInputDTO);
        inputDto.setRelatedType(ApiEntityType.VIRTUAL_MACHINE.apiStr());

        return inputDto;
    }

    /**
     * Returns a {@link CloudCostStatRecord.StatRecord.StatValue} using value on all fields.
     *
     * @param value the number set to all fields in {@link CloudCostStatRecord.StatRecord.StatValue}
     * @return {@link CloudCostStatRecord.StatRecord.StatValue} configured with value passed
     */
    public CloudCostStatRecord.StatRecord.StatValue getCostStatValue(float value) {
        return CloudCostStatRecord.StatRecord.StatValue.newBuilder()
                .setTotal(value)
                .setAvg(value)
                .setMax(value)
                .setMin(value)
                .build();
    }

    /**
     * Returns a {@link StatSnapshot.StatRecord.StatValue} using value on all fields.
     *
     * @param value the number set to all fields in {@link StatSnapshot.StatRecord.StatValue}
     * @return {@link StatSnapshot.StatRecord.StatValue} configured with value passed
     */
    public StatSnapshot.StatRecord.StatValue getHistoryStatValue(float value) {
        return StatSnapshot.StatRecord.StatValue.newBuilder()
                .setTotal(value)
                .setAvg(value)
                .setMax(value)
                .setMin(value)
                .build();
    }

    /**
     * Returns {@link GetEntityStatsResponse} response configured commodity.
     *
     * @param commodity     the commodity stat name to configure
     * @param entityUuid    the entity uuid to configure response with
     * @param statValue     the number set to all {@link StatSnapshot.StatRecord.StatValue} fields
     * @param snapShotDate  the snapShotDate statRecords will belong to
     * @return {@link GetEntityStatsResponse} response with statValue set for entityUuid
     */
    public GetEntityStatsResponse historyVCPUResponse(String commodity, long entityUuid, float statValue, long snapShotDate) {

        StatSnapshot.StatRecord statRecord = StatSnapshot.StatRecord.newBuilder()
                .setName(commodity)
                .setUsed(getHistoryStatValue(statValue))
                .build();
        StatSnapshot statSnapshot = StatSnapshot.newBuilder()
                .addStatRecords(statRecord)
                .setSnapshotDate(snapShotDate)
                .build();

        EntityStats entityStats = EntityStats.newBuilder()
                .setOid(entityUuid)
                .addStatSnapshots(statSnapshot)
                .build();

        return GetEntityStatsResponse.newBuilder().addEntityStats(entityStats).build();
    }

    /**
     * Returns {@link GetCloudCostStatsResponse} response with commodity stat configured.
     *
     * @param commodity     the commodity stat name to configure
     * @param entityUuid    the entity uuid to configure response with
     * @param statValue     the number set to all {@link CloudCostStatRecord.StatRecord.StatValue} fields
     * @param snapShotDate  the snapShotDate statRecords will belong to
     * @return {@link GetCloudCostStatsResponse} response with statValue set for entityUuid
     */
    public GetCloudCostStatsResponse costPriceResponse(String commodity, long entityUuid, float statValue, long snapShotDate) {
        return  GetCloudCostStatsResponse.newBuilder()
            .addAllCloudStatRecord(costPriceRecords(commodity, entityUuid, statValue, snapShotDate))
            .build();
    }

    public List<CloudCostStatRecord> costPriceRecords(String commodity, long entityUuid, float statValue, long snapShotDate) {
        CloudCostStatRecord.StatRecord statRecord = CloudCostStatRecord.StatRecord.newBuilder()
            .setAssociatedEntityId(entityUuid)
            .setValues(getCostStatValue(statValue))
            .setName(commodity)
            .build();

        CloudCostStatRecord cloudCostStatRecord =
            CloudCostStatRecord.newBuilder()
                .addStatRecords(statRecord)
                .setSnapshotDate(snapShotDate)
                .build();
        return Collections.singletonList(cloudCostStatRecord);
    }

    /**
     * Creates a {@link HashMap} of entityUuid to {@link MinimalEntity}.
     *
     * @param entityUuid the uuid to to configure {@link MinimalEntity} and map key
     * @return Map entityUuid to {@link MinimalEntity}
     */
    public Map<Long, MinimalEntity> getMinimalEntityResponse(Long entityUuid) {
        MinimalEntity minimalEntity = MinimalEntity.newBuilder().setOid(entityUuid)
                .setDisplayName("minimalEntity")
                .setEntityType(ApiEntityType.VIRTUAL_MACHINE.typeNumber())
                .setEnvironmentType(EnvironmentType.CLOUD)
                .build();
        Map<Long, MinimalEntity> map = new HashMap<>();
        map.put(entityUuid, minimalEntity);
        return map;
    }
}



