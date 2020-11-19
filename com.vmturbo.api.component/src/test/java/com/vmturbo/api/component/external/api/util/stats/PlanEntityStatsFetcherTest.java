package com.vmturbo.api.component.external.api.util.stats;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.util.Collections;
import java.util.List;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.collections4.CollectionUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import com.vmturbo.api.component.external.api.mapper.ServiceEntityMapper;
import com.vmturbo.api.component.external.api.mapper.StatsMapper;
import com.vmturbo.api.dto.statistic.EntityStatsApiDTO;
import com.vmturbo.api.dto.statistic.StatApiInputDTO;
import com.vmturbo.api.dto.statistic.StatFilterApiDTO;
import com.vmturbo.api.dto.statistic.StatPeriodApiInputDTO;
import com.vmturbo.api.dto.statistic.StatScopesApiInputDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.enums.Epoch;
import com.vmturbo.api.pagination.EntityStatsPaginationRequest;
import com.vmturbo.api.pagination.EntityStatsPaginationRequest.EntityStatsPaginationResponse;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatRecord;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatRecord.StatRecord.StatValue;
import com.vmturbo.common.protobuf.cost.Cost.CostCategory;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance.Builder;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.PlanCombinedStatsRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.PlanCombinedStatsResponse;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.PlanEntityAndCombinedStats;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.PlanEntityAndCombinedStatsChunk;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.PlanEntityStats;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.PlanEntityStatsChunk;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.PlanTopologyStatsRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.PlanTopologyStatsResponse;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.TopologyType;
import com.vmturbo.common.protobuf.repository.RepositoryDTOMoles.RepositoryServiceMole;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc;
import com.vmturbo.common.protobuf.stats.Stats.EntityStats;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot;
import com.vmturbo.common.protobuf.stats.Stats.StatEpoch;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity;
import com.vmturbo.components.api.test.GrpcTestServer;

/**
 * Tests for {@link PlanEntityStatsFetcher}.
 */
public class PlanEntityStatsFetcherTest {

    /**
     * The class under test.
     */
    private PlanEntityStatsFetcher planEntityStatsFetcher;

    /**
     * A mock of the statsMapper dependency.
     */
    private StatsMapper statsMapper = Mockito.mock(StatsMapper.class);

    /**
     * A mock of the serviceEntityMapper dependency.
     */
    private ServiceEntityMapper serviceEntityMapper = Mockito.mock(ServiceEntityMapper.class);

    /**
     * A spy of the repositoryService dependency, connected to a gRPC test server.
     */
    private RepositoryServiceMole repositoryServiceSpy = spy(new RepositoryServiceMole());

    /**
     * A gRPC test server that fakes the implementation of the actual gRPC interface.
     */
    @Rule
    public GrpcTestServer testServer = GrpcTestServer.newServer(repositoryServiceSpy);

    /**
     * Allow certain unit tests to declare expected exceptions.
     */
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    /**
     * Setup the gRPC test server and add it as a dependency to planEntityStatsFetcher, which is
     * the class under test.
     */
    @Before
    public void setup() {
        final RepositoryServiceGrpc.RepositoryServiceBlockingStub repositoryRpcService =
            RepositoryServiceGrpc.newBlockingStub(testServer.getChannel());
        planEntityStatsFetcher =
            new PlanEntityStatsFetcher(statsMapper, serviceEntityMapper, repositoryRpcService);
    }

    /**
     * Test the case where plan source and projected combined stats are requested.
     */
    @Test
    public void testGetCombinedStats() {
        // Prepare
        final long planOid = 999L;
        final long planStartTime = 1001L;
        final long sourceTopologyId = 63L;
        final long projectedTopologyId = 64L;
        final PlanInstance planInstance = planInstanceBuilder(planOid, planStartTime)
            .setSourceTopologyId(sourceTopologyId)
            .setProjectedTopologyId(projectedTopologyId)
            .build();

        final StatScopesApiInputDTO inputDto = new StatScopesApiInputDTO();
        final StatPeriodApiInputDTO period = new StatPeriodApiInputDTO();

        // To request both source and projected stats, the start date must be equal to (or less than)
        // the plan start date. The end date must be greater than the plan start date.
        final String startDate = Long.toString(planStartTime);
        final String futureEndDate = Long.toString(planStartTime + 1000);
        final String costPrice = "costPrice";
        period.setStartDate(startDate);
        period.setEndDate(futureEndDate);
        StatApiInputDTO statApiInputDTO = new StatApiInputDTO();
        statApiInputDTO.setName(costPrice);
        final StatFilterApiDTO filter = new StatFilterApiDTO();
        filter.setType("costComponent");
        filter.setValue("Storage");
        statApiInputDTO.setFilters(Collections.singletonList(filter));
        period.setStatistics(Collections.singletonList(statApiInputDTO));
        inputDto.setPeriod(period);

        final EntityStatsPaginationRequest paginationRequest =
            new EntityStatsPaginationRequest(null, null, false, null);

        // Mock the response from statsMapper, an intermediate step in making the call to the
        // Repository. The details of the request don't matter here, since they are only examined
        // by the repositoryRpcService dependency (which is also mocked).
        PlanCombinedStatsRequest planCombinedStatsRequest = PlanCombinedStatsRequest.newBuilder()
            .build();
        when(statsMapper
            .toPlanCombinedStatsRequest(planOid, TopologyType.PROJECTED, inputDto, paginationRequest))
            .thenReturn(planCombinedStatsRequest);

        final long statSnapshotDate = 1L;
        // Mock the response from repositoryRpcServices
        final StatSnapshot sourceStatSnapshot = getStatSnapshot(StatEpoch.PLAN_SOURCE, statSnapshotDate);
        final StatSnapshot projectedStatSnapshot = getStatSnapshot(StatEpoch.PLAN_PROJECTED, statSnapshotDate);
        final long cloudEntityOid = 7L;
        final PlanEntityAndCombinedStats sourceOnlyEntityStats =
            getPlanEntityCombinedStats(cloudEntityOid, sourceStatSnapshot, null, true);
        final PlanEntityAndCombinedStats projectedOnlyEntityStats =
            getPlanEntityCombinedStats(8L, null, projectedStatSnapshot);
        final PlanEntityAndCombinedStats combinedEntityStats =
            getPlanEntityCombinedStats(9L, sourceStatSnapshot, projectedStatSnapshot);
        when(repositoryServiceSpy.getPlanCombinedStats(planCombinedStatsRequest))
            .thenReturn(Collections.singletonList(PlanCombinedStatsResponse.newBuilder()
                .setEntityCombinedStatsWrapper(PlanEntityAndCombinedStatsChunk.newBuilder()
                    .addEntityAndCombinedStats(sourceOnlyEntityStats)
                    .addEntityAndCombinedStats(projectedOnlyEntityStats)
                    .addEntityAndCombinedStats(combinedEntityStats))
                .build()));
        // Mock a second call to statsMapper, which is used to process the returned stats
        final StatSnapshotApiDTO sourceStatSnapshotApiDTO = new StatSnapshotApiDTO();
        sourceStatSnapshotApiDTO.setEpoch(Epoch.PLAN_SOURCE);
        sourceStatSnapshotApiDTO.setDate(DateTimeUtil.toString(statSnapshotDate));
        final StatSnapshotApiDTO projectedStatSnapshotApiDTO = new StatSnapshotApiDTO();
        projectedStatSnapshotApiDTO.setEpoch(Epoch.PLAN_PROJECTED);
        projectedStatSnapshotApiDTO.setDate(DateTimeUtil.toString(statSnapshotDate + 1));
        when(statsMapper.toStatSnapshotApiDTO(sourceStatSnapshot)).thenReturn(sourceStatSnapshotApiDTO);
        when(statsMapper.toStatSnapshotApiDTO(projectedStatSnapshot)).thenReturn(projectedStatSnapshotApiDTO);
        final CloudCostStatRecord.StatRecord storageStatRecord = CloudCostStatRecord.StatRecord.newBuilder()
                .setName(costPrice)
                .setCategory(CostCategory.STORAGE)
                .setValues(StatValue.newBuilder()
                        .setAvg((float).15).build())
                .build();
        when(serviceEntityMapper.getCloudCostStatRecords(
                Collections.singletonList(cloudEntityOid),
                planOid,
                statApiInputDTO.getFilters(),
                Collections.emptyList())).thenReturn(Collections.singletonList(
                        CloudCostStatRecord.newBuilder()
                                .setSnapshotDate(statSnapshotDate)
                                .addAllStatRecords(
                                        Collections.singletonList(storageStatRecord))
                                .build()));

        // Act
        final EntityStatsPaginationResponse response =
            planEntityStatsFetcher.getPlanEntityStats(planInstance, inputDto, paginationRequest);

        // Verify
        verify(repositoryServiceSpy).getPlanCombinedStats(any(), any());
        verify(serviceEntityMapper).getCloudCostStatRecords(any(), any(), any(), any());
        final java.util.Optional<StatSnapshotApiDTO> cloudEntityStatistics = response.getRawResults().stream()
                .filter(entityStatsApiDTO -> Long.valueOf(entityStatsApiDTO.getUuid()) == cloudEntityOid)
                .flatMap(entityStatsApiDTO -> entityStatsApiDTO.getStats().stream())
                .filter(statSnapshotApiDTO -> CollectionUtils.isNotEmpty(statSnapshotApiDTO.getStatistics()))
                .findFirst();
        Assert.assertTrue(cloudEntityStatistics.isPresent());
        Assert.assertTrue(
                cloudEntityStatistics.get().getStatistics().get(0).getValue()
                        == storageStatRecord.getValues().getAvg());
        final List<EntityStatsApiDTO> rawResults = response.getRawResults();
        Assert.assertEquals(3, rawResults.size());
        final long countWithSource = getCountWithEpoch(rawResults, Epoch.PLAN_SOURCE);
        Assert.assertEquals(2, countWithSource);
        final long countWithProjected = getCountWithEpoch(rawResults, Epoch.PLAN_PROJECTED);
        Assert.assertEquals(2, countWithProjected);
        final long countWithCombined = rawResults.stream()
            .map(EntityStatsApiDTO::getStats)
            .filter(statSnapshotApiDTOS -> statSnapshotApiDTOS.stream()
                .map(StatSnapshotApiDTO::getEpoch)
                .anyMatch(epoch -> Epoch.PLAN_SOURCE == epoch)
                && statSnapshotApiDTOS.stream()
                .map(StatSnapshotApiDTO::getEpoch)
                .anyMatch(epoch -> Epoch.PLAN_PROJECTED == epoch))
            .count();
        Assert.assertEquals(1, countWithCombined);
    }

    private long getCountWithEpoch(final List<EntityStatsApiDTO> rawResults, final Epoch planSource) {
        return rawResults.stream()
            .map(EntityStatsApiDTO::getStats)
            .filter(statSnapshotApiDTOS -> statSnapshotApiDTOS.stream()
                .map(StatSnapshotApiDTO::getEpoch)
                .anyMatch(epoch -> planSource == epoch))
            .count();
    }

    /**
     * Test the case where plan source stats are requested.
     */
    @Test
    public void testGetSourceStats() {
        // Prepare
        final long planOid = 999L;
        final long planStartTime = 1001L;
        final long sourceTopologyId = 63L;
        final PlanInstance planInstance = planInstanceBuilder(planOid, planStartTime)
            .setSourceTopologyId(sourceTopologyId)
            .build();

        final StatScopesApiInputDTO inputDto = new StatScopesApiInputDTO();
        final StatPeriodApiInputDTO period = new StatPeriodApiInputDTO();

        // To request source stats, the start date must be equal to (or less than) the plan start date.
        // To avoid requesting projected stats, the end date should be the same.
        final String planStartDate = Long.toString(planStartTime);
        period.setStartDate(planStartDate);
        period.setEndDate(planStartDate);
        inputDto.setPeriod(period);

        final EntityStatsPaginationRequest paginationRequest =
            new EntityStatsPaginationRequest(null, null, false, null);

        // Mock the response from statsMapper, an intermediate step in making the call to the
        // Repository. The details of the request don't matter here, since they are only examined
        // by the repositoryRpcService dependency (which is also mocked).
        PlanTopologyStatsRequest planTopologyStatsRequest = PlanTopologyStatsRequest.newBuilder()
            .build();
        when(statsMapper.toPlanTopologyStatsRequest(sourceTopologyId, inputDto, paginationRequest))
            .thenReturn(planTopologyStatsRequest);

        // Mock the response from repositoryRpcServices
        final StatSnapshot statSnapshot = getStatSnapshot(StatEpoch.PLAN_SOURCE);
        final PlanEntityStats returnedEntityAndStats = getPlanEntityStats(7L, statSnapshot);
        when(repositoryServiceSpy.getPlanTopologyStats(planTopologyStatsRequest))
            .thenReturn(Collections.singletonList(PlanTopologyStatsResponse.newBuilder()
                .setEntityStatsWrapper(PlanEntityStatsChunk.newBuilder()
                    .addEntityStats(returnedEntityAndStats))
                .build()));

        // Mock a second call to statsMapper, which is used to process the returned stats
        final StatSnapshotApiDTO statSnapshotApiDTO = new StatSnapshotApiDTO();
        statSnapshotApiDTO.setEpoch(Epoch.PLAN_SOURCE);
        when(statsMapper.toStatSnapshotApiDTO(statSnapshot)).thenReturn(statSnapshotApiDTO);

        // Act
        final EntityStatsPaginationResponse response =
            planEntityStatsFetcher.getPlanEntityStats(planInstance, inputDto, paginationRequest);

        // Verify
        verify(repositoryServiceSpy).getPlanTopologyStats(any(), any());
        final List<EntityStatsApiDTO> rawResults = response.getRawResults();
        Assert.assertEquals(1, rawResults.size());
        final EntityStatsApiDTO entityStatsApiDTO = rawResults.get(0);
        Assert.assertEquals("foo", entityStatsApiDTO.getDisplayName());
        final List<StatSnapshotApiDTO> stats = entityStatsApiDTO.getStats();
        Assert.assertEquals(1, stats.size());
        final StatSnapshotApiDTO returnedStatSnapshotApiDTO = stats.get(0);
        Assert.assertEquals(statSnapshotApiDTO, returnedStatSnapshotApiDTO);
        Assert.assertEquals(Epoch.PLAN_SOURCE, returnedStatSnapshotApiDTO.getEpoch());
    }

    /**
     * Test the case where plan projected stats are requested.
     */
    @Test
    public void testGetProjectedStats() {
        // Prepare
        final long planOid = 999L;
        final long planStartTime = 1001L;
        final long sourceTopologyId = 63L;
        final long projectedTopologyId = 64L;
        final PlanInstance planInstance = planInstanceBuilder(planOid, planStartTime)
            .setSourceTopologyId(sourceTopologyId)
            .setProjectedTopologyId(projectedTopologyId)
            .build();

        final StatScopesApiInputDTO inputDto = new StatScopesApiInputDTO();
        final StatPeriodApiInputDTO period = new StatPeriodApiInputDTO();

        // To request (only) projected stats, the start date must be greater than the plan start date.
        final String futureStartDate = Long.toString(planStartTime + 1000);
        period.setStartDate(futureStartDate);
        period.setEndDate(futureStartDate);
        inputDto.setPeriod(period);

        final EntityStatsPaginationRequest paginationRequest =
            new EntityStatsPaginationRequest(null, null, false, null);

        // Mock the response from statsMapper, an intermediate step in making the call to the
        // Repository. The details of the request don't matter here, since they are only examined
        // by the repositoryRpcService dependency (which is also mocked).
        PlanTopologyStatsRequest planTopologyStatsRequest = PlanTopologyStatsRequest.newBuilder()
            .build();
        when(statsMapper.toPlanTopologyStatsRequest(sourceTopologyId, inputDto, paginationRequest))
            .thenReturn(planTopologyStatsRequest);

        // Mock the response from repositoryRpcServices
        final StatSnapshot statSnapshot = getStatSnapshot(StatEpoch.PLAN_PROJECTED);
        final PlanEntityStats returnedEntityAndStats = getPlanEntityStats(7L, statSnapshot);
        when(repositoryServiceSpy.getPlanTopologyStats(planTopologyStatsRequest))
            .thenReturn(Collections.singletonList(PlanTopologyStatsResponse.newBuilder()
                .setEntityStatsWrapper(PlanEntityStatsChunk.newBuilder()
                    .addEntityStats(returnedEntityAndStats))
                .build()));

        // Mock a second call to statsMapper, which is used to process the returned stats
        final StatSnapshotApiDTO statSnapshotApiDTO = new StatSnapshotApiDTO();
        statSnapshotApiDTO.setEpoch(Epoch.PLAN_PROJECTED);
        when(statsMapper.toStatSnapshotApiDTO(statSnapshot)).thenReturn(statSnapshotApiDTO);

        // Act
        final EntityStatsPaginationResponse response =
            planEntityStatsFetcher.getPlanEntityStats(planInstance, inputDto, paginationRequest);

        // Verify
        verify(repositoryServiceSpy).getPlanTopologyStats(any(), any());
        final List<EntityStatsApiDTO> rawResults = response.getRawResults();
        Assert.assertEquals(1, rawResults.size());
        final EntityStatsApiDTO entityStatsApiDTO = rawResults.get(0);
        Assert.assertEquals("foo", entityStatsApiDTO.getDisplayName());
        final List<StatSnapshotApiDTO> stats = entityStatsApiDTO.getStats();
        Assert.assertEquals(1, stats.size());
        final StatSnapshotApiDTO returnedStatSnapshotApiDTO = stats.get(0);
        Assert.assertEquals(statSnapshotApiDTO, returnedStatSnapshotApiDTO);
        Assert.assertEquals(Epoch.PLAN_PROJECTED, returnedStatSnapshotApiDTO.getEpoch());
    }

    /**
     * Test the case where a user does not provide a requested time range, which means we can't
     * determine whether to return source or projected data. An IllegalArgumentException is expected.
     */
    @Test
    public void testMissingStatPeriod() {
        // Prepare
        final long planOid = 999L;
        final long planStartTime = 1001L;
        final PlanInstance planInstance = planInstanceBuilder(planOid, planStartTime).build();

        final StatScopesApiInputDTO inputDto = new StatScopesApiInputDTO();

        final EntityStatsPaginationRequest paginationRequest =
            new EntityStatsPaginationRequest(null, null, false, null);

        // Expect an IllegalArgumentException, since the StatPeriod is missing from the request
        expectedException.expect(IllegalArgumentException.class);
        // Act
        final EntityStatsPaginationResponse response =
            planEntityStatsFetcher.getPlanEntityStats(planInstance, inputDto, paginationRequest);

        // Verify
        Assert.fail("An IllegalArgumentException should have been thrown, but was not.");
    }

    /**
     * Test the case where a user provides in invalid time range, which doesn't make sense
     * relative to the plan run time. An IllegalArgumentException is expected.
     */
    @Test
    public void testInvalidTimeRange() {
        // Prepare
        final long planOid = 999L;
        final long planStartTime = 1001L;
        final PlanInstance planInstance = planInstanceBuilder(planOid, planStartTime).build();

        final StatScopesApiInputDTO inputDto = new StatScopesApiInputDTO();
        final StatPeriodApiInputDTO period = new StatPeriodApiInputDTO();
        // Set start/end dates that don't make sense
        // The start date is after the planStartTime
        // The end date is before the planStartTime
        period.setStartDate("2000");
        period.setEndDate("0");
        inputDto.setPeriod(period);

        final EntityStatsPaginationRequest paginationRequest =
            new EntityStatsPaginationRequest(null, null, false, null);

        // Expect an IllegalArgumentException, since the startDate and endDate don't make sense
        expectedException.expect(IllegalArgumentException.class);
        // Act
        final EntityStatsPaginationResponse response =
            planEntityStatsFetcher.getPlanEntityStats(planInstance, inputDto, paginationRequest);

        // Verify
        Assert.fail("An IllegalArgumentException should have been thrown, but was not.");
    }

    private Builder planInstanceBuilder(long planOid, long startTime) {
        return PlanInstance.newBuilder()
            .setPlanId(planOid)
            .setStartTime(startTime)
            .setStatus(PlanInstance.PlanStatus.SUCCEEDED);
    }

    private StatSnapshot getStatSnapshot(final StatEpoch planProjected, final long statSnapshotDate) {
        return StatSnapshot.newBuilder()
                .setStatEpoch(planProjected)
                .setSnapshotDate(statSnapshotDate)
                // Omitting the stat details because they will be handed off to StatsMapper
                // anyway--which is a mock.
                .addStatRecords(StatRecord.newBuilder())
                .build();
    }

    private StatSnapshot getStatSnapshot(final StatEpoch planProjected) {
        return getStatSnapshot(planProjected, Clock.systemUTC().millis());
    }

    private static ApiPartialEntity.Builder getApiPartialEntity(final long entityId) {
        return ApiPartialEntity.newBuilder()
                .setEntityType(10)
                .setDisplayName("foo")
                .setOid(entityId);
    }

    private PlanEntityAndCombinedStats getPlanEntityCombinedStats(
            @Nonnull final Long entityId,
            @Nullable final StatSnapshot sourceStatSnapshot,
            @Nullable final StatSnapshot projectedStatSnapshot,
            final boolean isCloudEntity) {
        final PlanEntityAndCombinedStats.Builder planCombinedStatsBuilder =
            PlanEntityAndCombinedStats.newBuilder();
        EntityStats.Builder entityStatsBuilder = EntityStats.newBuilder();
        final ApiPartialEntity.Builder apiPartialEntityBuilder = getApiPartialEntity(entityId);
        if (isCloudEntity) {
              apiPartialEntityBuilder.setEnvironmentType(EnvironmentType.CLOUD);
        }
        if (sourceStatSnapshot != null) {
            planCombinedStatsBuilder.setPlanSourceEntity(PartialEntity.newBuilder()
                .setApi(apiPartialEntityBuilder))
                .setPlanCombinedStats(entityStatsBuilder.addStatSnapshots(sourceStatSnapshot));
        }
        if (projectedStatSnapshot != null) {
            planCombinedStatsBuilder.setPlanProjectedEntity(PartialEntity.newBuilder()
                .setApi(apiPartialEntityBuilder))
                .setPlanCombinedStats(entityStatsBuilder.addStatSnapshots(projectedStatSnapshot));
        }
        return planCombinedStatsBuilder.build();
    }

    private PlanEntityAndCombinedStats getPlanEntityCombinedStats(@Nonnull final Long entityId,
            @Nullable final StatSnapshot sourceStatSnapshot,
            @Nullable final StatSnapshot projectedStatSnapshot) {
        return getPlanEntityCombinedStats(entityId, sourceStatSnapshot, projectedStatSnapshot, false);
    }

    private PlanEntityStats getPlanEntityStats(
            final long oid,
            final StatSnapshot statSnapshot) {
        return PlanEntityStats.newBuilder()
            .setPlanEntity(PartialEntity.newBuilder()
                .setApi(getApiPartialEntity(oid)))
        .setPlanEntityStats(EntityStats.newBuilder()
                .addStatSnapshots(statSnapshot))
        .build();
    }
}
