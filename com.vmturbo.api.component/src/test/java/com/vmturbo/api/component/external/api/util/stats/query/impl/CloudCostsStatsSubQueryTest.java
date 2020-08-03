package com.vmturbo.api.component.external.api.util.stats.query.impl;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.vmturbo.api.component.ApiTestUtils;
import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.communication.RepositoryApi.MultiEntityRequest;
import com.vmturbo.api.component.communication.RepositoryApi.SearchRequest;
import com.vmturbo.api.component.external.api.mapper.UuidMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.component.external.api.util.BuyRiScopeHandler;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory.SupplyChainNodeFetcherBuilder;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryContextFactory.StatsQueryContext;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryScopeExpander.GlobalScope;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryScopeExpander.StatsQueryScope;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.statistic.StatApiInputDTO;
import com.vmturbo.api.dto.statistic.StatFilterApiDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.enums.CloudType;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.cost.Cost;
import com.vmturbo.common.protobuf.cost.Cost.AccountFilter;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatRecord;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatRecord.StatRecord;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatRecord.StatRecord.StatValue;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatsQuery;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatsQuery.CostSourceFilter;
import com.vmturbo.common.protobuf.cost.Cost.CostCategory;
import com.vmturbo.common.protobuf.cost.Cost.CostCategoryFilter;
import com.vmturbo.common.protobuf.cost.Cost.CostSource;
import com.vmturbo.common.protobuf.cost.Cost.EntityFilter;
import com.vmturbo.common.protobuf.cost.Cost.EntityTypeFilter;
import com.vmturbo.common.protobuf.cost.Cost.GetCloudCostStatsRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetCloudCostStatsResponse;
import com.vmturbo.common.protobuf.cost.Cost.GetCloudExpenseStatsRequest;
import com.vmturbo.common.protobuf.cost.Cost.RegionFilter;
import com.vmturbo.common.protobuf.cost.CostMoles.CostServiceMole;
import com.vmturbo.common.protobuf.cost.CostServiceGrpc;
import com.vmturbo.common.protobuf.cost.CostServiceGrpc.CostServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.BusinessAccountInfo;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.topology.processor.api.util.ImmutableThinProbeInfo;
import com.vmturbo.topology.processor.api.util.ImmutableThinTargetInfo;
import com.vmturbo.topology.processor.api.util.ThinTargetCache;


public class CloudCostsStatsSubQueryTest {

    private CostServiceMole costServiceMole = spy(new CostServiceMole());

    @Rule
    public GrpcTestServer grpcTestServer = GrpcTestServer.newServer(costServiceMole);

    private CloudCostsStatsSubQuery query;

    @Mock
    private RepositoryApi repositoryApi;

    @Mock
    private SupplyChainFetcherFactory supplyChainFetcherFactory;

    @Mock
    private ThinTargetCache thinTargetCache;

    @Mock
    private StorageStatsSubQuery storageStatsSubQuery;

    @Mock
    private UserSessionContext userSessionContext;

    private static final long TARGET_ID_1 = 11L;
    private static final long TARGET_ID_2 = 12L;
    private static final String CSP_AZURE = CloudType.AZURE.name();

    private static final long ACCOUNT_ID_1 = 1L;
    private static final long ACCOUNT_ID_2 = 2L;
    private static final String ACCOUNT_NAME_1 = "Development";
    private static final String ACCOUNT_NAME_2 = "Engineering";
    private static final long BILLING_FAMILY_ID = 3L;
    private static final long CLOUD_SERVICE_ID_1 = 1L;
    private static final long CLOUD_SERVICE_ID_2 = 2L;
    private static final String CLOUD_SERVICE_NAME_1 = "CLOUD_SERVICE_1";
    private static final String CLOUD_SERVICE_NAME_2 = "CLOUD_SERVICE_2";
    private static final Set<Long> billingFamilyEntityIds = ImmutableSet.of(5L, 6L);

    private final MinimalEntity businessAccount1 = MinimalEntity.newBuilder()
            .setDisplayName(ACCOUNT_NAME_1)
            .setEntityType(ApiEntityType.BUSINESS_ACCOUNT.typeNumber())
            .setOid(ACCOUNT_ID_1)
            .build();

    private final MinimalEntity businessAccount2 = MinimalEntity.newBuilder()
            .setDisplayName(ACCOUNT_NAME_2)
            .setEntityType(ApiEntityType.BUSINESS_ACCOUNT.typeNumber())
            .setOid(ACCOUNT_ID_2)
            .build();

    private final MinimalEntity cloudService1 = MinimalEntity.newBuilder()
            .setDisplayName(CLOUD_SERVICE_NAME_1)
            .setEntityType(ApiEntityType.CLOUD_SERVICE.typeNumber())
            .setOid(CLOUD_SERVICE_ID_1)
            .build();

    private final MinimalEntity cloudService2 = MinimalEntity.newBuilder()
            .setDisplayName(CLOUD_SERVICE_NAME_2)
            .setEntityType(ApiEntityType.CLOUD_SERVICE.typeNumber())
            .setOid(CLOUD_SERVICE_ID_2)
            .build();

    private final ThinTargetCache.ThinTargetInfo targetInfo1 = ImmutableThinTargetInfo.builder()
            .oid(TARGET_ID_1)
            .displayName("target1")
            .isHidden(false)
            .probeInfo(ImmutableThinProbeInfo.builder()
                    .oid(1L)
                    .category("Cloud")
                    .uiCategory("Cloud")
                    .type(SDKProbeType.AZURE.getProbeType())
                    .build())
            .build();

    private final ThinTargetCache.ThinTargetInfo targetInfo2 = ImmutableThinTargetInfo.builder()
            .oid(TARGET_ID_2)
            .displayName("target2")
            .isHidden(false)
            .probeInfo(ImmutableThinProbeInfo.builder()
                    .oid(2L)
                    .category("Cloud")
                    .uiCategory("Cloud")
                    .type(SDKProbeType.AZURE_EA.getProbeType())
                    .build())
            .build();

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        CostServiceBlockingStub costRpc = CostServiceGrpc.newBlockingStub(grpcTestServer.getChannel());
        when(thinTargetCache.getTargetInfo(TARGET_ID_1)).thenReturn(Optional.of(targetInfo1));
        when(thinTargetCache.getTargetInfo(TARGET_ID_2)).thenReturn(Optional.of(targetInfo2));
        query = spy(new CloudCostsStatsSubQuery(repositoryApi, costRpc,
                supplyChainFetcherFactory, thinTargetCache, new BuyRiScopeHandler(), storageStatsSubQuery, userSessionContext ));
    }

    private SupplyChainNodeFetcherBuilder mockSupplyChainNodeFetcherBuilder(
            Set<Long> fetchEntityIds) throws OperationFailedException {
        final SupplyChainNodeFetcherBuilder nodeFetcherBuilder =
                mock(SupplyChainNodeFetcherBuilder.class);
        when(nodeFetcherBuilder.addSeedOids(any())).thenReturn(nodeFetcherBuilder);
        when(nodeFetcherBuilder.environmentType(any())).thenReturn(nodeFetcherBuilder);
        when(nodeFetcherBuilder.entityTypes(any())).thenReturn(nodeFetcherBuilder);
        when(nodeFetcherBuilder.fetchEntityIds()).thenReturn(fetchEntityIds);
        return nodeFetcherBuilder;
    }

    @Test
    public void testApplicableInPlan() {
        final ApiId scope = mock(ApiId.class);
        when(scope.isPlan()).thenReturn(true);

        final StatsQueryContext context = mock(StatsQueryContext.class);
        when(context.getInputScope()).thenReturn(scope);
        final StatsQueryScope statsQueryScope = mock(StatsQueryScope.class);
        final GlobalScope globalScope = mock(GlobalScope.class);
        when(globalScope.environmentType()).thenReturn(Optional.of(EnvironmentType.CLOUD));
        when(statsQueryScope.getGlobalScope()).thenReturn(Optional.of(globalScope));
        when(context.getQueryScope()).thenReturn(statsQueryScope);

        assertThat(query.applicableInContext(context), is(true));
    }

    @Test
    public void testApplicableInNotPlanAndIsCloudEnvironment() {
        final ApiId scope = mock(ApiId.class);
        when(scope.isPlan()).thenReturn(false);

        final StatsQueryContext context = mock(StatsQueryContext.class);
        when(context.getInputScope()).thenReturn(scope);


        final GlobalScope globalScope = mock(GlobalScope.class);
        when(globalScope.entityTypes()).thenReturn(Sets.newHashSet(ApiEntityType.VIRTUAL_VOLUME));
        when(globalScope.environmentType()).thenReturn(Optional.of(EnvironmentType.CLOUD));
        StatsQueryScope statsQueryScope = mock(StatsQueryScope.class);
        when(statsQueryScope.getGlobalScope()).thenReturn(Optional.of(globalScope));

        when(context.getQueryScope()).thenReturn(statsQueryScope);

        assertThat(query.applicableInContext(context), is(true));
    }

    @Test
    public void testApplicableInNotPlanAndIsOnPermEnvironment() {
        final ApiId scope = mock(ApiId.class);
        when(scope.isPlan()).thenReturn(false);

        final StatsQueryContext context = mock(StatsQueryContext.class);
        when(context.getInputScope()).thenReturn(scope);


        final GlobalScope globalScope = mock(GlobalScope.class);
        when(globalScope.entityTypes()).thenReturn(Sets.newHashSet(ApiEntityType.VIRTUAL_VOLUME));
        when(globalScope.environmentType()).thenReturn(Optional.of(EnvironmentType.ON_PREM));
        StatsQueryScope statsQueryScope = mock(StatsQueryScope.class);
        when(statsQueryScope.getGlobalScope()).thenReturn(Optional.of(globalScope));

        when(context.getQueryScope()).thenReturn(statsQueryScope);

        assertThat(query.applicableInContext(context), is(false));
    }

    /**
     * Tests the case where we are getting the stats grouped by business account.
     */
    @Test
    public void testGetAggregateStatsGroupByAccount() {

        // expected filters
        StatFilterApiDTO filter1 = new StatFilterApiDTO();
        filter1.setType(CloudCostsStatsSubQuery.BUSINESS_UNIT);
        filter1.setValue(ACCOUNT_NAME_1);

        StatFilterApiDTO filter2 = new StatFilterApiDTO();
        filter2.setType(CloudCostsStatsSubQuery.BUSINESS_UNIT);
        filter2.setValue(ACCOUNT_NAME_2);

        // build
        Set<MinimalEntity> businessAccountDTOs = new HashSet<>();
        businessAccountDTOs.add(businessAccount1);
        businessAccountDTOs.add(businessAccount2);
        SearchRequest searchRequest = mock(SearchRequest.class);
        when(repositoryApi.newSearchRequest(any())).thenReturn(searchRequest);
        when(searchRequest.getMinimalEntities()).thenReturn(businessAccountDTOs.stream());

        StatApiInputDTO queryStat = new StatApiInputDTO();
        queryStat.setGroupBy(Collections.singletonList(CloudCostsStatsSubQuery.BUSINESS_UNIT));
        final Set<StatApiInputDTO> requestedStats = Collections.singleton(queryStat);

        StatsQueryContext context = mock(StatsQueryContext.class);
        ApiId apiId = mock(ApiId.class);
        when(apiId.getScopeTypes()).thenReturn(Optional.empty());
        when(apiId.isResourceGroupOrGroupOfResourceGroups()).thenReturn(false);
        when(context.getInputScope()).thenReturn(apiId);
        when(context.getTimeWindow()).thenReturn(Optional.empty());
        final GlobalScope globalScope = mock(GlobalScope.class);
        StatsQueryScope statsQueryScope = mock(StatsQueryScope.class);
        when(context.getQueryScope()).thenReturn(statsQueryScope);
        when(context.getQueryScope().getGlobalScope()).thenReturn(Optional.of(globalScope));

        GetCloudCostStatsResponse response = GetCloudCostStatsResponse.newBuilder()
                .addCloudStatRecord(CloudCostStatRecord.newBuilder()
                        .setSnapshotDate(1234L)
                        .addStatRecords(createCloudServiceStatRecord(ACCOUNT_ID_1, 10.0f))
                        .addStatRecords(createCloudServiceStatRecord(ACCOUNT_ID_2, 12.0f))
                        .build())
                .build();
        when(costServiceMole.getAccountExpenseStats(any(GetCloudExpenseStatsRequest.class)))
                .thenReturn(response);

        // test
        try {
            List<StatSnapshotApiDTO> statSnapshots = query.getAggregateStats(requestedStats, context);

            // assert
            assertThat(statSnapshots.size(), is(1));

            Map<String, List<StatFilterApiDTO>> accountIdToFilters = statSnapshots.stream()
                    .map(StatSnapshotApiDTO::getStatistics)
                    .flatMap(List::stream)
                    .collect(Collectors.toList())
                    .stream()
                    .collect(Collectors.toMap(
                            statApiDTO -> statApiDTO.getRelatedEntity().getUuid(),
                            StatApiDTO::getFilters));

            assertThat(accountIdToFilters.get(Long.toString(ACCOUNT_ID_1)).size(), is(1));
            assertThat(accountIdToFilters.get(Long.toString(ACCOUNT_ID_1)).get(0), is(filter1));
            assertThat(accountIdToFilters.get(Long.toString(ACCOUNT_ID_2)).size(), is(1));
            assertThat(accountIdToFilters.get(Long.toString(ACCOUNT_ID_2)).get(0), is(filter2));

        } catch (OperationFailedException e) {
            e.printStackTrace();
        }
    }

    /**
     * Tests the case where we are getting the stats grouped by cloud services.
     */
    @Test
    public void testGetAggregateStatsGroupByCloudService() {

        // expected filters
        StatFilterApiDTO filter1 = new StatFilterApiDTO();
        filter1.setType(CloudCostsStatsSubQuery.CLOUD_SERVICE);
        filter1.setValue(CLOUD_SERVICE_NAME_1);

        StatFilterApiDTO filter2 = new StatFilterApiDTO();
        filter2.setType(CloudCostsStatsSubQuery.CLOUD_SERVICE);
        filter2.setValue(CLOUD_SERVICE_NAME_2);

        // build
        Set<MinimalEntity> cloudServiceDTOs = new HashSet<>();
        cloudServiceDTOs.add(cloudService1);
        cloudServiceDTOs.add(cloudService2);
        SearchRequest searchRequest = mock(SearchRequest.class);
        when(repositoryApi.newSearchRequest(any())).thenReturn(searchRequest);
        when(searchRequest.getMinimalEntities()).thenReturn(cloudServiceDTOs.stream());

        StatApiInputDTO queryStat = new StatApiInputDTO();
        queryStat.setGroupBy(Collections.singletonList(CloudCostsStatsSubQuery.CLOUD_SERVICE));
        final Set<StatApiInputDTO> requestedStats = Collections.singleton(queryStat);

        StatsQueryContext context = mock(StatsQueryContext.class);
        ApiId apiId = mock(ApiId.class);
        when(apiId.getScopeTypes()).thenReturn(Optional.empty());
        when(apiId.isResourceGroupOrGroupOfResourceGroups()).thenReturn(false);
        when(context.getInputScope()).thenReturn(apiId);
        when(context.getTimeWindow()).thenReturn(Optional.empty());
        final GlobalScope globalScope = mock(GlobalScope.class);
        StatsQueryScope statsQueryScope = mock(StatsQueryScope.class);
        when(context.getQueryScope()).thenReturn(statsQueryScope);
        when(context.getQueryScope().getGlobalScope()).thenReturn(Optional.of(globalScope));

        GetCloudCostStatsResponse response = GetCloudCostStatsResponse.newBuilder()
                .addCloudStatRecord(CloudCostStatRecord.newBuilder()
                        .setSnapshotDate(1234L)
                        .addStatRecords(createCloudServiceStatRecord(CLOUD_SERVICE_ID_1, 10.0f))
                        .addStatRecords(createCloudServiceStatRecord(CLOUD_SERVICE_ID_2, 12.0f))
                        .build())
                .build();
        when(costServiceMole.getAccountExpenseStats(any(GetCloudExpenseStatsRequest.class)))
                .thenReturn(response);

        // test
        try {
            List<StatSnapshotApiDTO> statSnapshots = query.getAggregateStats(requestedStats, context);

            // assert
            assertThat(statSnapshots.size(), is(1));

            Map<String, List<StatFilterApiDTO>> cloudServiceToFilters = statSnapshots.stream()
                    .map(StatSnapshotApiDTO::getStatistics)
                    .flatMap(List::stream)
                    .collect(Collectors.toList())
                    .stream()
                    .collect(Collectors.toMap(
                            statApiDTO -> statApiDTO.getRelatedEntity().getUuid(),
                            StatApiDTO::getFilters));

            assertThat(cloudServiceToFilters.get(Long.toString(CLOUD_SERVICE_ID_1)).size(), is(1));
            assertThat(cloudServiceToFilters.get(Long.toString(CLOUD_SERVICE_ID_1)).get(0), is(filter1));
            assertThat(cloudServiceToFilters.get(Long.toString(CLOUD_SERVICE_ID_2)).size(), is(1));
            assertThat(cloudServiceToFilters.get(Long.toString(CLOUD_SERVICE_ID_2)).get(0), is(filter2));

        } catch (OperationFailedException e) {
            e.printStackTrace();
        }
    }

    /**
     * Test getAggregatedStats for resource group. Resource group contains only Virtual Machines as
     * member, so request cost cloud stats only for this entity type.
     * Test count of workloads.
     *
     * @throws OperationFailedException on exceptions occur
     */
    @Test
    public void testGetAggregateStatsForResourceGroupScope() throws OperationFailedException {
        final Set<Long> rgMembersOids = Sets.newHashSet(1L, 2L);
        final Set<StatApiInputDTO> requestedStats = createRequestStatsForResourceGroup();

        final UuidMapper.CachedGroupInfo cachedGroupInfo = mock(UuidMapper.CachedGroupInfo.class);
        when(cachedGroupInfo.getGroupType()).thenReturn(GroupType.RESOURCE);

        final StatsQueryContext context = mock(StatsQueryContext.class);

        final ApiId apiId = mock(ApiId.class);
        when(apiId.getScopeTypes()).thenReturn(
                Optional.of(Sets.newHashSet(ApiEntityType.VIRTUAL_MACHINE)));
        when(apiId.isCloud()).thenReturn(true);
        when(apiId.getCachedGroupInfo()).thenReturn(Optional.of(cachedGroupInfo));
        when(apiId.isResourceGroupOrGroupOfResourceGroups()).thenReturn(true);
        when(apiId.uuid()).thenReturn("123");
        when(context.getInputScope()).thenReturn(apiId);
        when(context.getTimeWindow()).thenReturn(Optional.empty());

        final StatsQueryScope queryScope = mock(StatsQueryScope.class);
        when(queryScope.getScopeOids()).thenReturn(rgMembersOids);
        when(context.getQueryScope()).thenReturn(queryScope);
        when(context.getPlanInstance()).thenReturn(Optional.empty());

        final SupplyChainNodeFetcherBuilder nodeFetcherBuilder =
                mockSupplyChainNodeFetcherBuilder(rgMembersOids);
        when(supplyChainFetcherFactory.newNodeFetcher()).thenReturn(nodeFetcherBuilder);

        // test that there is no scope expanding for resource groups
        verify(context.getQueryScope(), times(0)).getExpandedOids();

        final ArgumentCaptor<Cost.GetCloudCostStatsRequest> costParamCaptor =
                ArgumentCaptor.forClass(Cost.GetCloudCostStatsRequest.class);
        final Cost.GetCloudCostStatsResponse response =
                Cost.GetCloudCostStatsResponse.getDefaultInstance();
        when(costServiceMole.getCloudCostStats(costParamCaptor.capture()))
            .thenReturn(Collections.singletonList(response));

        final List<StatSnapshotApiDTO> aggregateStats =
                query.getAggregateStats(requestedStats, context);
        final List<CloudCostStatsQuery> cloudCostStatsQueryList =
                costParamCaptor.getValue().getCloudCostStatsQueryList();

        // Check that requests cost only for entity types existed in resource group.
        // In our case request only stats for VMs and don't request for volumes, dbs and
        // dbServers
        Assert.assertEquals(cloudCostStatsQueryList.size(), 1);
        final List<Integer> entityTypeIds =
                cloudCostStatsQueryList.get(0).getEntityTypeFilter().getEntityTypeIdList();
        Assert.assertEquals(entityTypeIds.size(), 1);
        Assert.assertEquals(EntityType.VIRTUAL_MACHINE.getNumber(),
                entityTypeIds.get(0).intValue());

        // check that count of workloads is calculated correctly (test RG has 2 VM as members)
        final Optional<StatApiDTO> numWorkloadsStat = aggregateStats.iterator()
                .next()
                .getStatistics()
                .stream()
                .filter(stat -> stat.getName().equals(StringConstants.NUM_WORKLOADS))
                .findFirst();
        Assert.assertEquals(Float.valueOf(rgMembersOids.size()), numWorkloadsStat.get().getValue());
    }

    /**
     * Test that for billing family scope the correct numWorkloads value is set in the StatApiDto
     * response.
     *
     * @throws OperationFailedException if getAggregateStats() throws OperationFailedException
     */
    @Test
    public void testGetNumWorkloadsForBillingFamilyScope() throws OperationFailedException {
        // given
        final Set<StatApiInputDTO> requestedStats =
                Collections.singleton(createNumWorkloadsInputDto());
        final ApiId apiId = createApiIdMock(GroupType.BILLING_FAMILY,
                Collections.singleton(ApiEntityType.BUSINESS_ACCOUNT));
        when(apiId.isResourceGroupOrGroupOfResourceGroups()).thenReturn(false);
        final StatsQueryContext context = createStatsQueryContextMock(apiId);

        // when
        final SupplyChainNodeFetcherBuilder nodeFetcherBuilder =
                mockSupplyChainNodeFetcherBuilder(billingFamilyEntityIds);
        when(supplyChainFetcherFactory.newNodeFetcher()).thenReturn(nodeFetcherBuilder);

        final List<StatSnapshotApiDTO> result = query.getAggregateStats(requestedStats, context);

        // then
        Assert.assertFalse(result.isEmpty());
        final StatSnapshotApiDTO apiDTO = result.iterator().next();
        Assert.assertFalse(apiDTO.getStatistics().isEmpty());
        final StatApiDTO statApiDTO = apiDTO.getStatistics().iterator().next();
        Assert.assertEquals(billingFamilyEntityIds.size(), statApiDTO.getValue(), 0);
    }

    private ApiId createApiIdMock(final GroupType groupType, final Set<ApiEntityType> scopeTypes) {
        final UuidMapper.CachedGroupInfo cachedGroupInfo = mock(UuidMapper.CachedGroupInfo.class);
        when(cachedGroupInfo.getGroupType()).thenReturn(groupType);
        final ApiId apiId = mock(ApiId.class);
        when(apiId.getScopeTypes()).thenReturn(Optional.of(scopeTypes));
        when(apiId.isCloud()).thenReturn(true);
        when(apiId.getCachedGroupInfo()).thenReturn(Optional.of(cachedGroupInfo));
        return apiId;
    }

    private StatsQueryContext createStatsQueryContextMock(ApiId apiId) {
        final StatsQueryContext context = mock(StatsQueryContext.class);
        when(context.getInputScope()).thenReturn(apiId);
        when(context.getTimeWindow()).thenReturn(Optional.empty());
        when(context.getInputScope().isRealtimeMarket()).thenReturn(false);
        when(context.getInputScope().uuid()).thenReturn("1");
        final StatsQueryScope queryScope = mock(StatsQueryScope.class);
        when(queryScope.getScopeOids()).thenReturn(Sets.newHashSet(1L));
        when(context.getQueryScope()).thenReturn(queryScope);
        when(context.getPlanInstance()).thenReturn(Optional.empty());
        return context;
    }

    private StatApiInputDTO createNumWorkloadsInputDto() {
        final StatFilterApiDTO filterApiDTO = new StatFilterApiDTO();
        filterApiDTO.setType(StringConstants.ENVIRONMENT_TYPE);
        filterApiDTO.setValue(EnvironmentType.CLOUD.name());
        final StatApiInputDTO inputDTO = new StatApiInputDTO();
        inputDTO.setName(StringConstants.NUM_WORKLOADS);
        inputDTO.setFilters(Collections.singletonList(filterApiDTO));
        return inputDTO;
    }


    /**
     * Tests the case where we are getting the stats grouped by cloud provider.
     */
    @Test
    public void testGetAggregateStatsGroupByCSP() {
        // expected filters
        StatFilterApiDTO filter = new StatFilterApiDTO();
        filter.setType(CloudCostsStatsSubQuery.CSP);
        filter.setValue(CSP_AZURE);

        // build
        StatApiInputDTO queryStat = new StatApiInputDTO();
        queryStat.setGroupBy(Collections.singletonList(CloudCostsStatsSubQuery.CSP));
        final Set<StatApiInputDTO> requestedStats = Collections.singleton(queryStat);

        StatsQueryContext context = mock(StatsQueryContext.class);
        ApiId apiId = mock(ApiId.class);
        final GlobalScope globalScope = mock(GlobalScope.class);
        StatsQueryScope statsQueryScope = mock(StatsQueryScope.class);
        when(context.getQueryScope()).thenReturn(statsQueryScope);
        when(context.getQueryScope().getGlobalScope()).thenReturn(Optional.of(globalScope));
        when(apiId.getScopeTypes()).thenReturn(Optional.empty());
        when(apiId.isResourceGroupOrGroupOfResourceGroups()).thenReturn(false);
        when(context.getInputScope()).thenReturn(apiId);
        when(context.getTimeWindow()).thenReturn(Optional.empty());

        GetCloudCostStatsResponse response = GetCloudCostStatsResponse.newBuilder()
                .addCloudStatRecord(CloudCostStatRecord.newBuilder()
                        .setSnapshotDate(1234L)
                        .addStatRecords(createCloudServiceStatRecord(TARGET_ID_1, 10.0f))
                        .addStatRecords(createCloudServiceStatRecord(TARGET_ID_2, 12.0f))
                        .build())
                .build();
        when(costServiceMole.getAccountExpenseStats(any(GetCloudExpenseStatsRequest.class)))
                .thenReturn(response);

        // test
        try {
            List<StatSnapshotApiDTO> statSnapshots = query.getAggregateStats(requestedStats, context);

            // assert
            assertThat(statSnapshots.size(), is(1));

            Map<String, List<StatFilterApiDTO>> cloudServiceToFilters = statSnapshots.stream()
                    .map(StatSnapshotApiDTO::getStatistics)
                    .flatMap(List::stream)
                    .collect(Collectors.toList())
                    .stream()
                    .collect(Collectors.toMap(
                            statApiDTO -> statApiDTO.getRelatedEntity().getUuid(),
                            StatApiDTO::getFilters));

            // both targets belong to the same CSP - both stats should have the same filter.
            assertThat(cloudServiceToFilters.get(Long.toString(TARGET_ID_1)).size(), is(1));
            assertThat(cloudServiceToFilters.get(Long.toString(TARGET_ID_1)).get(0), is(filter));
            assertThat(cloudServiceToFilters.get(Long.toString(TARGET_ID_2)).size(), is(1));
            assertThat(cloudServiceToFilters.get(Long.toString(TARGET_ID_2)).get(0), is(filter));
        } catch (OperationFailedException e) {
            e.printStackTrace();
        }
    }

    private StatRecord createCloudServiceStatRecord(long cloudServiceId, float cost) {
        return StatRecord.newBuilder()
                .setAssociatedEntityId(cloudServiceId)
                .setAssociatedEntityType(ApiEntityType.CLOUD_SERVICE.typeNumber())
                .setUnits("$/h")
                .setValues(StatValue.newBuilder().setAvg(cost).build())
                .build();
    }

    private Set<StatApiInputDTO> createRequestStats() {
        return Collections.singleton(createRequestStat(CloudCostsStatsSubQuery.COST_PRICE_QUERY_KEY,
                ApiEntityType.VIRTUAL_MACHINE.apiStr()));
    }

    private Set<StatApiInputDTO> createRequestStatsForCloudTab() {
        return Sets.newHashSet(createRequestStat(CloudCostsStatsSubQuery.COST_PRICE_QUERY_KEY,
                ApiEntityType.VIRTUAL_MACHINE.apiStr()), createNumWorkloadsInputDto());
    }

    private Set<StatApiInputDTO> createRequestStatsForResourceGroup() {
        final StatApiInputDTO vmStat =
                createRequestStat(CloudCostsStatsSubQuery.COST_PRICE_QUERY_KEY,
                        ApiEntityType.VIRTUAL_MACHINE.apiStr());
        final StatApiInputDTO dbServerStat =
                createRequestStat(CloudCostsStatsSubQuery.COST_PRICE_QUERY_KEY,
                        ApiEntityType.DATABASE_SERVER.apiStr());
        final StatApiInputDTO dbStat =
                createRequestStat(CloudCostsStatsSubQuery.COST_PRICE_QUERY_KEY,
                        ApiEntityType.DATABASE.apiStr());
        final StatApiInputDTO volumeStat =
                createRequestStat(CloudCostsStatsSubQuery.COST_PRICE_QUERY_KEY,
                        ApiEntityType.VIRTUAL_VOLUME.apiStr());
        final StatApiInputDTO numWorkloads = createNumWorkloadsInputDto();
        return Sets.newHashSet(vmStat, dbStat, dbServerStat, volumeStat, numWorkloads);
    }

    private StatApiInputDTO createRequestStat(String statName, String relatedEntityType) {
        final StatFilterApiDTO computeFilter = new StatFilterApiDTO();
        computeFilter.setType(CloudCostsStatsSubQuery.COST_COMPONENT);
        computeFilter.setValue(CostCategory.ON_DEMAND_COMPUTE.name());
        final StatApiInputDTO costStatApi = new StatApiInputDTO();
        costStatApi.setName(statName);
        costStatApi.setRelatedEntityType(relatedEntityType);
        costStatApi.setFilters(Collections.singletonList(computeFilter));
        return costStatApi;
    }

    /**
     * Tests getting the costs for a billing family.
     *
     * @throws Exception if something goes wrong.
     */
    @Test
    public void testGetAggregateStatsBillingFamily() throws Exception {
        // ARRANGE
        final Set<StatApiInputDTO> requestedStats = createRequestStats();
        final StatsQueryContext context = mock(StatsQueryContext.class);
        final ApiId inputScope = mock(ApiId.class);
        when(inputScope.getScopeTypes()).thenReturn(Optional.of(
            Collections.singleton(ApiEntityType.BUSINESS_ACCOUNT)));
        final StatsQueryScope queryScope = mock(StatsQueryScope.class);

        // Behaviors associated to context
        when(context.getInputScope()).thenReturn(inputScope);
        when(context.getQueryScope()).thenReturn(queryScope);
        when(context.getTimeWindow()).thenReturn(Optional.empty());
        when(context.requestProjected()).thenReturn(true);
        when(context.getPlanInstance()).thenReturn(Optional.empty());

        // Behaviors associated to inputScope
        when(inputScope.isRealtimeMarket()).thenReturn(false);
        when(inputScope.isPlan()).thenReturn(false);
        when(inputScope.isCloud()).thenReturn(true);
        when(inputScope.isEntity()).thenReturn(false);
        when(inputScope.isGroup()).thenReturn(true);
        when(inputScope.isResourceGroupOrGroupOfResourceGroups()).thenReturn(false);

        final UuidMapper.CachedGroupInfo cachedGroupInfo = mock(UuidMapper.CachedGroupInfo.class);
        when(cachedGroupInfo.getEntityTypes())
            .thenReturn(Collections.singleton(ApiEntityType.VIRTUAL_MACHINE));
        when(cachedGroupInfo.getGroupType()).thenReturn(GroupType.BILLING_FAMILY);
        when(inputScope.getCachedGroupInfo()).thenReturn(Optional.of(cachedGroupInfo));

        // Behaviors associated to costRpcService
        ArgumentCaptor<Cost.GetCloudCostStatsRequest> costParamCaptor =
            ArgumentCaptor.forClass(Cost.GetCloudCostStatsRequest.class);
        Cost.GetCloudCostStatsResponse response =
            Cost.GetCloudCostStatsResponse.getDefaultInstance();
        when(costServiceMole.getCloudCostStats(costParamCaptor.capture()))
            .thenReturn(Collections.singletonList(response));

        // Behaviors associated to query scope
        when(queryScope.getScopeOids()).thenReturn(Collections.singleton(5L));


        // ACT
        query.getAggregateStats(requestedStats, context);

        // ASSERT
        // make sure we made correct call to cost service
        assertThat(costParamCaptor.getValue(), is(GetCloudCostStatsRequest.newBuilder()
                .addCloudCostStatsQuery(CloudCostStatsQuery.newBuilder()
                        .setQueryId(0L)
                        .setCostCategoryFilter(CostCategoryFilter.newBuilder()
                                .addCostCategory(CostCategory.ON_DEMAND_COMPUTE).build())
            .setEntityTypeFilter(EntityTypeFilter.newBuilder()
                .addEntityTypeId(ApiEntityType.VIRTUAL_MACHINE.typeNumber()))
            .setAccountFilter(AccountFilter.newBuilder().addAccountId(5L).build())
            .setRequestProjected(true)
           .build()).build()));
    }

    /**
     * Tests getting the costs and count of workloads for for cloud tab of realtime market.
     *
     * @throws Exception if something goes wrong.
     */
    @Test
    public void testGetAggregateStatsCloudTab() throws Exception {
        // ARRANGE
        Set<Long> cloudTabWorkloads = Collections.singleton(1L);
        final Set<StatApiInputDTO> requestedStats = createRequestStatsForCloudTab();
        requestedStats.iterator().next().setRelatedEntityType(null);
        final StatsQueryContext context = mock(StatsQueryContext.class);
        final ApiId inputScope = mock(ApiId.class);
        when(inputScope.getScopeTypes()).thenReturn(Optional.empty());
        final StatsQueryScope queryScope = mock(StatsQueryScope.class);

        // Behaviors associated to context
        when(context.getInputScope()).thenReturn(inputScope);
        when(context.getQueryScope()).thenReturn(queryScope);
        when(context.getTimeWindow()).thenReturn(Optional.empty());
        when(context.requestProjected()).thenReturn(false);
        when(context.getPlanInstance()).thenReturn(Optional.empty());

        // Behaviors associated to inputScope
        when(inputScope.isRealtimeMarket()).thenReturn(true);
        when(inputScope.isPlan()).thenReturn(false);
        when(inputScope.isCloud()).thenReturn(true);
        when(inputScope.isEntity()).thenReturn(false);
        when(inputScope.isGroup()).thenReturn(false);
        when(inputScope.getCachedGroupInfo()).thenReturn(Optional.empty());
        when(inputScope.isResourceGroupOrGroupOfResourceGroups()).thenReturn(false);
        // Behaviors associated to costRpcService
        ArgumentCaptor<Cost.GetCloudCostStatsRequest> costParamCaptor =
            ArgumentCaptor.forClass(Cost.GetCloudCostStatsRequest.class);
        Cost.GetCloudCostStatsResponse response =
            Cost.GetCloudCostStatsResponse.getDefaultInstance();
        when(costServiceMole.getCloudCostStats(costParamCaptor.capture()))
            .thenReturn(Collections.singletonList(response));

        // Behaviors associated to query scope
        when(queryScope.getExpandedOids()).thenReturn(Collections.emptySet());

        final SupplyChainNodeFetcherBuilder nodeFetcherBuilder =
                mockSupplyChainNodeFetcherBuilder(cloudTabWorkloads);
        when(supplyChainFetcherFactory.newNodeFetcher()).thenReturn(nodeFetcherBuilder);


        // ACT
        List<StatSnapshotApiDTO> aggregateStats = query.getAggregateStats(requestedStats, context);

        // ASSERT
        // make sure we made correct call to cost service
        assertThat(costParamCaptor.getValue(), is(GetCloudCostStatsRequest.newBuilder()
                .addCloudCostStatsQuery(CloudCostStatsQuery.newBuilder()
                        .setQueryId(0L)
                        .setRequestProjected(false)
                        .setCostCategoryFilter(CostCategoryFilter.newBuilder()
                                .addCostCategory(CostCategory.ON_DEMAND_COMPUTE))
                        .build())
            .build()
        ));

        // check that count of workloads is calculated correctly (test cloud tab has 1 workload)
        final Optional<StatApiDTO> numWorkloadsStat = aggregateStats.iterator()
                .next()
                .getStatistics()
                .stream()
                .filter(stat -> stat.getName().equals(StringConstants.NUM_WORKLOADS))
                .findFirst();
        Assert.assertEquals(Float.valueOf(cloudTabWorkloads.size()),
                numWorkloadsStat.get().getValue());
    }

    /**
     * Tests getting the costs for a region.
     *
     * @throws Exception if something goes wrong.
     */
    @Test
    public void testGetAggregateStatsRegion() throws Exception {
        // ARRANGE
        final Set<StatApiInputDTO> requestedStats = createRequestStats();
        final StatsQueryContext context = mock(StatsQueryContext.class);
        final ApiId inputScope = mock(ApiId.class);
        when(inputScope.getScopeTypes()).thenReturn(Optional.of(Collections.singleton(ApiEntityType.REGION)));
        final StatsQueryScope queryScope = mock(StatsQueryScope.class);

        // Behaviors associated to context
        when(context.getInputScope()).thenReturn(inputScope);
        when(context.getQueryScope()).thenReturn(queryScope);
        when(context.getTimeWindow()).thenReturn(Optional.empty());
        when(context.requestProjected()).thenReturn(true);
        when(context.getPlanInstance()).thenReturn(Optional.empty());

        // Behaviors associated to inputScope
        when(inputScope.isRealtimeMarket()).thenReturn(false);
        when(inputScope.isPlan()).thenReturn(false);
        when(inputScope.isCloud()).thenReturn(true);
        when(inputScope.isEntity()).thenReturn(true);
        when(inputScope.isGroup()).thenReturn(false);
        when(inputScope.getScopeTypes()).thenReturn(Optional
                .of(Collections.singleton(ApiEntityType.REGION)));
        when(inputScope.getCachedGroupInfo()).thenReturn(Optional.empty());
        when(inputScope.isResourceGroupOrGroupOfResourceGroups()).thenReturn(false);

        // Behaviors associated to costRpcService
        ArgumentCaptor<Cost.GetCloudCostStatsRequest> costParamCaptor =
            ArgumentCaptor.forClass(GetCloudCostStatsRequest.class);
        Cost.GetCloudCostStatsResponse response =
            Cost.GetCloudCostStatsResponse.getDefaultInstance();
        when(costServiceMole.getCloudCostStats(costParamCaptor.capture()))
            .thenReturn(Collections.singletonList(response));

        // Behaviors associated to query scope
        when(queryScope.getScopeOids()).thenReturn(Collections.singleton(5L));


        // ACT
        query.getAggregateStats(requestedStats, context);

        // ASSERT
        // make sure we made correct call to cost service
        assertThat(costParamCaptor.getValue(), is(GetCloudCostStatsRequest.newBuilder()
                .addCloudCostStatsQuery(CloudCostStatsQuery.newBuilder()
                        .setQueryId(0L)
                        .setCostCategoryFilter(CostCategoryFilter.newBuilder()
                                .addCostCategory(CostCategory.ON_DEMAND_COMPUTE).build())
                        .setEntityTypeFilter(EntityTypeFilter.newBuilder()
                                .addEntityTypeId(ApiEntityType.VIRTUAL_MACHINE.typeNumber()))
                        .setRegionFilter(Cost.RegionFilter.newBuilder().addRegionId(5L).build())
                        .setRequestProjected(true)
                        .build()).build()
        ));
    }

    /**
     * Tests getting the costs for a group billing family.
     *
     * @throws Exception if something goes wrong.
     */
    @Test
    public void testGetAggregateStatsGroupOfBillingFamily() throws Exception {
        // ARRANGE
        final Set<StatApiInputDTO> requestedStats = createRequestStats();
        final StatsQueryContext context = mock(StatsQueryContext.class);
        final ApiId inputScope = mock(ApiId.class);
        when(inputScope.getScopeTypes()).thenReturn(Optional.of(Collections.singleton(ApiEntityType.BUSINESS_ACCOUNT)));
        final StatsQueryScope queryScope = mock(StatsQueryScope.class);

        // Behaviors associated to context
        when(context.getInputScope()).thenReturn(inputScope);
        when(context.getQueryScope()).thenReturn(queryScope);
        when(context.getTimeWindow()).thenReturn(Optional.empty());
        when(context.requestProjected()).thenReturn(true);
        when(context.getPlanInstance()).thenReturn(Optional.empty());

        // Behaviors associated to inputScope
        when(inputScope.isRealtimeMarket()).thenReturn(false);
        when(inputScope.isPlan()).thenReturn(false);
        when(inputScope.isCloud()).thenReturn(true);
        when(inputScope.isEntity()).thenReturn(false);
        when(inputScope.isGroup()).thenReturn(true);
        when(inputScope.isResourceGroupOrGroupOfResourceGroups()).thenReturn(false);

        final UuidMapper.CachedGroupInfo cachedGroupInfo = mock(UuidMapper.CachedGroupInfo.class);
        when(cachedGroupInfo.getEntityTypes())
                .thenReturn(Collections.singleton(ApiEntityType.VIRTUAL_MACHINE));
        when(cachedGroupInfo.getNestedGroupTypes())
            .thenReturn(Collections.singleton(GroupType.BILLING_FAMILY));
        when(cachedGroupInfo.getGroupType()).thenReturn(GroupType.REGULAR);
        when(inputScope.getCachedGroupInfo()).thenReturn(Optional.of(cachedGroupInfo));

        // Behaviors associated to costRpcService
        ArgumentCaptor<Cost.GetCloudCostStatsRequest> costParamCaptor =
            ArgumentCaptor.forClass(GetCloudCostStatsRequest.class);
        Cost.GetCloudCostStatsResponse response =
            Cost.GetCloudCostStatsResponse.getDefaultInstance();
        when(costServiceMole.getCloudCostStats(costParamCaptor.capture()))
            .thenReturn(Collections.singletonList(response));

        // Behaviors associated to query scope
        when(queryScope.getScopeOids()).thenReturn(Collections.singleton(5L));


        // ACT
        query.getAggregateStats(requestedStats, context);

        // ASSERT
        // make sure we made correct call to cost service
        assertThat(costParamCaptor.getValue(), is(GetCloudCostStatsRequest.newBuilder()
                .addCloudCostStatsQuery(CloudCostStatsQuery.newBuilder()
                        .setQueryId(0L)
                        .setCostCategoryFilter(CostCategoryFilter.newBuilder()
                                .addCostCategory(CostCategory.ON_DEMAND_COMPUTE).build())
                        .setEntityTypeFilter(EntityTypeFilter.newBuilder()
                                .addEntityTypeId(ApiEntityType.VIRTUAL_MACHINE.typeNumber()))
                        .setAccountFilter(AccountFilter.newBuilder().addAccountId(5L))
                        .setRequestProjected(true)
                        .build()).build()
        ));
    }

    /**
     * Tests getting the costs for a group regions.
     *
     * @throws Exception if something goes wrong.
     */
    @Test
    public void testGetAggregateStatsGroupOfRegions() throws Exception {
        // ARRANGE
        final Set<StatApiInputDTO> requestedStats = createRequestStats();
        final StatsQueryContext context = mock(StatsQueryContext.class);
        final ApiId inputScope = mock(ApiId.class);
        when(inputScope.getScopeTypes()).thenReturn(Optional.of(Collections.singleton(ApiEntityType.REGION)));
        final StatsQueryScope queryScope = mock(StatsQueryScope.class);

        // Behaviors associated to context
        when(context.getInputScope()).thenReturn(inputScope);
        when(context.getQueryScope()).thenReturn(queryScope);
        when(context.getTimeWindow()).thenReturn(Optional.empty());
        when(context.requestProjected()).thenReturn(true);
        when(context.getPlanInstance()).thenReturn(Optional.empty());

        // Behaviors associated to inputScope
        when(inputScope.isRealtimeMarket()).thenReturn(false);
        when(inputScope.isPlan()).thenReturn(false);
        when(inputScope.isCloud()).thenReturn(true);
        when(inputScope.isEntity()).thenReturn(false);
        when(inputScope.isGroup()).thenReturn(true);
        when(inputScope.isResourceGroupOrGroupOfResourceGroups()).thenReturn(false);

        final UuidMapper.CachedGroupInfo cachedGroupInfo = mock(UuidMapper.CachedGroupInfo.class);
        when(cachedGroupInfo.getEntityTypes())
            .thenReturn(Collections.singleton(ApiEntityType.REGION));
        when(cachedGroupInfo.getNestedGroupTypes())
                .thenReturn(Collections.emptySet());
        when(cachedGroupInfo.getGroupType()).thenReturn(GroupType.REGULAR);
        when(inputScope.getCachedGroupInfo()).thenReturn(Optional.of(cachedGroupInfo));

        // Behaviors associated to costRpcService
        ArgumentCaptor<Cost.GetCloudCostStatsRequest> costParamCaptor =
                ArgumentCaptor.forClass(Cost.GetCloudCostStatsRequest.class);
        Cost.GetCloudCostStatsResponse response =
                Cost.GetCloudCostStatsResponse.getDefaultInstance();
        when(costServiceMole.getCloudCostStats(costParamCaptor.capture()))
            .thenReturn(Collections.singletonList(response));

        // Behaviors associated to query scope
        when(queryScope.getScopeOids()).thenReturn(Collections.singleton(5L));


        // ACT
        query.getAggregateStats(requestedStats, context);

        // ASSERT
        // make sure we made correct call to cost service
        assertThat(costParamCaptor.getValue(), is(GetCloudCostStatsRequest.newBuilder()
                .addCloudCostStatsQuery(CloudCostStatsQuery.newBuilder()
                        .setCostCategoryFilter(CostCategoryFilter.newBuilder()
                                .addCostCategory(CostCategory.ON_DEMAND_COMPUTE).build())
                        .setQueryId(0L)
                        .setEntityTypeFilter(EntityTypeFilter.newBuilder()
                                .addEntityTypeId(ApiEntityType.VIRTUAL_MACHINE.typeNumber()))
                        .setRegionFilter(RegionFilter.newBuilder().addRegionId(5L))
                        .setRequestProjected(true)
                        .build()).build()
        ));
    }

    /**
     * Tests getting the costs for a group vms.
     *
     * @throws Exception if something goes wrong.
     */
    @Test
    public void testGetAggregateStatsGroupOfVms() throws Exception {
        // ARRANGE
        StatFilterApiDTO computeFilter = new StatFilterApiDTO();
        computeFilter.setType(CloudCostsStatsSubQuery.COST_COMPONENT);
        computeFilter.setValue(CostCategory.ON_DEMAND_COMPUTE.name());
        StatApiInputDTO vmCostStatApi = new StatApiInputDTO();
        vmCostStatApi.setName(CloudCostsStatsSubQuery.COST_PRICE_QUERY_KEY );
        vmCostStatApi.setRelatedEntityType(ApiEntityType.VIRTUAL_MACHINE.apiStr());
        vmCostStatApi.setFilters(Collections.singletonList(computeFilter));
        final Set<StatApiInputDTO> requestedStats = Collections.singleton(vmCostStatApi);
        final StatsQueryContext context = mock(StatsQueryContext.class);
        final ApiId inputScope = mock(ApiId.class);
        when(inputScope.getScopeTypes()).thenReturn(Optional.of(Collections.singleton(ApiEntityType.VIRTUAL_MACHINE)));
        final StatsQueryScope queryScope = mock(StatsQueryScope.class);

        // Behaviors associated to context
        when(context.getInputScope()).thenReturn(inputScope);
        when(context.getQueryScope()).thenReturn(queryScope);
        when(context.getTimeWindow()).thenReturn(Optional.empty());
        when(context.requestProjected()).thenReturn(true);
        when(context.getPlanInstance()).thenReturn(Optional.empty());

        // Behaviors associated to inputScope
        when(inputScope.isRealtimeMarket()).thenReturn(false);
        when(inputScope.isPlan()).thenReturn(false);
        when(inputScope.isCloud()).thenReturn(true);
        when(inputScope.isEntity()).thenReturn(false);
        when(inputScope.isGroup()).thenReturn(true);
        when(inputScope.isResourceGroupOrGroupOfResourceGroups()).thenReturn(false);
        final UuidMapper.CachedGroupInfo cachedGroupInfo = mock(UuidMapper.CachedGroupInfo.class);
        when(cachedGroupInfo.getEntityTypes())
                .thenReturn(Collections.singleton(ApiEntityType.VIRTUAL_MACHINE));
        when(cachedGroupInfo.getNestedGroupTypes())
                .thenReturn(Collections.emptySet());
        when(cachedGroupInfo.getGroupType()).thenReturn(GroupType.REGULAR);
        when(inputScope.getCachedGroupInfo()).thenReturn(Optional.of(cachedGroupInfo));

        final GlobalScope globalScope = mock(GlobalScope.class);
        when(context.getQueryScope().getGlobalScope()).thenReturn(Optional.of(globalScope));

        // Behaviors associated to costRpcService
        ArgumentCaptor<Cost.GetCloudCostStatsRequest> costParamCaptor =
                ArgumentCaptor.forClass(Cost.GetCloudCostStatsRequest.class);
        Cost.GetCloudCostStatsResponse response =
                Cost.GetCloudCostStatsResponse.getDefaultInstance();
        when(costServiceMole.getCloudCostStats(costParamCaptor.capture()))
            .thenReturn(Collections.singletonList(response));

        // Behaviors associated to query scope
        when(queryScope.getExpandedOids()).thenReturn(Collections.singleton(5L));


        // ACT
        query.getAggregateStats(requestedStats, context);

        // ASSERT
        // make sure we made correct call to cost service
        assertThat(costParamCaptor.getValue(), is(GetCloudCostStatsRequest.newBuilder()
                .addCloudCostStatsQuery(CloudCostStatsQuery.newBuilder()
                        .setQueryId(0L)
                        .setRequestProjected(true)
                        .setEntityFilter(EntityFilter.newBuilder().addEntityId(5L))
                        .setEntityTypeFilter(EntityTypeFilter.newBuilder()
                                .addEntityTypeId(ApiEntityType.VIRTUAL_MACHINE.sdkType().getNumber()))
                        .setCostCategoryFilter(CostCategoryFilter.newBuilder()
                                .addCostCategory(CostCategory.ON_DEMAND_COMPUTE))
                        .setCostSourceFilter(CostSourceFilter.newBuilder()
                                .setExclusionFilter(true)
                                .addCostSources(CostSource.BUY_RI_DISCOUNT))
                        .build())
                .build()
        ));
    }

    @Test
    public void testGlobalScope() {
        UuidMapper.ApiId inputScope = mock(UuidMapper.ApiId.class);
        when(inputScope.isRealtimeMarket()).thenReturn(true);
        when(inputScope.isGroup()).thenReturn(false);
        when(inputScope.getScopeTypes()).thenReturn(Optional.empty());
        when(inputScope.isResourceGroupOrGroupOfResourceGroups()).thenReturn(false);

        final StatsQueryContext context = mock(StatsQueryContext.class);
        when(context.getInputScope()).thenReturn(inputScope);

        Cost.AccountExpenseQueryScope.Builder scopeBuilder = query.getAccountScopeBuilder(context);

        Assert.assertTrue(scopeBuilder.getAllAccounts());
        Assert.assertEquals(0, scopeBuilder.getSpecificAccounts().getAccountIdsCount());
    }

    @Test
    public void testBusinessAccountScope() {
        final UuidMapper.ApiId inputScope = mock(UuidMapper.ApiId.class);
        final Set<Long> businessAccounts = Collections.singleton(ACCOUNT_ID_1);
        when(inputScope.getScopeEntitiesByType()).thenReturn(
                ImmutableMap.of(ApiEntityType.BUSINESS_ACCOUNT, businessAccounts));
        final StatsQueryContext context = mock(StatsQueryContext.class);
        when(context.getInputScope()).thenReturn(inputScope);
        final TopologyEntityDTO businessAccount1 = TopologyEntityDTO.newBuilder().setEntityType(
                EntityType.BUSINESS_ACCOUNT_VALUE).setOid(ACCOUNT_ID_1).setTypeSpecificInfo(
                TypeSpecificInfo.newBuilder()
                        .setBusinessAccount(
                                BusinessAccountInfo.newBuilder().setAssociatedTargetId(1))).build();
        final MultiEntityRequest multiEntityRequest = ApiTestUtils.mockMultiFullEntityReq(
                Collections.singletonList(businessAccount1));
        Mockito.when(repositoryApi.entitiesRequest(businessAccounts)).thenReturn(
                multiEntityRequest);
        final Cost.AccountExpenseQueryScope.Builder scopeBuilder = query.getAccountScopeBuilder(
                context);
        Assert.assertFalse(scopeBuilder.getAllAccounts());
        Assert.assertEquals(1, scopeBuilder.getSpecificAccounts().getAccountIdsCount());
        Assert.assertEquals(ACCOUNT_ID_1, scopeBuilder.getSpecificAccounts().getAccountIds(0));
    }

    /**
     * Test for {@link CloudCostsStatsSubQuery#getAccountScopeBuilder(StatsQueryContext)}.
     */
    @Test
    public void testBillingFamilyScope() {
        final UuidMapper.CachedGroupInfo cachedGroupInfo = mock(UuidMapper.CachedGroupInfo.class);
        when(cachedGroupInfo.getEntityIds()).thenReturn(
                Sets.newHashSet(ACCOUNT_ID_1, ACCOUNT_ID_2));

        final UuidMapper.ApiId inputScope = mock(UuidMapper.ApiId.class);

        final Set<Long> businessAccounts = ImmutableSet.of(ACCOUNT_ID_1, ACCOUNT_ID_2);
        Mockito.when(inputScope.getScopeEntitiesByType()).thenReturn(
                ImmutableMap.of(ApiEntityType.BUSINESS_ACCOUNT, businessAccounts));

        final StatsQueryContext context = mock(StatsQueryContext.class);
        when(context.getInputScope()).thenReturn(inputScope);

        final TopologyEntityDTO businessAccount1 = TopologyEntityDTO.newBuilder().setEntityType(
                EntityType.BUSINESS_ACCOUNT_VALUE).setOid(ACCOUNT_ID_1).setTypeSpecificInfo(
                TypeSpecificInfo.newBuilder()
                        .setBusinessAccount(
                                BusinessAccountInfo.newBuilder().setAssociatedTargetId(1))).build();
        final TopologyEntityDTO businessAccount2 = TopologyEntityDTO.newBuilder().setEntityType(
                EntityType.BUSINESS_ACCOUNT_VALUE).setOid(ACCOUNT_ID_2).build();
        final MultiEntityRequest multiEntityRequest = ApiTestUtils.mockMultiFullEntityReq(
                Arrays.asList(businessAccount1, businessAccount2));
        Mockito.when(repositoryApi.entitiesRequest(businessAccounts)).thenReturn(
                multiEntityRequest);
        final Cost.AccountExpenseQueryScope.Builder scopeBuilder = query.getAccountScopeBuilder(
                context);
        Assert.assertFalse(scopeBuilder.getAllAccounts());
        Assert.assertEquals(
                "If scope is a billing family, then scope should be resolved up to discovered accounts.",
                Collections.singletonList(ACCOUNT_ID_1),
                scopeBuilder.getSpecificAccounts().getAccountIdsList());
    }

    /**
     * Tests for {@link CloudCostsStatsSubQuery#getCostCategoryString(StatRecord)} method.
     */
    @Test
    public void testGetCostCategoryString() {
        final StatRecord onDemandComputeRecord = StatRecord.newBuilder()
                .setCategory(CostCategory.ON_DEMAND_COMPUTE).build();
        Assert.assertEquals("ON_DEMAND_COMPUTE",
                CloudCostsStatsSubQuery.getCostCategoryString(onDemandComputeRecord));

        final StatRecord spotRecord = StatRecord.newBuilder()
                .setCategory(CostCategory.SPOT).build();
        Assert.assertEquals("SPOT",
                CloudCostsStatsSubQuery.getCostCategoryString(spotRecord));

        final StatRecord nullRecord = StatRecord.newBuilder().build();
        Assert.assertNull(CloudCostsStatsSubQuery.getCostCategoryString(nullRecord));
    }
}
