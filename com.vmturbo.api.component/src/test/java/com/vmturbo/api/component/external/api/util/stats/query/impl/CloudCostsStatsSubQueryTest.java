package com.vmturbo.api.component.external.api.util.stats.query.impl;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.vmturbo.api.component.communication.RepositoryApi;
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
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.UIEntityType;
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
            .setEntityType(UIEntityType.BUSINESS_ACCOUNT.typeNumber())
            .setOid(ACCOUNT_ID_1)
            .build();

    private final MinimalEntity businessAccount2 = MinimalEntity.newBuilder()
            .setDisplayName(ACCOUNT_NAME_2)
            .setEntityType(UIEntityType.BUSINESS_ACCOUNT.typeNumber())
            .setOid(ACCOUNT_ID_2)
            .build();

    private final MinimalEntity cloudService1 = MinimalEntity.newBuilder()
            .setDisplayName(CLOUD_SERVICE_NAME_1)
            .setEntityType(UIEntityType.CLOUD_SERVICE.typeNumber())
            .setOid(CLOUD_SERVICE_ID_1)
            .build();

    private final MinimalEntity cloudService2 = MinimalEntity.newBuilder()
            .setDisplayName(CLOUD_SERVICE_NAME_2)
            .setEntityType(UIEntityType.CLOUD_SERVICE.typeNumber())
            .setOid(CLOUD_SERVICE_ID_2)
            .build();

    private final ThinTargetCache.ThinTargetInfo targetInfo1 = ImmutableThinTargetInfo.builder()
            .oid(TARGET_ID_1)
            .displayName("target1")
            .isHidden(false)
            .probeInfo(ImmutableThinProbeInfo.builder()
                    .oid(1L)
                    .category("Cloud")
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
                    .type(SDKProbeType.AZURE_EA.getProbeType())
                    .build())
            .build();

    @Before
    public void setup() throws OperationFailedException {
        MockitoAnnotations.initMocks(this);
        CostServiceBlockingStub costRpc = CostServiceGrpc.newBlockingStub(grpcTestServer.getChannel());
        when(thinTargetCache.getTargetInfo(TARGET_ID_1)).thenReturn(Optional.of(targetInfo1));
        when(thinTargetCache.getTargetInfo(TARGET_ID_2)).thenReturn(Optional.of(targetInfo2));
        final SupplyChainNodeFetcherBuilder nodeFetcherBuilder =
                mockSupplyChainNodeFetcherBuilder();
        when(supplyChainFetcherFactory.newNodeFetcher()).thenReturn(nodeFetcherBuilder);
        query = spy(new CloudCostsStatsSubQuery(repositoryApi, costRpc,
                supplyChainFetcherFactory, thinTargetCache, new BuyRiScopeHandler(), storageStatsSubQuery, userSessionContext ));
    }

    private SupplyChainNodeFetcherBuilder mockSupplyChainNodeFetcherBuilder()
            throws OperationFailedException {
        final SupplyChainNodeFetcherBuilder nodeFetcherBuilder =
                mock(SupplyChainNodeFetcherBuilder.class);
        when(nodeFetcherBuilder.addSeedOids(any())).thenReturn(nodeFetcherBuilder);
        when(nodeFetcherBuilder.environmentType(any())).thenReturn(nodeFetcherBuilder);
        when(nodeFetcherBuilder.entityTypes(any())).thenReturn(nodeFetcherBuilder);
        when(nodeFetcherBuilder.fetchEntityIds()).thenReturn(billingFamilyEntityIds);
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
        when(globalScope.entityTypes()).thenReturn(Sets.newHashSet(UIEntityType.VIRTUAL_VOLUME));
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
        when(globalScope.entityTypes()).thenReturn(Sets.newHashSet(UIEntityType.VIRTUAL_VOLUME));
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
     * Test getAggregatedStats for resource group. Resource group contains only Virtual Volume as
     * member, so request cost cloud stats only for this entity type.
     *
     * @throws OperationFailedException on exceptions occur
     */
    @Test
    public void testGetAggregateStatsForResourceGroupScope() throws OperationFailedException {
        final Set<StatApiInputDTO> requestedStats = createRequestStatsForResourceGroup();

        final UuidMapper.CachedGroupInfo cachedGroupInfo = mock(UuidMapper.CachedGroupInfo.class);
        when(cachedGroupInfo.getGroupType()).thenReturn(GroupType.RESOURCE);

        final StatsQueryContext context = mock(StatsQueryContext.class);

        final ApiId apiId = mock(ApiId.class);
        when(apiId.getScopeTypes()).thenReturn(
                Optional.of(Sets.newHashSet(UIEntityType.VIRTUAL_VOLUME)));
        when(apiId.isCloud()).thenReturn(true);
        when(apiId.getCachedGroupInfo()).thenReturn(Optional.of(cachedGroupInfo));
        when(context.getInputScope()).thenReturn(apiId);
        when(context.getTimeWindow()).thenReturn(Optional.empty());

        final StatsQueryScope queryScope = mock(StatsQueryScope.class);
        when(queryScope.getScopeOids()).thenReturn(Sets.newHashSet(1L, 2L));
        when(context.getQueryScope()).thenReturn(queryScope);
        when(context.getPlanInstance()).thenReturn(Optional.empty());

        // test that there is no scope expanding for resource groups
        verify(context.getQueryScope(), times(0)).getExpandedOids();

        final ArgumentCaptor<Cost.GetCloudCostStatsRequest> costParamCaptor =
                ArgumentCaptor.forClass(Cost.GetCloudCostStatsRequest.class);
        final Cost.GetCloudCostStatsResponse response =
                Cost.GetCloudCostStatsResponse.getDefaultInstance();
        when(costServiceMole.getCloudCostStats(costParamCaptor.capture())).thenReturn(response);

        query.getAggregateStats(requestedStats, context);
        final List<CloudCostStatsQuery> cloudCostStatsQueryList =
                costParamCaptor.getValue().getCloudCostStatsQueryList();

        // Check that requests cost only for entity types existed in resource group.
        // In our case request only stats for volume and don't request for vms, dbs and
        // dbServers
        Assert.assertEquals(cloudCostStatsQueryList.size(), 1);
        final List<Integer> entityTypeIds =
                cloudCostStatsQueryList.get(0).getEntityTypeFilter().getEntityTypeIdList();
        Assert.assertEquals(entityTypeIds.size(), 1);
        Assert.assertEquals(EntityType.VIRTUAL_VOLUME.getNumber(), entityTypeIds.get(0).intValue());
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
                Collections.singleton(UIEntityType.BUSINESS_ACCOUNT));
        final StatsQueryContext context = createStatsQueryContextMock(apiId);

        // when
        final List<StatSnapshotApiDTO> result = query.getAggregateStats(requestedStats, context);

        // then
        Assert.assertFalse(result.isEmpty());
        final StatSnapshotApiDTO apiDTO = result.iterator().next();
        Assert.assertFalse(apiDTO.getStatistics().isEmpty());
        final StatApiDTO statApiDTO = apiDTO.getStatistics().iterator().next();
        Assert.assertEquals(billingFamilyEntityIds.size(), statApiDTO.getValue(), 0);
    }

    private ApiId createApiIdMock(final GroupType groupType, final Set<UIEntityType> scopeTypes) {
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
        final StatsQueryScope queryScope = mock(StatsQueryScope.class);
        when(queryScope.getScopeOids()).thenReturn(Sets.newHashSet(1L));
        when(context.getQueryScope()).thenReturn(queryScope);
        when(context.getPlanInstance()).thenReturn(Optional.empty());
        return context;
    }

    private StatApiInputDTO createNumWorkloadsInputDto() {
        final StatFilterApiDTO filterApiDTO = new StatFilterApiDTO();
        filterApiDTO.setType("environmentType");
        filterApiDTO.setValue("CLOUD");
        final StatApiInputDTO inputDTO = new StatApiInputDTO();
        inputDTO.setName("numWorkloads");
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
                .setAssociatedEntityType(UIEntityType.CLOUD_SERVICE.typeNumber())
                .setUnits("$/h")
                .setValues(StatValue.newBuilder().setAvg(cost).build())
                .build();
    }

    private Set<StatApiInputDTO> createRequestStats() {
        return Collections.singleton(createRequestStat(CloudCostsStatsSubQuery.COST_PRICE_QUERY_KEY,
                UIEntityType.VIRTUAL_MACHINE.apiStr()));
    }

    private Set<StatApiInputDTO> createRequestStatsForResourceGroup() {
        final StatApiInputDTO vmStat =
                createRequestStat(CloudCostsStatsSubQuery.COST_PRICE_QUERY_KEY,
                        UIEntityType.VIRTUAL_MACHINE.apiStr());
        final StatApiInputDTO dbServerStat =
                createRequestStat(CloudCostsStatsSubQuery.COST_PRICE_QUERY_KEY,
                        UIEntityType.DATABASE_SERVER.apiStr());
        final StatApiInputDTO dbStat =
                createRequestStat(CloudCostsStatsSubQuery.COST_PRICE_QUERY_KEY,
                        UIEntityType.DATABASE.apiStr());
        final StatApiInputDTO volumeStat =
                createRequestStat(CloudCostsStatsSubQuery.COST_PRICE_QUERY_KEY,
                        UIEntityType.VIRTUAL_VOLUME.apiStr());
        return Sets.newHashSet(vmStat, dbStat, dbServerStat, volumeStat);
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
            Collections.singleton(UIEntityType.BUSINESS_ACCOUNT)));
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
        final UuidMapper.CachedGroupInfo cachedGroupInfo = mock(UuidMapper.CachedGroupInfo.class);
        when(cachedGroupInfo.getEntityTypes())
            .thenReturn(Collections.singleton(UIEntityType.VIRTUAL_MACHINE));
        when(cachedGroupInfo.getGroupType()).thenReturn(GroupType.BILLING_FAMILY);
        when(inputScope.getCachedGroupInfo()).thenReturn(Optional.of(cachedGroupInfo));

        // Behaviors associated to costRpcService
        ArgumentCaptor<Cost.GetCloudCostStatsRequest> costParamCaptor =
            ArgumentCaptor.forClass(Cost.GetCloudCostStatsRequest.class);
        Cost.GetCloudCostStatsResponse response =
            Cost.GetCloudCostStatsResponse.getDefaultInstance();
        when(costServiceMole.getCloudCostStats(costParamCaptor.capture())).thenReturn(response);

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
                .addEntityTypeId(UIEntityType.VIRTUAL_MACHINE.typeNumber()))
            .setAccountFilter(AccountFilter.newBuilder().addAccountId(5L).build())
            .setRequestProjected(true)
           .build()).build()));
    }

    /**gi
     * Tests getting the costs for for cloud tab of realtime market.
     *
     * @throws Exception if something goes wrong.
     */
    @Test
    public void testGetAggregateStatsCloudTab() throws Exception {
        // ARRANGE
        final Set<StatApiInputDTO> requestedStats = createRequestStats();
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
        // Behaviors associated to costRpcService
        ArgumentCaptor<Cost.GetCloudCostStatsRequest> costParamCaptor =
            ArgumentCaptor.forClass(Cost.GetCloudCostStatsRequest.class);
        Cost.GetCloudCostStatsResponse response =
            Cost.GetCloudCostStatsResponse.getDefaultInstance();
        when(costServiceMole.getCloudCostStats(costParamCaptor.capture())).thenReturn(response);

        // Behaviors associated to query scope
        when(queryScope.getExpandedOids()).thenReturn(Collections.emptySet());


        // ACT
        query.getAggregateStats(requestedStats, context);

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
        when(inputScope.getScopeTypes()).thenReturn(Optional.of(Collections.singleton(UIEntityType.REGION)));
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
                .of(Collections.singleton(UIEntityType.REGION)));
        when(inputScope.getCachedGroupInfo()).thenReturn(Optional.empty());

        // Behaviors associated to costRpcService
        ArgumentCaptor<Cost.GetCloudCostStatsRequest> costParamCaptor =
            ArgumentCaptor.forClass(GetCloudCostStatsRequest.class);
        Cost.GetCloudCostStatsResponse response =
            Cost.GetCloudCostStatsResponse.getDefaultInstance();
        when(costServiceMole.getCloudCostStats(costParamCaptor.capture())).thenReturn(response);

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
                                .addEntityTypeId(UIEntityType.VIRTUAL_MACHINE.typeNumber()))
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
        when(inputScope.getScopeTypes()).thenReturn(Optional.of(Collections.singleton(UIEntityType.BUSINESS_ACCOUNT)));
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
        final UuidMapper.CachedGroupInfo cachedGroupInfo = mock(UuidMapper.CachedGroupInfo.class);
        when(cachedGroupInfo.getEntityTypes())
                .thenReturn(Collections.singleton(UIEntityType.VIRTUAL_MACHINE));
        when(cachedGroupInfo.getNestedGroupTypes())
            .thenReturn(Collections.singleton(GroupType.BILLING_FAMILY));
        when(cachedGroupInfo.getGroupType()).thenReturn(GroupType.REGULAR);
        when(inputScope.getCachedGroupInfo()).thenReturn(Optional.of(cachedGroupInfo));

        // Behaviors associated to costRpcService
        ArgumentCaptor<Cost.GetCloudCostStatsRequest> costParamCaptor =
            ArgumentCaptor.forClass(GetCloudCostStatsRequest.class);
        Cost.GetCloudCostStatsResponse response =
            Cost.GetCloudCostStatsResponse.getDefaultInstance();
        when(costServiceMole.getCloudCostStats(costParamCaptor.capture())).thenReturn(response);

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
                                .addEntityTypeId(UIEntityType.VIRTUAL_MACHINE.typeNumber()))
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
        when(inputScope.getScopeTypes()).thenReturn(Optional.of(Collections.singleton(UIEntityType.REGION)));
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
        final UuidMapper.CachedGroupInfo cachedGroupInfo = mock(UuidMapper.CachedGroupInfo.class);
        when(cachedGroupInfo.getEntityTypes())
            .thenReturn(Collections.singleton(UIEntityType.REGION));
        when(cachedGroupInfo.getNestedGroupTypes())
                .thenReturn(Collections.emptySet());
        when(cachedGroupInfo.getGroupType()).thenReturn(GroupType.REGULAR);
        when(inputScope.getCachedGroupInfo()).thenReturn(Optional.of(cachedGroupInfo));

        // Behaviors associated to costRpcService
        ArgumentCaptor<Cost.GetCloudCostStatsRequest> costParamCaptor =
                ArgumentCaptor.forClass(Cost.GetCloudCostStatsRequest.class);
        Cost.GetCloudCostStatsResponse response =
                Cost.GetCloudCostStatsResponse.getDefaultInstance();
        when(costServiceMole.getCloudCostStats(costParamCaptor.capture())).thenReturn(response);

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
                                .addEntityTypeId(UIEntityType.VIRTUAL_MACHINE.typeNumber()))
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
        vmCostStatApi.setRelatedEntityType(UIEntityType.VIRTUAL_MACHINE.apiStr());
        vmCostStatApi.setFilters(Collections.singletonList(computeFilter));
        final Set<StatApiInputDTO> requestedStats = Collections.singleton(vmCostStatApi);
        final StatsQueryContext context = mock(StatsQueryContext.class);
        final ApiId inputScope = mock(ApiId.class);
        when(inputScope.getScopeTypes()).thenReturn(Optional.of(Collections.singleton(UIEntityType.VIRTUAL_MACHINE)));
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
        final UuidMapper.CachedGroupInfo cachedGroupInfo = mock(UuidMapper.CachedGroupInfo.class);
        when(cachedGroupInfo.getEntityTypes())
                .thenReturn(Collections.singleton(UIEntityType.VIRTUAL_MACHINE));
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
        when(costServiceMole.getCloudCostStats(costParamCaptor.capture())).thenReturn(response);

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
                                .addEntityTypeId(UIEntityType.VIRTUAL_MACHINE.sdkType().getNumber()))
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

        final StatsQueryContext context = mock(StatsQueryContext.class);
        when(context.getInputScope()).thenReturn(inputScope);

        Cost.AccountExpenseQueryScope.Builder scopeBuilder = query.getAccountScopeBuilder(context);

        Assert.assertTrue(scopeBuilder.getAllAccounts());
        Assert.assertEquals(0, scopeBuilder.getSpecificAccounts().getAccountIdsCount());
    }

    @Test
    public void testBusinessAccountScope() {
        UuidMapper.ApiId inputScope = mock(UuidMapper.ApiId.class);
        when(inputScope.isRealtimeMarket()).thenReturn(false);
        when(inputScope.isGroup()).thenReturn(false);
        when(inputScope.getScopeTypes())
                .thenReturn(Optional.of(Collections.singleton(UIEntityType.BUSINESS_ACCOUNT)));
        when(inputScope.oid()).thenReturn(ACCOUNT_ID_1);

        final StatsQueryContext context = mock(StatsQueryContext.class);
        when(context.getInputScope()).thenReturn(inputScope);

        Cost.AccountExpenseQueryScope.Builder scopeBuilder = query.getAccountScopeBuilder(context);

        Assert.assertFalse(scopeBuilder.getAllAccounts());
        Assert.assertEquals(1, scopeBuilder.getSpecificAccounts().getAccountIdsCount());
        Assert.assertEquals(ACCOUNT_ID_1, scopeBuilder.getSpecificAccounts().getAccountIds(0));
    }

    @Test
    public void testBillingFamilyScope() {
        UuidMapper.CachedGroupInfo cachedGroupInfo = mock(UuidMapper.CachedGroupInfo.class);
        when(cachedGroupInfo.getEntityIds()).thenReturn(Sets.newHashSet(ACCOUNT_ID_1, ACCOUNT_ID_2));

        UuidMapper.ApiId inputScope = mock(UuidMapper.ApiId.class);
        when(inputScope.isRealtimeMarket()).thenReturn(false);
        when(inputScope.isGroup()).thenReturn(true);
        when(inputScope.getCachedGroupInfo()).thenReturn(Optional.of(cachedGroupInfo));
        when(inputScope.getScopeTypes())
                .thenReturn(Optional.of(Collections.singleton(UIEntityType.BUSINESS_ACCOUNT)));
        when(inputScope.oid()).thenReturn(BILLING_FAMILY_ID);

        final StatsQueryContext context = mock(StatsQueryContext.class);
        when(context.getInputScope()).thenReturn(inputScope);

        Cost.AccountExpenseQueryScope.Builder scopeBuilder = query.getAccountScopeBuilder(context);

        Assert.assertFalse(scopeBuilder.getAllAccounts());
        Assert.assertEquals(2, scopeBuilder.getSpecificAccounts().getAccountIdsCount());
        assertThat(scopeBuilder.getSpecificAccounts().getAccountIdsList(),
                containsInAnyOrder(ACCOUNT_ID_1, ACCOUNT_ID_2));
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
