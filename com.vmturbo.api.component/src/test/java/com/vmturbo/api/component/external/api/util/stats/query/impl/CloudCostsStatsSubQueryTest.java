package com.vmturbo.api.component.external.api.util.stats.query.impl;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isOneOf;
import static org.hamcrest.number.IsCloseTo.closeTo;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anySetOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.communication.RepositoryApi.SearchRequest;
import com.vmturbo.api.component.external.api.mapper.UuidMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.component.external.api.util.BuyRiScopeHandler;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory;
import com.vmturbo.api.component.external.api.util.stats.ImmutableTimeWindow;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryContextFactory.StatsQueryContext;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryScopeExpander.GlobalScope;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryScopeExpander.StatsQueryScope;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.statistic.StatApiInputDTO;
import com.vmturbo.api.dto.statistic.StatFilterApiDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.cost.Cost;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatRecord;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatRecord.StatRecord;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatRecord.StatRecord.StatValue;
import com.vmturbo.common.protobuf.cost.CostMoles.CostServiceMole;
import com.vmturbo.common.protobuf.cost.CostServiceGrpc;
import com.vmturbo.common.protobuf.cost.CostServiceGrpc.CostServiceBlockingStub;
import com.vmturbo.common.protobuf.search.Search;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity.RelatedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;
import com.vmturbo.platform.common.dto.CommonDTOREST.EntityDTO.EntityType;
import com.vmturbo.topology.processor.api.util.ThinTargetCache;

public class CloudCostsStatsSubQueryTest {
    private static final long MILLIS = 1_000_000;

    private CostServiceMole costServiceMole = spy(new CostServiceMole());
    @Rule
    public GrpcTestServer grpcTestServer = GrpcTestServer.newServer(costServiceMole);

    CloudCostsStatsSubQuery query;

    CostServiceBlockingStub costRpc;

    @Mock
    private RepositoryApi repositoryApi;

    @Mock
    private SupplyChainFetcherFactory supplyChainFetcherFactory;
    @Mock
    private ThinTargetCache thinTargetCache;

    private static final long ACCOUNT_ID_1 = 1L;
    private static final long ACCOUNT_ID_2 = 2L;
    private static final long BILLING_FAMILY_ID = 3L;
    private static final long CLOUD_SERVICE_ID_1 = 1L;
    private static final long CLOUD_SERVICE_ID_2 = 2L;
    private static final String CLOUD_SERVICE_NAME_1 = "CLOUD_SERVICE_1";
    private static final String CLOUD_SERVICE_NAME_2 = "CLOUD_SERVICE_2";

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

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        CostServiceBlockingStub costRpc = CostServiceGrpc.newBlockingStub(grpcTestServer.getChannel());
        query = Mockito.spy(new CloudCostsStatsSubQuery(repositoryApi, costRpc,
                supplyChainFetcherFactory, thinTargetCache, new BuyRiScopeHandler()));
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

    @Test
    public void testGetGroupByAttachmentStat() {
        final long attachedVolumeId1 = 111L;
        final long attachedVolumeId2 = 222L;
        final long unattachedVolumeId = 333L;
        final long storageTierOid = 99999L;

        CloudCostStatRecord ccsr = CloudCostStatRecord.newBuilder()
            .addStatRecords(
                StatRecord.newBuilder()
                    .setAssociatedEntityId(attachedVolumeId1)
                    .setAssociatedEntityType(EntityType.VIRTUAL_VOLUME.getValue())
                    .setValues(StatValue.newBuilder().setAvg(1).setMin(0).setMax(2).setTotal(3).build())
                    .build()
            )
            .addStatRecords(
                StatRecord.newBuilder()
                    .setAssociatedEntityId(attachedVolumeId2)
                    .setAssociatedEntityType(EntityType.VIRTUAL_VOLUME.getValue())
                    .setValues(StatValue.newBuilder().setAvg(1).setMin(0).setMax(2).setTotal(3).build())
                    .build()
            )
            .addStatRecords(
                StatRecord.newBuilder()
                    .setAssociatedEntityId(unattachedVolumeId)
                    .setAssociatedEntityType(EntityType.VIRTUAL_VOLUME.getValue())
                    .setValues(StatValue.newBuilder().setAvg(3).setMin(2).setMax(4).setTotal(4).build())
                    .build()
            )
            .build();

        // Setup Context for Global Scope
        final StatsQueryContext context = setupGlobalScope(UIEntityType.VIRTUAL_VOLUME);

        // When trying to get the list of attached VV from repository service
        List<PartialEntity.ApiPartialEntity> attachedVVEntities = Arrays.asList(
            createApiPartialEntity(attachedVolumeId1, storageTierOid),
            createApiPartialEntity(attachedVolumeId2, storageTierOid)
        );
        SearchRequest searchRequest = mock(SearchRequest.class);
        when(repositoryApi.newSearchRequest(any(Search.SearchParameters.class)))
            .thenReturn(searchRequest);
        //when(searchRequest.getExpandedOids()).thenReturn(attachedVVEntities.stream());
        when(searchRequest.getOids()).thenReturn(attachedVVEntities.stream().mapToLong(e -> e.getOid()).boxed().collect(Collectors.toSet()));


        final Set<StatApiInputDTO> requestedStats = Sets.newHashSet(
            createStatApiInputDTO(StringConstants.COST_PRICE, UIEntityType.VIRTUAL_VOLUME, Collections.singletonList(StringConstants.ATTACHMENT))
        );
        List<StatSnapshotApiDTO> results = query
            .getGroupByVVAttachmentStat(Collections.singletonList(ccsr),
                                       requestedStats);

        assertThat(results.size(), is(1));
        StatSnapshotApiDTO result = results.get(0);
        assertThat(result.getDisplayName(), is(StringConstants.COST_PRICE));
        List<StatApiDTO> stats = result.getStatistics();
        assertThat(stats.size(), is(2));

        stats.stream().forEach(stat -> {
            assertThat(stat.getName(), is(StringConstants.COST_PRICE));
            assertThat(stat.getUnits(), is(StringConstants.DOLLARS_PER_HOUR));
            List<StatFilterApiDTO> filters = stat.getFilters();
            assertThat(filters.size(), is(1));
            StatFilterApiDTO filter = filters.get(0);
            assertThat(filter.getType(), is(StringConstants.ATTACHMENT));
            assertThat(filter.getValue(), isOneOf(StringConstants.ATTACHED, StringConstants.UNATTACHED));
            if (filter.getValue().equals(StringConstants.ATTACHED)) {
                assertThat(stat.getValue(), is(1f));
            } else {
                assertThat(stat.getValue(), is(3f));
            }
        });
    }

    /**
     * Tests the case where we are getting the stats for entities inside an account.
     */
    @Test
    public void testAggregateAccountStats() {
        // ARRANGE
        CloudCostsStatsSubQuery.CloudStatRecordAggregator aggregator =
            new CloudCostsStatsSubQuery.CloudStatRecordAggregator();

        StatRecord vmCompute = StatRecord.newBuilder()
            .setAssociatedEntityType(UIEntityType.VIRTUAL_MACHINE.typeNumber())
            .setCategory(Cost.CostCategory.ON_DEMAND_COMPUTE)
            .setAssociatedEntityId(1L)
            .setValues(StatValue.newBuilder().setAvg(10.0f).build())
            .build();

        StatRecord vmStorage = StatRecord.newBuilder()
            .setAssociatedEntityType(UIEntityType.VIRTUAL_MACHINE.typeNumber())
            .setCategory(Cost.CostCategory.STORAGE)
            .setAssociatedEntityId(1L)
            .setValues(StatValue.newBuilder().setAvg(23.0f).build())
            .build();

        StatRecord volumeStorage = StatRecord.newBuilder()
            .setAssociatedEntityType(UIEntityType.VIRTUAL_VOLUME.typeNumber())
            .setCategory(Cost.CostCategory.ON_DEMAND_COMPUTE)
            .setAssociatedEntityId(2L)
            .setValues(StatValue.newBuilder().setAvg(23.0f).build())
            .build();

        StatRecord detachedVolumeStorage = StatRecord.newBuilder()
            .setAssociatedEntityType(UIEntityType.VIRTUAL_VOLUME.typeNumber())
            .setCategory(Cost.CostCategory.ON_DEMAND_COMPUTE)
            .setAssociatedEntityId(3L)
            .setValues(StatValue.newBuilder().setAvg(55.0f).build())
            .build();

        CloudCostStatRecord record = CloudCostStatRecord.newBuilder()
            .addStatRecords(vmCompute)
            .addStatRecords(vmStorage)
            .addStatRecords(volumeStorage)
            .addStatRecords(detachedVolumeStorage)
            .build();

        List<CloudCostStatRecord> cloudStatRecords = Collections.singletonList(record);

        Set<StatApiInputDTO> requestedStats = Collections.emptySet();
        StatsQueryContext context = mock(StatsQueryContext.class);
        ApiId apiId = mock(ApiId.class);
        when(context.getInputScope()).thenReturn(apiId);
        when(apiId.getScopeTypes()).thenReturn(
            Optional.of(Collections.singleton(UIEntityType.BUSINESS_ACCOUNT)));


        // ACT
        List<CloudCostStatRecord> aggregatedRecords = aggregator.aggregate(cloudStatRecords,
            requestedStats, context);

        // ASSERT
        assertThat(aggregatedRecords.size(), is(1));
        assertThat(aggregatedRecords.get(0).getStatRecordsCount(), is(1));
        assertThat((double)aggregatedRecords.get(0).getStatRecords(0).getValues().getTotal(),
            closeTo(88d, 0.01d));
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
        Map<Long, MinimalEntity> cloudServiceDTOs = ImmutableMap.<Long, MinimalEntity>builder()
                .put(CLOUD_SERVICE_ID_1, cloudService1)
                .put(CLOUD_SERVICE_ID_2, cloudService2)
                .build();
        Mockito.doReturn(cloudServiceDTOs).when(query).getDiscoveredServiceDTO();

        StatApiInputDTO queryStat = new StatApiInputDTO();
        queryStat.setGroupBy(Collections.singletonList(CloudCostsStatsSubQuery.CLOUD_SERVICE));
        queryStat.setRelatedEntityType("CloudService");
        final Set<StatApiInputDTO> requestedStats = Collections.singleton(queryStat);

        StatsQueryContext context = mock(StatsQueryContext.class);
        ApiId apiId = mock(ApiId.class);
        when(context.getInputScope()).thenReturn(apiId);

        List<CloudCostStatRecord> records = Collections.singletonList(CloudCostStatRecord.newBuilder()
                .setSnapshotDate(1234L)
                .addStatRecords(createCloudServiceStatRecord(CLOUD_SERVICE_ID_1, 10.0f))
                .addStatRecords(createCloudServiceStatRecord(CLOUD_SERVICE_ID_2, 12.0f))
                .build());
        Mockito.doReturn(records).when(query).getCloudExpensesRecordList(anySetOf(String.class),
                anySetOf(Long.class), any(StatsQueryContext.class));

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

    private StatRecord createCloudServiceStatRecord(long cloudServiceId, float cost) {
        return StatRecord.newBuilder()
                .setAssociatedEntityId(cloudServiceId)
                .setAssociatedEntityType(UIEntityType.CLOUD_SERVICE.typeNumber())
                .setUnits("$/h")
                .setValues(StatValue.newBuilder().setAvg(cost).build())
                .build();
    }

    @Test
    public void testGetGroupByStorageTierStat() {
        final long st1VolumeId1 = 901L;
        final long st1VolumeId2 = 902L;
        final long st2VolumeId = 801L;
        final long storageTierOid1 = 99999L;
        final long storageTierOid2 = 88888L;
        final String stDisplayName1 = "storage tier 1";
        final String stDisplayName2 = "storage tier 2";
        final List<MinimalEntity> stMinimalEntities = Arrays.asList(
            createMinimalEntities(storageTierOid1, stDisplayName1, EntityType.STORAGE_TIER),
            createMinimalEntities(storageTierOid2, stDisplayName2, EntityType.STORAGE_TIER)
        );

        CloudCostStatRecord ccsr = CloudCostStatRecord.newBuilder()
            .addStatRecords(
                StatRecord.newBuilder()
                    .setAssociatedEntityId(st1VolumeId1)
                    .setAssociatedEntityType(EntityType.VIRTUAL_VOLUME.getValue())
                    .setValues(StatValue.newBuilder().setAvg(1).setMin(0).setMax(2).setTotal(3).build())
                    .build()
            )
            .addStatRecords(
                StatRecord.newBuilder()
                    .setAssociatedEntityId(st1VolumeId2)
                    .setAssociatedEntityType(EntityType.VIRTUAL_VOLUME.getValue())
                    .setValues(StatValue.newBuilder().setAvg(1).setMin(0).setMax(2).setTotal(3).build())
                    .build()
            )
            .addStatRecords(
                StatRecord.newBuilder()
                    .setAssociatedEntityId(st2VolumeId)
                    .setAssociatedEntityType(EntityType.VIRTUAL_VOLUME.getValue())
                    .setValues(StatValue.newBuilder().setAvg(3).setMin(2).setMax(4).setTotal(4).build())
                    .build()
            )
            .build();

        // Setup Context for Global Scope
        final StatsQueryContext context = setupGlobalScope(UIEntityType.VIRTUAL_VOLUME);

        // When trying to get the list of attached VV from repository service
        List<PartialEntity.ApiPartialEntity> attachedVVEntities = Arrays.asList(
            createApiPartialEntity(st1VolumeId1, storageTierOid1),
            createApiPartialEntity(st1VolumeId2, storageTierOid1),
            createApiPartialEntity(st2VolumeId, storageTierOid2)
        );
        SearchRequest searchRequest = mock(SearchRequest.class);
        ArgumentCaptor<SearchParameters> searchParametersArgumentCaptor = new ArgumentCaptor<>();
        when(repositoryApi.newSearchRequest(searchParametersArgumentCaptor.capture()))
            .thenReturn(searchRequest);
        when(searchRequest.getEntities()).thenReturn(attachedVVEntities.stream());
        when(searchRequest.getMinimalEntities()).thenReturn(stMinimalEntities.stream());

        final Set<StatApiInputDTO> requestedStats = Sets.newHashSet(
            createStatApiInputDTO(StringConstants.COST_PRICE, UIEntityType.VIRTUAL_VOLUME, Collections.singletonList(UIEntityType.STORAGE_TIER.apiStr()))
        );
        List<StatSnapshotApiDTO> results = query
            .getGroupByStorageTierStat(Collections.singletonList(ccsr),
                requestedStats);

        assertThat(results.size(), is(1));
        StatSnapshotApiDTO result = results.get(0);
        assertThat(result.getDisplayName(), is(StringConstants.COST_PRICE));
        List<StatApiDTO> stats = result.getStatistics();
        assertThat(stats.size(), is(2));

        stats.stream().forEach(stat -> {
            assertThat(stat.getName(), is(StringConstants.COST_PRICE));
            assertThat(stat.getUnits(), is(StringConstants.DOLLARS_PER_HOUR));
            List<StatFilterApiDTO> filters = stat.getFilters();
            assertThat(filters.size(), is(1));
            StatFilterApiDTO filter = filters.get(0);
            assertThat(filter.getType(), is(UIEntityType.STORAGE_TIER.apiStr()));
            assertThat(filter.getValue(), isOneOf(stDisplayName1, stDisplayName2));
            if (filter.getValue().equals(stDisplayName1)) {
                assertThat(stat.getValue(), is(1f));
            } else if (filter.getValue().equals(stDisplayName2)) {
                assertThat(stat.getValue(), is(3f));
            }
        });
    }

    private Set<StatApiInputDTO> createRequestStats() {
        StatFilterApiDTO computeFilter = new StatFilterApiDTO();
        computeFilter.setType("costComponent");
        computeFilter.setValue("COMPUTE");
        StatApiInputDTO vmCostStatApi = new StatApiInputDTO();
        vmCostStatApi.setName("cost");
        vmCostStatApi.setRelatedEntityType(UIEntityType.VIRTUAL_MACHINE.apiStr());
        vmCostStatApi.setFilters(Collections.singletonList(computeFilter));
        return Collections.singleton(vmCostStatApi);
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
        assertThat(costParamCaptor.getValue(), is(Cost.GetCloudCostStatsRequest.newBuilder()
            .setEntityTypeFilter(Cost.EntityTypeFilter.newBuilder()
                .addEntityTypeId(UIEntityType.VIRTUAL_MACHINE.typeNumber()))
            .setAccountFilter(Cost.AccountFilter.newBuilder().addAccountId(5L).build())
            .setRequestProjected(true)
           .build()
        ));
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

        // Behaviors associated to inputScope
        when(inputScope.isRealtimeMarket()).thenReturn(true);
        when(inputScope.isPlan()).thenReturn(false);
        when(inputScope.isCloud()).thenReturn(true);
        when(inputScope.isEntity()).thenReturn(false);
        when(inputScope.isGroup()).thenReturn(false);

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
        assertThat(costParamCaptor.getValue(), is(Cost.GetCloudCostStatsRequest.newBuilder()
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

        // Behaviors associated to inputScope
        when(inputScope.isRealtimeMarket()).thenReturn(false);
        when(inputScope.isPlan()).thenReturn(false);
        when(inputScope.isCloud()).thenReturn(true);
        when(inputScope.isEntity()).thenReturn(true);
        when(inputScope.isGroup()).thenReturn(false);
        when(inputScope.getScopeTypes()).thenReturn(Optional
                .of(Collections.singleton(UIEntityType.REGION)));

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
        assertThat(costParamCaptor.getValue(), is(Cost.GetCloudCostStatsRequest.newBuilder()
            .setEntityTypeFilter(Cost.EntityTypeFilter.newBuilder()
                .addEntityTypeId(UIEntityType.VIRTUAL_MACHINE.typeNumber()))
            .setRegionFilter(Cost.RegionFilter.newBuilder().addRegionId(5L).build())
            .setRequestProjected(true)
            .build()
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
        assertThat(costParamCaptor.getValue(), is(Cost.GetCloudCostStatsRequest.newBuilder()
            .setEntityTypeFilter(Cost.EntityTypeFilter.newBuilder()
                .addEntityTypeId(UIEntityType.VIRTUAL_MACHINE.typeNumber()))
            .setAccountFilter(Cost.AccountFilter.newBuilder().addAccountId(5L).build())
            .setRequestProjected(true)
            .build()
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
        assertThat(costParamCaptor.getValue(), is(Cost.GetCloudCostStatsRequest.newBuilder()
            .setEntityTypeFilter(Cost.EntityTypeFilter.newBuilder()
                .addEntityTypeId(UIEntityType.VIRTUAL_MACHINE.typeNumber()))
            .setRegionFilter(Cost.RegionFilter.newBuilder().addRegionId(5L).build())
            .setRequestProjected(true)
            .build()
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
        computeFilter.setType("costComponent");
        computeFilter.setValue("COMPUTE");
        StatApiInputDTO vmCostStatApi = new StatApiInputDTO();
        vmCostStatApi.setName("cost");
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
        assertThat(costParamCaptor.getValue(), is(Cost.GetCloudCostStatsRequest.newBuilder()
            .setEntityTypeFilter(Cost.EntityTypeFilter.newBuilder()
                .addEntityTypeId(UIEntityType.VIRTUAL_MACHINE.typeNumber()))
            .setEntityFilter(Cost.EntityFilter.newBuilder().addEntityId(5L))
            .setCostSourceFilter(Cost.GetCloudCostStatsRequest.CostSourceFilter.newBuilder()
                .setExclusionFilter(true)
                .addCostSources(Cost.CostSource.BUY_RI_DISCOUNT)
                .build())
            .setRequestProjected(true)
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

    @Nonnull
    private StatsQueryContext setupGlobalScope(@Nonnull UIEntityType entityType) {
        // Setup Context for Global Scope
        final GlobalScope globalScope = mock(GlobalScope.class);
        when(globalScope.entityTypes()).thenReturn(Sets.newHashSet(entityType));
        StatsQueryScope statsQueryScope = mock(StatsQueryScope.class);
        when(statsQueryScope.getGlobalScope()).thenReturn(Optional.of(globalScope));

        UuidMapper.ApiId inputScope = mock(UuidMapper.ApiId.class);
        when(inputScope.isRealtimeMarket()).thenReturn(true);

        final StatsQueryContext context = mock(StatsQueryContext.class);
        when(context.isGlobalScope()).thenReturn(true);
        when(context.getQueryScope()).thenReturn(statsQueryScope);
        when(context.getInputScope()).thenReturn(inputScope);
        when(context.getTimeWindow()).thenReturn(Optional.of(ImmutableTimeWindow.builder()
            .startTime(MILLIS)
            .endTime(MILLIS + 1_000)
            .build()));

        return context;
    }

    /**
     * Create ApiPartialEntity Helper.
     *
     * @param vvOid Virtual Volume Oid
     * @param storageTierOid Storage Tier Oid which the Virtual Volume connected to
     * @return {@link ApiPartialEntity}
     */
    @Nonnull
    private ApiPartialEntity createApiPartialEntity(final long vvOid, final long storageTierOid) {
        return ApiPartialEntity.newBuilder()
            .setOid(vvOid)
            .setEnvironmentType(EnvironmentType.CLOUD)
            .setEntityType(EntityType.VIRTUAL_VOLUME.getValue())
            .addConnectedTo(RelatedEntity.newBuilder()
                .setEntityType(EntityType.STORAGE_TIER.getValue())
                .setOid(storageTierOid)
                .build())
            .build();
    }

    @Nonnull
    private MinimalEntity createMinimalEntities(final long oid,
                                                @Nonnull final String displayName,
                                                @Nonnull final EntityType entityType) {
        return MinimalEntity.newBuilder()
            .setOid(oid)
            .setEntityType(entityType.getValue())
            .setDisplayName(displayName)
            .build();
    }

    /**
     * Create StatApiInputDTO Helper.
     *
     * @param name name of the stats
     * @param relatedEntityType {@link UIEntityType} related entity type
     * @param groupBy list of group by
     * @return {@link StatApiInputDTO}
     */
    @Nonnull
    private StatApiInputDTO createStatApiInputDTO(@Nonnull final String name,
                                                  @Nonnull final UIEntityType relatedEntityType,
                                                  @Nonnull final List<String> groupBy) {
        StatApiInputDTO statApiInputDTO = new StatApiInputDTO();
        statApiInputDTO.setName(name);
        statApiInputDTO.setRelatedEntityType(relatedEntityType.apiStr());
        statApiInputDTO.setGroupBy(groupBy);
        return statApiInputDTO;
    }
}
