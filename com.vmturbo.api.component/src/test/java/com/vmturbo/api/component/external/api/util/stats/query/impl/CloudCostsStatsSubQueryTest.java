package com.vmturbo.api.component.external.api.util.stats.query.impl;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isOneOf;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

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
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.google.common.collect.Sets;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.communication.RepositoryApi.SearchRequest;
import com.vmturbo.api.component.external.api.mapper.UuidMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory;
import com.vmturbo.api.component.external.api.util.stats.ImmutableTimeWindow;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryContextFactory.StatsQueryContext;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryScopeExpander.GlobalScope;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryScopeExpander.StatsQueryScope;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.statistic.StatApiInputDTO;
import com.vmturbo.api.dto.statistic.StatFilterApiDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatRecord;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatRecord.StatRecord;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatRecord.StatRecord.StatValue;
import com.vmturbo.common.protobuf.cost.CostMoles.CostServiceMole;
import com.vmturbo.common.protobuf.cost.CostServiceGrpc;
import com.vmturbo.common.protobuf.cost.CostServiceGrpc.CostServiceBlockingStub;
import com.vmturbo.common.protobuf.search.Search;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity.RelatedEntity;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.common.utils.StringConstants;
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

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        CostServiceBlockingStub costRpc = CostServiceGrpc.newBlockingStub(grpcTestServer.getChannel());
        query = new CloudCostsStatsSubQuery(repositoryApi, costRpc, supplyChainFetcherFactory, thinTargetCache);
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
        final long attachedVolumeId2= 222L;
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
        //when(searchRequest.getEntities()).thenReturn(attachedVVEntities.stream());
        when(searchRequest.getOids()).thenReturn(attachedVVEntities.stream().mapToLong(e -> e.getOid()).boxed().collect(Collectors.toSet()));


        final Set<StatApiInputDTO> requestedStats = Sets.newHashSet(
            createStatApiInputDTO(StringConstants.COST_PRICE, UIEntityType.VIRTUAL_VOLUME, Collections.singletonList(StringConstants.ATTACHMENT))
        );
        List<StatSnapshotApiDTO> results = query
            .getGroupByVVAttachmentStat(Collections.singletonList(ccsr),
                                       requestedStats,
                                       context);

        assertThat(results.size(), is(1));
        StatSnapshotApiDTO result = results.get(0);
        assertThat(result.getDisplayName(), is(StringConstants.COST_PRICE));
        List<StatApiDTO> stats = result.getStatistics();
        assertThat(stats.size(), is(2));

        stats.stream().forEach(stat -> {
            assertThat(stat.getName(), is(StringConstants.ATTACHMENT));
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


    @Test
    public void testGetGroupByStorageTierStat() {
        final long st1VolumeId1 = 901L;
        final long st1VolumeId2 = 902L;
        final long st2VolumeId = 801L;
        final long storageTierOid1 = 99999L;
        final long storageTierOid2 = 88888L;

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
        when(repositoryApi.newSearchRequest(any(Search.SearchParameters.class)))
            .thenReturn(searchRequest);
        when(searchRequest.getEntities()).thenReturn(attachedVVEntities.stream());

        final Set<StatApiInputDTO> requestedStats = Sets.newHashSet(
            createStatApiInputDTO(StringConstants.COST_PRICE, UIEntityType.VIRTUAL_VOLUME, Collections.singletonList(UIEntityType.STORAGE_TIER.apiStr()))
        );
        List<StatSnapshotApiDTO> results = query
            .getGroupByStorageTierStat(Collections.singletonList(ccsr),
                requestedStats,
                context);

        assertThat(results.size(), is(1));
        StatSnapshotApiDTO result = results.get(0);
        assertThat(result.getDisplayName(), is(StringConstants.COST_PRICE));
        List<StatApiDTO> stats = result.getStatistics();
        assertThat(stats.size(), is(2));

        stats.stream().forEach(stat -> {
            assertThat(stat.getName(), is(UIEntityType.STORAGE_TIER.apiStr()));
            assertThat(stat.getUnits(), is(StringConstants.DOLLARS_PER_HOUR));
            List<StatFilterApiDTO> filters = stat.getFilters();
            assertThat(filters.size(), is(1));
            StatFilterApiDTO filter = filters.get(0);
            assertThat(filter.getType(), is(UIEntityType.STORAGE_TIER.apiStr()));
            assertThat(filter.getValue(), isOneOf(storageTierOid1 + "", storageTierOid2 + ""));
            if (filter.getValue().equals(storageTierOid1 + "")) {
                assertThat(stat.getValue(), is(1f));
            } else {
                assertThat(stat.getValue(), is(3f));
            }
        });
    }

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

    /**
     * Create StatApiInputDTO Helper.
     *
     * @param name name of the stats
     * @param relatedEntityType {@link UIEntityType} related entity type
     * @param groupBy list of group by
     * @return {@link StatApiInputDTO}
     */
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
