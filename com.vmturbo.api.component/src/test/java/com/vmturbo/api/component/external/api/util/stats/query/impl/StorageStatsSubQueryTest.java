package com.vmturbo.api.component.external.api.util.stats.query.impl;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isOneOf;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.google.common.collect.Sets;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.communication.RepositoryApi.SearchRequest;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryContextFactory.StatsQueryContext;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryScopeExpander.GlobalScope;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryScopeExpander.StatsQueryScope;
import com.vmturbo.api.component.external.api.util.stats.query.SubQuerySupportedStats;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.statistic.StatApiInputDTO;
import com.vmturbo.api.dto.statistic.StatFilterApiDTO;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.search.Search;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter.TraversalDirection;
import com.vmturbo.common.protobuf.search.SearchREST.ComparisonOperator;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity.RelatedEntity;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.platform.common.dto.CommonDTOREST.EntityDTO.EntityType;

public class StorageStatsSubQueryTest {

    private StorageStatsSubQuery query;
    private static final String NUM_VOLUMES = StorageStatsSubQuery.NUM_VOL;

    @Mock
    private RepositoryApi repositoryApi;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        query = new StorageStatsSubQuery(repositoryApi);
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
    public void testGetHandledStatsWithVirtualVolumeAttachment() {
        final StatsQueryContext context = mock(StatsQueryContext.class);

        ArgumentCaptor<Set<String>> getStats = new ArgumentCaptor();
        StatApiInputDTO statApiInputDTO = createStatApiInputDTO(NUM_VOLUMES,
            UIEntityType.VIRTUAL_VOLUME,
            Collections.singletonList(StringConstants.ATTACHMENT));
        when(context.findStats(getStats.capture())).thenReturn(Sets.newHashSet(statApiInputDTO));

        SubQuerySupportedStats results = query.getHandledStats(context);

        Set<String> handledStats = getStats.getValue();
        assertThat(handledStats, containsInAnyOrder(NUM_VOLUMES));

        assertThat(results.containsExplicitStat(statApiInputDTO), is(true));
    }

    @Test
    public void testGetHandledStatsWithVirtualVolumeStorageTier() {
        final StatsQueryContext context = mock(StatsQueryContext.class);

        ArgumentCaptor<Set<String>> getStats = new ArgumentCaptor();
        StatApiInputDTO statApiInputDTO = createStatApiInputDTO(NUM_VOLUMES,
            UIEntityType.VIRTUAL_VOLUME,
            Collections.singletonList(UIEntityType.STORAGE_TIER.apiStr()));
        when(context.findStats(getStats.capture())).thenReturn(Sets.newHashSet(statApiInputDTO));

        SubQuerySupportedStats results = query.getHandledStats(context);

        Set<String> handledStats = getStats.getValue();
        assertThat(handledStats, containsInAnyOrder(NUM_VOLUMES));

        assertThat(results.containsExplicitStat(statApiInputDTO), is(true));
    }

    @Test
    public void testGetHandledStatsWithVirtualVolumeUnsupportedGrouping() {
        final StatsQueryContext context = mock(StatsQueryContext.class);

        final String someOtherGrouping = "someOtherGrouping";

        ArgumentCaptor<Set<String>> getStats = new ArgumentCaptor();
        StatApiInputDTO statApiInputDTO = createStatApiInputDTO(NUM_VOLUMES,
            UIEntityType.VIRTUAL_VOLUME,
            Collections.singletonList(someOtherGrouping));
        when(context.findStats(getStats.capture())).thenReturn(Sets.newHashSet(statApiInputDTO));

        SubQuerySupportedStats results = query.getHandledStats(context);

        Set<String> handledStats = getStats.getValue();
        assertThat(handledStats, containsInAnyOrder(NUM_VOLUMES));

        assertThat(results.containsExplicitStat(statApiInputDTO), is(false));
    }

    @Test
    public void testGetHandledStatsWithVirtualVolumeAttachmentAndStorage() {
        final StatsQueryContext context = mock(StatsQueryContext.class);

        ArgumentCaptor<Set<String>> getStats = new ArgumentCaptor();
        StatApiInputDTO statApiInputDTO = createStatApiInputDTO(NUM_VOLUMES,
            UIEntityType.VIRTUAL_VOLUME,
            Arrays.asList(StringConstants.ATTACHMENT, UIEntityType.STORAGE_TIER.apiStr()));
        when(context.findStats(getStats.capture())).thenReturn(Sets.newHashSet(statApiInputDTO));

        SubQuerySupportedStats results = query.getHandledStats(context);

        Set<String> handledStats = getStats.getValue();
        assertThat(handledStats, containsInAnyOrder(NUM_VOLUMES));

        assertThat(results.containsExplicitStat(statApiInputDTO), is(false));
    }


    @Test
    public void testGetAggregateStatsWhenGroupByAttachment() throws OperationFailedException {
        final StatsQueryContext context = setupGlobalScope();

        ArgumentCaptor<Search.SearchParameters> searchParametersArgumentCaptor = new ArgumentCaptor<>();

        SearchRequest searchRequest = mock(SearchRequest.class);
        when(repositoryApi.newSearchRequest(searchParametersArgumentCaptor.capture()))
            .thenReturn(searchRequest);
        when(searchRequest.count()).thenReturn(10L, 6L);

        Set<StatApiInputDTO> requestedStats =
            Sets.newHashSet(createStatApiInputDTO(NUM_VOLUMES, UIEntityType.VIRTUAL_VOLUME, Collections.singletonList(StringConstants.ATTACHMENT)));

        Map<Long, List<StatApiDTO>> results = query.getAggregateStats(requestedStats, context);

        List<Search.SearchParameters> searchParameters = searchParametersArgumentCaptor.getAllValues();
        assertThat(searchParameters.size(), is(2));
        Search.SearchParameters firstParam = searchParameters.get(0);
        assertThat(firstParam.getSearchFilterCount(), is(1));
        assertThat(firstParam.getSearchFilter(0).getTraversalFilter().getTraversalDirection(), is(TraversalDirection.CONNECTED_TO));
        assertThat(firstParam.getSearchFilter(0).getTraversalFilter().getStoppingCondition().getStoppingPropertyFilter().getNumericFilter().getComparisonOperator().getNumber(), is(ComparisonOperator.EQ.getValue()));
        assertThat(firstParam.getSearchFilter(0).getTraversalFilter().getStoppingCondition().getStoppingPropertyFilter().getNumericFilter().getValue(), is(new Long(EntityType.VIRTUAL_VOLUME.getValue()).longValue()));

        Search.SearchParameters secondParam = searchParameters.get(1);
        assertThat(secondParam.getSearchFilterCount(), is(2));
        assertThat(secondParam.getSearchFilter(0).getTraversalFilter().getTraversalDirection(), is(TraversalDirection.CONNECTED_FROM));
        assertThat(secondParam.getSearchFilter(0).getTraversalFilter().getStoppingCondition().getStoppingPropertyFilter().getNumericFilter().getComparisonOperator().getNumber(), is(ComparisonOperator.EQ.getValue()));
        assertThat(secondParam.getSearchFilter(0).getTraversalFilter().getStoppingCondition().getStoppingPropertyFilter().getNumericFilter().getValue(), is(new Long(EntityType.VIRTUAL_MACHINE.getValue()).longValue()));

        assertThat(secondParam.getSearchFilter(1).getTraversalFilter().getTraversalDirection(), is(TraversalDirection.CONNECTED_TO));
        assertThat(secondParam.getSearchFilter(1).getTraversalFilter().getStoppingCondition().getStoppingPropertyFilter().getNumericFilter().getComparisonOperator().getNumber(), is(ComparisonOperator.EQ.getValue()));
        assertThat(secondParam.getSearchFilter(1).getTraversalFilter().getStoppingCondition().getStoppingPropertyFilter().getNumericFilter().getValue(), is(new Long(EntityType.VIRTUAL_VOLUME.getValue()).longValue()));


        assertThat(results.size(), is(1));
        assertThat(results.values().stream().findAny().get().size(), is(2));

        List<StatApiDTO> dtos = results.values().stream().findAny().get();
        dtos.forEach(dto -> {
            assertThat(dto.getName(), is(NUM_VOLUMES));
            assertThat(dto.getRelatedEntityType(), is(UIEntityType.VIRTUAL_VOLUME.apiStr()));

            List<StatFilterApiDTO> statFilterApiDTOS = dto.getFilters();
            assertThat(statFilterApiDTOS.size(), is(1));
            StatFilterApiDTO statFilterApiDTO = statFilterApiDTOS.get(0);
            assertThat(statFilterApiDTO.getType(), is(StringConstants.ATTACHMENT));

            assertThat(statFilterApiDTO.getValue(), isOneOf(StringConstants.ATTACHED, StringConstants.UNATTACHED));

            if (statFilterApiDTO.getValue().equals(StringConstants.ATTACHED)) {
                assertThat(dto.getValue(), is(6f));
            } else {
                assertThat(dto.getValue(), is(4f));
            }
        });
    }

    @Test
    public void testGetAggregateStatsWhenContextIsGlobalScopeAndGroupByStorageTier() throws OperationFailedException {
        final long stOid1 = 1000L;
        final long stOid2 = 2000L;
        final long stOid3 = 3000L;
        final List<ApiPartialEntity> partialEntities = Arrays.asList(
            createApiPartialEntity(111L, stOid1),
            createApiPartialEntity(222L, stOid2),
            createApiPartialEntity(223L, stOid2),
            createApiPartialEntity(333L, stOid3),
            createApiPartialEntity(334L, stOid3),
            createApiPartialEntity(335L, stOid3)
        );

        // Setup Context for Global Scope
        final StatsQueryContext context = setupGlobalScope();

        SearchRequest searchRequest = mock(SearchRequest.class);
        when(repositoryApi.newSearchRequest(any(Search.SearchParameters.class)))
            .thenReturn(searchRequest);
        when(searchRequest.getEntities()).thenReturn(partialEntities.stream());

        Set<StatApiInputDTO> requestedStats =
            Sets.newHashSet(createStatApiInputDTO(NUM_VOLUMES, UIEntityType.VIRTUAL_VOLUME, Collections.singletonList(UIEntityType.STORAGE_TIER.apiStr())));

        Map<Long, List<StatApiDTO>> results = query.getAggregateStats(requestedStats, context);

        assertThat(results.size(), is(1));
        assertThat(results.values().stream().findAny().get().size(), is(3));

        List<StatApiDTO> dtos = results.values().stream().findAny().get();
        dtos.forEach(dto -> {
            assertThat(dto.getName(), is(NUM_VOLUMES));
            assertThat(dto.getRelatedEntityType(), is(UIEntityType.VIRTUAL_VOLUME.apiStr()));

            List<StatFilterApiDTO> statFilterApiDTOS = dto.getFilters();
            assertThat(statFilterApiDTOS.size(), is(1));
            StatFilterApiDTO statFilterApiDTO = statFilterApiDTOS.get(0);
            assertThat(statFilterApiDTO.getType(), is(UIEntityType.STORAGE_TIER.apiStr()));

            assertThat(statFilterApiDTO.getValue(), isOneOf(stOid1 + "", stOid2 + "", stOid3 + ""));
            float expectedCount = Long.parseLong(statFilterApiDTO.getValue()) / 1000f;
            assertThat(dto.getValue(), is(expectedCount));

        });
    }

    @Test
    public void testGetAggregateStatsWhenContextIsNotGlobalScopeAndGroupByStorageTier() throws OperationFailedException {
        final long stOid1 = 1000L;
        final long stOid2 = 2000L;
        final long stOid3 = 3000L;
        final List<ApiPartialEntity> partialEntities = Arrays.asList(
            createApiPartialEntity(111L, stOid1),
            createApiPartialEntity(222L, stOid2),
            createApiPartialEntity(223L, stOid2),
            createApiPartialEntity(333L, stOid3),
            createApiPartialEntity(334L, stOid3),
            createApiPartialEntity(335L, stOid3)
        );

        // Setup scope for context
        StatsQueryScope statsQueryScope = mock(StatsQueryScope.class);
        when(statsQueryScope.getEntities())
            .thenReturn(partialEntities.stream().mapToLong(pe -> pe.getOid()).boxed().collect(Collectors.toSet()));
        final StatsQueryContext context = mock(StatsQueryContext.class);
        when(context.isGlobalScope()).thenReturn(false);
        when(context.getQueryScope()).thenReturn(statsQueryScope);

        // Setup response from repository api
        SearchRequest searchRequest = mock(SearchRequest.class);
        when(repositoryApi.newSearchRequest(any(Search.SearchParameters.class)))
            .thenReturn(searchRequest);
        when(searchRequest.getEntities()).thenReturn(partialEntities.stream());

        // when getting stat
        Set<StatApiInputDTO> requestedStats =
            Sets.newHashSet(createStatApiInputDTO(NUM_VOLUMES, UIEntityType.VIRTUAL_VOLUME, Collections.singletonList(UIEntityType.STORAGE_TIER.apiStr())));
        Map<Long, List<StatApiDTO>> results = query.getAggregateStats(requestedStats, context);

        // Result
        assertThat(results.size(), is(1));
        assertThat(results.values().stream().findAny().get().size(), is(3));

        List<StatApiDTO> dtos = results.values().stream().findAny().get();
        dtos.forEach(dto -> {
            assertThat(dto.getName(), is(NUM_VOLUMES));
            assertThat(dto.getRelatedEntityType(), is(UIEntityType.VIRTUAL_VOLUME.apiStr()));

            List<StatFilterApiDTO> statFilterApiDTOS = dto.getFilters();
            assertThat(statFilterApiDTOS.size(), is(1));
            StatFilterApiDTO statFilterApiDTO = statFilterApiDTOS.get(0);
            assertThat(statFilterApiDTO.getType(), is(UIEntityType.STORAGE_TIER.apiStr()));

            assertThat(statFilterApiDTO.getValue(), isOneOf(stOid1+"", stOid2+"", stOid3+""));
            float expectedCount = Long.parseLong(statFilterApiDTO.getValue()) / 1000f;
            assertThat(dto.getValue(), is(expectedCount));

        });
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

    /**
     * Setup Mocked StatsQueryContext for Global Scope.
     *
     * @return {@link StatsQueryScope}
     */
    private StatsQueryContext setupGlobalScope() {
        // Setup Context for Global Scope
        final GlobalScope globalScope = mock(GlobalScope.class);
        when(globalScope.entityTypes()).thenReturn(Sets.newHashSet(UIEntityType.VIRTUAL_VOLUME));
        StatsQueryScope statsQueryScope = mock(StatsQueryScope.class);
        when(statsQueryScope.getGlobalScope()).thenReturn(Optional.of(globalScope));

        final StatsQueryContext context = mock(StatsQueryContext.class);
        when(context.isGlobalScope()).thenReturn(true);
        when(context.getQueryScope()).thenReturn(statsQueryScope);
        return context;
    }
}
