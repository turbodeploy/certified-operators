package com.vmturbo.api.component.external.api.util.stats.query.impl;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isOneOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.platform.common.dto.CommonDTO;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.google.common.collect.ImmutableSet;
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
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.search.Search;
import com.vmturbo.common.protobuf.search.Search.ComparisonOperator;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter.TraversalDirection;
import com.vmturbo.common.protobuf.search.SearchableProperties;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity.RelatedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.common.protobuf.topology.UIEnvironmentType;
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

    // Test SubQuery Method applicableInContext
    // case: in plan
    @Test
    public void testNotApplicableInPlan() {
        final ApiId scope = mock(ApiId.class);
        when(scope.isPlan()).thenReturn(true);

        final StatsQueryContext context = mock(StatsQueryContext.class);
        when(context.getInputScope()).thenReturn(scope);

        assertThat(query.applicableInContext(context), is(false));
    }

    // Test SubQuery Method applicableInContext
    // case: not in plan
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
        StatFilterApiDTO statFilterApiDTO = new StatFilterApiDTO();
        statFilterApiDTO .setType(StringConstants.ENVIRONMENT_TYPE);
        statFilterApiDTO .setValue(EnvironmentType.ON_PREM.name());
        statApiInputDTO.setFilters(Collections.singletonList(statFilterApiDTO));
        when(context.findStats(getStats.capture())).thenReturn(Sets.newHashSet(statApiInputDTO));

        SubQuerySupportedStats results = query.getHandledStats(context);

        Set<String> handledStats = getStats.getValue();
        assertThat(handledStats, containsInAnyOrder(NUM_VOLUMES));

        assertThat(results.containsExplicitStat(statApiInputDTO), is(true));
    }

    @Test
    public void testGetHandledStatsCloudStatRequest() {
        final StatsQueryContext context = mock(StatsQueryContext.class);

        final ArgumentCaptor<Set<String>> getStats = new ArgumentCaptor();
        StatApiInputDTO statApiInputDTO = createStatApiInputDTO(NUM_VOLUMES,
                UIEntityType.VIRTUAL_VOLUME,
                Collections.singletonList(StringConstants.ATTACHMENT));
        StatFilterApiDTO statFilterApiDTO = new StatFilterApiDTO();
        statFilterApiDTO .setType(StringConstants.ENVIRONMENT_TYPE);
        statFilterApiDTO .setValue(EnvironmentType.CLOUD.name());
        statApiInputDTO.setFilters(Collections.singletonList(statFilterApiDTO));
        when(context.findStats(getStats.capture())).thenReturn(Sets.newHashSet(statApiInputDTO));

        SubQuerySupportedStats results = query.getHandledStats(context);

        Set<String> handledStats = getStats.getValue();
        assertThat(handledStats, containsInAnyOrder(NUM_VOLUMES));

        assertThat(results.containsExplicitStat(statApiInputDTO), is(false));
    }

    @Test
    public void testGetHandledStatsWithVirtualVolumeStorageTier() {
        final StatsQueryContext context = mock(StatsQueryContext.class);

        ArgumentCaptor<Set<String>> getStats = new ArgumentCaptor();
        StatApiInputDTO statApiInputDTO = createStatApiInputDTO(NUM_VOLUMES,
            UIEntityType.VIRTUAL_VOLUME,
            Collections.singletonList(UIEntityType.STORAGE_TIER.apiStr()));
        StatFilterApiDTO statFilterApiDTO = new StatFilterApiDTO();
        statFilterApiDTO.setType(StringConstants.ENVIRONMENT_TYPE);
        statFilterApiDTO.setValue(EnvironmentType.ON_PREM.name());
        statApiInputDTO.setFilters(Collections.singletonList(statFilterApiDTO));
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
        Long oidInScope = 1L;
        Set<Long> oids = Sets.newHashSet(oidInScope, 2L);
        final StatsQueryContext context = setupOidsScope(oids);

        ArgumentCaptor<Search.SearchParameters> searchParametersArgumentCaptor = new ArgumentCaptor<>();

        SearchRequest searchRequest = mock(SearchRequest.class);
        TopologyDTO.TopologyEntityDTO topologyEntityDTO = TopologyDTO.TopologyEntityDTO.newBuilder()
            .setOid(oidInScope)
            .setEntityType(CommonDTO.EntityDTO.EntityType.VIRTUAL_VOLUME_VALUE)
            .setTypeSpecificInfo(TopologyDTO.TypeSpecificInfo.newBuilder().setVirtualVolume(
                TopologyDTO.TypeSpecificInfo.VirtualVolumeInfo.newBuilder()
                    .setAttachmentState(CommonDTO.EntityDTO.VirtualVolumeData.AttachmentState.ATTACHED)
                    .build()))
            .build();
        when(repositoryApi.newSearchRequest(searchParametersArgumentCaptor.capture()))
                .thenReturn(searchRequest);
        when(searchRequest.getOids()).thenReturn(Sets.newHashSet(oidInScope));
        when(searchRequest.getFullEntities()).thenReturn(Sets.newHashSet(topologyEntityDTO).stream());
        when(searchRequest.count()).thenReturn(2L);

        Set<StatApiInputDTO> requestedStats =
            Sets.newHashSet(createStatApiInputDTO(NUM_VOLUMES, UIEntityType.VIRTUAL_VOLUME, Collections.singletonList(StringConstants.ATTACHMENT)));

        final List<StatSnapshotApiDTO> results = query.getAggregateStats(requestedStats, context);

        // assert that querying Repository API for getting VVs in the scope
        List<Search.SearchParameters> searchParameters = searchParametersArgumentCaptor.getAllValues();
        assertThat(searchParameters.size(), is(1));
        Search.SearchParameters firstParam = searchParameters.get(0);
        assertThat(firstParam.getSearchFilterCount(), is(1));
        assertThat(firstParam.getSearchFilter(0).getPropertyFilter().getNumericFilter().getValue(),
            is(Long.valueOf(UIEntityType.VIRTUAL_VOLUME.typeNumber())));

        // assert that to get one statistic with two records: attached and unattached
        assertThat(results.size(), is(1));
        final List<StatApiDTO> statistics = results.get(0).getStatistics();
        assertThat(statistics.size(), is(2));
        statistics.forEach(dto -> {
            assertThat(dto.getName(), is(NUM_VOLUMES));
            assertThat(dto.getRelatedEntityType(), is(UIEntityType.VIRTUAL_VOLUME.apiStr()));

            List<StatFilterApiDTO> statFilterApiDTOS = dto.getFilters();
            assertThat(statFilterApiDTOS.size(), is(1));
            StatFilterApiDTO statFilterApiDTO = statFilterApiDTOS.get(0);
            assertThat(statFilterApiDTO.getType(), is(StringConstants.ATTACHMENT));

            assertThat(statFilterApiDTO.getValue(), isOneOf(StringConstants.ATTACHED, StringConstants.UNATTACHED));

            if (statFilterApiDTO.getValue().equals(StringConstants.ATTACHED)) {
                assertThat(dto.getValue(), is(1f));
            } else {
                assertThat(dto.getValue(), is(1f));
            }
        });
    }

    @Test
    public void testGetAggregateStatsWhenContextIsGlobalScopeAndGroupByStorageTier() throws OperationFailedException {
        final long stOid1 = 1000L;
        final String stDisplayName1 = "storage tier 1";
        final long stOid2 = 2000L;
        final String stDisplayName2 = "storage tier 2";
        final long stOid3 = 3000L;
        final String stDisplayName3 = "storage tier 3";
        final List<ApiPartialEntity> vvPartialEntities = Arrays.asList(
            createVVApiPartialEntity(111L, stOid1),
            createVVApiPartialEntity(222L, stOid2),
            createVVApiPartialEntity(223L, stOid2),
            createVVApiPartialEntity(333L, stOid3),
            createVVApiPartialEntity(334L, stOid3),
            createVVApiPartialEntity(335L, stOid3)
        );
        final List<MinimalEntity> stMinimalEntities = Arrays.asList(
            createMinimalEntities(stOid1, stDisplayName1, EntityType.STORAGE_TIER),
            createMinimalEntities(stOid2, stDisplayName2, EntityType.STORAGE_TIER),
            createMinimalEntities(stOid3, stDisplayName3, EntityType.STORAGE_TIER)
        );

        // Setup Context for Global Scope
        final StatsQueryContext context = setupGlobalScope();

        SearchRequest searchRequest = mock(SearchRequest.class);

        ArgumentCaptor<Search.SearchParameters> searchParametersArgumentCaptor = new ArgumentCaptor<>();
        when(repositoryApi.newSearchRequest(searchParametersArgumentCaptor.capture()))
            .thenReturn(searchRequest);
        when(searchRequest.getEntities()).thenReturn(vvPartialEntities.stream());
        when(searchRequest.getMinimalEntities()).thenReturn(stMinimalEntities.stream());

        Set<StatApiInputDTO> requestedStats =
            Sets.newHashSet(createStatApiInputDTO(NUM_VOLUMES, UIEntityType.VIRTUAL_VOLUME, Collections.singletonList(UIEntityType.STORAGE_TIER.apiStr())));

        final List<StatSnapshotApiDTO> results = query.getAggregateStats(requestedStats, context);

        List<Search.SearchParameters> searchParameters = searchParametersArgumentCaptor.getAllValues();
        assertThat(searchParameters.size(), is(2));
        // first API call should do VV search
        Search.SearchParameters firstParam = searchParameters.get(0);
        assertThat(firstParam.getStartingFilter().hasNumericFilter(), is(true));
        assertThat(firstParam.getStartingFilter().getPropertyName(), is("entityType"));
        assertThat(firstParam.getStartingFilter().getNumericFilter().getComparisonOperator(), is(ComparisonOperator.EQ));
        assertThat(firstParam.getStartingFilter().getNumericFilter().getValue(), is(new Long(EntityType.VIRTUAL_VOLUME.getValue()).longValue()));
        assertThat(firstParam.getSearchFilterCount(), is(1));
        assertThat(firstParam.getSearchFilter(0).hasPropertyFilter(), is(true));
        assertThat(firstParam.getSearchFilter(0).getPropertyFilter().hasStringFilter(), is(true));
        assertThat(firstParam.getSearchFilter(0).getPropertyFilter().getPropertyName(), is(SearchableProperties.ENVIRONMENT_TYPE));
        assertThat(firstParam.getSearchFilter(0).getPropertyFilter().getStringFilter().getOptionsCount(), is(1));
        assertThat(firstParam.getSearchFilter(0).getPropertyFilter().getStringFilter().getOptions(0), is(UIEnvironmentType.CLOUD.getApiEnumStringValue()));


        // second API should get STs from the VV connected to list
        Search.SearchParameters secondParam = searchParameters.get(1);
        assertThat(secondParam.getStartingFilter().hasStringFilter(), is(true));
        assertThat(secondParam.getStartingFilter().getPropertyName(), is("oid"));
        assertThat(secondParam.getStartingFilter().getStringFilter().getOptionsList(),
            containsInAnyOrder("1000", "2000", "3000"));

        assertThat(results.size(), is(1));
        final List<StatApiDTO> statistics = results.get(0).getStatistics();
        assertThat(statistics.size(), is(3));
        statistics.forEach(dto -> {
            assertThat(dto.getName(), is(NUM_VOLUMES));
            assertThat(dto.getRelatedEntityType(), is(UIEntityType.VIRTUAL_VOLUME.apiStr()));

            List<StatFilterApiDTO> statFilterApiDTOS = dto.getFilters();
            assertThat(statFilterApiDTOS.size(), is(1));
            StatFilterApiDTO statFilterApiDTO = statFilterApiDTOS.get(0);
            assertThat(statFilterApiDTO.getType(), is(UIEntityType.STORAGE_TIER.apiStr()));

            assertThat(statFilterApiDTO.getValue(), isOneOf(stDisplayName1, stDisplayName2, stDisplayName3));

            final Function<String, Float> getExpectedCount = displayName -> {
                switch (displayName) {
                    case stDisplayName1:
                        return 1f;
                    case stDisplayName2:
                        return 2f;
                    case stDisplayName3:
                        return 3f;
                    default:
                        return 0f;
                }
            };

            float expectedCount = getExpectedCount.apply(statFilterApiDTO.getValue());
            assertThat(dto.getValue(), is(expectedCount));

        });
    }

    /***
     * Test non global scope with group by storage tier.
     *
     * @throws OperationFailedException operation failed
     */
    @Test
    public void testGetAggregateStatsWhenContextIsNotGlobalScopeAndGroupByStorageTier() throws OperationFailedException {
        final long stOid1 = 1000L;
        final String stDisplayName1 = "storage tier 1";
        final long stOid2 = 2000L;
        final String stDisplayName2 = "storage tier 2";
        final long stOid3 = 3000L;
        final String stDisplayName3 = "storage tier 3";
        final List<ApiPartialEntity> vvPartialEntities = Arrays.asList(
            createVVApiPartialEntity(111L, stOid1),
            createVVApiPartialEntity(222L, stOid2),
            createVVApiPartialEntity(223L, stOid2),
            createVVApiPartialEntity(333L, stOid3),
            createVVApiPartialEntity(334L, stOid3),
            createVVApiPartialEntity(335L, stOid3)
        );
        final List<MinimalEntity> stMinimalEntities = Arrays.asList(
            createMinimalEntities(stOid1, stDisplayName1, EntityType.STORAGE_TIER),
            createMinimalEntities(stOid2, stDisplayName2, EntityType.STORAGE_TIER),
            createMinimalEntities(stOid3, stDisplayName3, EntityType.STORAGE_TIER)
        );

        // Setup Context for Global Scope
        StatsQueryScope statsQueryScope = mock(StatsQueryScope.class);
        when(statsQueryScope.getExpandedOids())
            .thenReturn(vvPartialEntities.stream().mapToLong(pe -> pe.getOid()).boxed().collect(Collectors.toSet()));
        final StatsQueryContext context = mock(StatsQueryContext.class);
        when(context.isGlobalScope()).thenReturn(false);
        when(context.getQueryScope()).thenReturn(statsQueryScope);

        SearchRequest searchRequest = mock(SearchRequest.class);

        ArgumentCaptor<Search.SearchParameters> searchParametersArgumentCaptor = new ArgumentCaptor<>();
        when(repositoryApi.newSearchRequest(searchParametersArgumentCaptor.capture()))
            .thenReturn(searchRequest);
        when(searchRequest.getEntities()).thenReturn(vvPartialEntities.stream());
        when(searchRequest.getMinimalEntities()).thenReturn(stMinimalEntities.stream());

        Set<StatApiInputDTO> requestedStats =
            Sets.newHashSet(createStatApiInputDTO(NUM_VOLUMES, UIEntityType.VIRTUAL_VOLUME, Collections.singletonList(UIEntityType.STORAGE_TIER.apiStr())));

        final List<StatSnapshotApiDTO> results = query.getAggregateStats(requestedStats, context);

        List<Search.SearchParameters> searchParameters = searchParametersArgumentCaptor.getAllValues();
        assertThat(searchParameters.size(), is(2));
        // first API call should do VV search
        Search.SearchParameters firstParam = searchParameters.get(0);
        assertThat(firstParam.getStartingFilter().hasStringFilter(), is(true));
        assertThat(firstParam.getStartingFilter().getPropertyName(), is("oid"));
        assertThat(firstParam.getStartingFilter().getStringFilter().getOptionsList(),
            containsInAnyOrder("111", "222", "223", "333", "334", "335"));
        assertThat(firstParam.getSearchFilterCount(), is(1));
        assertThat(firstParam.getSearchFilter(0).hasPropertyFilter(), is(true));
        assertThat(firstParam.getSearchFilter(0).getPropertyFilter().hasStringFilter(), is(true));
        assertThat(firstParam.getSearchFilter(0).getPropertyFilter().getPropertyName(), is(SearchableProperties.ENVIRONMENT_TYPE));
        assertThat(firstParam.getSearchFilter(0).getPropertyFilter().getStringFilter().getOptionsCount(), is(1));
        assertThat(firstParam.getSearchFilter(0).getPropertyFilter().getStringFilter().getOptions(0), is(UIEnvironmentType.CLOUD.getApiEnumStringValue()));

        // second API should get STs from the VV connected to list
        Search.SearchParameters secondParam = searchParameters.get(1);
        assertThat(secondParam.getStartingFilter().hasStringFilter(), is(true));
        assertThat(secondParam.getStartingFilter().getPropertyName(), is("oid"));
        assertThat(secondParam.getStartingFilter().getStringFilter().getOptionsList(),
            containsInAnyOrder("1000", "2000", "3000"));

        assertThat(results.size(), is(1));
        final List<StatApiDTO> statistics = results.get(0).getStatistics();
        assertThat(statistics.size(), is(3));
        statistics.forEach(dto -> {
            assertThat(dto.getName(), is(NUM_VOLUMES));
            assertThat(dto.getRelatedEntityType(), is(UIEntityType.VIRTUAL_VOLUME.apiStr()));

            List<StatFilterApiDTO> statFilterApiDTOS = dto.getFilters();
            assertThat(statFilterApiDTOS.size(), is(1));
            StatFilterApiDTO statFilterApiDTO = statFilterApiDTOS.get(0);
            assertThat(statFilterApiDTO.getType(), is(UIEntityType.STORAGE_TIER.apiStr()));

            assertThat(statFilterApiDTO.getValue(), isOneOf(stDisplayName1, stDisplayName2, stDisplayName3));

            final Function<String, Float> getExpectedCount = displayName -> {
                switch (displayName) {
                    case stDisplayName1:
                        return 1f;
                    case stDisplayName2:
                        return 2f;
                    case stDisplayName3:
                        return 3f;
                    default:
                        return 0f;
                }
            };

            float expectedCount = getExpectedCount.apply(statFilterApiDTO.getValue());
            assertThat(dto.getValue(), is(expectedCount));

        });
    }

    /**
     * Tests aggregation of stats in the case where there is no groupBy element.
     * @throws OperationFailedException only if something is seriously wrong.
     */
    @Test
    public void testGetAggregateStatsNoGroupBy() throws OperationFailedException {
        StatsQueryScope statsQueryScope = mock(StatsQueryScope.class);
        when(statsQueryScope.getExpandedOids())
            .thenReturn(ImmutableSet.of(123L, 456L, 789L));
        final StatsQueryContext context = mock(StatsQueryContext.class);
        when(context.isGlobalScope()).thenReturn(false);
        when(context.getQueryScope()).thenReturn(statsQueryScope);

        SearchRequest searchRequest = mock(SearchRequest.class);
        when(repositoryApi.newSearchRequest(any())).thenReturn(searchRequest);
        when(searchRequest.count()).thenReturn(3L);

        Set<StatApiInputDTO> requestedStats =
            Sets.newHashSet(createStatApiInputDTO(NUM_VOLUMES, null, Collections.emptyList()));

        final List<StatSnapshotApiDTO> results = query.getAggregateStats(requestedStats, context);

        assertEquals(1, results.size());
        final List<StatApiDTO> statistics = results.get(0).getStatistics();
        assertEquals(1, statistics.size());
        final StatApiDTO resultStat = statistics.get(0);
        assertEquals(NUM_VOLUMES, resultStat.getName());

        assertTrue(resultStat.getFilters().isEmpty());
        assertEquals(3, resultStat.getValue(), 0);
        assertNull(resultStat.getRelatedEntityType());
    }

    /**
     * Test GetAggregateStats() method for context is not Global with a VV without ST.
     *
     * @throws OperationFailedException unable to operate
     */
    @Test
    public void testGetAggregateStatsWhenContextIsNotGlobalScopeAndGroupByStorageTierWithVVHasNoST() throws OperationFailedException {
        final long stOid1 = 1000L;
        final String stDisplayName1 = "storage tier 1";
        final long stOid2 = 2000L;
        final String stDisplayName2 = "storage tier 2";
        final long stOid3 = 3000L;
        final String stDisplayName3 = "storage tier 3";
        final List<Long> vvOids = Arrays.asList(111L, 222L, 223L, 333L, 334L, 535L);
        final List<ApiPartialEntity> vvPartialEntities = Arrays.asList(
            createVVApiPartialEntity(vvOids.get(0), stOid1),
            createVVApiPartialEntity(vvOids.get(1), stOid2),
            createVVApiPartialEntity(vvOids.get(2), stOid2),
            createVVApiPartialEntity(vvOids.get(3), stOid3),
            createVVApiPartialEntity(vvOids.get(4), stOid3),
            createVVApiPartialEntityWithOutST(vvOids.get(5)) // VV without ST related.
        );
        final List<MinimalEntity> stMinimalEntities = Arrays.asList(
            createMinimalEntities(stOid1, stDisplayName1, EntityType.STORAGE_TIER),
            createMinimalEntities(stOid2, stDisplayName2, EntityType.STORAGE_TIER),
            createMinimalEntities(stOid3, stDisplayName3, EntityType.STORAGE_TIER)
        );

        // Setup scope for context
        StatsQueryScope statsQueryScope = mock(StatsQueryScope.class);
        when(statsQueryScope.getExpandedOids())
            .thenReturn(vvPartialEntities.stream().mapToLong(pe -> pe.getOid()).boxed().collect(Collectors.toSet()));
        final StatsQueryContext context = mock(StatsQueryContext.class);
        when(context.isGlobalScope()).thenReturn(false);
        when(context.getQueryScope()).thenReturn(statsQueryScope);

        // Setup response from repository api
        ArgumentCaptor<Search.SearchParameters> searchParametersArgumentCaptor = new ArgumentCaptor<>();
        SearchRequest searchRequest = mock(SearchRequest.class);
        when(repositoryApi.newSearchRequest(searchParametersArgumentCaptor.capture()))
            .thenReturn(searchRequest);
        when(searchRequest.getEntities()).thenReturn(vvPartialEntities.stream());
        when(searchRequest.getMinimalEntities()).thenReturn(stMinimalEntities.stream());

        // when getting stat
        Set<StatApiInputDTO> requestedStats =
            Sets.newHashSet(createStatApiInputDTO(NUM_VOLUMES, UIEntityType.VIRTUAL_VOLUME, Collections.singletonList(UIEntityType.STORAGE_TIER.apiStr())));
        final List<StatSnapshotApiDTO> results = query.getAggregateStats(requestedStats, context);

        // Result

        List<Search.SearchParameters> searchParameters = searchParametersArgumentCaptor.getAllValues();
        assertThat(searchParameters.size(), is(2));
        // first API call should do VV search
        Search.SearchParameters firstParam = searchParameters.get(0);
        assertThat(firstParam.getStartingFilter().hasStringFilter(), is(true));
        assertThat(firstParam.getStartingFilter().getPropertyName(), is("oid"));
        assertThat(firstParam.getStartingFilter().getStringFilter().getOptionsList(),
            containsInAnyOrder("111", "222", "223", "333", "334", "535"));
        assertThat(firstParam.getSearchFilterCount(), is(1));
        assertThat(firstParam.getSearchFilter(0).hasPropertyFilter(), is(true));
        assertThat(firstParam.getSearchFilter(0).getPropertyFilter().hasStringFilter(), is(true));
        assertThat(firstParam.getSearchFilter(0).getPropertyFilter().getPropertyName(), is(SearchableProperties.ENVIRONMENT_TYPE));
        assertThat(firstParam.getSearchFilter(0).getPropertyFilter().getStringFilter().getOptionsCount(), is(1));
        assertThat(firstParam.getSearchFilter(0).getPropertyFilter().getStringFilter().getOptions(0), is(UIEnvironmentType.CLOUD.getApiEnumStringValue()));

        // second API should get STs from the VV connected to list
        Search.SearchParameters secondParam = searchParameters.get(1);
        assertThat(secondParam.getStartingFilter().hasStringFilter(), is(true));
        assertThat(secondParam.getStartingFilter().getPropertyName(), is("oid"));
        assertThat(secondParam.getStartingFilter().getStringFilter().getOptionsList(),
            containsInAnyOrder("1000", "2000", "3000"));

        assertThat(results.size(), is(1));
        final List<StatApiDTO> statistics = results.get(0).getStatistics();
        assertThat(statistics.size(), is(3));
        statistics.forEach(dto -> {
            assertThat(dto.getName(), is(NUM_VOLUMES));
            assertThat(dto.getRelatedEntityType(), is(UIEntityType.VIRTUAL_VOLUME.apiStr()));

            List<StatFilterApiDTO> statFilterApiDTOS = dto.getFilters();
            assertThat(statFilterApiDTOS.size(), is(1));
            StatFilterApiDTO statFilterApiDTO = statFilterApiDTOS.get(0);
            assertThat(statFilterApiDTO.getType(), is(UIEntityType.STORAGE_TIER.apiStr()));

            assertThat(statFilterApiDTO.getValue(), isOneOf(stDisplayName1, stDisplayName2, stDisplayName3));
            final Function<String, Float> getExpectedCount = displayName -> {
                switch (displayName) {
                    case stDisplayName1:
                        return 1f;
                    case stDisplayName2:
                    case stDisplayName3:
                        return 2f;
                    default:
                        return 0f;
                }
            };

            float expectedCount = getExpectedCount.apply(statFilterApiDTO.getValue());
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
    @Nonnull
    private ApiPartialEntity createVVApiPartialEntity(final long vvOid, final long storageTierOid) {
        return createBaseVVApiPartialEntity(vvOid, EnvironmentType.CLOUD)
            .addConnectedTo(RelatedEntity.newBuilder()
                    .setEntityType(EntityType.STORAGE_TIER.getValue())
                    .setOid(storageTierOid)
                    .build())
            .build();
    }

    /**
     * Create ApiPartialEntity Helper.
     *
     * @param vvOid Virtual Volume Oid
     * @return {@link ApiPartialEntity}
     */
    @Nonnull
    private ApiPartialEntity createVVApiPartialEntityWithOutST(final long vvOid) {
        return createBaseVVApiPartialEntity(vvOid, EnvironmentType.CLOUD).build();
    }

    /**
     * Create {@link ApiPartialEntity.Builder} object for Virtual Volume.
     *
     * @param vvOid Virtual Volume Oid
     * @param environmentType {@link EnvironmentType} environment type for VV
     * @return {@link ApiPartialEntity.Builder}
     */
    @Nonnull
    private ApiPartialEntity.Builder createBaseVVApiPartialEntity(final long vvOid, @Nonnull final EnvironmentType environmentType) {
        return ApiPartialEntity.newBuilder()
            .setOid(vvOid)
            .setEnvironmentType(environmentType)
            .setEntityType(EntityType.VIRTUAL_VOLUME.getValue());
    }

    /**
     * Create MinimalEntity Helper.
     *
     * @param oid oid of entity
     * @param displayName display name of entity
     * @param  entityType {@link EntityType} entity type
     * @return {@link ApiPartialEntity}
     */
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
    private StatApiInputDTO createStatApiInputDTO(@Nonnull final String name,
                                                  @Nullable final UIEntityType relatedEntityType,
                                                  @Nonnull final List<String> groupBy) {
        StatApiInputDTO statApiInputDTO = new StatApiInputDTO();
        statApiInputDTO.setName(name);
        if (relatedEntityType != null) {
            statApiInputDTO.setRelatedEntityType(relatedEntityType.apiStr());
        }
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

    private StatsQueryContext setupOidsScope(@Nullable Set<Long> entityOids) {
        StatsQueryContext scope = setupGlobalScope();
        when(scope.getQueryScope().getExpandedOids()).thenReturn(entityOids);
        return scope;
    }
}
