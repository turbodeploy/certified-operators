package com.vmturbo.api.component.external.api.mapper;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import org.apache.commons.collections4.CollectionUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import com.vmturbo.api.component.external.api.service.StatsService;
import com.vmturbo.api.component.external.api.service.TargetsService;
import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.statistic.StatApiInputDTO;
import com.vmturbo.api.dto.statistic.StatFilterApiDTO;
import com.vmturbo.api.dto.statistic.StatPeriodApiInputDTO;
import com.vmturbo.api.dto.statistic.StatScopesApiInputDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.dto.statistic.StatValueApiDTO;
import com.vmturbo.api.dto.target.TargetApiDTO;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.api.pagination.EntityStatsPaginationRequest;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.common.protobuf.common.Pagination.PaginationParameters;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatRecord;
import com.vmturbo.common.protobuf.cost.Cost.CostCategory;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance.PlanStatus;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.PlanTopologyStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats;
import com.vmturbo.common.protobuf.stats.Stats.ClusterStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats.EntityStats;
import com.vmturbo.common.protobuf.stats.Stats.EntityStatsScope;
import com.vmturbo.common.protobuf.stats.Stats.EntityStatsScope.EntityList;
import com.vmturbo.common.protobuf.stats.Stats.GetAveragedEntityStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats.GetEntityStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats.ProjectedEntityStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord.StatValue;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter.CommodityRequest;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.history.schema.RelationType;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTOREST.EntityDTO.EntityType;

/**
 * Unit tests for the static Mapper utility functions for the {@link StatsService}.
 */
public class StatsMapperTest {

    public static final long START_DATE = 1234L;
    public static final long END_DATE = 5678L;
    public static final String PUID = "puid-";
    public static final String CSP = "CSP";
    public static final String AWS = "AWS";
    public static final String COST_COMPONENT = "costComponent";
    private static final String VIRTUAL_MACHINE = "VirtualMachine";

    private PaginationMapper paginationMapper = mock(PaginationMapper.class);

    private TargetsService targetsService = mock(TargetsService.class);

    private StatsMapper statsMapper = spy(new StatsMapper(paginationMapper));

    /**
     * Test Conversion of gRPC stats call result to the ApiDTO to return for the REST API caller.
     */
    @Test
    public void toStatSnapshotApiDTOTest() throws Exception {
        String[] postfixes = {"A", "B", "C"};
        String[] relations = {RelationType.COMMODITIES.getLiteral(),
                              RelationType.COMMODITIESBOUGHT.getLiteral(),
                              RelationType.METRICS.getLiteral()};

        // Arrange
        Stats.StatSnapshot testSnapshot = Stats.StatSnapshot.newBuilder()
                .setSnapshotDate(START_DATE)
                .addAllStatRecords(buildStatRecords(postfixes, relations))
                .build();

        // Act
        StatSnapshotApiDTO mapped = statsMapper.toStatSnapshotApiDTO(testSnapshot);
        // Assert
        assertThat(DateTimeUtil.toString(testSnapshot.getSnapshotDate()), is(mapped.getDate()));
        assertThat(testSnapshot.getStatRecordsCount(), is(mapped.getStatistics().size()));
        assertEquals(3, testSnapshot.getStatRecordsCount());
        verifyMappedStatRecord(testSnapshot.getStatRecords(0), mapped.getStatistics().get(0),
                               "sold");
        verifyMappedStatRecord(testSnapshot.getStatRecords(1), mapped.getStatistics().get(1),
                               "bought");
        verifyMappedStatRecord(testSnapshot.getStatRecords(2), mapped.getStatistics().get(2),
                               "attribute");
    }

    @Test (expected = IllegalArgumentException.class)
    public void toStatSnapshotApiDTOWrongStatRelationTest() throws Exception {
        String[] postfixes = {"A"};
        String[] relations = {"WrongStatRelation"};

        // Arrange
        Stats.StatSnapshot testSnapshot = Stats.StatSnapshot.newBuilder()
                .setSnapshotDate(START_DATE)
                .addAllStatRecords(buildStatRecords(postfixes, relations))
                .build();

        // Act
        statsMapper.toStatSnapshotApiDTO(testSnapshot);
    }
    @Test
    public void toStatApiDTOStatKeyFilter() throws Exception {
        final String statKey = "foo";
        StatSnapshot snapshot = StatSnapshot.newBuilder()
                .addStatRecords(StatSnapshot.StatRecord.newBuilder()
                        .setStatKey(statKey))
                .build();

        final StatSnapshotApiDTO dto = statsMapper.toStatSnapshotApiDTO(snapshot);
        assertThat(dto.getStatistics().size(), is(1));
        assertThat(dto.getStatistics().get(0).getFilters().size(), is(1));
        StatFilterApiDTO filter = dto.getStatistics().get(0).getFilters().get(0);
        assertThat(filter.getType(), is(StatsMapper.FILTER_NAME_KEY));
        assertThat(filter.getValue(), is(statKey));
    }
    @Test
    public void testMetricsDoNotIncludeCapacityOrReserved() throws Exception {
        // Price index is a metric and metrics should not include capacities or reserved
        // or else the UI will render them as commodities with donut charts and utilizations.
        final String statMetricName = StatsMapper.METRIC_NAMES.iterator().next();
        StatSnapshot snapshot = StatSnapshot.newBuilder()
                .addStatRecords(StatSnapshot.StatRecord.newBuilder()
                        .setName(statMetricName))
                .build();

        final StatApiDTO dto = statsMapper.toStatSnapshotApiDTO(snapshot).getStatistics().get(0);
        assertNull(dto.getCapacity());
        assertNull(dto.getReserved());
    }

    @Test
    public void testToEntityStatsRequest() {
        // Arrange
        final EntityStatsScope scope = EntityStatsScope.newBuilder()
                .setEntityList(EntityList.newBuilder()
                        .addEntities(1L))
                .build();
        StatPeriodApiInputDTO apiRequestInput = new StatPeriodApiInputDTO();
        apiRequestInput.setStartDate(Long.toString(START_DATE));
        apiRequestInput.setEndDate(Long.toString(END_DATE));

        // first stat to fetch
        StatApiInputDTO statApiInputDTO = new StatApiInputDTO();
        statApiInputDTO.setName("stat1");
        statApiInputDTO.setRelatedEntityType("relatedType1");
        StatFilterApiDTO statFilterApiDTO1 = new StatFilterApiDTO();
        statFilterApiDTO1.setType("filter-type-1");
        statFilterApiDTO1.setValue("filter-value-1");

        StatFilterApiDTO statFilterApiDTO2 = new StatFilterApiDTO();
        statApiInputDTO.setRelatedEntityType("relatedType1");

        statFilterApiDTO2.setType("filter-type-2");
        statFilterApiDTO2.setValue("filter-value-2");
        statApiInputDTO.setFilters(Lists.newArrayList(statFilterApiDTO1, statFilterApiDTO2));

        // second stat to fetch
        StatApiInputDTO statApiInputDTO2 = new StatApiInputDTO();
        statApiInputDTO2.setName("stat2");
        apiRequestInput.setStatistics(Lists.newArrayList(statApiInputDTO, statApiInputDTO2));

        // Act
        final EntityStatsPaginationRequest paginationRequest = mock(EntityStatsPaginationRequest.class);
        when(paginationMapper.toProtoParams(paginationRequest))
            .thenReturn(PaginationParameters.getDefaultInstance());

        final GetEntityStatsRequest requestProtobuf = statsMapper.toEntityStatsRequest(scope,
                apiRequestInput, paginationRequest);

        // Assert
        assertThat(requestProtobuf.getScope(), equalTo(scope));
        assertTrue(requestProtobuf.hasFilter());
        final Stats.StatsFilter filter = requestProtobuf.getFilter();
        assertThat(filter.getStartDate(), equalTo(Long.valueOf(apiRequestInput.getStartDate())));
        assertThat(filter.getEndDate(), equalTo(Long.valueOf(apiRequestInput.getEndDate())));
        assertThat(filter.getCommodityRequestsCount(), equalTo(2));
        assertThat(filter.getCommodityRequestsList(), containsInAnyOrder(
                Stats.StatsFilter.CommodityRequest.newBuilder()
                        .setCommodityName("stat1")
                        .addPropertyValueFilter(Stats.StatsFilter.PropertyValueFilter.newBuilder()
                                .setProperty("filter-type-1")
                                .setValue("filter-value-1")
                                .build())
                        .addPropertyValueFilter(Stats.StatsFilter.PropertyValueFilter.newBuilder()
                                .setProperty("filter-type-2")
                                .setValue("filter-value-2")
                                .build())
                        .setRelatedEntityType("relatedType1")
                        .build(),
                Stats.StatsFilter.CommodityRequest.newBuilder()
                        .setCommodityName("stat2")
                        .build()
        ));
    }

    /**
     * Test to make sure that both start and end times are being parsed with the utility that
     * understands relative times, not just millis-since-epoch
     *
     * <p>This test proves the fix for OM-48318. It's just a single case that failed prior to the
     * fix. The parsing utility method has its own existing suite of tests.</p>
     */
    @Test
    public void testToEntityStatsRequestOffsetTimes() {
        // Arrange
        final EntityStatsScope scope = EntityStatsScope.newBuilder()
            .setEntityList(EntityList.newBuilder()
                .addEntities(1L))
            .build();
        StatPeriodApiInputDTO apiRequestInput = new StatPeriodApiInputDTO();
        apiRequestInput.setStartDate("-5d");
        apiRequestInput.setEndDate("+2h");
        final EntityStatsPaginationRequest paginationRequest = mock(EntityStatsPaginationRequest.class);
        when(paginationMapper.toProtoParams(paginationRequest))
            .thenReturn(PaginationParameters.getDefaultInstance());
        final GetEntityStatsRequest requestProtobuf = statsMapper.toEntityStatsRequest(scope,
            apiRequestInput, paginationRequest);

        // calculate time ranges to test, with some slop to account for clock advance since mapping
        Instant now = Instant.now();
        Instant start = Instant.ofEpochMilli(requestProtobuf.getFilter().getStartDate());
        Instant end = Instant.ofEpochMilli(requestProtobuf.getFilter().getEndDate());
        Duration slop = Duration.ofSeconds(5);
        assertThat(Duration.between(start, now).toDays(), equalTo(5L));
        assertThat(Duration.between(now.minus(slop), end).toHours(), equalTo(2L));
    }

    @Test
    public void testToEntityStatsRequestDefaults() {
        // Arrange
        final EntityStatsScope scope = EntityStatsScope.newBuilder()
                .setEntityList(EntityList.newBuilder()
                        .addEntities(1L))
                .build();
        StatPeriodApiInputDTO apiRequestInput = new StatPeriodApiInputDTO();

        final EntityStatsPaginationRequest paginationRequest = mock(EntityStatsPaginationRequest.class);
        when(paginationMapper.toProtoParams(paginationRequest))
                .thenReturn(PaginationParameters.getDefaultInstance());

        // Act
        GetEntityStatsRequest requestProtobuf = statsMapper.toEntityStatsRequest(scope,
                apiRequestInput, paginationRequest);

        // Assert
        assertThat(requestProtobuf.getScope(), equalTo(scope));
        assertTrue(requestProtobuf.hasFilter());
        final Stats.StatsFilter filter = requestProtobuf.getFilter();
        assertFalse(filter.hasStartDate());
        assertFalse(filter.hasEndDate());
        assertThat(filter.getCommodityRequestsCount(), equalTo(0));
        assertThat(filter.getCommodityRequestsCount(), equalTo(0));
        assertThat(filter.getCommodityAttributesCount(), equalTo(0));
    }

    @Test
    public void testToEntityStatsRequestDefaultsWithNullPeriod() {
        // Arrange
        final EntityStatsScope scope = EntityStatsScope.newBuilder()
                .setEntityList(EntityList.newBuilder()
                        .addEntities(1L))
                .build();

        final EntityStatsPaginationRequest paginationRequest = mock(EntityStatsPaginationRequest.class);
        when(paginationMapper.toProtoParams(paginationRequest))
                .thenReturn(PaginationParameters.getDefaultInstance());

        // Act
        GetEntityStatsRequest requestProtobuf = statsMapper.toEntityStatsRequest(scope,
                null, paginationRequest);

        // Assert
        assertThat(requestProtobuf.getScope(), equalTo(scope));
        assertTrue(requestProtobuf.hasFilter());
        final Stats.StatsFilter filter = requestProtobuf.getFilter();
        assertFalse(filter.hasStartDate());
        assertFalse(filter.hasEndDate());
        assertThat(filter.getCommodityRequestsCount(), equalTo(0));
        assertThat(filter.getCommodityRequestsCount(), equalTo(0));
        assertThat(filter.getCommodityAttributesCount(), equalTo(0));
    }

    private static final PlanInstance PLAN_INSTANCE = PlanInstance.newBuilder()
            .setPlanId(7L)
            .setProjectedTopologyId(77L)
            .setStatus(PlanStatus.SUCCEEDED)
            .build();

    private static final EntityStatsPaginationRequest ENTITY_PAGINATION_REQUEST =
            new EntityStatsPaginationRequest("foo", 1, true, "orderBy");

    private static final PaginationParameters MAPPED_PAGINATION_PARAMS = PaginationParameters.newBuilder()
                .setCursor("this is fake")
                .build();

    @Before
    public void setup() throws UnknownObjectException {
        when(paginationMapper.toProtoParams(ENTITY_PAGINATION_REQUEST))
                .thenReturn(MAPPED_PAGINATION_PARAMS);
        final TargetApiDTO targetApiDTO = new TargetApiDTO();
        targetApiDTO.setUuid("11111");
        targetApiDTO.setType("AWS Billing");
        targetApiDTO.setDisplayName("engineering.aws.com");
        when(targetsService.getTarget(anyString())).thenReturn(targetApiDTO);
    }

    @Test
    public void testToPlanTopologyStatsRequestTopologyIdSet() {
        final StatScopesApiInputDTO inputDto = new StatScopesApiInputDTO();

        final PlanTopologyStatsRequest request =
                statsMapper.toPlanTopologyStatsRequest(PLAN_INSTANCE, inputDto, ENTITY_PAGINATION_REQUEST);
        assertThat(request.getTopologyId(), is(PLAN_INSTANCE.getProjectedTopologyId()));
    }

    @Test
    public void testToPlanTopologyStatsRequestPeriodStartDate() {
        final StatScopesApiInputDTO inputDto = new StatScopesApiInputDTO();
        final StatPeriodApiInputDTO period = new StatPeriodApiInputDTO();
        period.setStartDate("7");
        inputDto.setPeriod(period);

        final PlanTopologyStatsRequest request =
                statsMapper.toPlanTopologyStatsRequest(PLAN_INSTANCE, inputDto, ENTITY_PAGINATION_REQUEST);
        assertThat(request.getFilter().getStartDate(), is(7L));
    }

    @Test
    public void testToPlanTopologyStatsRequestPeriodEndDate() {
        final StatScopesApiInputDTO inputDto = new StatScopesApiInputDTO();
        final StatPeriodApiInputDTO period = new StatPeriodApiInputDTO();
        period.setEndDate("7");
        inputDto.setPeriod(period);

        final PlanTopologyStatsRequest request =
                statsMapper.toPlanTopologyStatsRequest(PLAN_INSTANCE, inputDto, ENTITY_PAGINATION_REQUEST);
        assertThat(request.getFilter().getEndDate(), is(7L));
    }

    @Test
    public void testToPlanTopologyStatsRequestPeriodStats() {
        final StatScopesApiInputDTO inputDto = new StatScopesApiInputDTO();
        final StatPeriodApiInputDTO period = new StatPeriodApiInputDTO();
        final StatApiInputDTO statDto = new StatApiInputDTO();
        statDto.setName("Wolf");
        period.setStatistics(Collections.singletonList(statDto));
        inputDto.setPeriod(period);

        final PlanTopologyStatsRequest request =
                statsMapper.toPlanTopologyStatsRequest(PLAN_INSTANCE, inputDto, ENTITY_PAGINATION_REQUEST);
        assertThat(request.getFilter().getCommodityRequestsList(), containsInAnyOrder(
            CommodityRequest.newBuilder()
                .setCommodityName(statDto.getName())
                .build()));
    }

    @Test
    public void testToPlanTopologyStatsRequestRelatedType() {
        final StatScopesApiInputDTO inputDto = new StatScopesApiInputDTO();
        inputDto.setRelatedType("Cousin");

        final PlanTopologyStatsRequest request =
                statsMapper.toPlanTopologyStatsRequest(PLAN_INSTANCE, inputDto, ENTITY_PAGINATION_REQUEST);
        assertThat(request.getRelatedEntityType(), is(inputDto.getRelatedType()));
    }

    @Test
    public void testToPlanTopologyStatsRequestPaginationParams() {
        final StatScopesApiInputDTO inputDto = new StatScopesApiInputDTO();

        final PlanTopologyStatsRequest request =
                statsMapper.toPlanTopologyStatsRequest(PLAN_INSTANCE, inputDto, ENTITY_PAGINATION_REQUEST);
        assertThat(request.getPaginationParams(), is(MAPPED_PAGINATION_PARAMS));
    }

    @Test
    public void testToPlanTopologyStatsRequestScopes() {
        final StatScopesApiInputDTO inputDto = new StatScopesApiInputDTO();
        inputDto.setScopes(Lists.newArrayList("1", "2"));

        final PlanTopologyStatsRequest request =
                statsMapper.toPlanTopologyStatsRequest(PLAN_INSTANCE, inputDto, ENTITY_PAGINATION_REQUEST);
        assertThat(request.getEntityFilter().getEntityIdsList(), containsInAnyOrder(1L, 2L));
    }

    @Test
    public void testToPlanTopologyStatsRequestEmptyScopes() {
        final StatScopesApiInputDTO inputDto = new StatScopesApiInputDTO();
        inputDto.setScopes(Collections.emptyList());

        final PlanTopologyStatsRequest request =
                statsMapper.toPlanTopologyStatsRequest(PLAN_INSTANCE, inputDto, ENTITY_PAGINATION_REQUEST);
        // Request shouldn't have an entity filter at all, instead of having an entity filter
        // with no entity IDS.
        assertFalse(request.hasEntityFilter());
    }

    private static final StatsFilter STATS_FILTER = StatsFilter.newBuilder()
            .setStartDate(7777)
            .build();

    @Test
    public void testClusterStatsRequest() {
        final StatPeriodApiInputDTO period = new StatPeriodApiInputDTO();
        when(statsMapper.newPeriodStatsFilter(period, Optional.empty())).thenReturn(STATS_FILTER);
        final ClusterStatsRequest clusterStatsRequest =
                statsMapper.toClusterStatsRequest("7", period);
        assertThat(clusterStatsRequest.getClusterId(), is(7L));
        assertThat(clusterStatsRequest.getStats(), is(STATS_FILTER));
    }

    @Test
    public void testClusterStatsRequestWithNullPeriod() {
        final StatPeriodApiInputDTO period = null;
        when(statsMapper.newPeriodStatsFilter(period, Optional.empty())).thenReturn(STATS_FILTER);
        final ClusterStatsRequest clusterStatsRequest =
                statsMapper.toClusterStatsRequest("7", period);
        assertThat(clusterStatsRequest.getClusterId(), is(7L));
        assertThat(clusterStatsRequest.getStats(), is(STATS_FILTER));
    }

    @Test
    public void testAveragedEntityStatsRequestNoTempGroupType() {
        final StatPeriodApiInputDTO period = new StatPeriodApiInputDTO();
        when(statsMapper.newPeriodStatsFilter(period, Optional.empty())).thenReturn(STATS_FILTER);

        final Set<Long> uuids = Sets.newHashSet(1L, 2L);

        final GetAveragedEntityStatsRequest request =
                statsMapper.toAveragedEntityStatsRequest(uuids, period, Optional.empty());
        assertThat(request.getEntitiesList(), containsInAnyOrder(uuids.toArray()));
        assertThat(request.getFilter(), is(STATS_FILTER));
        assertTrue(request.getRelatedEntityType().isEmpty());
    }

    @Test
    public void testAveragedEntityStatsRequestWithTempGroupType() {
        final StatPeriodApiInputDTO period = new StatPeriodApiInputDTO();
        final Optional<Integer> tempGroupType = Optional.of(10);
        when(statsMapper.newPeriodStatsFilter(period, tempGroupType)).thenReturn(STATS_FILTER);

        final Set<Long> uuids = Sets.newHashSet(1L, 2L);

        final GetAveragedEntityStatsRequest request =
                statsMapper.toAveragedEntityStatsRequest(uuids, period, tempGroupType);
        assertTrue(request.getEntitiesList().containsAll(uuids));
        assertEquals(2, request.getEntitiesCount());
        assertThat(request.getFilter(), is(STATS_FILTER));
        assertEquals(
            UIEntityType.fromType(tempGroupType.get()).apiStr(), request.getRelatedEntityType());
    }

    @Test
    public void testAveragedEntityStatsRequestWithNullPeriod() {
        final Optional<Integer> tempGroupType = Optional.of(10);
        when(statsMapper.newPeriodStatsFilter(null, tempGroupType)).thenReturn(STATS_FILTER);

        final Set<Long> uuids = Sets.newHashSet(1L, 2L);

        final GetAveragedEntityStatsRequest request =
                statsMapper.toAveragedEntityStatsRequest(uuids, null, tempGroupType);
        assertTrue(request.getEntitiesList().containsAll(uuids));
        assertThat(request.getFilter(), is(STATS_FILTER));
        assertThat(request.getRelatedEntityType(), is(VIRTUAL_MACHINE));
    }

    @Test
    public void testAverageEntityStatsRequestWithMultipleEntityTypes() {
        final StatPeriodApiInputDTO periodApiInputDTO = new StatPeriodApiInputDTO();
        final StatApiInputDTO apiInputDTO1 = new StatApiInputDTO();
        final StatApiInputDTO apiInputDTO2 = new StatApiInputDTO();
        periodApiInputDTO.setStatistics(ImmutableList.of(apiInputDTO1, apiInputDTO2));
        apiInputDTO1.setRelatedEntityType(UIEntityType.APPLICATION.apiStr());
        apiInputDTO1.setName("name1");
        apiInputDTO2.setRelatedEntityType(UIEntityType.VIRTUAL_MACHINE.apiStr());
        apiInputDTO2.setName("name2");
        final GetAveragedEntityStatsRequest request =
            statsMapper.toAveragedEntityStatsRequest(
                Collections.emptySet(), periodApiInputDTO, Optional.empty());
        assertEquals(2, request.getFilter().getCommodityRequestsCount());
        assertEquals(
            UIEntityType.APPLICATION.apiStr(),
            request.getFilter().getCommodityRequests(0).getRelatedEntityType());
        assertEquals(
            UIEntityType.VIRTUAL_MACHINE.apiStr(),
            request.getFilter().getCommodityRequests(1).getRelatedEntityType());
    }

    @Test
    public void testAveragedEntityStatsRequestWithStatistics() {
        final StatPeriodApiInputDTO statPeriodApiInputDTO = new StatPeriodApiInputDTO();
        StatApiInputDTO statApiInputDTO = new StatApiInputDTO();
        statApiInputDTO.setRelatedEntityType(VIRTUAL_MACHINE);
        statPeriodApiInputDTO.setStatistics(Lists.newArrayList(statApiInputDTO));
        final GetAveragedEntityStatsRequest request =
            statsMapper.toAveragedEntityStatsRequest(new HashSet<>(), statPeriodApiInputDTO,
                Optional.empty());
        assertTrue(request.getEntitiesList().isEmpty());
        assertEquals(
            UIEntityType.VIRTUAL_MACHINE.apiStr(),
            request.getFilter().getCommodityRequests(0).getRelatedEntityType());
        assertFalse(request.hasRelatedEntityType());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAveragedEntityStatsNotMatchingRelatedEntityTypes() {
        final StatPeriodApiInputDTO statPeriodApiInputDTO = new StatPeriodApiInputDTO();
        StatApiInputDTO statApiInputDTO = new StatApiInputDTO();
        statApiInputDTO.setRelatedEntityType(VIRTUAL_MACHINE);
        statPeriodApiInputDTO.setStatistics(Lists.newArrayList(statApiInputDTO));
        statsMapper.toAveragedEntityStatsRequest(
            new HashSet<>(), statPeriodApiInputDTO, Optional.of(UIEntityType.PHYSICAL_MACHINE.ordinal()));
    }

    @Test
    public void testAveragedEntityStatsRequestWithoutRelatedEntityType() {
        final StatPeriodApiInputDTO statPeriodApiInputDTO = new StatPeriodApiInputDTO();
        when(statsMapper.newPeriodStatsFilter(statPeriodApiInputDTO,
            Optional.empty())).thenReturn(STATS_FILTER);
        StatApiInputDTO statApiInputDTO = new StatApiInputDTO();
        statPeriodApiInputDTO.setStatistics(Lists.newArrayList(statApiInputDTO));
        final GetAveragedEntityStatsRequest request =
            statsMapper.toAveragedEntityStatsRequest(new HashSet<>(), statPeriodApiInputDTO,
                Optional.empty());
        assertTrue(request.getEntitiesList().isEmpty());
        assertThat(request.getFilter(), is(STATS_FILTER));
        assertTrue(request.getRelatedEntityType().isEmpty());
    }

    /**
     * "1M" is one month as defined in {@link DateTimeUtil}.
     * The unit tests for {@link DateTimeUtil} is covered in OpsMgr, here we just verify it doesn't
     * throw exception.
     */
    @Test
    public void testNewPeriodStatsFilterWith1MendDate() {
        StatsMapper localStatsMapper = new StatsMapper(new PaginationMapper());
        StatPeriodApiInputDTO statPeriodApiInputDTO = new StatPeriodApiInputDTO();
        statPeriodApiInputDTO.setEndDate("1M");
        StatsFilter filter = localStatsMapper.newPeriodStatsFilter(statPeriodApiInputDTO, Optional.empty());
        assertTrue(filter.hasEndDate());
    }

    /**
     * Test that data center stats can be retrieved successfully, even though the search is really
     * done on stats of a group of physical machines.
     */
    @Test
    public void testNewPeriodStatsFilterWithDataCenters() {
        StatsMapper localStatsMapper = new StatsMapper(new PaginationMapper());
        StatPeriodApiInputDTO statPeriodApiInputDTO = new StatPeriodApiInputDTO();
        List<StatApiInputDTO> statistics = Lists.newArrayListWithCapacity(1);
        // The API caller is requesting stats for a DATACENTER
        statistics.add(new StatApiInputDTO(CommodityType.CPU_ALLOCATION.name(),
            UIEntityType.DATACENTER.apiStr(), null, null));
        statPeriodApiInputDTO.setStatistics(statistics);
        // Stats for a data center will be collected with a group entity type of PHYSICAL_MACHINE
        Optional<Integer> groupEntityType = Optional.of(EntityType.PHYSICAL_MACHINE.getValue());
        StatsFilter filter =
            localStatsMapper.newPeriodStatsFilter(statPeriodApiInputDTO, groupEntityType);
        // All resulting commodity requests should have the related entity type of DATACENTER,
        // expressed in the API format
        filter.getCommodityRequestsList().stream()
            .filter(CommodityRequest::hasRelatedEntityType)
            .map(CommodityRequest::getRelatedEntityType)
            .forEach(relatedEntityType -> assertEquals(UIEntityType.DATACENTER.apiStr(), relatedEntityType));
    }

    @Test
    public void testProjectedEntityStatsRequestIdsAndPaginationParams() {
        final StatPeriodApiInputDTO period = new StatPeriodApiInputDTO();
        final Set<Long> uuids = Sets.newHashSet(1L, 2L);

        final ProjectedEntityStatsRequest request =
                statsMapper.toProjectedEntityStatsRequest(uuids, period, ENTITY_PAGINATION_REQUEST);
        assertThat(request.getEntitiesList(), containsInAnyOrder(uuids.toArray()));
        assertThat(request.getPaginationParams(), is(MAPPED_PAGINATION_PARAMS));
    }

    @Test
    public void testProjectedEntityStatsRequestWithNullPeriod() {
        final Set<Long> uuids = Sets.newHashSet(1L, 2L);

        final ProjectedEntityStatsRequest request =
                statsMapper.toProjectedEntityStatsRequest(uuids, null, ENTITY_PAGINATION_REQUEST);
        assertThat(request.getEntitiesList(), containsInAnyOrder(uuids.toArray()));
        assertThat(request.getPaginationParams(), is(MAPPED_PAGINATION_PARAMS));
    }

    @Test
    public void testProjectedEntityStatsRequestStatistics() {
        final StatPeriodApiInputDTO period = new StatPeriodApiInputDTO();

        final List<String> stats = Lists.newArrayList("peanut", "walnut");
        period.setStatistics(stats.stream().map(name -> {
            final StatApiInputDTO statDto = new StatApiInputDTO();
            statDto.setName(name);
            return statDto;
        }).collect(Collectors.toList()));

        final Set<Long> uuids = Sets.newHashSet(1L, 2L);

        final ProjectedEntityStatsRequest request =
                statsMapper.toProjectedEntityStatsRequest(uuids, period, ENTITY_PAGINATION_REQUEST);

        assertThat(request.getCommodityNameList(), containsInAnyOrder(stats.toArray()));
    }

    @Test
    public void testToStatSnapshotApiDTOWithCloudData() throws Exception {
        verifyFilters(CSP, AWS);
        verifyFilters("target", "engineering.aws.com");
    }

    @Test
    public void testToStatSnapshotApiDTOWithCloudService() throws Exception {
        String[] postfixes = {"A", "B", "C"};
        String[] relations = {RelationType.COMMODITIES.getLiteral(),
                RelationType.COMMODITIESBOUGHT.getLiteral(),
                RelationType.METRICS.getLiteral()};

       // Arrange
        StatSnapshot testSnapshot = StatSnapshot.newBuilder()
                .setSnapshotDate(START_DATE)
                .addAllStatRecords(buildStatRecords(postfixes, relations))
                .build();

        TargetApiDTO targetApiDTO1 = new TargetApiDTO();
        targetApiDTO1.setUuid("4");
        targetApiDTO1.setType("AWS");
        TargetApiDTO targetApiDTO2 = new TargetApiDTO();
        targetApiDTO2.setUuid("5");
        TargetApiDTO targetApiDTO3 = new TargetApiDTO();
        targetApiDTO3.setUuid("6");
        targetApiDTO3.setType("AWS Cost");

        BaseApiDTO apiDTO1 = new BaseApiDTO();
        apiDTO1.setDisplayName("name1");
        apiDTO1.setUuid("4");


        BaseApiDTO apiDTO2 = new BaseApiDTO();
        apiDTO2.setDisplayName("name2");
        apiDTO2.setUuid("5");

        BaseApiDTO apiDTO3 = new BaseApiDTO();
        apiDTO3.setDisplayName("name3");
        apiDTO3.setUuid("6");

        final CloudCostStatRecord cloudStatRecord = CloudCostStatRecord.newBuilder()
                .setSnapshotDate("date-value")
                .addStatRecords(getStatRecordBuilder(null, 1l, Optional.of(4l)))
                .addStatRecords(getStatRecordBuilder(null, 2l, Optional.of(5l)))
                .addStatRecords(getStatRecordBuilder(null, 3l,  Optional.of(6l)))
                .build();

        // Act
        StatSnapshotApiDTO mapped = statsMapper.toStatSnapshotApiDTO(cloudStatRecord,
                ImmutableList.of(targetApiDTO1, targetApiDTO2, targetApiDTO3),
                s -> StatsService.CLOUD_SERVICE,
                s -> StatsService.CLOUD_SERVICE,
                ImmutableList.of(apiDTO1, apiDTO2, apiDTO3),
                targetsService);
        // Assert
        assertThat(cloudStatRecord.getSnapshotDate(), is(mapped.getDate()));
        assertThat(cloudStatRecord.getStatRecordsCount(), is(mapped.getStatistics().size()));
        assertEquals(3, cloudStatRecord.getStatRecordsCount());
        assertTrue(mapped.getStatistics().stream().allMatch(statApiDTO ->
                statApiDTO.getFilters().stream().allMatch(statFilterApiDTO ->
                        statFilterApiDTO.getType().equals(StatsService.CLOUD_SERVICE))));
        assertTrue(mapped.getStatistics().stream().anyMatch(statApiDTO ->
                statApiDTO.getFilters().stream().anyMatch(statFilterApiDTO ->
                        statFilterApiDTO.getValue().equals("name1"))));
        assertTrue(mapped.getStatistics().stream().anyMatch(statApiDTO ->
                statApiDTO.getFilters().stream().anyMatch(statFilterApiDTO ->
                        statFilterApiDTO.getValue().equals("name2"))));
        assertTrue(mapped.getStatistics().stream().anyMatch(statApiDTO ->
                statApiDTO.getFilters().stream().anyMatch(statFilterApiDTO ->
                        statFilterApiDTO.getValue().equals("name3"))));
        assertTrue(mapped.getStatistics().stream().allMatch(statApiDTO -> statApiDTO.getFilters().size() ==1));

    }

    private void verifyFilters(final String csp, final String aws) {
        String[] postfixes = {"A", "B", "C"};
        String[] relations = {RelationType.COMMODITIES.getLiteral(),
                RelationType.COMMODITIESBOUGHT.getLiteral(),
                RelationType.METRICS.getLiteral()};

        // Arrange
        StatSnapshot testSnapshot = StatSnapshot.newBuilder()
                .setSnapshotDate(START_DATE)
                .addAllStatRecords(buildStatRecords(postfixes, relations))
                .build();

        TargetApiDTO targetApiDTO1 = new TargetApiDTO();
        targetApiDTO1.setType("AWS");
        targetApiDTO1.setUuid("4");
        TargetApiDTO targetApiDTO2 = new TargetApiDTO();
        targetApiDTO2.setUuid("11111");
        targetApiDTO2.setType("AWS");
        TargetApiDTO targetApiDTO3 = new TargetApiDTO();
        targetApiDTO3.setUuid("22222");
        targetApiDTO3.setType("AWS");

        final CloudCostStatRecord cloudStatRecord = CloudCostStatRecord.newBuilder()
                .setSnapshotDate("date-value")
                .addStatRecords(getStatRecordBuilder(null, 1l, Optional.empty()))
                .addStatRecords(getStatRecordBuilder(null, 2l, Optional.empty()))
                .addStatRecords(getStatRecordBuilder(null, 4l, Optional.empty()))
                .build();

        // Act
        StatSnapshotApiDTO mapped = statsMapper.toStatSnapshotApiDTO(cloudStatRecord,
                ImmutableList.of(targetApiDTO1, targetApiDTO2, targetApiDTO3),
                s -> csp,
                s -> aws,
                Collections.EMPTY_LIST,
                targetsService);
        // Assert
        assertThat(cloudStatRecord.getSnapshotDate(), is(mapped.getDate()));
        assertThat(cloudStatRecord.getStatRecordsCount(), is(mapped.getStatistics().size()));
        assertEquals(3, cloudStatRecord.getStatRecordsCount());
        assertTrue(mapped.getStatistics().stream().allMatch(statApiDTO ->
                statApiDTO.getFilters().stream().allMatch(statFilterApiDTO ->
                        statFilterApiDTO.getType().equals(csp))));
        assertTrue(mapped.getStatistics().stream().allMatch(statApiDTO ->
                statApiDTO.getFilters().stream().allMatch(statFilterApiDTO ->
                        statFilterApiDTO.getValue().equals(aws))));
    }

    @Test
    public void testToStatSnapshotApiDTOWithCloudDataAndHiddenProbe() throws Exception {
        String[] postfixes = {"A", "B", "C"};
        String[] relations = {RelationType.COMMODITIES.getLiteral(),
                RelationType.COMMODITIESBOUGHT.getLiteral(),
                RelationType.METRICS.getLiteral()};

        // Arrange
        StatSnapshot testSnapshot = StatSnapshot.newBuilder()
                .setSnapshotDate(START_DATE)
                .addAllStatRecords(buildStatRecords(postfixes, relations))
                .build();

        TargetApiDTO targetApiDTO1 = new TargetApiDTO();
        targetApiDTO1.setType("AWS");
        targetApiDTO1.setUuid("4");
        TargetApiDTO targetApiDTO3 = new TargetApiDTO();
        targetApiDTO3.setUuid("22222");
        targetApiDTO3.setType("AWS");

        final CloudCostStatRecord cloudStatRecord = CloudCostStatRecord.newBuilder()
                .setSnapshotDate("date-value")
                .addStatRecords(getStatRecordBuilder(null, 1l, Optional.of(11111L)))
                .addStatRecords(getStatRecordBuilder(null, 2l, Optional.empty()))
                .addStatRecords(getStatRecordBuilder(null, 4l, Optional.empty()))
                .build();

        // Act
        StatSnapshotApiDTO mapped = statsMapper.toStatSnapshotApiDTO(cloudStatRecord,
                ImmutableList.of(targetApiDTO1, targetApiDTO3),
                s -> "CSP",
                s -> "AWS",
                Collections.EMPTY_LIST,
                targetsService);
        // Assert
        assertThat(cloudStatRecord.getSnapshotDate(), is(mapped.getDate()));
        assertThat(cloudStatRecord.getStatRecordsCount(), is(mapped.getStatistics().size()));
        assertEquals(3, cloudStatRecord.getStatRecordsCount());
        assertTrue(mapped.getStatistics().stream().allMatch(statApiDTO ->
                statApiDTO.getFilters().stream().allMatch(statFilterApiDTO ->
                        statFilterApiDTO.getType().equals("CSP"))));
        assertTrue(mapped.getStatistics().stream().allMatch(statApiDTO ->
                statApiDTO.getFilters().stream().allMatch(statFilterApiDTO ->
                        statFilterApiDTO.getValue().equals("AWS"))));
    }

    @Test
    public void testToSnapShotApiDTODisplayNames() {
        final long oid = 0L;
        final String providerName = "provider";
        final String key = "key";
        final EntityStats entityStats =
            EntityStats.newBuilder()
                .setOid(oid)
                .addStatSnapshots(
                    StatSnapshot.newBuilder()
                        .addStatRecords(StatRecord.newBuilder()
                                          .setRelation(RelationType.COMMODITIES.getLiteral())
                                          .setName(CommodityType.VCPU.toString()))
                        .addStatRecords(StatRecord.newBuilder()
                                           .setRelation(RelationType.COMMODITIESBOUGHT.getLiteral())
                                           .setName(CommodityType.CPU.toString())
                                           .setProviderDisplayName(providerName))
                        .addStatRecords(StatRecord.newBuilder()
                                           .setRelation(RelationType.COMMODITIESBOUGHT.getLiteral())
                                           .setName(CommodityType.CLUSTER.toString())
                                           .setProviderDisplayName(providerName)
                                           .setStatKey(key)))
                .build();

        final StatSnapshotApiDTO result = statsMapper.toStatSnapshotApiDTO(entityStats.getStatSnapshots(0));

        Assert.assertEquals(3L, result.getStatistics().size());
        Assert.assertEquals("", result.getStatistics().get(0).getDisplayName());
        Assert.assertEquals("FROM: " + providerName + " ", result.getStatistics().get(1).getDisplayName());
        Assert.assertEquals("FROM: " + providerName + " KEY: " + key,
                            result.getStatistics().get(2).getDisplayName());
    }

    @Test
    public void testToCloudStatSnapshotApiDTO() throws Exception{
        final CloudCostStatRecord cloudStatRecord = CloudCostStatRecord.newBuilder()
                .setSnapshotDate(DateTimeUtil.toString(1))
                .addStatRecords(getStatRecordBuilder(CostCategory.ON_DEMAND_COMPUTE, 1l, Optional.empty()))
                .addStatRecords(getStatRecordBuilder(CostCategory.IP, 2l, Optional.empty()))
                .addStatRecords(getStatRecordBuilder(CostCategory.LICENSE, 3l, Optional.empty()))
                .addStatRecords(getStatRecordBuilder(CostCategory.STORAGE, 4l, Optional.empty()))
                .addStatRecords(getStatRecordBuilder(CostCategory.RI_COMPUTE, 5l, Optional.empty()))
                .build();
        final StatSnapshotApiDTO mapped = statsMapper.toCloudStatSnapshotApiDTO(cloudStatRecord);
        assertThat(cloudStatRecord.getSnapshotDate(), is(mapped.getDate()));
        assertThat(cloudStatRecord.getStatRecordsCount(), is(mapped.getStatistics().size()));
        assertEquals(5, cloudStatRecord.getStatRecordsCount());
        assertTrue(mapped.getStatistics().stream().allMatch(statApiDTO -> statApiDTO.getFilters() != null ));
        assertTrue(mapped.getStatistics().stream().anyMatch(statApiDTO -> statApiDTO.getFilters().get(0).getValue().equals(CostCategory.ON_DEMAND_COMPUTE.name())
                && statApiDTO.getFilters().get(0).getType().equals(COST_COMPONENT) ));
        assertTrue(mapped.getStatistics().stream().anyMatch(statApiDTO -> statApiDTO.getFilters().get(0).getValue().equals(CostCategory.IP.name())
                && statApiDTO.getFilters().get(0).getType().equals(COST_COMPONENT) ));
        assertTrue(mapped.getStatistics().stream().anyMatch(statApiDTO -> statApiDTO.getFilters().get(0).getValue().equals(CostCategory.LICENSE.name())
                && statApiDTO.getFilters().get(0).getType().equals(COST_COMPONENT) ));
        assertTrue(mapped.getStatistics().stream().anyMatch(statApiDTO -> statApiDTO.getFilters().get(0).getValue().equals(CostCategory.STORAGE.name())
                && statApiDTO.getFilters().get(0).getType().equals(COST_COMPONENT) ));
        assertTrue(mapped.getStatistics().stream().anyMatch(statApiDTO -> statApiDTO.getFilters().get(0).getValue().equals(CostCategory.RI_COMPUTE.name())
                && statApiDTO.getFilters().get(0).getType().equals(COST_COMPONENT) ));
    }

    @Test
    public void testToCloudStatSnapshotApiDTOWithEmptyCostCategory() throws Exception{
        final CloudCostStatRecord cloudStatRecord = CloudCostStatRecord.newBuilder()
                .setSnapshotDate(DateTimeUtil.toString(1))
                .addStatRecords(getStatRecordBuilder(null, 1l, Optional.empty()))
                .build();
        final StatSnapshotApiDTO mapped = statsMapper.toCloudStatSnapshotApiDTO(cloudStatRecord);
        assertThat(cloudStatRecord.getSnapshotDate(), is(mapped.getDate()));
        assertThat(cloudStatRecord.getStatRecordsCount(), is(mapped.getStatistics().size()));
        assertEquals(1, cloudStatRecord.getStatRecordsCount());
        assertTrue(mapped.getStatistics().stream().allMatch(statApiDTO -> CollectionUtils.isEmpty(statApiDTO.getFilters())));
    }

    private CloudCostStatRecord.StatRecord.Builder getStatRecordBuilder(@Nullable CostCategory costCategory, float value, Optional<Long> associatedEntityId) {
        final CloudCostStatRecord.StatRecord.Builder statRecordBuilder = CloudCostStatRecord.StatRecord.newBuilder();
        statRecordBuilder.setName(StringConstants.COST_PRICE);
        statRecordBuilder.setUnits(StringConstants.DOLLARS_PER_HOUR);

        statRecordBuilder.setAssociatedEntityId(associatedEntityId.orElse(4l));

        statRecordBuilder.setAssociatedEntityType(1);
        if (costCategory != null) {
            statRecordBuilder.setCategory(costCategory);
        }
        CloudCostStatRecord.StatRecord.StatValue.Builder statValueBuilder = CloudCostStatRecord.StatRecord.StatValue.newBuilder();

        statValueBuilder.setAvg(value);

        statValueBuilder.setTotal(value);
        statValueBuilder.setMax(value);
        statValueBuilder.setMin(value);

        statRecordBuilder.setValues(statValueBuilder.build());
        return statRecordBuilder;
    }

    private void verifyMappedStatRecord(StatRecord test,
                                        StatApiDTO mappedStat,
                                        String relationshipType) {
        assertThat(mappedStat.getName(), is(test.getName()));
        assertThat(mappedStat.getReserved().getTotal(), is(test.getReserved()));

        assertEquals(mappedStat.getCapacity().getMin(), test.getCapacity().getMin(), 0);
        assertEquals(mappedStat.getCapacity().getMax(), test.getCapacity().getMax(), 0);
        assertEquals(mappedStat.getCapacity().getAvg(), test.getCapacity().getAvg(), 0);
        assertEquals(mappedStat.getCapacity().getTotal(), test.getCapacity().getTotal(), 0);

        // Check the relationship type type.
        if (relationshipType.equals("bought") || relationshipType.equals("sold")) {
            assertNotNull(mappedStat.getFilters());
            assertEquals(1, mappedStat.getFilters().size());
            assertEquals(StatsMapper.RELATION_FILTER_TYPE, mappedStat.getFilters().get(0).getType());
            assertEquals(relationshipType, mappedStat.getFilters().get(0).getValue());
        } else {
            assertTrue(mappedStat.getFilters() == null || mappedStat.getFilters().isEmpty());
        }

        assertThat(mappedStat.getRelatedEntity().getDisplayName(), is(test.getProviderDisplayName()));
        assertThat(mappedStat.getRelatedEntity().getUuid(), is(test.getProviderUuid()));
        assertThat(mappedStat.getUnits(), is(test.getUnits()));
        assertThat(mappedStat.getValue(), is(test.getUsed().getAvg()));
        validateStatValue(mappedStat.getValues(), test.getUsed());
    }

    /**
     * Build a list of StatRecord objects initialized based on the given postfixes.
     *
     * String fields are initialized with the field name plus "-" plus the postfix.
     * Numeric fields are initialized with the
     *
     * @param postfixes an array of strings to use as postfixes for new instances of StatRecord
     * @param relations an array of strings (of the same size as postfixes) to use as relations
     *        for new instances of StatRecord
     * @return a list of new StatRecord objects with fields initialized based on the given postfix
     */
    private List<StatRecord> buildStatRecords(String[] postfixes, String[] relations) {
        List<StatRecord> records = new ArrayList<>();
        for (int i = 0; i < postfixes.length; i++) {
            records.add(buildStatRecord(i, postfixes[i], relations[i]));
        }
        return records;
    }

    /**
     * Validate the fields of a {@link StatValueApiDTO} mapped from the original {@link StatValue}.
     *
     * @param mapped the {@link StatValueApiDTO} output of the mapping process
     * @param original the {@link StatValue} being mapped from
     */
    private void validateStatValue(StatValueApiDTO mapped, StatValue original) {
        assertThat(mapped.getMin(), is(original.getMin()));
        assertThat(mapped.getMax(), is(original.getMax()));
        assertThat(mapped.getAvg(), is(original.getAvg()));
        assertThat(mapped.getTotal(), is(original.getTotal()));
    }

    /**
     * Create a test StatRecord populated with values based on the postfix for string fields and the index for
     * numeric fields.
     *
     * @param index an ascending index to be used to salt numeric fields.
     * @param postfix a string postfix to be added to string-based fields.
     * @return a newly initialized StatRecord initialized based on the input index and postfix.
     */
    private StatRecord buildStatRecord(int index, String postfix, String relation) {
        return StatRecord.newBuilder()
            .setName("name-" + postfix)
            .setProviderUuid(PUID + postfix)
            .setProviderDisplayName("provider-" + postfix)
            .setCapacity(buildStatValue(1000 + index))
            .setReserved(2000 + index)
            .setCurrentValue(3000 + index)
            .setPeak(buildStatValue(index))
            .setUsed(buildStatValue(index + 100))
            .setValues(buildStatValue(index + 200))
            .setRelation(relation)
            .build();
    }

    private static StatValue buildStatValue(int seed) {
        return StatValue.newBuilder()
                .setTotal(4000+seed)
                .setMax(5000+seed)
                .setMin(6000+seed)
                .setAvg(7000+seed)
                .build();
    }
}
