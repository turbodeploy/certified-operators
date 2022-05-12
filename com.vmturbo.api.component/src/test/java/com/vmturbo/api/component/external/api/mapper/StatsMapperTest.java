package com.vmturbo.api.component.external.api.mapper;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.apache.commons.collections4.CollectionUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.context.annotation.Bean;

import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.component.external.api.service.StatsService;
import com.vmturbo.api.component.external.api.service.TargetsService;
import com.vmturbo.api.component.external.api.util.stats.StatsTestUtil;
import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.statistic.EntityStatsApiDTO;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.statistic.StatApiInputDTO;
import com.vmturbo.api.dto.statistic.StatFilterApiDTO;
import com.vmturbo.api.dto.statistic.StatHistUtilizationApiDTO;
import com.vmturbo.api.dto.statistic.StatPeriodApiInputDTO;
import com.vmturbo.api.dto.statistic.StatScopesApiInputDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.dto.statistic.StatValueApiDTO;
import com.vmturbo.api.dto.target.TargetApiDTO;
import com.vmturbo.api.dto.target.TargetDetailLevel;
import com.vmturbo.api.enums.EnvironmentType;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.api.pagination.EntityStatsPaginationRequest;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.auth.api.authorization.jwt.JwtClientInterceptor;
import com.vmturbo.common.api.mappers.EnvironmentTypeMapper;
import com.vmturbo.common.protobuf.common.Pagination.PaginationParameters;
import com.vmturbo.common.protobuf.cost.Cost;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatRecord;
import com.vmturbo.common.protobuf.cost.Cost.CostCategory;
import com.vmturbo.common.protobuf.cost.Cost.CostSource;
import com.vmturbo.common.protobuf.cost.Cost.CostStatsSnapshot;
import com.vmturbo.common.protobuf.cost.Cost.CostStatsSnapshot.StatRecord.TagKeyValuePair;
import com.vmturbo.common.protobuf.cost.CostMoles;
import com.vmturbo.common.protobuf.cost.CostServiceGrpc;
import com.vmturbo.common.protobuf.group.PolicyDTOMoles.PolicyServiceMole;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.PlanTopologyStatsRequest;
import com.vmturbo.common.protobuf.repository.SupplyChainProtoMoles.SupplyChainServiceMole;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc;
import com.vmturbo.common.protobuf.stats.Stats;
import com.vmturbo.common.protobuf.stats.Stats.ClusterStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats.EntityStats;
import com.vmturbo.common.protobuf.stats.Stats.EntityStatsScope;
import com.vmturbo.common.protobuf.stats.Stats.EntityStatsScope.EntityList;
import com.vmturbo.common.protobuf.stats.Stats.GetEntityStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats.ProjectedEntityStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord.HistUtilizationValue;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord.StatValue;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter.CommodityRequest;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Origin;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.PlanScenarioOrigin;
import com.vmturbo.common.protobuf.topology.UICommodityType;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.commons.TimeFrame;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.common.featureflags.FeatureFlags;
import com.vmturbo.history.schema.RelationType;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.test.utils.FeatureFlagTestRule;
import com.vmturbo.topology.processor.api.util.ThinTargetCache;

/**
 * Unit tests for the static Mapper utility functions for the {@link StatsService}.
 */
public class StatsMapperTest {

    private static final double DELTA = 0.0001;
    private static final long START_DATE = 1234L;
    private static final String START_DATE_STR = DateTimeUtil.toString(1234L);
    private static final long END_DATE = 5678L;
    private static final String PUID = "puid-";
    private static final String CSP = "CSP";
    private static final String AWS = "AWS";
    private static final String COST_COMPONENT = "costComponent";
    private static final String COST_SOURCE = "costSource";
    private static final String PERCENTILE = "percentile";
    private static final String BYTE_PER_SEC = "Byte/sec";
    private final PaginationMapper paginationMapper = mock(PaginationMapper.class);
    private final TargetsService targetsService = mock(TargetsService.class);
    /**
     * Rule to manage feature flag enablement to make sure FeatureFlagManager store is set up.
     */
    @Rule
    public FeatureFlagTestRule featureFlagTestRule = new FeatureFlagTestRule(
            FeatureFlags.DELAYED_DATA_HANDLING);
    /**
     * Rule to provide GRPC server and channels for GRPC services for test purposes.
     */
    @Rule
    public GrpcTestServer grpcServer =
            GrpcTestServer.newServer(Mockito.spy(new PolicyServiceMole()),
                                     Mockito.spy(new CostMoles.CostServiceMole()),
                                     Mockito.spy(new CostMoles.ReservedInstanceBoughtServiceMole()),
                                     Mockito.spy(new SupplyChainServiceMole()));
    @Bean
    public JwtClientInterceptor jwtClientInterceptor() {
        return new JwtClientInterceptor();
    }
    private ServiceEntityMapper serviceEntityMapper;
    private StatsMapper statsMapper;
    private static final long PLAN_ID = 7L;
    private static final long projectedTopologyId = 77L;
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
        when(targetsService.getTarget(anyString(), eq(TargetDetailLevel.BASIC)))
                .thenReturn(targetApiDTO);
        serviceEntityMapper =
                new ServiceEntityMapper(Mockito.mock(ThinTargetCache.class),
                                        CostServiceGrpc.newBlockingStub(grpcServer.getChannel()),
                                        SupplyChainServiceGrpc.newBlockingStub(grpcServer.getChannel())
                                                .withInterceptors(jwtClientInterceptor()),
                                        Mockito.mock(ConnectedEntityMapper.class));
        statsMapper = spy(new StatsMapper(paginationMapper, serviceEntityMapper));
    }

    /**
     * Test Conversion of gRPC stats call result to the ApiDTO to return for the REST API caller.
     */
    @Test
    public void toStatSnapshotApiDTOTest() {
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
    public void toStatSnapshotApiDTOWrongStatRelationTest() {
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
    public void toStatApiDTOStatKeyFilter() {
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

    /**
     * When a {@link StatRecord} is given in data transfer units that are multiples of
     * Byte/sec (i.e., KByte/sec, MByte/sec, GByte/sec), the method
     * {@link StatsMapper#toStatSnapshotApiDTO} should perform a correct conversion
     * of all the values to multiples of bit/sec.
     */
    @Test
    public void testDataTransferConversionUnit() {
        final String originalUnits = "KByte/sec";
        final String expectedUnits = "Kbit/sec";
        final float epsilon = 0.01f;
        final StatRecord statRecord = makeStatRecordBuilder(0, "", RelationType.COMMODITIES.getLiteral())
                                          .setUnits(originalUnits)
                                          .build();
        final StatSnapshot snapshot = StatSnapshot.newBuilder().addStatRecords(statRecord).build();

        final StatApiDTO dto = statsMapper.toStatSnapshotApiDTO(snapshot).getStatistics().get(0);

        Assert.assertEquals(expectedUnits, dto.getUnits());
        Assert.assertEquals(dto.getValues().getAvg(), dto.getValue(), epsilon);
        Assert.assertEquals(statRecord.getCapacity().getMax() * 8, dto.getCapacity().getMax(), epsilon);
        Assert.assertEquals(statRecord.getCapacity().getAvg() * 8, dto.getCapacity().getAvg(), epsilon);
        Assert.assertEquals(statRecord.getCapacity().getMin() * 8, dto.getCapacity().getMin(), epsilon);
        Assert.assertEquals(statRecord.getCapacity().getTotal() * 8, dto.getCapacity().getTotal(), epsilon);
        Assert.assertEquals(statRecord.getUsed().getMax() * 8, dto.getValues().getMax(), epsilon);
        Assert.assertEquals(statRecord.getUsed().getAvg() * 8, dto.getValues().getAvg(), epsilon);
        Assert.assertEquals(statRecord.getUsed().getMin() * 8, dto.getValues().getMin(), epsilon);
        Assert.assertEquals(statRecord.getUsed().getTotal() * 8, dto.getValues().getTotal(), epsilon);
    }

    /**
     * Test Conversion of gRPC cost stats call result to the ApiDTO to return for the REST API caller.
     */
    @Test
    public void testToCostStatSnapshotApiDTO() {
        // ARRANGE
        final String tagKey1 = "tagKey1";
        final String tagValue1 = "tagValue1";
        final String tagKey2 = "tagKey2";
        final String tagValue2 = "tagValue2";
        final CostStatsSnapshot snapshot = CostStatsSnapshot.newBuilder()
                .setSnapshotDate(START_DATE)
                .addStatRecords(CostStatsSnapshot.StatRecord.newBuilder()
                        .addTag(TagKeyValuePair.newBuilder()
                                .setKey(tagKey1)
                                .setValue(tagValue1)
                                .build())
                        .setValue(Cost.StatValue.newBuilder()
                                .setTotal(1)
                                .setAvg(2)
                                .setMin(3)
                                .setMax(4)
                                .build())
                        .setUnits(TimeFrame.DAY.getUnits())
                        .build())
                .addStatRecords(CostStatsSnapshot.StatRecord.newBuilder()
                        .addTag(TagKeyValuePair.newBuilder()
                                .setKey(tagKey2)
                                .setValue(tagValue2)
                                .build())
                        .setValue(Cost.StatValue.newBuilder()
                                .setTotal(5)
                                .setAvg(6)
                                .setMin(7)
                                .setMax(8)
                                .build())
                        .setUnits(TimeFrame.DAY.getUnits())
                        .build())
                .build();

        // ACT
        final StatSnapshotApiDTO result = statsMapper.toCostStatSnapshotApiDTO(snapshot);

        // ASSERT
        assertEquals(DateTimeUtil.toString(snapshot.getSnapshotDate()), result.getDate());
        assertEquals(2, snapshot.getStatRecordsCount());
        final StatApiDTO stat1 = result.getStatistics().get(0);
        assertEquals(2, stat1.getFilters().size());
        final StatFilterApiDTO filter11 = stat1.getFilters().get(0);
        assertEquals(StatsMapper.TAG_KEY, filter11.getType());
        assertEquals(tagKey1, filter11.getValue());
        final StatFilterApiDTO filter12 = stat1.getFilters().get(1);
        assertEquals(StatsMapper.TAG_VALUE, filter12.getType());
        assertEquals(tagValue1, filter12.getValue());
        assertEquals(1, stat1.getValues().getTotal(), DELTA);
        assertEquals(2, stat1.getValues().getAvg(), DELTA);
        assertEquals(3, stat1.getValues().getMin(), DELTA);
        assertEquals(4, stat1.getValues().getMax(), DELTA);
        assertEquals(TimeFrame.DAY.getUnits(), stat1.getUnits());
        final StatApiDTO stat2 = result.getStatistics().get(1);
        assertEquals(2, stat2.getFilters().size());
        final StatFilterApiDTO filter21 = stat2.getFilters().get(0);
        assertEquals(StatsMapper.TAG_KEY, filter21.getType());
        assertEquals(tagKey2, filter21.getValue());
        final StatFilterApiDTO filter22 = stat2.getFilters().get(1);
        assertEquals(StatsMapper.TAG_VALUE, filter22.getType());
        assertEquals(tagValue2, filter22.getValue());
        assertEquals(5, stat2.getValues().getTotal(), DELTA);
        assertEquals(6, stat2.getValues().getAvg(), DELTA);
        assertEquals(7, stat2.getValues().getMin(), DELTA);
        assertEquals(8, stat2.getValues().getMax(), DELTA);
        assertEquals(TimeFrame.DAY.getUnits(), stat2.getUnits());
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
        final String startRelativeTime = "-5d";
        final String endRelativeTime = "+2h";

        final long expectedStart = DateTimeUtil.parseTime(startRelativeTime);
        final long expectedEnd = DateTimeUtil.parseTime(endRelativeTime);

        apiRequestInput.setStartDate(startRelativeTime);
        apiRequestInput.setEndDate(endRelativeTime);
        final EntityStatsPaginationRequest paginationRequest = mock(EntityStatsPaginationRequest.class);
        when(paginationMapper.toProtoParams(paginationRequest))
            .thenReturn(PaginationParameters.getDefaultInstance());
        final GetEntityStatsRequest requestProtobuf = statsMapper.toEntityStatsRequest(scope,
            apiRequestInput, paginationRequest);

        // calculate time ranges to test, with some slop to account for clock advance since mapping
        final long slop_ms = 2_000;
        assertThat(Math.abs(requestProtobuf.getFilter().getStartDate() - expectedStart), lessThan(slop_ms));
        assertThat(Math.abs(requestProtobuf.getFilter().getEndDate() - expectedEnd), lessThan(slop_ms));
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

    @Test
    public void testToPlanTopologyStatsRequestTopologyIdSet() {
        final StatScopesApiInputDTO inputDto = new StatScopesApiInputDTO();

        final PlanTopologyStatsRequest request = statsMapper
            .toPlanTopologyStatsRequest(projectedTopologyId, inputDto, ENTITY_PAGINATION_REQUEST);
        assertThat(request.getTopologyId(), is(projectedTopologyId));
    }

    @Test
    public void testToPlanTopologyStatsRequestPeriodStartDate() {
        final StatScopesApiInputDTO inputDto = new StatScopesApiInputDTO();
        final StatPeriodApiInputDTO period = new StatPeriodApiInputDTO();
        period.setStartDate("117");
        inputDto.setPeriod(period);

        final PlanTopologyStatsRequest request = statsMapper
            .toPlanTopologyStatsRequest(projectedTopologyId, inputDto, ENTITY_PAGINATION_REQUEST);
        assertThat(request.getRequestDetails().getFilter().getStartDate(), is(117L));
    }

    @Test
    public void testToPlanTopologyStatsRequestPeriodEndDate() {
        final StatScopesApiInputDTO inputDto = new StatScopesApiInputDTO();
        final StatPeriodApiInputDTO period = new StatPeriodApiInputDTO();
        period.setEndDate("118");
        inputDto.setPeriod(period);

        final PlanTopologyStatsRequest request = statsMapper
            .toPlanTopologyStatsRequest(projectedTopologyId, inputDto, ENTITY_PAGINATION_REQUEST);
        assertThat(request.getRequestDetails().getFilter().getEndDate(), is(118L));
    }

    @Test
    public void testToPlanTopologyStatsRequestPeriodStats() {
        final StatScopesApiInputDTO inputDto = new StatScopesApiInputDTO();
        final StatPeriodApiInputDTO period = new StatPeriodApiInputDTO();
        final StatApiInputDTO statDto = new StatApiInputDTO();
        statDto.setName("Wolf");
        period.setStatistics(Collections.singletonList(statDto));
        inputDto.setPeriod(period);

        final PlanTopologyStatsRequest request = statsMapper
            .toPlanTopologyStatsRequest(projectedTopologyId, inputDto, ENTITY_PAGINATION_REQUEST);
        assertThat(request.getRequestDetails().getFilter().getCommodityRequestsList(), containsInAnyOrder(
            CommodityRequest.newBuilder()
                .setCommodityName(statDto.getName())
                .build()));
    }

    @Test
    public void testToPlanTopologyStatsRequestRelatedType() {
        final StatScopesApiInputDTO inputDto = new StatScopesApiInputDTO();
        inputDto.setRelatedType("Cousin");

        final PlanTopologyStatsRequest request = statsMapper
            .toPlanTopologyStatsRequest(projectedTopologyId, inputDto, ENTITY_PAGINATION_REQUEST);
        assertThat(request.getRequestDetails().getRelatedEntityType(), is(inputDto.getRelatedType()));
    }

    @Test
    public void testToPlanTopologyStatsRequestPaginationParams() {
        final StatScopesApiInputDTO inputDto = new StatScopesApiInputDTO();

        final PlanTopologyStatsRequest request = statsMapper
            .toPlanTopologyStatsRequest(projectedTopologyId, inputDto, ENTITY_PAGINATION_REQUEST);
        assertThat(request.getRequestDetails().getPaginationParams(), is(MAPPED_PAGINATION_PARAMS));
    }

    @Test
    public void testToPlanTopologyStatsRequestScopes() {
        final StatScopesApiInputDTO inputDto = new StatScopesApiInputDTO();
        inputDto.setScopes(Lists.newArrayList("1", "2"));

        final PlanTopologyStatsRequest request = statsMapper
            .toPlanTopologyStatsRequest(projectedTopologyId, inputDto, ENTITY_PAGINATION_REQUEST);
        assertThat(request.getRequestDetails().getEntityFilter().getEntityIdsList(), containsInAnyOrder(1L, 2L));
    }

    @Test
    public void testToPlanTopologyStatsRequestEmptyScopes() {
        final StatScopesApiInputDTO inputDto = new StatScopesApiInputDTO();
        inputDto.setScopes(Collections.emptyList());

        final PlanTopologyStatsRequest request = statsMapper
            .toPlanTopologyStatsRequest(projectedTopologyId, inputDto, ENTITY_PAGINATION_REQUEST);
        // Request shouldn't have an entity filter at all, instead of having an entity filter
        // with no entity IDS.
        assertFalse(request.getRequestDetails().hasEntityFilter());
    }

    private static final StatsFilter STATS_FILTER = StatsFilter.newBuilder()
            .setStartDate(7777)
            .setRequestProjectedHeadroom(true)
            .build();

    @Test
    public void testClusterStatsRequest() {
        final StatPeriodApiInputDTO period = new StatPeriodApiInputDTO();
        when(statsMapper.newPeriodStatsFilter(period, true)).thenReturn(STATS_FILTER);
        final ClusterStatsRequest clusterStatsRequest =
                statsMapper.toClusterStatsRequest(Long.toString(PLAN_ID), period);
        assertThat(clusterStatsRequest.getClusterIds(0), is(PLAN_ID));
        assertThat(clusterStatsRequest.getStats(), is(STATS_FILTER));
    }

    @Test
    public void testClusterStatsRequestWithNullPeriod() {
        final StatPeriodApiInputDTO period = null;
        when(statsMapper.newPeriodStatsFilter(period, true)).thenReturn(STATS_FILTER);
        final ClusterStatsRequest clusterStatsRequest =
                statsMapper.toClusterStatsRequest(Long.toString(PLAN_ID), period);
        assertThat(clusterStatsRequest.getClusterIds(0), is(PLAN_ID));
        assertThat(clusterStatsRequest.getStats(), is(STATS_FILTER));
    }

    /**
     * Tests {@link StatsMapper#toClusterStatsRequest(String, StatPeriodApiInputDTO)}
     * with a market scope.
     */
    @Test
    public void testClusterStatsRequestWithMarketScope() {
        when(statsMapper.newPeriodStatsFilter(null, true)).thenReturn(STATS_FILTER);
        assertEquals(Collections.emptyList(),
                     statsMapper.toClusterStatsRequest("Market", null).getClusterIdsList());
    }

    /**
     * "1M" is one month as defined in {@link DateTimeUtil}.
     * The unit tests for {@link DateTimeUtil} is covered in OpsMgr, here we just verify it doesn't
     * throw exception.
     */
    @Test
    public void testNewPeriodStatsFilterWith1MendDate() {
        StatsMapper localStatsMapper = new StatsMapper(new PaginationMapper(), serviceEntityMapper);
        StatPeriodApiInputDTO statPeriodApiInputDTO = new StatPeriodApiInputDTO();
        statPeriodApiInputDTO.setEndDate("1M");
        StatsFilter filter = localStatsMapper.newPeriodStatsFilter(statPeriodApiInputDTO);
        assertTrue(filter.hasEndDate());
    }

    /**
     * Test that data center stats can be retrieved successfully, even though the search is really
     * done on stats of a group of physical machines.
     */
    @Test
    public void testNewPeriodStatsFilterWithDataCenters() {
        StatsMapper localStatsMapper = new StatsMapper(new PaginationMapper(), serviceEntityMapper);
        StatPeriodApiInputDTO statPeriodApiInputDTO = new StatPeriodApiInputDTO();
        List<StatApiInputDTO> statistics = Lists.newArrayListWithCapacity(1);
        // The API caller is requesting stats for a DATACENTER
        statistics.add(new StatApiInputDTO(CommodityType.CPU_ALLOCATION.name(),
            ApiEntityType.DATACENTER.apiStr(), null, null, null));
        statPeriodApiInputDTO.setStatistics(statistics);
        // Stats for a data center will be collected with a group entity type of PHYSICAL_MACHINE
        StatsFilter filter =
            localStatsMapper.newPeriodStatsFilter(statPeriodApiInputDTO);
        // All resulting commodity requests should have the related entity type of PHYSICAL_MACHINE,
        // expressed in the API format
        filter.getCommodityRequestsList().stream()
            .filter(CommodityRequest::hasRelatedEntityType)
            .map(CommodityRequest::getRelatedEntityType)
            .forEach(relatedEntityType -> assertEquals(ApiEntityType.PHYSICAL_MACHINE.apiStr(), relatedEntityType));
    }

    @Test
    public void testProjectedEntityStatsRequestIdsAndPaginationParams() {
        final StatPeriodApiInputDTO period = new StatPeriodApiInputDTO();
        final Set<Long> uuids = Sets.newHashSet(1L, 2L);

        final ProjectedEntityStatsRequest request = statsMapper.toProjectedEntityStatsRequest(
            StatsTestUtil.createEntityStatsScope(uuids), period, ENTITY_PAGINATION_REQUEST);
        assertThat(request.getScope().getEntityList().getEntitiesList(),
            containsInAnyOrder(uuids.toArray()));
        assertThat(request.getPaginationParams(), is(MAPPED_PAGINATION_PARAMS));
    }

    @Test
    public void testProjectedEntityStatsRequestWithNullPeriod() {
        final Set<Long> uuids = Sets.newHashSet(1L, 2L);

        final ProjectedEntityStatsRequest request = statsMapper.toProjectedEntityStatsRequest(
            StatsTestUtil.createEntityStatsScope(uuids), null, ENTITY_PAGINATION_REQUEST);
        assertThat(request.getScope().getEntityList().getEntitiesList(),
            containsInAnyOrder(uuids.toArray()));
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

        final ProjectedEntityStatsRequest request = statsMapper.toProjectedEntityStatsRequest(
            StatsTestUtil.createEntityStatsScope(uuids), period, ENTITY_PAGINATION_REQUEST);

        assertThat(request.getFilterList().stream().map(CommodityRequest::getCommodityName).collect(Collectors.toList()), containsInAnyOrder(stats.toArray()));
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
                .setSnapshotDate(1_000_000)
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
        assertThat(mapped.getDate(), is(DateTimeUtil.toString(cloudStatRecord.getSnapshotDate())));
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
                .setSnapshotDate(START_DATE)
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
        assertThat(mapped.getDate(), is(START_DATE_STR));
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
        TargetApiDTO targetApiDTO1 = new TargetApiDTO();
        targetApiDTO1.setType("AWS");
        targetApiDTO1.setUuid("4");
        TargetApiDTO targetApiDTO3 = new TargetApiDTO();
        targetApiDTO3.setUuid("22222");
        targetApiDTO3.setType("AWS");

        final CloudCostStatRecord cloudStatRecord = CloudCostStatRecord.newBuilder()
                .setSnapshotDate(START_DATE)
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
        assertThat(mapped.getDate(), is(START_DATE_STR));
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
        Assert.assertNull(result.getStatistics().get(0).getDisplayName());
        Assert.assertEquals("FROM: " + providerName + " ", result.getStatistics().get(1).getDisplayName());
        Assert.assertEquals("FROM: " + providerName + " KEY: " + key,
                            result.getStatistics().get(2).getDisplayName());
    }

    @Test
    public void testToCloudStatSnapshotApiDTO() throws Exception{
        final CloudCostStatRecord cloudStatRecord = CloudCostStatRecord.newBuilder()
                .setSnapshotDate(START_DATE)
                .addStatRecords(getStatRecordBuilder(CostCategory.ON_DEMAND_COMPUTE, 1l, Optional.empty()).setCostSource(CostSource.ENTITY_UPTIME_DISCOUNT))
                .addStatRecords(getStatRecordBuilder(CostCategory.IP, 2l, Optional.empty()))
                .addStatRecords(getStatRecordBuilder(CostCategory.ON_DEMAND_LICENSE, 3l, Optional.empty()))
                .addStatRecords(getStatRecordBuilder(CostCategory.STORAGE, 4l, Optional.empty()))
                .addStatRecords(getStatRecordBuilder(CostCategory.RI_COMPUTE, 5l, Optional.empty()))
                .build();
        final StatSnapshotApiDTO mapped = statsMapper.toCloudStatSnapshotApiDTO(cloudStatRecord);
        assertThat(mapped.getDate(), is(START_DATE_STR));
        assertThat(cloudStatRecord.getStatRecordsCount(), is(mapped.getStatistics().size()));
        assertEquals(5, cloudStatRecord.getStatRecordsCount());
        assertTrue(mapped.getStatistics().stream().allMatch(statApiDTO -> statApiDTO.getFilters() != null ));
        assertTrue(mapped.getStatistics().stream().anyMatch(statApiDTO -> statApiDTO.getFilters().get(0).getValue().equals(CostCategory.ON_DEMAND_COMPUTE.name())
                && statApiDTO.getFilters().get(0).getType().equals(COST_COMPONENT) && statApiDTO.getFilters().get(1).getType().equals(COST_SOURCE)));
        assertTrue(mapped.getStatistics().stream().anyMatch(statApiDTO -> statApiDTO.getFilters().get(0).getValue().equals(CostCategory.IP.name())
                && statApiDTO.getFilters().get(0).getType().equals(COST_COMPONENT) ));
        assertTrue(mapped.getStatistics().stream().anyMatch(statApiDTO -> statApiDTO.getFilters().get(0).getValue().equals(CostCategory.ON_DEMAND_LICENSE.name())
                && statApiDTO.getFilters().get(0).getType().equals(COST_COMPONENT) ));
        assertTrue(mapped.getStatistics().stream().anyMatch(statApiDTO -> statApiDTO.getFilters().get(0).getValue().equals(CostCategory.STORAGE.name())
                && statApiDTO.getFilters().get(0).getType().equals(COST_COMPONENT) ));
        assertTrue(mapped.getStatistics().stream().anyMatch(statApiDTO -> statApiDTO.getFilters().get(0).getValue().equals(CostCategory.RI_COMPUTE.name())
                && statApiDTO.getFilters().get(0).getType().equals(COST_COMPONENT) ));
    }

    @Test
    public void testToCloudStatSnapshotApiDTOWithEmptyCostCategory() throws Exception{
        final CloudCostStatRecord cloudStatRecord = CloudCostStatRecord.newBuilder()
                .setSnapshotDate(START_DATE)
                .addStatRecords(getStatRecordBuilder(null, 1l, Optional.empty()))
                .build();
        final StatSnapshotApiDTO mapped = statsMapper.toCloudStatSnapshotApiDTO(cloudStatRecord);
        assertThat(mapped.getDate(), is(START_DATE_STR));
        assertThat(cloudStatRecord.getStatRecordsCount(), is(mapped.getStatistics().size()));
        assertEquals(1, cloudStatRecord.getStatRecordsCount());
        assertTrue(mapped.getStatistics().stream().allMatch(statApiDTO -> CollectionUtils.isEmpty(statApiDTO.getFilters())));
    }

    /**
     * Test the population of an entity stats api dto from a minimal entity dto.
     */
    @Test
    public void testPopulateEntityDataEntityStatApiDTOFromMinimalEntity() {
        MinimalEntity vm = MinimalEntity.newBuilder()
                .setOid(123L)
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .setDisplayName("Test VM 1")
                .setEnvironmentType(Objects.requireNonNull(
                        EnvironmentTypeMapper.fromApiToXL(EnvironmentType.ONPREM)))
                .build();
        EntityStatsApiDTO outputDTO = StatsMapper.toEntityStatsApiDTO(vm);
        // Verify the data in the output
        assertEquals("123", outputDTO.getUuid());
        assertEquals("VirtualMachine", outputDTO.getClassName());
        assertEquals("Test VM 1", outputDTO.getDisplayName());
        assertEquals(com.vmturbo.api.enums.EnvironmentType.ONPREM, outputDTO.getEnvironmentType());

        // Also test an entity without an env type
        MinimalEntity pm = MinimalEntity.newBuilder()
                .setOid(123L)
                .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
                .setDisplayName("Test PM 1")
                .build();
        EntityStatsApiDTO outputDTO2 = StatsMapper.toEntityStatsApiDTO(pm);
        // Verify the data in the output
        assertEquals("123", outputDTO2.getUuid());
        assertEquals("PhysicalMachine", outputDTO2.getClassName());
        assertEquals("Test PM 1", outputDTO2.getDisplayName());
        //  no env type was specified.  So, this will be null
        assertNull(outputDTO2.getEnvironmentType());

    }


    /**
     * Test the population of an entity stats api dto from an api id.
     */
    @Test
    public void testPopulateEntityDataEntityStatApiDTOFromApiId() {

        ApiId apiId = mock(ApiId.class);
        when(apiId.oid()).thenReturn(123L);
        when(apiId.uuid()).thenReturn("123");
        when(apiId.getClassName()).thenReturn("VirtualMachine");
        when(apiId.getDisplayName()).thenReturn("Test VM 1");
        when(apiId.getEnvironmentType())
                .thenReturn(
                        EnvironmentTypeMapper.fromApiToXL(
                                com.vmturbo.api.enums.EnvironmentType.ONPREM));

        EntityStatsApiDTO outputDTO = StatsMapper.toEntityStatsApiDTO(apiId);
        // Verify the data in the output
        assertEquals("123", outputDTO.getUuid());
        assertEquals("VirtualMachine", outputDTO.getClassName());
        assertEquals("Test VM 1", outputDTO.getDisplayName());
        assertEquals(com.vmturbo.api.enums.EnvironmentType.ONPREM, outputDTO.getEnvironmentType());
    }

    /**
     * Test the population of an entity stats api dto from an api partial entity dto.
     */
    @Test
    public void testPopulateEntityDataEntityStatApiDTOFromApiPartialEntity() {

        ApiPartialEntity apiPartialEntity = ApiPartialEntity.newBuilder()
                .setOid(123L)
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .setDisplayName("Test VM 1")
                .setEnvironmentType(Objects.requireNonNull(
                        EnvironmentTypeMapper.fromApiToXL(EnvironmentType.ONPREM)))
                .build();
        EntityStatsApiDTO outputDTO = statsMapper.toEntityStatsApiDTO(
                apiPartialEntity, Collections.emptyList());
        // Verify the data in the output
        assertEquals("123", outputDTO.getUuid());
        assertEquals("VirtualMachine", outputDTO.getClassName());
        assertEquals("Test VM 1", outputDTO.getDisplayName());
        assertEquals(com.vmturbo.api.enums.EnvironmentType.ONPREM, outputDTO.getEnvironmentType());

        // Test without env type
        ApiPartialEntity apiPartialEntityNoEnv = ApiPartialEntity.newBuilder()
                .setOid(456L)
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .setDisplayName("Test VM 2")
                .build();
        EntityStatsApiDTO outputDTONoEnv = statsMapper.toEntityStatsApiDTO(
                apiPartialEntityNoEnv, Collections.emptyList());
        // Verify the data in the output
        assertEquals("456", outputDTONoEnv.getUuid());
        assertEquals("VirtualMachine", outputDTONoEnv.getClassName());
        assertEquals("Test VM 2", outputDTONoEnv.getDisplayName());
        assertNull(outputDTONoEnv.getEnvironmentType());

        // Test entity with plan origin
        ApiPartialEntity apiPartialEntityWithPlanOrigin = ApiPartialEntity.newBuilder()
                .setOid(789L)
                .setEntityType(EntityType.CONTAINER_SPEC_VALUE)
                .setDisplayName("Test ContainerSpec")
                .setOrigin(Origin.newBuilder()
                        .setPlanScenarioOrigin(PlanScenarioOrigin.newBuilder()
                                .setPlanId(1000L)
                                .setOriginalEntityId(135L)))
                .build();
        EntityStatsApiDTO outputDTOWithPlanOrigin = statsMapper.toEntityStatsApiDTO(
                apiPartialEntityWithPlanOrigin, Collections.emptyList());
        // Verify the data in the output
        assertEquals("789", outputDTOWithPlanOrigin.getUuid());
        assertNotNull(outputDTOWithPlanOrigin.getRealtimeMarketReference());
        assertEquals("135", outputDTOWithPlanOrigin.getRealtimeMarketReference().getUuid());
    }

    /**
     * Tests that when converting from {@link StatRecord} to {@link StatApiDTO} with commodities
     * that need conversion from bytes to bits, totalMax & totalMin in capacity & values fields are
     * being populated correctly. Also verifies that {@link StatApiDTO##histUtilizations} is properly converted.
     */
    @Test
    public void testToStatApiDtoTotalMaxMinWithByteToBitConversion() {
        // GIVEN
        float totalMax = 123.0f;
        float totalMin = 9.0f;
        float average = 100.0f;
        StatRecord record = StatRecord.newBuilder()
                .setName(StringConstants.IO_THROUGHPUT)
                .setUnits(BYTE_PER_SEC)
                .setCapacity(buildStatValueWithCustomValues(totalMax, totalMin, average, 110.0f,
                        totalMax, totalMin))
                .setUsed(buildStatValueWithCustomValues(totalMax, totalMin, average, 110.0f,
                        totalMax, totalMin))
                .setValues(buildStatValueWithCustomValues(totalMax, totalMin, average, 110.0f,
                        totalMax, totalMin))
                .addHistUtilizationValue(HistUtilizationValue.newBuilder()
                        .setType("percentile")
                        .setCapacity(buildStatValueWithCustomValues(totalMax, totalMax, average, 110.0f,
                                totalMax, totalMax))
                        .setUsage(buildStatValueWithCustomValues(totalMax, totalMax, average, 110.0f,
                                totalMax, totalMax)).build())
                .build();

        // WHEN
        StatApiDTO dto = this.statsMapper.toStatApiDto(record);

        // THEN
        assertEquals(totalMax * 8, dto.getCapacity().getTotalMax().doubleValue(), 0.001);
        assertEquals(totalMin * 8, dto.getCapacity().getTotalMin().doubleValue(), 0.001);
        assertEquals(totalMax * 8, dto.getValues().getTotalMax().doubleValue(), 0.001);
        assertEquals(totalMin * 8, dto.getValues().getTotalMin().doubleValue(), 0.001);
        final StatHistUtilizationApiDTO histUtilizationValue = dto.getHistUtilizations().get(0);
        assertEquals(average * 8, histUtilizationValue.getCapacity().doubleValue(), 0.001);
        assertEquals(average * 8, histUtilizationValue.getUsage().doubleValue(), 0.001);
    }

    /**
     * Tests {@link StatRecord} name matched to UICommodityType.
     *
     * <p>Name has STAT_PREFIX_CURRENT in the front, these are plan_source aggregated stats</p>
     */
    @Test
    public void testToStatApiDtoMappingStatsWithCurrentPrefixToCorrectUICommodityType() {
        //GIVEN
        StatRecord record = makeStatRecordBuilder(0, "", RelationType.COMMODITIES.getLiteral())
                .setName(StringConstants.STAT_PREFIX_CURRENT.concat(UICommodityType.STORAGE_AMOUNT.apiStr()))
                .build();

        //WHEN
        StatApiDTO dto = this.statsMapper.toStatApiDto(record);

        //THEN
        assertEquals(dto.getName(), UICommodityType.STORAGE_AMOUNT.apiStr());
    }

    /**
     * Tests {@link StatRecord} name not matched to CommodityType.
     *
     * <p>Name has STAT_PREFIX_CURRENT in the front, these are plan_source aggregated stats</p>
     */
    @Test
    public void testToStatApiDtoMappingStatsWithCurrentPrefixButNotUICommodityType() {
        //GIVEN
        StatRecord record = makeStatRecordBuilder(0, "", RelationType.COMMODITIES.getLiteral())
                .setName(StringConstants.STAT_PREFIX_CURRENT.concat(StringConstants.NUM_CPUS))
                .build();

        //WHEN
        StatApiDTO dto = this.statsMapper.toStatApiDto(record);

        //THEN
        assertEquals(dto.getName(), StringConstants.NUM_CPUS);
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
        Cost.StatValue.Builder statValueBuilder = Cost.StatValue.newBuilder();

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
        if (test.hasUnits()) {
            assertThat(mappedStat.getUnits(), is(test.getUnits()));
        } else {
            assertThat(mappedStat.getUnits(), is(nullValue()));
        }
        assertThat(mappedStat.getValue(), is(test.getUsed().getAvg()));
        final StatHistUtilizationApiDTO percentile = mappedStat.getHistUtilizations().get(0);
        final HistUtilizationValue expected = test.getHistUtilizationValueList().stream()
                        .filter(value -> PERCENTILE.equals(value.getType())).findAny().get();
        assertThat(percentile.getUsage(),
                        is(expected.getUsage().getAvg()));
        assertThat(percentile.getCapacity(),
                        is(expected.getCapacity().getAvg()));
        assertThat(percentile.getType(),
                        is(expected.getType()));
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
            records.add(makeStatRecordBuilder(i, postfixes[i], relations[i]).build());
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
     * Create a test {@link StatRecord.Builder} populated with values based
     * on the postfix for string fields and the index for numeric fields.
     *
     * @param index an ascending index to be used to salt numeric fields.
     * @param postfix a string postfix to be added to string-based fields.
     * @param relation a string to be used as the {@code relation} field of the new record.
     * @return a newly initialized {@link StatRecord.Builder} initialized based on the input index and postfix.
     */
    private StatRecord.Builder makeStatRecordBuilder(int index, String postfix, String relation) {
        final StatValue capacity = buildStatValue(1000 + index);
        return StatRecord.newBuilder()
            .setName("name-" + postfix)
            .setProviderUuid(PUID + postfix)
            .setProviderDisplayName("provider-" + postfix)
            .setCapacity(capacity)
            .setReserved(2000 + index)
            .setCurrentValue(3000 + index)
            .setPeak(buildStatValue(index))
            .setUsed(buildStatValue(index + 100))
            .setValues(buildStatValue(index + 200)).addHistUtilizationValue(
                                        HistUtilizationValue.newBuilder()
                                                        .setType(PERCENTILE)
                                                        .setUsage(buildStatValue(index + 300))
                                                        .setCapacity(capacity)
                                                        .build())
            .setRelation(relation);
    }

    private static StatValue buildStatValue(int seed) {
        return StatValue.newBuilder()
                .setTotal(4000+seed)
                .setMax(5000+seed)
                .setMin(6000+seed)
                .setAvg(7000+seed)
                .build();
    }

    /**
     * Creates a StatValue object with custom values.
     * @param max max value
     * @param min min value
     * @param avg avg value
     * @param total total value
     * @param totalMax totalMax value
     * @param totalMin totalMin value
     * @return the created object
     */
    private static StatValue buildStatValueWithCustomValues(float max, float min, float avg,
            float total, float totalMax, float totalMin) {
        return StatValue.newBuilder()
                .setMax(max)
                .setMin(min)
                .setAvg(avg)
                .setTotal(total)
                .setTotalMax(totalMax)
                .setTotalMin(totalMin)
                .build();
    }
}
