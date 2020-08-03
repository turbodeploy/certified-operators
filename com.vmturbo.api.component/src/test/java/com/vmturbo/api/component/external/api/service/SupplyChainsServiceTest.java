package com.vmturbo.api.component.external.api.service;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import com.google.common.collect.ImmutableList;

import org.apache.commons.collections4.CollectionUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;

import com.vmturbo.api.component.external.api.mapper.ActionSpecMapper;
import com.vmturbo.api.component.external.api.mapper.aspect.EntityAspectMapper;
import com.vmturbo.api.component.external.api.service.SupplyChainsService.SupplyChainStatMapper;
import com.vmturbo.api.component.external.api.util.GroupExpander;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory.SupplychainApiDTOFetcherBuilder;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.dto.supplychain.SupplyChainStatsApiInputDTO;
import com.vmturbo.api.enums.EntitiesCountCriteria;
import com.vmturbo.api.enums.EnvironmentType;
import com.vmturbo.api.exceptions.InvalidOperationException;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.auth.api.authorization.AuthorizationException.UserAccessException;
import com.vmturbo.auth.api.licensing.LicenseCheckClient;
import com.vmturbo.auth.api.licensing.LicenseFeaturesRequiredException;
import com.vmturbo.auth.api.usermgmt.AuthUserDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionCategory;
import com.vmturbo.common.protobuf.action.ActionDTO.Severity;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.plan.PlanDTO.OptionalPlanInstance;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanId;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance.PlanStatus;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.PlanScope;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.PlanScopeEntry;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.Scenario;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioInfo;
import com.vmturbo.common.protobuf.plan.PlanDTOMoles.PlanServiceMole;
import com.vmturbo.common.protobuf.plan.PlanServiceGrpc;
import com.vmturbo.common.protobuf.plan.PlanServiceGrpc.PlanServiceBlockingStub;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainGroupBy;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainStat;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainStat.StatGroup;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.UIEntityState;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.api.test.MutableFixedClock;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.platform.sdk.common.util.ProbeLicense;

/**
 * Unit tests for {@link SupplyChainsService}.
 */
public class SupplyChainsServiceTest {
    private static final long LIVE_TOPOLOGY_CONTEXT_ID = 7777777L;

    private SupplyChainFetcherFactory supplyChainsFetcherMock = mock(SupplyChainFetcherFactory.class);

    private GroupExpander groupExpanderMock = mock(GroupExpander.class);

    private PlanServiceMole planServiceMole = Mockito.spy(new PlanServiceMole());

    /**
     * Test server for gRPC dependencies of {@link SupplyChainsService}.
     */
    @Rule
    public GrpcTestServer grpcTestServer = GrpcTestServer.newServer(planServiceMole);

    private final SupplyChainTestUtils supplyChainTestUtils = new SupplyChainTestUtils();

    private final SupplyChainStatMapper mockStatMapper = mock(SupplyChainStatMapper.class);

    private final LicenseCheckClient licenseCheckClient = mock(LicenseCheckClient.class);

    private final MutableFixedClock clock = new MutableFixedClock(1_000_000);

    private SupplyChainsService service;

    /**
     * Common setup code before every test method.
     */
    @Before
    public void setup() {
        final PlanServiceBlockingStub planServiceMock =
                PlanServiceGrpc.newBlockingStub(grpcTestServer.getChannel());

        service = new SupplyChainsService(supplyChainsFetcherMock, planServiceMock,
                LIVE_TOPOLOGY_CONTEXT_ID, groupExpanderMock, mock(EntityAspectMapper.class),
            clock, mockStatMapper, licenseCheckClient);
    }

    @Test
    public void testGetSupplyChainByUuidsOneAlphanumeric() throws Exception {
        final SupplychainApiDTOFetcherBuilder supplyChainMock = supplyChainTestUtils.mockApiDtoBuilder(supplyChainsFetcherMock);

        final List<String> uuids = Collections.singletonList("Abcdef123");

        service.getSupplyChainByUuids(uuids, null, null, null, null, null, false);

        verify(supplyChainMock).topologyContextId(LIVE_TOPOLOGY_CONTEXT_ID);
        verify(supplyChainMock).addSeedUuids(uuids);
    }

    @Test
    public void testGetSupplyChainByUuidsMultiple() throws Exception {
        final SupplychainApiDTOFetcherBuilder supplyChainMock = supplyChainTestUtils.mockApiDtoBuilder(supplyChainsFetcherMock);

        final List<String> uuids = Arrays.asList("Abcdef123", "123456789");

        service.getSupplyChainByUuids(uuids, null, null, null, null, null, false);

        verify(supplyChainMock).topologyContextId(LIVE_TOPOLOGY_CONTEXT_ID);
        verify(supplyChainMock).addSeedUuids(uuids);
    }

    @Test
    public void testGetSupplyChainByUuidsPlanExists() throws Exception {
        final SupplychainApiDTOFetcherBuilder supplyChainMock = supplyChainTestUtils.mockApiDtoBuilder(supplyChainsFetcherMock);

        final long planId = 123456789;
        final PlanId planIdObj = PlanId.newBuilder().setPlanId(planId).build();
        final OptionalPlanInstance planInstance = OptionalPlanInstance.newBuilder()
            .setPlanInstance(PlanInstance.newBuilder()
                .setPlanId(planId).setStatus(PlanStatus.READY)).build();

        when(planServiceMole.getPlan(planIdObj)).thenReturn(planInstance);

        service.getSupplyChainByUuids(Collections.singletonList(Long.toString(planId)),
            null, null, null, null, null, false);

        verify(supplyChainMock).topologyContextId(planId);
    }

    @Test
    public void testGetSupplyChainByUuidsPlanDoesntExist() throws Exception {
        final SupplychainApiDTOFetcherBuilder supplyChainMock = supplyChainTestUtils.mockApiDtoBuilder(supplyChainsFetcherMock);

        final long planId = 123456789;
        final List<String> uuids = Collections.singletonList(Long.toString(planId));

        final PlanId planIdObj = PlanId.newBuilder().setPlanId(planId).build();
        final OptionalPlanInstance planInstance = OptionalPlanInstance.getDefaultInstance();

        when(planServiceMole.getPlan(planIdObj)).thenReturn(planInstance);

        service.getSupplyChainByUuids(uuids, null, null, null, null, null, false);

        verify(supplyChainMock).topologyContextId(LIVE_TOPOLOGY_CONTEXT_ID);
        verify(supplyChainMock).addSeedUuids(uuids);
    }

    /**
     * Test that a request for a plan supply chain fails if the planner feature is unavailable
     * @throws Exception
     */
    @Test(expected = LicenseFeaturesRequiredException.class)
    public void testGetSupplyChainByUuidsPlanUnlicensed() throws Exception {
        final SupplychainApiDTOFetcherBuilder supplyChainMock = supplyChainTestUtils.mockApiDtoBuilder(supplyChainsFetcherMock);

        final long planId = 123456789;
        final PlanId planIdObj = PlanId.newBuilder().setPlanId(planId).build();
        final OptionalPlanInstance planInstance = OptionalPlanInstance.newBuilder()
                .setPlanInstance(PlanInstance.newBuilder()
                        .setPlanId(planId).setStatus(PlanStatus.READY)).build();

        when(planServiceMole.getPlan(planIdObj)).thenReturn(planInstance);

        // plot twist -- the planner feature is unavailable
        doThrow(new LicenseFeaturesRequiredException(Collections.singleton(ProbeLicense.PLANNER)))
                .when(licenseCheckClient).checkFeatureAvailable(ProbeLicense.PLANNER);
        // this should trigger a LicenseFeqturesRequiredException
        service.getSupplyChainByUuids(Collections.singletonList(Long.toString(planId)), null, null, null, null, null, false);
    }

    @Test
    public void testGetSupplyChainByUuidsCreatorMatch() throws Exception {
        final SupplychainApiDTOFetcherBuilder supplyChainMock = supplyChainTestUtils.mockApiDtoBuilder(supplyChainsFetcherMock);

        final long planId = 123456789;
        final PlanId planIdObj = PlanId.newBuilder().setPlanId(planId).build();
        final OptionalPlanInstance planInstance = OptionalPlanInstance.newBuilder()
                .setPlanInstance(PlanInstance.newBuilder()
                        .setPlanId(planId)
                        .setCreatedByUser("1")
                        .setStatus(PlanStatus.READY)).build();

        when(planServiceMole.getPlan(planIdObj)).thenReturn(planInstance);

        // put a non-admin user into the security context
        Authentication auth = new UsernamePasswordAuthenticationToken(
                new AuthUserDTO(null, "admin", "pass", "10.10.10.10",
                        "1", "token", ImmutableList.of("NOT_ADMIN"), null),
                "admin000",
                CollectionUtils.emptyCollection());
        SecurityContextHolder.getContext().setAuthentication(auth);
        // we should get access to the plan supply chain since we are the creator id
        service.getSupplyChainByUuids(Collections.singletonList(Long.toString(planId)),
            null, null, null, null, null, false);

        verify(supplyChainMock).topologyContextId(planId);
        SecurityContextHolder.getContext().setAuthentication(null);
    }

    @Test(expected = UserAccessException.class)
    public void testGetSupplyChainByUuidsCreatorNotMatch() throws Exception {
        final SupplychainApiDTOFetcherBuilder supplyChainMock = supplyChainTestUtils.mockApiDtoBuilder(supplyChainsFetcherMock);

        final long planId = 123456789;
        final PlanId planIdObj = PlanId.newBuilder().setPlanId(planId).build();
        final OptionalPlanInstance planInstance = OptionalPlanInstance.newBuilder()
                .setPlanInstance(PlanInstance.newBuilder()
                        .setPlanId(planId)
                        .setCreatedByUser("1")
                        .setStatus(PlanStatus.READY)).build();

        when(planServiceMole.getPlan(planIdObj)).thenReturn(planInstance);

        // put a non-admin user into the security context
        Authentication auth = new UsernamePasswordAuthenticationToken(
                new AuthUserDTO(null, "admin", "pass", "10.10.10.10",
                        "11111", "token", ImmutableList.of("NOT_ADMIN"), null),
                "admin000",
                CollectionUtils.emptyCollection());
        SecurityContextHolder.getContext().setAuthentication(auth);
        // we should get denied access to the plan supply chain
        service.getSupplyChainByUuids(Collections.singletonList(Long.toString(planId)),
            null, null, null, null, null, false);
        SecurityContextHolder.getContext().setAuthentication(null);
    }

    @Test
    public void testGetSupplyChainByUuidsAdmin() throws Exception {
        final SupplychainApiDTOFetcherBuilder supplyChainMock = supplyChainTestUtils.mockApiDtoBuilder(supplyChainsFetcherMock);

        // verify that an admin can access a plan that someone else created
        final long planId = 123456789;
        final PlanId planIdObj = PlanId.newBuilder().setPlanId(planId).build();
        final OptionalPlanInstance planInstance = OptionalPlanInstance.newBuilder()
                .setPlanInstance(PlanInstance.newBuilder()
                        .setPlanId(planId)
                        .setCreatedByUser("2")
                        .setStatus(PlanStatus.READY)).build();

        when(planServiceMole.getPlan(planIdObj)).thenReturn(planInstance);

        Authentication auth = new UsernamePasswordAuthenticationToken(
                new AuthUserDTO(null, "admin", "pass", "10.10.10.10",
                        "1", "token", ImmutableList.of("Administrator"), null),
                "admin000",
                CollectionUtils.emptyCollection());
        SecurityContextHolder.getContext().setAuthentication(auth);
        // we should get access to the plan supply chain since we are an admin
        service.getSupplyChainByUuids(Collections.singletonList(Long.toString(planId)),
                null, null, null, null, null, false);

        verify(supplyChainMock).topologyContextId(planId);
        SecurityContextHolder.getContext().setAuthentication(null);
    }

    @Test
    public void testGetSupplyChainByUuidsScopedPlan() throws Exception {
        final SupplychainApiDTOFetcherBuilder supplyChainMock = supplyChainTestUtils.mockApiDtoBuilder(supplyChainsFetcherMock);

        final long planId = 123456789;
        final List<String> uuids = Collections.singletonList(Long.toString(planId));

        final PlanId planIdObj = PlanId.newBuilder().setPlanId(planId).build();
        final OptionalPlanInstance planInstance = OptionalPlanInstance.newBuilder()
                .setPlanInstance(PlanInstance.newBuilder()
                    .setPlanId(planId)
                    .setScenario(Scenario.newBuilder()
                        .setScenarioInfo(ScenarioInfo.newBuilder()
                            .setScope(PlanScope.newBuilder()
                                .addScopeEntries(PlanScopeEntry.newBuilder()
                                    .setScopeObjectOid(1)))))
                    .setStatus(PlanStatus.READY))
                .build();

        when(planServiceMole.getPlan(planIdObj)).thenReturn(planInstance);

        service.getSupplyChainByUuids(uuids, null, null, null, null, null, false);

        verify(supplyChainMock).topologyContextId(123456789);
        verify(supplyChainMock).addSeedUuid("1");
    }

    /**
     * Test getting supply chain stats from the underlying gRPC service.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testSupplyChainStats() throws Exception {
        // ARRANGE
        final SupplychainApiDTOFetcherBuilder supplyChainMock = supplyChainTestUtils.mockApiDtoBuilder(supplyChainsFetcherMock);

        final long uuid = 1;
        final SupplyChainStatsApiInputDTO inputDTO = new SupplyChainStatsApiInputDTO();
        inputDTO.setEnvironmentType(EnvironmentType.CLOUD);
        inputDTO.setTypes(Collections.singletonList(ApiEntityType.VIRTUAL_MACHINE.apiStr()));
        inputDTO.setGroupBy(Arrays.asList(EntitiesCountCriteria.businessUnit,
                EntitiesCountCriteria.resourceGroup));
        inputDTO.setStates(Collections.singletonList(com.vmturbo.api.enums.EntityState.ACTIVE));
        inputDTO.setUuids(Collections.singletonList(Long.toString(uuid)));

        final SupplyChainStat stat = SupplyChainStat.newBuilder()
            .setNumEntities(100)
            .build();

        when(mockStatMapper.countCriteriaToGroupBy(EntitiesCountCriteria.businessUnit)).thenReturn(Optional.of(SupplyChainGroupBy.BUSINESS_ACCOUNT_ID));
        when(mockStatMapper.countCriteriaToGroupBy(EntitiesCountCriteria.resourceGroup)).thenReturn(Optional.of(SupplyChainGroupBy.RESOURCE_GROUP));
        final StatApiDTO retDto = mock(StatApiDTO.class);
        when(mockStatMapper.supplyChainStatToApi(any())).thenReturn(retDto);
        when(supplyChainMock.fetchStats(any())).thenReturn(Collections.singletonList(stat));

        // ACT
        final List<StatSnapshotApiDTO> retSnapshot = service.getSupplyChainStats(inputDTO);

        // ASSERT
        verify(supplyChainMock).addSeedUuids(inputDTO.getUuids());
        verify(supplyChainMock).entityTypes(inputDTO.getTypes());
        verify(supplyChainMock).entityStates(inputDTO.getStates());
        verify(supplyChainMock).apiEnvironmentType(inputDTO.getEnvironmentType());
        verify(mockStatMapper).countCriteriaToGroupBy(EntitiesCountCriteria.businessUnit);
        verify(mockStatMapper).countCriteriaToGroupBy(EntitiesCountCriteria.resourceGroup);
        verify(supplyChainMock).fetchStats(Arrays.asList(SupplyChainGroupBy.BUSINESS_ACCOUNT_ID,
                SupplyChainGroupBy.RESOURCE_GROUP));
        verify(mockStatMapper).supplyChainStatToApi(stat);

        assertThat(retSnapshot.size(), is(1));
        StatSnapshotApiDTO snapshot = retSnapshot.get(0);
        assertThat(snapshot.getDate(), is(DateTimeUtil.toString(clock.millis())));
        assertThat(snapshot.getStatistics(), containsInAnyOrder(retDto));
    }

    /**
     * Test proper mapping of supported {@link EntitiesCountCriteria}.
     *
     * @throws InvalidOperationException To satisfy compiler.
     */
    @Test
    public void testSupplyChainStatMapperCriteria() throws InvalidOperationException {
        final SupplyChainStatMapper mapper = new SupplyChainStatMapper();
        assertThat(mapper.countCriteriaToGroupBy(EntitiesCountCriteria.businessUnit),
            is(Optional.of(SupplyChainGroupBy.BUSINESS_ACCOUNT_ID)));
        assertThat(mapper.countCriteriaToGroupBy(EntitiesCountCriteria.entityType), is(Optional.of(SupplyChainGroupBy.ENTITY_TYPE)));
        assertThat(mapper.countCriteriaToGroupBy(EntitiesCountCriteria.riskSubCategory), is(Optional.of(SupplyChainGroupBy.ACTION_CATEGORY)));
        assertThat(mapper.countCriteriaToGroupBy(EntitiesCountCriteria.severity), is(Optional.of(SupplyChainGroupBy.SEVERITY)));
        assertThat(mapper.countCriteriaToGroupBy(EntitiesCountCriteria.state), is(Optional.of(SupplyChainGroupBy.ENTITY_STATE)));
        assertThat(mapper.countCriteriaToGroupBy(EntitiesCountCriteria.target), is(Optional.of(SupplyChainGroupBy.TARGET)));
        assertThat(mapper.countCriteriaToGroupBy(EntitiesCountCriteria.template), is(Optional.of(SupplyChainGroupBy.TEMPLATE)));
        assertThat(mapper.countCriteriaToGroupBy(EntitiesCountCriteria.resourceGroup), is(Optional.of(SupplyChainGroupBy.RESOURCE_GROUP)));
    }


    /**
     * Test mapping {@link SupplyChainStat} to {@link StatApiDTO} with the action category filter.
     */
    @Test
    public void testSupplyChainStatMapperStatToApiRiskSubCategory() {
        final SupplyChainStatMapper mapper = new SupplyChainStatMapper();

        SupplyChainStat supplyChainStat = SupplyChainStat.newBuilder()
            .setNumEntities(100)
            .setStatGroup(StatGroup.newBuilder()
                .setActionCategory(ActionCategory.PERFORMANCE_ASSURANCE))
            .build();
        StatApiDTO statApiDTO = mapper.supplyChainStatToApi(supplyChainStat);
        assertThat(statApiDTO.getValue(), is(100.0f));
        assertThat(statApiDTO.getName(), is(StringConstants.ENTITIES));
        assertThat(statApiDTO.getFilters().size(), is(1));
        assertThat(statApiDTO.getFilters().get(0).getType(), is(EntitiesCountCriteria.riskSubCategory.name()));
        assertThat(statApiDTO.getFilters().get(0).getValue(),
            is(ActionSpecMapper.mapXlActionCategoryToApi(ActionCategory.PERFORMANCE_ASSURANCE)));
    }

    /**
     * Test mapping {@link SupplyChainStat} to {@link StatApiDTO} with the severity filter.
     */
    @Test
    public void testSupplyChainStatMapperStatToApiSeverity() {
        final SupplyChainStatMapper mapper = new SupplyChainStatMapper();

        SupplyChainStat supplyChainStat = SupplyChainStat.newBuilder()
            .setNumEntities(100)
            .setStatGroup(StatGroup.newBuilder()
                .setSeverity(Severity.CRITICAL))
            .build();
        StatApiDTO statApiDTO = mapper.supplyChainStatToApi(supplyChainStat);
        assertThat(statApiDTO.getValue(), is(100.0f));
        assertThat(statApiDTO.getName(), is(StringConstants.ENTITIES));
        assertThat(statApiDTO.getFilters().size(), is(1));
        assertThat(statApiDTO.getFilters().get(0).getType(), is(EntitiesCountCriteria.severity.name()));
        assertThat(statApiDTO.getFilters().get(0).getValue(), is(ActionDTOUtil.getSeverityName(Severity.CRITICAL)));
    }

    /**
     * Test mapping {@link SupplyChainStat} to {@link StatApiDTO} with the entity type filter.
     */
    @Test
    public void testSupplyChainStatMapperStatToApiEntityType() {
        final SupplyChainStatMapper mapper = new SupplyChainStatMapper();

        SupplyChainStat supplyChainStat = SupplyChainStat.newBuilder()
            .setNumEntities(100)
            .setStatGroup(StatGroup.newBuilder()
                .setEntityType(ApiEntityType.VIRTUAL_MACHINE.typeNumber()))
            .build();
        StatApiDTO statApiDTO = mapper.supplyChainStatToApi(supplyChainStat);
        assertThat(statApiDTO.getValue(), is(100.0f));
        assertThat(statApiDTO.getName(), is(StringConstants.ENTITIES));
        assertThat(statApiDTO.getFilters().size(), is(1));
        assertThat(statApiDTO.getFilters().get(0).getType(), is(EntitiesCountCriteria.entityType.name()));
        assertThat(statApiDTO.getFilters().get(0).getValue(), is(ApiEntityType.VIRTUAL_MACHINE.apiStr()));
    }

    /**
     * Test mapping {@link SupplyChainStat} to {@link StatApiDTO} with the resource group filter.
     */
    @Test
    public void testSupplyChainStatMapperStatToApiResourceGroup() {
        long associatedResourceGroupId = 1L;
        final SupplyChainStatMapper mapper = new SupplyChainStatMapper();

        SupplyChainStat supplyChainStat = SupplyChainStat.newBuilder()
                .setNumEntities(100)
                .setStatGroup(StatGroup.newBuilder().setResourceGroupId(associatedResourceGroupId))
                .build();
        StatApiDTO statApiDTO = mapper.supplyChainStatToApi(supplyChainStat);
        assertThat(statApiDTO.getValue(), is(100.0f));
        assertThat(statApiDTO.getFilters().size(), is(1));
        assertThat(statApiDTO.getFilters().get(0).getType(), is(EntitiesCountCriteria.resourceGroup.name()));
        Assert.assertEquals(statApiDTO.getFilters().get(0).getValue(), String.valueOf(associatedResourceGroupId));
    }

    /**
     * Test mapping {@link SupplyChainStat} to {@link StatApiDTO} with the target id filter.
     */
    @Test
    public void testSupplyChainStatMapperStatToApiTargetId() {
        final SupplyChainStatMapper mapper = new SupplyChainStatMapper();

        final long targetId = 777;
        SupplyChainStat supplyChainStat = SupplyChainStat.newBuilder()
            .setNumEntities(100)
            .setStatGroup(StatGroup.newBuilder()
                .setTargetId(targetId))
            .build();
        StatApiDTO statApiDTO = mapper.supplyChainStatToApi(supplyChainStat);
        assertThat(statApiDTO.getValue(), is(100.0f));
        assertThat(statApiDTO.getName(), is(StringConstants.ENTITIES));
        assertThat(statApiDTO.getFilters().size(), is(1));
        assertThat(statApiDTO.getFilters().get(0).getType(), is(EntitiesCountCriteria.target.name()));
        assertThat(statApiDTO.getFilters().get(0).getValue(), is(Long.toString(targetId)));
    }

    /**
     * Test mapping {@link SupplyChainStat} to {@link StatApiDTO} with the business unit id filter.
     */
    @Test
    public void testSupplyChainStatMapperStatToApiRiskBusinessUnit() {
        final SupplyChainStatMapper mapper = new SupplyChainStatMapper();

        final long accountId = 888;
        SupplyChainStat supplyChainStat = SupplyChainStat.newBuilder()
            .setNumEntities(100)
            .setStatGroup(StatGroup.newBuilder()
                .setAccountId(accountId))
            .build();
        StatApiDTO statApiDTO = mapper.supplyChainStatToApi(supplyChainStat);
        assertThat(statApiDTO.getValue(), is(100.0f));
        assertThat(statApiDTO.getName(), is(StringConstants.ENTITIES));
        assertThat(statApiDTO.getFilters().size(), is(1));
        assertThat(statApiDTO.getFilters().get(0).getType(), is(EntitiesCountCriteria.businessUnit.name()));
        assertThat(statApiDTO.getFilters().get(0).getValue(), is(Long.toString(accountId)));
    }

    /**
     * Test mapping {@link SupplyChainStat} to {@link StatApiDTO} with the entity state filter.
     */
    @Test
    public void testSupplyChainStatMapperStatToApiState() {
        final SupplyChainStatMapper mapper = new SupplyChainStatMapper();

        SupplyChainStat supplyChainStat = SupplyChainStat.newBuilder()
            .setNumEntities(100)
            .setStatGroup(StatGroup.newBuilder()
                .setEntityState(EntityState.POWERED_OFF))
            .build();
        StatApiDTO statApiDTO = mapper.supplyChainStatToApi(supplyChainStat);
        assertThat(statApiDTO.getValue(), is(100.0f));
        assertThat(statApiDTO.getName(), is(StringConstants.ENTITIES));
        assertThat(statApiDTO.getFilters().size(), is(1));
        assertThat(statApiDTO.getFilters().get(0).getType(), is(EntitiesCountCriteria.state.name()));
        assertThat(statApiDTO.getFilters().get(0).getValue(),
            is(UIEntityState.fromEntityState(EntityState.POWERED_OFF).apiStr()));
    }

    /**
     * Test mapping {@link SupplyChainStat} to {@link StatApiDTO} with no filters.
     */
    @Test
    public void testSupplyChainStatMapperStatToApiNoFilters() {
        final SupplyChainStatMapper mapper = new SupplyChainStatMapper();

        SupplyChainStat supplyChainStat = SupplyChainStat.newBuilder()
            .setNumEntities(100)
            .build();
        StatApiDTO statApiDTO = mapper.supplyChainStatToApi(supplyChainStat);
        assertThat(statApiDTO.getValue(), is(100.0f));
        assertThat(statApiDTO.getName(), is(StringConstants.ENTITIES));
        assertThat(statApiDTO.getFilters().size(), is(0));
    }
}
