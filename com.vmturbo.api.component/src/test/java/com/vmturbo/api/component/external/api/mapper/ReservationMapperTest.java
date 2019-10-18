package com.vmturbo.api.component.external.api.mapper;

import static com.vmturbo.api.component.external.api.mapper.ReservationMapper.ONE_UNTRACKED_PLACEMENT_FAILED;
import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.api.component.ApiTestUtils;
import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.communication.RepositoryApi.MultiEntityRequest;
import com.vmturbo.api.component.communication.RepositoryApi.SingleEntityRequest;
import com.vmturbo.api.component.external.api.mapper.ReservationMapper.PlacementInfo;
import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.reservation.DemandEntityInfoDTO;
import com.vmturbo.api.dto.reservation.DemandReservationApiDTO;
import com.vmturbo.api.dto.reservation.DemandReservationApiInputDTO;
import com.vmturbo.api.dto.reservation.DemandReservationParametersDTO;
import com.vmturbo.api.dto.reservation.DeploymentParametersDTO;
import com.vmturbo.api.dto.reservation.PlacementInfoDTO;
import com.vmturbo.api.dto.reservation.PlacementParametersDTO;
import com.vmturbo.api.dto.template.ResourceApiDTO;
import com.vmturbo.api.enums.ReservationAction;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupID;
import com.vmturbo.common.protobuf.group.GroupDTOMoles.GroupServiceMole;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.group.PolicyDTO.Policy;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo.BindToGroupPolicy;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyRequest;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyResponse;
import com.vmturbo.common.protobuf.group.PolicyDTOMoles.PolicyServiceMole;
import com.vmturbo.common.protobuf.group.PolicyServiceGrpc;
import com.vmturbo.common.protobuf.plan.DeploymentProfileDTO.DeploymentProfile;
import com.vmturbo.common.protobuf.plan.DeploymentProfileDTO.DeploymentProfileInfo;
import com.vmturbo.common.protobuf.plan.DeploymentProfileDTO.DeploymentProfileInfo.Scope;
import com.vmturbo.common.protobuf.plan.DeploymentProfileDTO.DeploymentProfileInfo.ScopeAccessType;
import com.vmturbo.common.protobuf.plan.DeploymentProfileDTO.GetDeploymentProfileRequest;
import com.vmturbo.common.protobuf.plan.DeploymentProfileDTOMoles.DeploymentProfileServiceMole;
import com.vmturbo.common.protobuf.plan.DeploymentProfileServiceGrpc;
import com.vmturbo.common.protobuf.plan.PlanDTO.ReservationConstraintInfo;
import com.vmturbo.common.protobuf.plan.PlanDTO.ReservationConstraintInfo.Type;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.TopologyAddition;
import com.vmturbo.common.protobuf.plan.ReservationDTO.Reservation;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationStatus;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection.ReservationTemplate;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection.ReservationTemplate.ReservationInstance;
import com.vmturbo.common.protobuf.plan.TemplateDTO.GetTemplateRequest;
import com.vmturbo.common.protobuf.plan.TemplateDTO.SingleTemplateResponse;
import com.vmturbo.common.protobuf.plan.TemplateDTO.Template;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateInfo;
import com.vmturbo.common.protobuf.plan.TemplateDTOMoles.TemplateServiceMole;
import com.vmturbo.common.protobuf.plan.TemplateServiceGrpc;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Test cases for {@link ReservationMapper}.
 */
public class ReservationMapperTest {

    private TemplateServiceMole templateServiceMole = Mockito.spy(new TemplateServiceMole());

    private GroupServiceMole groupServiceMole = Mockito.spy(new GroupServiceMole());

    private PolicyServiceMole policyServiceMole = Mockito.spy(new PolicyServiceMole());

    private DeploymentProfileServiceMole deploymentProfileServiceMole =
        Mockito.spy(DeploymentProfileServiceMole.class);

    private RepositoryApi repositoryApi;

    private ReservationMapper reservationMapper;

    private static final long TEMPLATE_ID = 123L;

    private ServiceEntityApiDTO vmServiceEntity = new ServiceEntityApiDTO();

    private  ServiceEntityApiDTO pmServiceEntity = new ServiceEntityApiDTO();

    private ServiceEntityApiDTO stServiceEntity = new ServiceEntityApiDTO();

    private static final String PLACEMENT_SUCCEEDED = "PLACEMENT_SUCCEEDED";

    private static final String PLACEMENT_FAILED = "PLACEMENT_FAILED";

    private static final Template TEMPLATE = Template.newBuilder()
            .setId(TEMPLATE_ID)
            .setTemplateInfo(TemplateInfo.newBuilder()
                    .setName("VM-template")
                    .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE))
            .build();

    /**
     * Utility gRPC server to mock out gRPC service dependencies.
     */
    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(templateServiceMole, groupServiceMole,
            policyServiceMole, deploymentProfileServiceMole);

    /**
     * Common setup code to run before each test.
     */
    @Before
    public void setup() {
        repositoryApi = Mockito.mock(RepositoryApi.class);

        reservationMapper = new ReservationMapper(repositoryApi,
            TemplateServiceGrpc.newBlockingStub(grpcServer.getChannel()),
            GroupServiceGrpc.newBlockingStub(grpcServer.getChannel()),
            PolicyServiceGrpc.newBlockingStub(grpcServer.getChannel()),
            DeploymentProfileServiceGrpc.newBlockingStub(grpcServer.getChannel()));

        vmServiceEntity.setClassName("VirtualMachine");
        vmServiceEntity.setDisplayName("VM-template Clone #1");
        vmServiceEntity.setUuid("1");
        pmServiceEntity.setClassName("PhysicalMachine");
        pmServiceEntity.setDisplayName("PM #1");
        pmServiceEntity.setUuid("2");
        stServiceEntity.setClassName("Storage");
        stServiceEntity.setDisplayName("ST #1");
        stServiceEntity.setUuid("3");

    }

    /**
     * Test converting a {@link DemandReservationParametersDTO} to a list of scenario changes
     * protobuf objects.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testConvertToScenarioChange() throws Exception {
        final DemandReservationParametersDTO demandParameters = new DemandReservationParametersDTO();
        final PlacementParametersDTO placementParameter = new PlacementParametersDTO();
        placementParameter.setCount(2);
        placementParameter.setTemplateID(String.valueOf(TEMPLATE_ID));
        demandParameters.setPlacementParameters(placementParameter);
        final List<ScenarioChange> scenarioChangeList =
                reservationMapper.placementToScenarioChange(Lists.newArrayList(demandParameters));
        assertEquals(1, scenarioChangeList.size());
        final ScenarioChange scenarioChange = scenarioChangeList.get(0);
        Assert.assertTrue(scenarioChange.hasTopologyAddition());
        assertEquals(2, scenarioChange.getTopologyAddition().getAdditionCount());
        assertEquals(123L, scenarioChange.getTopologyAddition().getTemplateId());
    }

    /**
     * Test converting a {@link DemandReservationParametersDTO} to a list of scenario changes
     * protobuf objects when there is an associated deployment profile ID specified in the input.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testConvertToScenarioChangeWithDeploymentProfile() throws Exception {
        final long dcId = 321123;
        final DeploymentProfile deploymentProfile = DeploymentProfile.newBuilder()
            .setId(7777)
            .setDeployInfo(DeploymentProfileInfo.newBuilder()
                .setName("foo")
                .addScopes(Scope.newBuilder()
                    .setScopeAccessType(ScopeAccessType.And)
                    .addIds(dcId)))
            .build();
        doReturn(deploymentProfile).when(deploymentProfileServiceMole)
            .getDeploymentProfile(GetDeploymentProfileRequest.newBuilder()
                .setDeploymentProfileId(deploymentProfile.getId())
                .build());
        final DemandReservationParametersDTO demandParameters = new DemandReservationParametersDTO();
        final PlacementParametersDTO placementParameter = new PlacementParametersDTO();
        placementParameter.setCount(2);
        placementParameter.setTemplateID(String.valueOf(TEMPLATE_ID));
        final DeploymentParametersDTO deploymentParameter = new DeploymentParametersDTO();
        deploymentParameter.setDeploymentProfileID(Long.toString(deploymentProfile.getId()));
        demandParameters.setPlacementParameters(placementParameter);
        demandParameters.setDeploymentParameters(deploymentParameter);

        final SingleEntityRequest mockReq = ApiTestUtils.mockSingleEntityRequest(MinimalEntity.newBuilder()
            .setOid(dcId)
            .setEntityType(UIEntityType.DATACENTER.typeNumber())
            .build());
        when(repositoryApi.entityRequest(dcId)).thenReturn(mockReq);

        final List<ScenarioChange> scenarioChangeList =
            reservationMapper.placementToScenarioChange(Lists.newArrayList(demandParameters));

        final ScenarioChange planChange = scenarioChangeList.stream()
            .filter(ScenarioChange::hasPlanChanges)
            .findFirst()
            .get();
        final List<ReservationConstraintInfo> constraints = planChange.getPlanChanges()
            .getInitialPlacementConstraintsList();
        assertThat(constraints, containsInAnyOrder(ReservationConstraintInfo.newBuilder()
            .setType(Type.DATA_CENTER)
            .setConstraintId(dcId)
            .build()));
    }

    /**
     * Test converting a {@link DemandReservationParametersDTO} to a list of scenario changes
     * protobuf objects when there is a placement constraint in the input.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testConvertToScenarioChangeWithConstraint() throws Exception {
        final DemandReservationParametersDTO demandParameters = new DemandReservationParametersDTO();
        final PlacementParametersDTO placementParameter = new PlacementParametersDTO();
        placementParameter.setCount(2);
        placementParameter.setTemplateID(String.valueOf(TEMPLATE_ID));
        placementParameter.setConstraintIDs(Sets.newHashSet("123", "456"));
        demandParameters.setPlacementParameters(placementParameter);
        when(groupServiceMole.getGroup(GroupID.newBuilder()
                .setId(123L)
                .build()))
                .thenReturn(GetGroupResponse.newBuilder()
                        .setGroup(Group.newBuilder()
                                .setType(Group.Type.CLUSTER))
                        .build());
        when(policyServiceMole.getPolicy(PolicyRequest.newBuilder()
                .setPolicyId(456L).build()))
                .thenReturn(PolicyResponse.newBuilder()
                        .setPolicy(Policy.newBuilder()
                                .setId(456)
                                .setPolicyInfo(PolicyInfo.newBuilder()
                                    .setBindToGroup(BindToGroupPolicy.newBuilder()
                                            .setConsumerGroupId(8)
                                            .setProviderGroupId(9))))
                        .build());
        final List<ScenarioChange> scenarioChangeList =
                reservationMapper.placementToScenarioChange(Lists.newArrayList(demandParameters));
        assertEquals(2, scenarioChangeList.size());
        Assert.assertTrue(scenarioChangeList.stream()
                .anyMatch(ScenarioChange::hasPlanChanges));
        final ScenarioChange planChange = scenarioChangeList.stream()
                .filter(ScenarioChange::hasPlanChanges)
                .findFirst()
                .get();
        final List<ReservationConstraintInfo> constraints = planChange.getPlanChanges()
                .getInitialPlacementConstraintsList();
        assertEquals(2L, constraints.size());
        assertTrue(constraints.stream().anyMatch(constraint -> constraint.getConstraintId() == 123L));
        assertTrue(constraints.stream().anyMatch(constraint -> constraint.getConstraintId() == 456L));
        assertTrue(constraints.stream()
                .filter(constraint -> constraint.getConstraintId() == 123L)
                .anyMatch(constraint -> constraint.getType() == ReservationConstraintInfo.Type.CLUSTER));
        assertTrue(constraints.stream()
                .filter(constraint -> constraint.getConstraintId() == 456L)
                .anyMatch(constraint -> constraint.getType() == ReservationConstraintInfo.Type.POLICY));
    }

    /**
     * Test converting a {@link ScenarioChange} protobuf object to a {@link DemandReservationApiDTO}
     * that describes it in the API.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testConvertToDemandReservationApiDTO() throws Exception {
        final ScenarioChange scenarioChange = ScenarioChange.newBuilder()
                .setTopologyAddition(TopologyAddition.newBuilder()
                        .setTemplateId(TEMPLATE_ID)
                        .setAdditionCount(1))
                .build();

        final PlacementInfo placementInfo = new PlacementInfo(1L, ImmutableList.of(2L, 3L));

        when(templateServiceMole.getTemplate(GetTemplateRequest.newBuilder()
                .setTemplateId(TEMPLATE_ID)
                .build()))
                .thenReturn(SingleTemplateResponse.newBuilder()
                    .setTemplate(TEMPLATE)
                    .build());

        MultiEntityRequest req = ApiTestUtils.mockMultiSEReq(Lists.newArrayList(vmServiceEntity, pmServiceEntity, stServiceEntity));
        when(repositoryApi.entitiesRequest(Mockito.any())).thenReturn(req);

        final DemandReservationApiDTO demandReservationApiDTO =
                reservationMapper.convertToDemandReservationApiDTO(scenarioChange.getTopologyAddition(),
                        Lists.newArrayList(placementInfo));
        assertEquals(1, (int)demandReservationApiDTO.getCount());
        assertEquals(PLACEMENT_SUCCEEDED, demandReservationApiDTO.getStatus());
        final List<DemandEntityInfoDTO> demandEntities = demandReservationApiDTO.getDemandEntities();
        assertEquals(1L, demandEntities.size());
        final BaseApiDTO templateResponse = demandEntities.get(0).getTemplate();
        assertEquals("VM-template", templateResponse.getDisplayName());
        assertEquals("VirtualMachineProfile", templateResponse.getClassName());
        assertEquals(String.valueOf(TEMPLATE_ID), templateResponse.getUuid());
        final PlacementInfoDTO placementInfoDTO = demandEntities.get(0).getPlacements();
        assertEquals(1L, placementInfoDTO.getComputeResources().size());
        assertEquals(1L, placementInfoDTO.getStorageResources().size());
        Assert.assertNull(placementInfoDTO.getNetworkResources());
        assertEquals("PM #1",
                placementInfoDTO.getComputeResources().get(0).getProvider().getDisplayName());
        assertEquals("ST #1",
                placementInfoDTO.getStorageResources().get(0).getProvider().getDisplayName());

        final DemandReservationApiDTO emptyDemandReservationApiDTO =
                reservationMapper.convertToDemandReservationApiDTO(scenarioChange.getTopologyAddition(),
                        Collections.EMPTY_LIST);
        assertEquals(1, (int)emptyDemandReservationApiDTO.getCount());
        assertEquals(PLACEMENT_FAILED, emptyDemandReservationApiDTO.getStatus());
        assertEquals(ONE_UNTRACKED_PLACEMENT_FAILED, emptyDemandReservationApiDTO.getPlacementResultMessage());
    }

    /**
     * Verify two failed placements should return two failed messages in placement result message.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testConvertToDemandReservationApiDTOTwoPlacementFailure() throws Exception {
        final ScenarioChange scenarioChange = ScenarioChange.newBuilder()
                .setTopologyAddition(TopologyAddition.newBuilder()
                        .setTemplateId(TEMPLATE_ID)
                        .setAdditionCount(2))
                .build();

        MultiEntityRequest req = ApiTestUtils.mockMultiSEReq(
                Lists.newArrayList(vmServiceEntity, pmServiceEntity, stServiceEntity));
        when(repositoryApi.entitiesRequest(Mockito.any())).thenReturn(req);

        final DemandReservationApiDTO emptyDemandReservationApiDTO =
                reservationMapper.convertToDemandReservationApiDTO(
                        scenarioChange.getTopologyAddition(), Collections.EMPTY_LIST);
        assertEquals(2, (int)emptyDemandReservationApiDTO.getCount());
        assertEquals(PLACEMENT_FAILED, emptyDemandReservationApiDTO.getStatus());
        // Two placement failures
        final String expected = ONE_UNTRACKED_PLACEMENT_FAILED + ONE_UNTRACKED_PLACEMENT_FAILED;
        assertEquals(expected, emptyDemandReservationApiDTO.getPlacementResultMessage());
    }

    /**
     * Test converting a {@link DemandReservationApiInputDTO} to a {@link Reservation} protobuf
     * object that describes it inside XL.
     *
     * @throws Exception If anything goes wrong.
     */
    @Ignore("Error due to DST? OM-51170 to fix.")
    @Test()
    public void testConvertToReservation() throws Exception {
        DemandReservationApiInputDTO demandApiInputDTO = new DemandReservationApiInputDTO();
        final Date today = new Date();
        LocalDateTime ldt = today.toInstant().atOffset(ZoneOffset.UTC).toLocalDateTime();
        final Date tomorrow = Date.from(ldt.plusDays(1).atOffset(ZoneOffset.UTC).toInstant());
        final Date nextMonth = Date.from(ldt.plusMonths(1).atOffset(ZoneOffset.UTC).toInstant());
        demandApiInputDTO.setDemandName("test-reservation");
        demandApiInputDTO.setReserveDateTime(DateTimeUtil.toString(tomorrow));
        demandApiInputDTO.setExpireDateTime(DateTimeUtil.toString(nextMonth));
        demandApiInputDTO.setAction(ReservationAction.RESERVATION);
        DemandReservationParametersDTO demandReservationParametersDTO =
                new DemandReservationParametersDTO();
        PlacementParametersDTO placementParametersDTO = new PlacementParametersDTO();
        placementParametersDTO.setCount(2);
        placementParametersDTO.setTemplateID("123");
        demandReservationParametersDTO.setPlacementParameters(placementParametersDTO);
        demandApiInputDTO.setParameters(Lists.newArrayList(demandReservationParametersDTO));
        final Reservation reservation = reservationMapper.convertToReservation(demandApiInputDTO);
        assertTrue(reservation.getStatus() == ReservationStatus.FUTURE);
        assertEquals(tomorrow.getTime(), reservation.getStartDate(), 1000);
        assertEquals(nextMonth.getTime(), reservation.getExpirationDate(), 1000);
    }

    /**
     * Test converting a {@link Reservation} protobuf message to a {@link DemandReservationApiDTO}.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testConvertReservationToApiDTO() throws Exception {
        final Date today = new Date();
        LocalDateTime ldt = today.toInstant().atOffset(ZoneOffset.UTC).toLocalDateTime();
        final Date nextMonth = Date.from(ldt.plusMonths(1).atOffset(ZoneOffset.UTC).toInstant());
        Reservation.Builder reservationBuider = Reservation.newBuilder();
        reservationBuider.setId(111);
        reservationBuider.setName("test-reservation");
        reservationBuider.setStartDate(today.getTime());
        reservationBuider.setExpirationDate(nextMonth.getTime());
        reservationBuider.setStatus(ReservationStatus.RESERVED);
        ReservationTemplate reservationTemplate = ReservationTemplate.newBuilder()
                .setTemplateId(TEMPLATE_ID)
                .setCount(1)
                .addReservationInstance(ReservationInstance.newBuilder()
                        .setEntityId(1L)
                        .addPlacementInfo(ReservationInstance.PlacementInfo.newBuilder()
                                .setProviderId(2L))
                        .addPlacementInfo(ReservationInstance.PlacementInfo.newBuilder()
                                .setProviderId(3L)))
                .build();
        final Reservation reservation = reservationBuider.setReservationTemplateCollection(
                ReservationTemplateCollection.newBuilder()
                        .addReservationTemplate(reservationTemplate))
                .build();
        when(templateServiceMole.getTemplate(GetTemplateRequest.newBuilder()
                .setTemplateId(TEMPLATE_ID)
                .build()))
                .thenReturn(SingleTemplateResponse.newBuilder()
                    .setTemplate(TEMPLATE)
                    .build());

        MultiEntityRequest req = ApiTestUtils.mockMultiSEReq(Lists.newArrayList(vmServiceEntity, pmServiceEntity, stServiceEntity));
        when(repositoryApi.entitiesRequest(Mockito.any())).thenReturn(req);

        final DemandReservationApiDTO reservationApiDTO =
                reservationMapper.convertReservationToApiDTO(reservation);
        assertEquals("test-reservation", reservationApiDTO.getDisplayName());
        assertEquals("RESERVED", reservationApiDTO.getStatus());
        assertEquals(1L, reservationApiDTO.getDemandEntities().size());
        assertEquals(1L, reservationApiDTO.getDemandEntities().get(0)
                .getPlacements().getComputeResources().size());
        assertEquals(1L, reservationApiDTO.getDemandEntities().get(0)
                .getPlacements().getStorageResources().size());
        final ResourceApiDTO computeResource = reservationApiDTO.getDemandEntities().get(0)
                .getPlacements().getComputeResources().get(0);
        final ResourceApiDTO storageResource = reservationApiDTO.getDemandEntities().get(0)
                .getPlacements().getStorageResources().get(0);
        assertEquals(pmServiceEntity.getDisplayName(), computeResource.getProvider().getDisplayName());
        assertEquals(stServiceEntity.getDisplayName(), storageResource.getProvider().getDisplayName());
    }
}
