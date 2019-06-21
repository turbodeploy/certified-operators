package com.vmturbo.api.component.external.api.mapper;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import com.vmturbo.api.component.ApiTestUtils;
import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.communication.RepositoryApi.MultiEntityRequest;
import com.vmturbo.api.component.external.api.mapper.ReservationMapper.PlacementInfo;
import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.reservation.DemandEntityInfoDTO;
import com.vmturbo.api.dto.reservation.DemandReservationApiDTO;
import com.vmturbo.api.dto.reservation.DemandReservationApiInputDTO;
import com.vmturbo.api.dto.reservation.DemandReservationParametersDTO;
import com.vmturbo.api.dto.reservation.PlacementInfoDTO;
import com.vmturbo.api.dto.reservation.PlacementParametersDTO;
import com.vmturbo.api.dto.template.ResourceApiDTO;
import com.vmturbo.api.enums.ReservationAction;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupID;
import com.vmturbo.common.protobuf.group.GroupDTOMoles.GroupServiceMole;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.group.PolicyDTO.Policy;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo.BindToGroupPolicy;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyRequest;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyResponse;
import com.vmturbo.common.protobuf.group.PolicyDTOMoles.PolicyServiceMole;
import com.vmturbo.common.protobuf.group.PolicyServiceGrpc;
import com.vmturbo.common.protobuf.group.PolicyServiceGrpc.PolicyServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.PlanDTO.ReservationConstraintInfo;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.TopologyAddition;
import com.vmturbo.common.protobuf.plan.ReservationDTO.Reservation;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationStatus;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection.ReservationTemplate;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection.ReservationTemplate.ReservationInstance;
import com.vmturbo.common.protobuf.plan.TemplateDTO.GetTemplateRequest;
import com.vmturbo.common.protobuf.plan.TemplateDTO.Template;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateInfo;
import com.vmturbo.common.protobuf.plan.TemplateDTOMoles.TemplateServiceMole;
import com.vmturbo.common.protobuf.plan.TemplateServiceGrpc;
import com.vmturbo.common.protobuf.plan.TemplateServiceGrpc.TemplateServiceBlockingStub;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Test cases for {@link ReservationMapper}.
 */
public class ReservationMapperTest {

    private TemplateServiceMole templateServiceMole = Mockito.spy(new TemplateServiceMole());

    private TemplateServiceBlockingStub templateServiceBlockingStub;

    private GroupServiceMole groupServiceMole = Mockito.spy(new GroupServiceMole());

    private GroupServiceBlockingStub groupServiceBlockingStub;

    private PolicyServiceMole policyServiceMole = Mockito.spy(new PolicyServiceMole());

    private PolicyServiceBlockingStub policyServiceBlockingStub;

    private RepositoryApi repositoryApi;

    private ReservationMapper reservationMapper;

    private final long TEMPLATE_ID = 123L;

    private ServiceEntityApiDTO vmServiceEntity = new ServiceEntityApiDTO();

    private  ServiceEntityApiDTO pmServiceEntity = new ServiceEntityApiDTO();

    private ServiceEntityApiDTO stServiceEntity = new ServiceEntityApiDTO();

    private final String PLACEMENT_SUCCEEDED = "PLACEMENT_SUCCEEDED";

    private final String PLACEMENT_FAILED = "PLACEMENT_FAILED";

    final Template template = Template.newBuilder()
            .setId(TEMPLATE_ID)
            .setTemplateInfo(TemplateInfo.newBuilder()
                    .setName("VM-template")
                    .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE))
            .build();

    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(templateServiceMole, groupServiceMole,
            policyServiceMole);

    @Before
    public void setup() {
        repositoryApi = Mockito.mock(RepositoryApi.class);
        templateServiceBlockingStub = TemplateServiceGrpc.newBlockingStub(grpcServer.getChannel());
        groupServiceBlockingStub = GroupServiceGrpc.newBlockingStub(grpcServer.getChannel());
        policyServiceBlockingStub = PolicyServiceGrpc.newBlockingStub(grpcServer.getChannel());

        reservationMapper = new ReservationMapper(repositoryApi, templateServiceBlockingStub,
                groupServiceBlockingStub, policyServiceBlockingStub);

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

    @Test
    public void testConvertToScenarioChangeWithConstraint() throws Exception {
        final DemandReservationParametersDTO demandParameters = new DemandReservationParametersDTO();
        final PlacementParametersDTO placementParameter = new PlacementParametersDTO();
        placementParameter.setCount(2);
        placementParameter.setTemplateID(String.valueOf(TEMPLATE_ID));
        placementParameter.setConstraintIDs(Sets.newHashSet("123", "456"));
        demandParameters.setPlacementParameters(placementParameter);
        Mockito.when(groupServiceMole.getGroup(GroupID.newBuilder()
                .setId(123L)
                .build()))
                .thenReturn(GetGroupResponse.newBuilder()
                        .setGroup(Group.newBuilder()
                                .setType(Group.Type.CLUSTER))
                        .build());
        Mockito.when(policyServiceMole.getPolicy(PolicyRequest.newBuilder()
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

    @Test
    public void testConvertToDemandReservationApiDTO() throws Exception {
        final ScenarioChange scenarioChange = ScenarioChange.newBuilder()
                .setTopologyAddition(TopologyAddition.newBuilder()
                        .setTemplateId(TEMPLATE_ID)
                        .setAdditionCount(1))
                .build();

        final PlacementInfo placementInfo = new PlacementInfo(1L, ImmutableList.of(2L, 3L));

        Mockito.when(templateServiceMole.getTemplate(GetTemplateRequest.newBuilder()
                .setTemplateId(TEMPLATE_ID)
                .build()))
                .thenReturn(template);

        MultiEntityRequest req = ApiTestUtils.mockMultiSEReq(Lists.newArrayList(vmServiceEntity, pmServiceEntity, stServiceEntity));
        Mockito.when(repositoryApi.entitiesRequest(Mockito.any())).thenReturn(req);

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
    }

    @Test
    public void testConvertToReservation() throws Exception {
        DemandReservationApiInputDTO demandApiInputDTO = new DemandReservationApiInputDTO();
        final DateTime today = DateTime.now(DateTimeZone.UTC);
        final DateTime tomorrow = today.plusDays(1);
        final DateTime nextMonth = today.plusMonths(1);
        demandApiInputDTO.setDemandName("test-reservation");
        demandApiInputDTO.setReserveDateTime(tomorrow.toString());
        demandApiInputDTO.setExpireDateTime(nextMonth.toString());
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
        assertEquals(tomorrow.getMillis(), reservation.getStartDate());
        assertEquals(nextMonth.getMillis(), reservation.getExpirationDate());
    }

    @Test
    public void testConvertReservationToApiDTO() throws Exception {
        DateTime today = DateTime.now(DateTimeZone.UTC);
        DateTime nextMonth = today.plusMonths(1);
        Reservation.Builder reservationBuider = Reservation.newBuilder();
        reservationBuider.setId(111);
        reservationBuider.setName("test-reservation");
        reservationBuider.setStartDate(today.getMillis());
        reservationBuider.setExpirationDate(nextMonth.getMillis());
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
        Mockito.when(templateServiceMole.getTemplate(GetTemplateRequest.newBuilder()
                .setTemplateId(TEMPLATE_ID)
                .build()))
                .thenReturn(template);

        MultiEntityRequest req = ApiTestUtils.mockMultiSEReq(Lists.newArrayList(vmServiceEntity, pmServiceEntity, stServiceEntity));
        Mockito.when(repositoryApi.entitiesRequest(Mockito.any())).thenReturn(req);

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
