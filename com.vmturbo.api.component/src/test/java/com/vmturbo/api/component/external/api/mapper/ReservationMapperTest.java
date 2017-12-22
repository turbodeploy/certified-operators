package com.vmturbo.api.component.external.api.mapper;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.external.api.mapper.ReservationMapper.PlacementInfo;
import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.reservation.DemandEntityInfoDTO;
import com.vmturbo.api.dto.reservation.DemandReservationApiDTO;
import com.vmturbo.api.dto.reservation.DemandReservationParametersDTO;
import com.vmturbo.api.dto.reservation.PlacementInfoDTO;
import com.vmturbo.api.dto.reservation.PlacementParametersDTO;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.GroupDTO.Group.Type;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupID;
import com.vmturbo.common.protobuf.group.GroupDTOMoles.GroupServiceMole;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.PlanChanges.InitialPlacementConstraint;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.TopologyAddition;
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

    private RepositoryApi repositoryApi;

    private ReservationMapper reservationMapper;

    private final long TEMPLATE_ID = 123L;

    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(templateServiceMole, groupServiceMole);

    @Before
    public void setup() {
        repositoryApi = Mockito.mock(RepositoryApi.class);
        templateServiceBlockingStub = TemplateServiceGrpc.newBlockingStub(grpcServer.getChannel());
        groupServiceBlockingStub = GroupServiceGrpc.newBlockingStub(grpcServer.getChannel());
        reservationMapper = new ReservationMapper(repositoryApi, templateServiceBlockingStub,
                groupServiceBlockingStub);
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
        Assert.assertEquals(1, scenarioChangeList.size());
        final ScenarioChange scenarioChange = scenarioChangeList.get(0);
        Assert.assertTrue(scenarioChange.hasTopologyAddition());
        Assert.assertEquals(2, scenarioChange.getTopologyAddition().getAdditionCount());
        Assert.assertEquals(123L, scenarioChange.getTopologyAddition().getTemplateId());
    }

    @Test
    public void testConvertToScenarioChangeWithConstraint() throws Exception {
        final DemandReservationParametersDTO demandParameters = new DemandReservationParametersDTO();
        final PlacementParametersDTO placementParameter = new PlacementParametersDTO();
        placementParameter.setCount(2);
        placementParameter.setTemplateID(String.valueOf(TEMPLATE_ID));
        placementParameter.setConstraintIDs(Sets.newHashSet("123"));
        demandParameters.setPlacementParameters(placementParameter);
        Mockito.when(groupServiceMole.getGroup(GroupID.newBuilder()
                .setId(123L)
                .build()))
                .thenReturn(GetGroupResponse.newBuilder()
                        .setGroup(Group.newBuilder()
                                .setType(Group.Type.CLUSTER))
                        .build());
        final List<ScenarioChange> scenarioChangeList =
                reservationMapper.placementToScenarioChange(Lists.newArrayList(demandParameters));
        Assert.assertEquals(2, scenarioChangeList.size());
        Assert.assertTrue(scenarioChangeList.stream()
                .anyMatch(ScenarioChange::hasPlanChanges));
        final ScenarioChange planChange = scenarioChangeList.stream()
                .filter(ScenarioChange::hasPlanChanges)
                .findFirst()
                .get();
        final List<InitialPlacementConstraint> constraints = planChange.getPlanChanges()
                .getInitialPlacementConstraintsList();
        Assert.assertEquals(1L, constraints.size());
        final InitialPlacementConstraint constraint = constraints.get(0);
        Assert.assertEquals(123L, constraint.getConstraintId());
        Assert.assertEquals(InitialPlacementConstraint.Type.CLUSTER, constraint.getType());
    }

    @Test
    public void testConvertToDemandReservationApiDTO() throws Exception {
        final ScenarioChange scenarioChange = ScenarioChange.newBuilder()
                .setTopologyAddition(TopologyAddition.newBuilder()
                        .setTemplateId(TEMPLATE_ID)
                        .setAdditionCount(1))
                .build();

        final PlacementInfo placementInfo = new PlacementInfo(1L, ImmutableList.of(2L, 3L));
        final Template template = Template.newBuilder()
                .setId(TEMPLATE_ID)
                .setTemplateInfo(TemplateInfo.newBuilder()
                        .setName("VM-template")
                        .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE))
                .build();
        Mockito.when(templateServiceMole.getTemplate(GetTemplateRequest.newBuilder()
                .setTemplateId(TEMPLATE_ID)
                .build()))
                .thenReturn(template);

        ServiceEntityApiDTO vmServiceEntity = new ServiceEntityApiDTO();
        vmServiceEntity.setClassName("VirtualMachine");
        vmServiceEntity.setDisplayName("VM-template Clone #1");
        vmServiceEntity.setUuid("1");
        ServiceEntityApiDTO pmServiceEntity = new ServiceEntityApiDTO();
        pmServiceEntity.setClassName("PhysicalMachine");
        pmServiceEntity.setDisplayName("PM #1");
        pmServiceEntity.setUuid("2");
        ServiceEntityApiDTO stServiceEntity = new ServiceEntityApiDTO();
        stServiceEntity.setClassName("Storage");
        stServiceEntity.setDisplayName("ST #1");
        stServiceEntity.setUuid("3");

        final Map<Long, Optional<ServiceEntityApiDTO>> serviceEntityApiDTOMap =
                ImmutableMap.of(1L, Optional.of(vmServiceEntity), 2L, Optional.of(pmServiceEntity),
                        3L, Optional.of(stServiceEntity));
        Mockito.when(repositoryApi.getServiceEntitiesById(Mockito.any()))
                .thenReturn(serviceEntityApiDTOMap);
        final DemandReservationApiDTO demandReservationApiDTO =
                reservationMapper.convertToDemandReservationApiDTO(scenarioChange.getTopologyAddition(),
                        Lists.newArrayList(placementInfo));
        Assert.assertEquals(1, (int)demandReservationApiDTO.getCount());
        final List<DemandEntityInfoDTO> demandEntities = demandReservationApiDTO.getDemandEntities();
        Assert.assertEquals(1L, demandEntities.size());
        final BaseApiDTO templateResponse = demandEntities.get(0).getTemplate();
        Assert.assertEquals("VM-template", templateResponse.getDisplayName());
        Assert.assertEquals("VirtualMachineProfile", templateResponse.getClassName());
        Assert.assertEquals(String.valueOf(TEMPLATE_ID), templateResponse.getUuid());
        final PlacementInfoDTO placementInfoDTO = demandEntities.get(0).getPlacements();
        Assert.assertEquals(1L, placementInfoDTO.getComputeResources().size());
        Assert.assertEquals(1L, placementInfoDTO.getStorageResources().size());
        Assert.assertNull(placementInfoDTO.getNetworkResources());
        Assert.assertEquals("PM #1",
                placementInfoDTO.getComputeResources().get(0).getProvider().getDisplayName());
        Assert.assertEquals("ST #1",
                placementInfoDTO.getStorageResources().get(0).getProvider().getDisplayName());
    }
}
