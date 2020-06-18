package com.vmturbo.api.component.external.api.mapper;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.api.ReservationNotificationDTO.ReservationNotification;
import com.vmturbo.api.component.ApiTestUtils;
import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.communication.RepositoryApi.MultiEntityRequest;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.reservation.DemandReservationApiDTO;
import com.vmturbo.api.dto.reservation.DemandReservationApiInputDTO;
import com.vmturbo.api.dto.reservation.DemandReservationParametersDTO;
import com.vmturbo.api.dto.reservation.FailureInfoDTO;
import com.vmturbo.api.dto.reservation.PlacementParametersDTO;
import com.vmturbo.api.dto.template.ResourceApiDTO;
import com.vmturbo.api.enums.ReservationAction;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.common.protobuf.group.GroupDTOMoles.GroupServiceMole;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.group.PolicyDTOMoles.PolicyServiceMole;
import com.vmturbo.common.protobuf.group.PolicyServiceGrpc;
import com.vmturbo.common.protobuf.plan.DeploymentProfileDTOMoles.DeploymentProfileServiceMole;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ConstraintInfoCollection;
import com.vmturbo.common.protobuf.plan.ReservationDTO.InitialPlacementFailureInfo;
import com.vmturbo.common.protobuf.plan.ReservationDTO.Reservation;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationChange;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationChanges;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationStatus;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection.ReservationTemplate;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection.ReservationTemplate.ReservationInstance;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection.ReservationTemplate.ReservationInstance.PlacementInfo;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ReservationConstraintInfo;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ReservationConstraintInfo.Type;
import com.vmturbo.common.protobuf.plan.TemplateDTO.Template;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateInfo;
import com.vmturbo.common.protobuf.plan.TemplateDTOMoles.TemplateServiceMole;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
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

    private static final ReservationChanges RES_CHANGES = ReservationChanges.newBuilder()
        .buildPartial();
    private static final ReservationChanges RESERVED_CHANGE = RES_CHANGES.toBuilder()
        .addReservationChange(ReservationChange.newBuilder()
            .setId(1L)
            .setStatus(ReservationStatus.RESERVED)
            .build())
        .build();
    private static final ReservationChanges FAILED_INPROGRESS_CHANGE = RES_CHANGES.toBuilder()
        .addReservationChange(ReservationChange.newBuilder()
            .setId(2L)
            .setStatus(ReservationStatus.PLACEMENT_FAILED)
            .build())
        .addReservationChange(ReservationChange.newBuilder()
            .setId(3L)
            .setStatus(ReservationStatus.INPROGRESS)
            .build())
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
            GroupServiceGrpc.newBlockingStub(grpcServer.getChannel()),
            PolicyServiceGrpc.newBlockingStub(grpcServer.getChannel()));

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
        assertTrue(reservation.getStatus() == ReservationStatus.INITIAL);
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
        reservationBuider.setConstraintInfoCollection(ConstraintInfoCollection
                .newBuilder()
                .addReservationConstraintInfo(ReservationConstraintInfo
                        .newBuilder().setConstraintId(123456L)
                .setType(Type.CLUSTER)));
        reservationBuider.setStartDate(today.getTime());
        reservationBuider.setExpirationDate(nextMonth.getTime());
        reservationBuider.setStatus(ReservationStatus.RESERVED);
        final CommodityBoughtDTO cpuProvisionedCommodityBoughtDTO =
                CommodityBoughtDTO.newBuilder()
                        .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                                .setType(CommodityType.CPU_PROVISIONED_VALUE))
                        .setUsed(1000)
                        .build();
        final CommodityBoughtDTO memProvisionedCommodityBoughtDTO =
                CommodityBoughtDTO.newBuilder()
                        .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                                .setType(CommodityType.MEM_PROVISIONED_VALUE))
                        .setUsed(2000)
                        .build();
        final CommodityBoughtDTO storageProvisionedCommodityBoughtDTO =
                CommodityBoughtDTO.newBuilder()
                        .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                                .setType(CommodityType.STORAGE_PROVISIONED_VALUE))
                        .setUsed(3000)
                        .build();
        ReservationTemplate reservationTemplate = ReservationTemplate.newBuilder()
                .setTemplate(TEMPLATE)
                .setCount(1)
                .addReservationInstance(ReservationInstance.newBuilder()
                        .setEntityId(1L)
                        .addPlacementInfo(ReservationInstance.PlacementInfo.newBuilder()
                                .setProviderId(2L)
                                .addCommodityBought(cpuProvisionedCommodityBoughtDTO)
                                .addCommodityBought(memProvisionedCommodityBoughtDTO))
                        .addPlacementInfo(ReservationInstance.PlacementInfo.newBuilder()
                                .setProviderId(3L)
                                .addCommodityBought(storageProvisionedCommodityBoughtDTO)))
                .build();
        final Reservation reservation = reservationBuider.setReservationTemplateCollection(
                ReservationTemplateCollection.newBuilder()
                        .addReservationTemplate(reservationTemplate))
                .build();

        MultiEntityRequest req = ApiTestUtils.mockMultiSEReq(Lists.newArrayList(vmServiceEntity, pmServiceEntity, stServiceEntity));
        when(repositoryApi.entitiesRequest(Mockito.any())).thenReturn(req);

        final DemandReservationApiDTO reservationApiDTO =
                reservationMapper.convertReservationToApiDTO(reservation);
        assertEquals("test-reservation", reservationApiDTO.getDisplayName());
        assertEquals("RESERVED", reservationApiDTO.getStatus());
        assertEquals(1L, reservationApiDTO.getDemandEntities().size());
        assertEquals(1L, reservationApiDTO.getDemandEntities().get(0)
                .getPlacements().getComputeResources().size());
        assertEquals("123456", reservationApiDTO.getConstraintInfos().get(0).getUuid());
        assertEquals("CLUSTER", reservationApiDTO.getConstraintInfos()
                .get(0).getConstraintType());
        assertEquals(1L, reservationApiDTO.getDemandEntities().get(0)
                .getPlacements().getStorageResources().size());
        final ResourceApiDTO computeResource = reservationApiDTO.getDemandEntities().get(0)
                .getPlacements().getComputeResources().get(0);
        final ResourceApiDTO storageResource = reservationApiDTO.getDemandEntities().get(0)
                .getPlacements().getStorageResources().get(0);
        assertEquals(pmServiceEntity.getDisplayName(), computeResource.getProvider().getDisplayName());
        assertEquals(stServiceEntity.getDisplayName(), storageResource.getProvider().getDisplayName());
        assertEquals(1000L, Math.round(computeResource.getStats()
                .stream().filter(a -> a.getName().equals("CPU"))
                .collect(Collectors.toList())
                .get(0).getValue()));
        assertEquals(2000L, Math.round(computeResource.getStats()
                .stream().filter(a -> a.getName().equals("MEM"))
                .collect(Collectors.toList())
                .get(0).getValue()));
        assertEquals(3000L, Math.round(storageResource.getStats()
                .stream().filter(a -> a.getName().equals("STORAGE"))
                .collect(Collectors.toList())
                .get(0).getValue()));
    }

    /**
     * Test converting a {@link Reservation} protobuf message to a {@link DemandReservationApiDTO}.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testConvertReservationToApiDTOFailure() throws Exception {
        final Date today = new Date();
        LocalDateTime ldt = today.toInstant().atOffset(ZoneOffset.UTC).toLocalDateTime();
        final Date nextMonth = Date.from(ldt.plusMonths(1).atOffset(ZoneOffset.UTC).toInstant());
        Reservation.Builder reservationBuider = Reservation.newBuilder();
        reservationBuider.setId(111);
        reservationBuider.setName("test-reservation");
        reservationBuider.setConstraintInfoCollection(ConstraintInfoCollection
                .newBuilder()
                .addReservationConstraintInfo(ReservationConstraintInfo
                        .newBuilder().setConstraintId(123456L)
                        .setType(Type.CLUSTER)));
        reservationBuider.setStartDate(today.getTime());
        reservationBuider.setExpirationDate(nextMonth.getTime());
        reservationBuider.setStatus(ReservationStatus.PLACEMENT_FAILED);
        final CommodityBoughtDTO cpuProvisionedCommodityBoughtDTO =
                CommodityBoughtDTO.newBuilder()
                        .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                                .setType(CommodityType.CPU_PROVISIONED_VALUE))
                        .setUsed(1000)
                        .build();
        final CommodityBoughtDTO memProvisionedCommodityBoughtDTO =
                CommodityBoughtDTO.newBuilder()
                        .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                                .setType(CommodityType.MEM_PROVISIONED_VALUE))
                        .setUsed(2000)
                        .build();
        final CommodityBoughtDTO storageProvisionedCommodityBoughtDTO =
                CommodityBoughtDTO.newBuilder()
                        .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                                .setType(CommodityType.STORAGE_PROVISIONED_VALUE))
                        .setUsed(3000)
                        .build();
        ReservationTemplate reservationTemplate = ReservationTemplate.newBuilder()
                .setTemplate(TEMPLATE)
                .setCount(1)
                .addReservationInstance(ReservationInstance.newBuilder()
                        .setEntityId(1L)
                        .addFailureInfo(InitialPlacementFailureInfo
                                .newBuilder()
                                .setClosestSeller(2L)
                                .setCommType(TopologyDTO.CommodityType
                                        .newBuilder().setType(CommodityType.MEM_PROVISIONED_VALUE))
                                .setMaxQuantityAvailable(1000)
                                .setRequestedQuantity(1200))
                        .addPlacementInfo(PlacementInfo.newBuilder()
                                .addCommodityBought(cpuProvisionedCommodityBoughtDTO)
                                .addCommodityBought(memProvisionedCommodityBoughtDTO)
                                .setPlacementInfoId(1L)
                                .setProviderType(1))
                        .addPlacementInfo(PlacementInfo.newBuilder()
                                .addCommodityBought(storageProvisionedCommodityBoughtDTO)
                                .setPlacementInfoId(2L)
                                .setProviderType(1)))
                .build();
        final Reservation reservation = reservationBuider.setReservationTemplateCollection(
                ReservationTemplateCollection.newBuilder()
                        .addReservationTemplate(reservationTemplate))
                .build();

        MultiEntityRequest req = ApiTestUtils.mockMultiSEReq(Lists.newArrayList(vmServiceEntity, pmServiceEntity, stServiceEntity));
        when(repositoryApi.entitiesRequest(Mockito.any())).thenReturn(req);

        final DemandReservationApiDTO reservationApiDTO =
                reservationMapper.convertReservationToApiDTO(reservation);
        assertEquals("test-reservation", reservationApiDTO.getDisplayName());
        assertEquals("PLACEMENT_FAILED", reservationApiDTO.getStatus());
        assertEquals(1L, reservationApiDTO.getDemandEntities().size());
        assertEquals("123456", reservationApiDTO.getConstraintInfos().get(0).getUuid());
        assertEquals("CLUSTER", reservationApiDTO.getConstraintInfos()
                .get(0).getConstraintType());
        assert (reservationApiDTO.getDemandEntities().get(0)
                .getPlacements().getComputeResources() == null);
        assert (reservationApiDTO.getDemandEntities().get(0)
                .getPlacements().getStorageResources() == null);
        assertEquals(1, reservationApiDTO.getDemandEntities().get(0)
                .getPlacements().getFailureInfos().size());
        FailureInfoDTO failureInfo = reservationApiDTO.getDemandEntities().get(0)
                .getPlacements().getFailureInfos().get(0);
        assertEquals(pmServiceEntity.getDisplayName(), failureInfo.getClosestSeller().getDisplayName());
        assertEquals("MEM", failureInfo.getCommodity());
        assertEquals(1000, Math.round(failureInfo.getMaxQuantityAvailable().doubleValue()));
        assertEquals(1200, Math.round(failureInfo.getQuantityRequested().doubleValue()));
    }

    /**
     * Test that a {@link ReservationNotification} has proper fields after being converted from
     * {@link ReservationChanges} with single {@link ReservationChange} that is Reserved.
     */
    @Test
    public void testNotificationFromReservationChangesSucceeded() {
        final ReservationNotification success = ReservationMapper.notificationFromReservationChanges(RESERVED_CHANGE);
        assertTrue(success.hasStatusNotification());
        assertEquals(1, success.getStatusNotification().getReservationStatusCount());
        assertEquals(Long.toString(1L),
            success.getStatusNotification().getReservationStatus(0).getId());
        assertEquals(ReservationStatus.RESERVED.toString(),
            success.getStatusNotification().getReservationStatus(0).getStatus());
    }

    /**
     * Test that a {@link ReservationNotification} has proper fields after being converted from
     * {@link ReservationChanges} with 2 {@link ReservationChange} where 1 is Failed and 1 is InProgress.
     */
    @Test
    public void testNotificationFromReservationChangesInProgressAndFailed() {
        final ReservationNotification success = ReservationMapper.notificationFromReservationChanges(FAILED_INPROGRESS_CHANGE);
        assertTrue(success.hasStatusNotification());
        assertEquals(2, success.getStatusNotification().getReservationStatusCount());
        assertEquals(Long.toString(2L),
            success.getStatusNotification().getReservationStatus(0).getId());
        assertEquals(ReservationStatus.PLACEMENT_FAILED.toString(),
            success.getStatusNotification().getReservationStatus(0).getStatus());
        assertEquals(Long.toString(3L),
            success.getStatusNotification().getReservationStatus(1).getId());
        assertEquals(ReservationStatus.INPROGRESS.toString(),
            success.getStatusNotification().getReservationStatus(1).getStatus());
    }

}
