package com.vmturbo.api.component.external.api.mapper;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;

import com.vmturbo.api.ReservationNotificationDTO.ReservationNotification;
import com.vmturbo.api.component.ApiTestUtils;
import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.communication.RepositoryApi.MultiEntityRequest;
import com.vmturbo.api.conversion.entity.CommodityTypeMapping;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.reservation.DemandReservationApiDTO;
import com.vmturbo.api.dto.reservation.DemandReservationApiInputDTO;
import com.vmturbo.api.dto.reservation.DemandReservationParametersDTO;
import com.vmturbo.api.dto.reservation.PlacementParametersDTO;
import com.vmturbo.api.dto.reservation.ReservationFailureInfoDTO;
import com.vmturbo.api.dto.template.ResourceApiDTO;
import com.vmturbo.api.exceptions.InvalidOperationException;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.common.protobuf.group.GroupDTO.CountGroupsResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupDTOMoles.GroupServiceMole;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.group.PolicyDTOMoles.PolicyServiceMole;
import com.vmturbo.common.protobuf.group.PolicyServiceGrpc;
import com.vmturbo.common.protobuf.plan.DeploymentProfileDTOMoles.DeploymentProfileServiceMole;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ConstraintInfoCollection;
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
import com.vmturbo.common.protobuf.plan.TemplateServiceGrpc;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.UnplacementReason;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.UnplacementReason.FailedResources;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;

/**
 * Test cases for {@link ReservationMapper}.
 */
public class ReservationMapperTest {
    private static final ReservationChanges RES_CHANGES =
            ReservationChanges.newBuilder().buildPartial();

    private static final long TEMPLATE_ID = 123L;

    private static final long CLUSTER_ID = 123L;

    private static final String CLUSTER_NAME = "cluster1";

    private static final Template TEMPLATE = Template
            .newBuilder()
            .setId(TEMPLATE_ID)
            .setTemplateInfo(TemplateInfo
                    .newBuilder()
                    .setName("VM-template")
                    .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE))
            .build();

    private static final ReservationChanges RESERVED_CHANGE = RES_CHANGES
            .toBuilder()
            .addReservationChange(ReservationChange
                    .newBuilder()
                    .setId(1L)
                    .setStatus(ReservationStatus.RESERVED)
                    .build())
            .build();

    private static final ReservationChanges FAILED_IN_PROGRESS_CHANGE = RES_CHANGES
            .toBuilder()
            .addReservationChange(ReservationChange
                    .newBuilder()
                    .setId(2L)
                    .setStatus(ReservationStatus.PLACEMENT_FAILED)
                    .build())
            .addReservationChange(ReservationChange
                    .newBuilder()
                    .setId(3L)
                    .setStatus(ReservationStatus.INPROGRESS)
                    .build())
            .build();

    private static final Date DATE_FROM = Date.from(Instant.ofEpochMilli(5000000000000L));

    private static final Date DATE_TO =
            Date.from(Instant.ofEpochMilli(DATE_FROM.getTime() + TimeUnit.DAYS.toMillis(10)));

    private final ServiceEntityApiDTO vmServiceEntity = new ServiceEntityApiDTO();

    private final ServiceEntityApiDTO stServiceEntity = new ServiceEntityApiDTO();

    private final ServiceEntityApiDTO pmServiceEntity = new ServiceEntityApiDTO();

    private ReservationMapper reservationMapper;

    @Mock
    private RepositoryApi repositoryApi;

    @Spy
    private TemplateServiceMole templateServiceMole = spy(new TemplateServiceMole());

    @Spy
    private GroupServiceMole groupServiceMole = spy(new GroupServiceMole());

    @Spy
    private PolicyServiceMole policyServiceMole = spy(new PolicyServiceMole());

    @Spy
    private DeploymentProfileServiceMole deploymentProfileServiceMole =
            spy(new DeploymentProfileServiceMole());

    /**
     * Utility gRPC server to mock out gRPC service dependencies.
     */
    @Rule
    public GrpcTestServer grpcServer =
            GrpcTestServer.newServer(templateServiceMole, groupServiceMole, policyServiceMole,
                    deploymentProfileServiceMole);

    /**
     * Common setup code to run before each test.
     */
    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);

        reservationMapper = new ReservationMapper(repositoryApi,
                TemplateServiceGrpc.newBlockingStub(grpcServer.getChannel()),
                GroupServiceGrpc.newBlockingStub(grpcServer.getChannel()),
                PolicyServiceGrpc.newBlockingStub(grpcServer.getChannel()), true);

        vmServiceEntity.setClassName("VirtualMachine");
        vmServiceEntity.setDisplayName("VM-template Clone #1");
        vmServiceEntity.setUuid("1");
        pmServiceEntity.setClassName("PhysicalMachine");
        pmServiceEntity.setDisplayName("PM #1");
        pmServiceEntity.setUuid("2");
        stServiceEntity.setClassName("Storage");
        stServiceEntity.setDisplayName("ST #1");
        stServiceEntity.setUuid("3");
        final Grouping emptyGroup = Grouping
                .newBuilder()
                .setId(CLUSTER_ID)
                .setDefinition(GroupDefinition
                        .newBuilder()
                        .setType(GroupType.COMPUTE_HOST_CLUSTER)
                        .setDisplayName(CLUSTER_NAME)
                        .build())
                .build();
        when(groupServiceMole.getGroups(any())).thenReturn(Collections.singletonList(emptyGroup));
    }

    @NotNull
    private static DemandReservationApiInputDTO createReservationApiDTOWithAllRequiredFields() {
        DemandReservationApiInputDTO demandApiInputDTO = new DemandReservationApiInputDTO();
        demandApiInputDTO.setDemandName("test-reservation");
        demandApiInputDTO.setReserveDateTime(DateTimeUtil.toString(DATE_FROM));
        demandApiInputDTO.setExpireDateTime(DateTimeUtil.toString(DATE_TO));
        DemandReservationParametersDTO demandReservationParametersDTO =
                new DemandReservationParametersDTO();
        PlacementParametersDTO placementParametersDTO = new PlacementParametersDTO();
        placementParametersDTO.setCount(2);
        placementParametersDTO.setTemplateID("123");
        demandReservationParametersDTO.setPlacementParameters(placementParametersDTO);
        demandApiInputDTO.setParameters(Lists.newArrayList(demandReservationParametersDTO));
        return demandApiInputDTO;
    }

    private static void assertThatAllDTORequiredFieldWereMappedCorrectly(
            @NotNull Reservation reservation) {
        assertEquals("test-reservation", reservation.getName());
        assertEquals(ReservationStatus.INITIAL, reservation.getStatus());
        assertEquals(DATE_FROM.getTime(), reservation.getStartDate(), 1000);
        assertEquals(DATE_TO.getTime(), reservation.getExpirationDate(), 1000);
    }

    /**
     * Test converting a minimal {@link DemandReservationApiInputDTO} to a {@link Reservation}
     * protobuf object that describes it inside XL.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test()
    public void testConvertToReservationRequiredFields() throws Exception {

        DemandReservationApiInputDTO demandApiInputDTO =
                createReservationApiDTOWithAllRequiredFields();
        final Reservation reservation = reservationMapper.convertToReservation(demandApiInputDTO);

        assertThatAllDTORequiredFieldWereMappedCorrectly(reservation);
    }

    /**
     * Tests that the incoming scope is correctly converted to the internal representation.
     *
     * @throws Exception - if the conversion fails
     */
    @Test
    public void testConvertToReservationScopesMatch() throws Exception {
        // ARRANGE
        DemandReservationApiInputDTO demandReservationApiInputDTO =
                createReservationApiDTOWithAllRequiredFields();
        demandReservationApiInputDTO.setScope(ImmutableList.of("1"));

        when(groupServiceMole.countGroups(any())).thenReturn(
                CountGroupsResponse.newBuilder().setCount(1).build());

        // ACT
        final Reservation reservation =
                reservationMapper.convertToReservation(demandReservationApiInputDTO);

        // ASSERT
        assertThatAllDTORequiredFieldWereMappedCorrectly(reservation);
        // TODO: add an assertion ensuring that the copied scopes match the supplied once.
    }

    /**
     * If any of the given group ids is not a valid group, then the mapping should fail and a
     * {@link InvalidOperationException} should be thrown.
     *
     * @throws Exception - if the mapping fails
     */
    @Test(expected = InvalidOperationException.class)
    public void testConvertToReservationWithInvalidScopeThrowsInvalid() throws Exception {
        // ARRANGE
        DemandReservationApiInputDTO demandReservationApiInputDTO =
                createReservationApiDTOWithAllRequiredFields();
        demandReservationApiInputDTO.setScope(ImmutableList.of("1", "2"));

        // Only one of the 2 groups actually fits our criteria
        when(groupServiceMole.countGroups(any())).thenReturn(
                CountGroupsResponse.newBuilder().setCount(1).build());

        // ACT
        reservationMapper.convertToReservation(demandReservationApiInputDTO);
    }

    /**
     * Make sure that we catch invalid uuids, meaning uuids that are not longs.
     *
     * @throws Exception - if the mapping fails
     */
    @Test(expected = InvalidOperationException.class)
    public void testConvertToReservationWithInvalidUuidInScope() throws Exception {
        // ARRANGE
        DemandReservationApiInputDTO demandReservationApiInputDTO =
                createReservationApiDTOWithAllRequiredFields();
        demandReservationApiInputDTO.setScope(ImmutableList.of("invalid uuid"));

        // ACT
        reservationMapper.convertToReservation(demandReservationApiInputDTO);
    }

    /**
     * Test converting a {@link Reservation} protobuf message to a {@link DemandReservationApiDTO}.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testConvertReservationToApiDTO() throws Exception {
        Reservation.Builder reservationBuider = Reservation.newBuilder();
        reservationBuider.setId(111);
        reservationBuider.setName("test-reservation");
        reservationBuider.setConstraintInfoCollection(ConstraintInfoCollection
                .newBuilder()
                .addReservationConstraintInfo(ReservationConstraintInfo
                        .newBuilder()
                        .setConstraintId(123456L)
                        .setType(Type.CLUSTER)));
        reservationBuider.setStartDate(DATE_FROM.getTime());
        reservationBuider.setExpirationDate(DATE_TO.getTime());
        reservationBuider.setStatus(ReservationStatus.RESERVED);
        final CommodityBoughtDTO cpuProvisionedCommodityBoughtDTO = CommodityBoughtDTO
                .newBuilder()
                .setCommodityType(TopologyDTO.CommodityType
                        .newBuilder()
                        .setType(CommodityType.CPU_PROVISIONED_VALUE))
                .setUsed(1000)
                .build();
        final CommodityBoughtDTO memProvisionedCommodityBoughtDTO = CommodityBoughtDTO
                .newBuilder()
                .setCommodityType(TopologyDTO.CommodityType
                        .newBuilder()
                        .setType(CommodityType.MEM_PROVISIONED_VALUE))
                .setUsed(2000)
                .build();
        final CommodityBoughtDTO storageProvisionedCommodityBoughtDTO = CommodityBoughtDTO
                .newBuilder()
                .setCommodityType(TopologyDTO.CommodityType
                        .newBuilder()
                        .setType(CommodityType.STORAGE_PROVISIONED_VALUE))
                .setUsed(3000)
                .build();
        ReservationTemplate reservationTemplate = ReservationTemplate
                .newBuilder()
                .setTemplateId(TEMPLATE_ID)
                .setTemplate(TEMPLATE)
                .setCount(1)
                .addReservationInstance(ReservationInstance
                        .newBuilder()
                        .setEntityId(1L)
                        .addPlacementInfo(PlacementInfo
                                .newBuilder()
                                .setProviderId(2L)
                                .addCommodityBought(cpuProvisionedCommodityBoughtDTO)
                                .addCommodityBought(memProvisionedCommodityBoughtDTO)
                                .setClusterId(123L))
                        .addPlacementInfo(PlacementInfo.newBuilder()
                                .setProviderId(3L)
                                .addCommodityBought(storageProvisionedCommodityBoughtDTO)))
                .build();
        final Reservation reservation = reservationBuider.setReservationTemplateCollection(
                ReservationTemplateCollection.newBuilder()
                        .addReservationTemplate(reservationTemplate))
                .build();

        MultiEntityRequest req = ApiTestUtils.mockMultiSEReq(Lists.newArrayList(vmServiceEntity, pmServiceEntity, stServiceEntity));
        when(repositoryApi.entitiesRequest(any())).thenReturn(req);

        final DemandReservationApiDTO reservationApiDTO =
                reservationMapper.convertReservationToApiDTO(reservation);
        assertEquals("test-reservation", reservationApiDTO.getDisplayName());
        assertEquals("RESERVED", reservationApiDTO.getStatus());
        assertEquals(1L, reservationApiDTO.getDemandEntities().size());
        assertEquals(1L, reservationApiDTO.getDemandEntities().get(0)
                .getPlacements().getComputeResources().size());
        assertEquals("123456", reservationApiDTO.getConstraintInfos().get(0).getUuid());
        assertEquals("CLUSTER", reservationApiDTO.getConstraintInfos().get(0).getConstraintType());
        assertEquals(1L, reservationApiDTO
                .getDemandEntities()
                .get(0)
                .getPlacements()
                .getStorageResources()
                .size());
        final ResourceApiDTO computeResource = reservationApiDTO
                .getDemandEntities()
                .get(0)
                .getPlacements()
                .getComputeResources()
                .get(0);
        final ResourceApiDTO storageResource = reservationApiDTO
                .getDemandEntities()
                .get(0)
                .getPlacements()
                .getStorageResources()
                .get(0);
        assertEquals(String.valueOf(CLUSTER_ID),
                computeResource.getLinkedResources().get(0).getUuid());
        assertEquals(CLUSTER_NAME, computeResource.getLinkedResources().get(0).getDisplayName());
        assertEquals(pmServiceEntity.getDisplayName(),
                computeResource.getProvider().getDisplayName());
        assertEquals(stServiceEntity.getDisplayName(),
                storageResource.getProvider().getDisplayName());
        assertEquals(1000L, Math.round(computeResource
                .getStats()
                .stream()
                .filter(a -> a
                        .getName()
                        .equals(CommodityTypeMapping.getMixedCaseFromCommodityType(
                                CommodityType.CPU_PROVISIONED)))
                .collect(Collectors.toList())
                .get(0)
                .getValues()
                .getAvg()));
        assertEquals(2000L, Math.round(computeResource
                .getStats()
                .stream()
                .filter(a -> a
                        .getName()
                        .equals(CommodityTypeMapping.getMixedCaseFromCommodityType(
                                CommodityType.MEM_PROVISIONED)))
                .collect(Collectors.toList())
                .get(0)
                .getValues()
                .getAvg()));
        assertEquals(3000L, Math.round(storageResource
                .getStats()
                .stream()
                .filter(a -> a
                        .getName()
                        .equals(CommodityTypeMapping.getMixedCaseFromCommodityType(
                                CommodityType.STORAGE_PROVISIONED)))
                .collect(Collectors.toList())
                .get(0)
                .getValues()
                .getAvg()));
        assertEquals(
                CommodityTypeMapping.getUnitForCommodityType(CommodityType.STORAGE_PROVISIONED),
                (storageResource
                        .getStats()
                        .stream()
                        .filter(a -> a
                                .getName()
                                .equals(CommodityTypeMapping.getMixedCaseFromCommodityType(
                                        CommodityType.STORAGE_PROVISIONED)))
                        .collect(Collectors.toList())
                        .get(0)
                        .getUnits()));
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
                        .addUnplacedReason(UnplacementReason.newBuilder()
                                .setClosestSeller(2L)
                                .addFailedResources(FailedResources.newBuilder()
                                        .setCommType(TopologyDTO.CommodityType
                                        .newBuilder().setType(CommodityType.MEM_PROVISIONED_VALUE))
                                .setMaxAvailable(1000)
                                .setRequestedAmount(1200)))
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

        MultiEntityRequest req = ApiTestUtils.mockMultiSEReq(
                Lists.newArrayList(vmServiceEntity, pmServiceEntity, stServiceEntity));
        when(repositoryApi.entitiesRequest(any())).thenReturn(req);

        final DemandReservationApiDTO reservationApiDTO =
                reservationMapper.convertReservationToApiDTO(reservation);
        assertEquals("test-reservation", reservationApiDTO.getDisplayName());
        assertEquals("PLACEMENT_FAILED", reservationApiDTO.getStatus());
        assertEquals(1L, reservationApiDTO.getDemandEntities().size());
        assertEquals("123456", reservationApiDTO.getConstraintInfos().get(0).getUuid());
        assertEquals("CLUSTER", reservationApiDTO.getConstraintInfos().get(0).getConstraintType());
        assert (reservationApiDTO.getDemandEntities().get(0).getPlacements().getComputeResources()
                == null);
        assert (reservationApiDTO.getDemandEntities().get(0).getPlacements().getStorageResources()
                == null);
        assertEquals(1, reservationApiDTO
                .getDemandEntities()
                .get(0)
                .getPlacements()
                .getFailureInfos()
                .size());
        ReservationFailureInfoDTO failureInfo = reservationApiDTO
                .getDemandEntities()
                .get(0)
                .getPlacements()
                .getFailureInfos()
                .get(0);
        assertEquals(pmServiceEntity.getDisplayName(),
                failureInfo.getClosestSeller().getDisplayName());
        assertEquals(
                CommodityTypeMapping.getMixedCaseFromCommodityType(CommodityType.MEM_PROVISIONED),
                failureInfo.getResource());
        assertEquals(1000, Math.round(failureInfo.getMaxQuantityAvailable()));
        assertEquals(1200, Math.round(failureInfo.getQuantityRequested()));
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
        final ReservationNotification success =
                ReservationMapper.notificationFromReservationChanges(FAILED_IN_PROGRESS_CHANGE);
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
