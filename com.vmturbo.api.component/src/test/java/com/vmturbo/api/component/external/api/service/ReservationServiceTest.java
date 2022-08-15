package com.vmturbo.api.component.external.api.service;

import static com.vmturbo.api.enums.ReservationAction.RESERVATION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Matchers.eq;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.google.common.collect.ImmutableList;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.api.component.external.api.mapper.ReservationMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.conversion.entity.CommodityTypeMapping;
import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.reservation.DemandEntityInfoDTO;
import com.vmturbo.api.dto.reservation.DemandReservationApiDTO;
import com.vmturbo.api.dto.reservation.DemandReservationApiInputDTO;
import com.vmturbo.api.dto.reservation.DemandReservationParametersDTO;
import com.vmturbo.api.dto.reservation.PlacementInfoDTO;
import com.vmturbo.api.dto.reservation.PlacementParametersDTO;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.statistic.StatApiInputDTO;
import com.vmturbo.api.dto.statistic.StatPeriodApiInputDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.dto.statistic.StatValueApiDTO;
import com.vmturbo.api.dto.template.ResourceApiDTO;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.common.protobuf.action.ActionDTOMoles.ActionsServiceMole;
import com.vmturbo.common.protobuf.group.GroupDTOMoles.GroupServiceMole;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.plan.PlanDTOMoles.PlanServiceMole;
import com.vmturbo.common.protobuf.plan.ReservationDTO.DeleteReservationByIdRequest;
import com.vmturbo.common.protobuf.plan.ReservationDTO.GetReservationByIdRequest;
import com.vmturbo.common.protobuf.plan.ReservationDTO.Reservation;
import com.vmturbo.common.protobuf.plan.ReservationDTOMoles.ReservationServiceMole;
import com.vmturbo.common.protobuf.plan.ReservationServiceGrpc;
import com.vmturbo.common.protobuf.plan.ReservationServiceGrpc.ReservationServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.TemplateDTOMoles.TemplateServiceMole;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTO;

public class ReservationServiceTest {

    private static final int MAXIMUM_PLACEMENT_COUNT = 100;

    private ReservationServiceMole reservationServiceMole =
            Mockito.spy(new ReservationServiceMole());

    private ReservationMapper reservationMapper;
    private StatsService statsService;

    private PlanServiceMole planServiceMole = Mockito.spy(new PlanServiceMole());

    private ActionsServiceMole actionsServiceMole = Mockito.spy(new ActionsServiceMole());

    private TemplateServiceMole templateServiceMole = Mockito.spy(new TemplateServiceMole());

    private GroupServiceMole groupServiceMole = Mockito.spy(new GroupServiceMole());

    private UuidMapper mockUuidMapper = Mockito.mock(UuidMapper.class);

    private ReservationsService reservationsService;

    private final long hostId = 123456L;
    private final long storageId = 234567L;

    // reservationApiDTO1 corresponds to reservation1 which is successful and has both
    // ComputeResources and StorageResources set.
    private DemandReservationApiDTO reservationApiDTO1;

    // reservationApiDTO2 corresponds to reservation2 which is failed and has both
    // ComputeResources and StorageResources not set.
    private DemandReservationApiDTO reservationApiDTO2;


    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(reservationServiceMole,
            planServiceMole, actionsServiceMole, templateServiceMole, groupServiceMole);

    @Before
    public void setup() {
        reservationMapper = Mockito.mock(ReservationMapper.class);
        statsService = Mockito.mock(StatsService.class);
        reservationsService = new ReservationsService(
                ReservationServiceGrpc.newBlockingStub(grpcServer.getChannel()), reservationMapper,
                MAXIMUM_PLACEMENT_COUNT, statsService,
                GroupServiceGrpc.newBlockingStub(grpcServer.getChannel()), mockUuidMapper);
    }

    /**
     * Set up for the GetReservationAwareStats tests.
     */
    private void setupForGetReservationAwareStats() throws Exception {

        // reservation specific StatPeriodApiInputDTO for mock.
        StatPeriodApiInputDTO statPeriodApiInputDTO = new StatPeriodApiInputDTO();
        statPeriodApiInputDTO.setStatistics(new ArrayList<>());
        // trim the stats to just the resources related to reservation.
        for (String commName : ReservationsService.RESERVATION_RELATED_COMMODITIES) {
            StatApiInputDTO statApiInputDTO = new StatApiInputDTO();
            statApiInputDTO.setName(commName);
            statPeriodApiInputDTO.getStatistics().add(statApiInputDTO);
        }

        // Host Stats to be return from stats service.
        final ApiId apiIdH = Mockito.mock(ApiId.class);
        List<StatSnapshotApiDTO> entityStatSnapshotH = createEntityStatSnapshot(2000f,
                CommodityTypeMapping.getMixedCaseFromCommodityType(CommonDTO.CommodityDTO.CommodityType.MEM_PROVISIONED));
        Mockito.when(apiIdH.isGroup()).thenReturn(false);
        Mockito.when(apiIdH.oid()).thenReturn(hostId);
        Mockito.when(mockUuidMapper.fromUuid(String.valueOf(hostId))).thenReturn(apiIdH);
        Mockito.when(statsService.getStatsByEntityQuery(
                String.valueOf(hostId),statPeriodApiInputDTO)).thenReturn(entityStatSnapshotH);

        // Storage Stats to be returned from stats service
        final ApiId apiIdS = Mockito.mock(ApiId.class);
        Mockito.when(mockUuidMapper.fromUuid(any())).thenReturn(apiIdH);
        List<StatSnapshotApiDTO> entityStatSnapshotS = createEntityStatSnapshot(3000f,
                CommodityTypeMapping.getMixedCaseFromCommodityType(CommonDTO.CommodityDTO.CommodityType.STORAGE_PROVISIONED));
        Mockito.when(apiIdS.isGroup()).thenReturn(false);
        Mockito.when(apiIdS.oid()).thenReturn(storageId);
        Mockito.when(mockUuidMapper.fromUuid(String.valueOf(storageId))).thenReturn(apiIdS);
        Mockito.when(statsService.getStatsByEntityQuery(
                String.valueOf(storageId),statPeriodApiInputDTO)).thenReturn(entityStatSnapshotS);


        reservationApiDTO1 = new DemandReservationApiDTO();
        final List<DemandEntityInfoDTO> demandEntityInfoDTOS1 = new ArrayList<>();
        DemandEntityInfoDTO demandEntityInfoDTO1 = new DemandEntityInfoDTO();
        PlacementInfoDTO placementInfoApiDTO1 = new PlacementInfoDTO();
        final ResourceApiDTO resourceApiDTOC1 = createResourceApiDTO(200f,
                CommodityTypeMapping.getMixedCaseFromCommodityType(CommonDTO.CommodityDTO.CommodityType.MEM_PROVISIONED), "123456");
        placementInfoApiDTO1.setComputeResources(Arrays.asList(resourceApiDTOC1));
        final ResourceApiDTO resourceApiDTOS1 = createResourceApiDTO(300f,
                CommodityTypeMapping.getMixedCaseFromCommodityType(CommonDTO.CommodityDTO.CommodityType.STORAGE_PROVISIONED), "234567");
        placementInfoApiDTO1.setStorageResources(Arrays.asList(resourceApiDTOS1));
        demandEntityInfoDTO1.setPlacements(placementInfoApiDTO1);
        demandEntityInfoDTOS1.add(demandEntityInfoDTO1);
        reservationApiDTO1.setDemandEntities(demandEntityInfoDTOS1);
        final Reservation reservation1 = Reservation.newBuilder().setName("reservation1").build();
        Mockito.when(reservationMapper.convertReservationToApiDTO(eq(reservation1),any(),any(),any()))
                .thenReturn(reservationApiDTO1);

        reservationApiDTO2 = new DemandReservationApiDTO();
        final List<DemandEntityInfoDTO> demandEntityInfoDTOS2 = new ArrayList<>();
        DemandEntityInfoDTO demandEntityInfoDTO2 = new DemandEntityInfoDTO();
        PlacementInfoDTO placementInfoApiDTO2 = new PlacementInfoDTO();
        demandEntityInfoDTO2.setPlacements(placementInfoApiDTO2);
        demandEntityInfoDTOS2.add(demandEntityInfoDTO2);
        reservationApiDTO2.setDemandEntities(demandEntityInfoDTOS2);
        final Reservation reservation2 = Reservation.newBuilder().setName("reservation2").build();
        Mockito.when(reservationMapper.convertReservationToApiDTO(eq(reservation2),any(),any(),any()))
                .thenReturn(reservationApiDTO2);


        Mockito.when(reservationMapper.generateReservationList(any()))
                .thenReturn(Arrays.asList(reservationApiDTO1,reservationApiDTO2));

    }

    /**
     * test getReservationAwareStats. 2 reservations. one is reserved and one is placement_failed.
     * Only the reserved reservation contribute to the entity stats.
     * @throws Exception
     */
    @Test
    public void testGetReservationAwareStats()  throws Exception {
        setupForGetReservationAwareStats();
        // host has the mem provisioned values added up.
        List<StatApiDTO> resultH = reservationsService.getReservationAwareStats(String.valueOf(hostId));
        Assert.assertEquals(2200f, resultH.get(0).getValues().getAvg(), 0.001);

        // storage has the storage provisioned values added up.
        List<StatApiDTO> resultS = reservationsService.getReservationAwareStats(String.valueOf(storageId));
        Assert.assertEquals(3300f, resultS.get(0).getValues().getAvg(), 0.001);

    }

    /**
     * test getReservationAwareStats. 2 reservations. one is reserved and one is placement_failed.
     * The reserved one will not be added to stats because the deployedCount is > 0.
     * @throws Exception
     */
    @Test
    public void testGetReservationAwareStatsWithDeployedField()  throws Exception {
        setupForGetReservationAwareStats();
        reservationApiDTO1.setReservationDeployed(true);

        // host mem_provisioned will not change because deployed count is != 0.
        List<StatApiDTO> resultH = reservationsService.getReservationAwareStats(String.valueOf(hostId));
        Assert.assertEquals(2000f, resultH.get(0).getValues().getAvg(), 0.001);

        // storage storage_provisioned will not change because deployed count is != 0..
        List<StatApiDTO> resultS = reservationsService.getReservationAwareStats(String.valueOf(storageId));
        Assert.assertEquals(3000f, resultS.get(0).getValues().getAvg(), 0.001);

    }

    /**
     * Create a ResourceApiDTO with the used value as used, name as commodityName and provider as providerId.
     * @param used the used value of commodity
     * @param commodityName the name of the commodity
     * @param providerId the provider oid.
     * @return ResourceApiDTO for the corresponding input.
     */
    private ResourceApiDTO createResourceApiDTO(float used, String commodityName, String providerId) {
        ResourceApiDTO resourceApiDTO = new ResourceApiDTO();
        final BaseApiDTO providerBaseApiDTO = new BaseApiDTO();
        providerBaseApiDTO.setUuid(providerId);
        resourceApiDTO.setProvider(providerBaseApiDTO);
        List<StatApiDTO> statApiDTOS = new ArrayList<>();
        final StatApiDTO statApiDTO = new StatApiDTO();
        statApiDTO.setName(commodityName);
        final StatValueApiDTO statValueApiDTO = new StatValueApiDTO();
        statValueApiDTO.setAvg(used);
        statApiDTO.setValues(statValueApiDTO);
        statApiDTOS.add(statApiDTO);
        resourceApiDTO.setStats(statApiDTOS);
        return resourceApiDTO;
    }

    /**
     * create singleton list of StatSnapshotApiDTO with the used value and commodity name.
     * @param used the used value of the commodity.
     * @param commodityName the name of the commodity.
     * @return a singleton StatSnapshotApiDTO with the used value and name set.
     */
    private List<StatSnapshotApiDTO> createEntityStatSnapshot(float used, String commodityName) {
        List<StatSnapshotApiDTO> entityStatSnapshot = new ArrayList<>();
        StatSnapshotApiDTO statSnapshotApiDTO = new StatSnapshotApiDTO();
        StatApiDTO statApiDTO = new StatApiDTO();
        statApiDTO.setName(commodityName);
        StatValueApiDTO statValueApiDTO = new StatValueApiDTO();
        statValueApiDTO.setAvg(used);
        statApiDTO.setCapacity(statValueApiDTO);
        statApiDTO.setValues(statValueApiDTO);
        List<StatApiDTO> statApiDTOs = new ArrayList<>();
        statApiDTOs.add(statApiDTO);
        statSnapshotApiDTO.setStatistics(statApiDTOs);
        entityStatSnapshot.add(statSnapshotApiDTO);
        return entityStatSnapshot;
    }

    @Test
    public void testCreateReservationForReservation() throws Exception {
        final DemandReservationApiInputDTO demandApiInputDTO = new DemandReservationApiInputDTO();
        final Reservation reservation = Reservation.newBuilder().setName("Test-reservation").build();
        final DemandReservationApiDTO demandReservationApiDTO = new DemandReservationApiDTO();
        demandReservationApiDTO.setCount(2);
        Mockito.when(reservationMapper.convertToReservation(any())).thenReturn(reservation);
        Mockito.when(reservationServiceMole.createReservation(any()))
                .thenReturn(reservation);
        Mockito.when(reservationMapper.generateReservationApiDto(any()))
                .thenReturn(demandReservationApiDTO);
        final DemandReservationApiDTO result =
                reservationsService.createReservationForDemand(RESERVATION,
                        demandApiInputDTO);
        Mockito.verify(reservationServiceMole, Mockito.times(1))
                .createReservation(any(), any());
        assertEquals(2L, (int)result.getCount());
    }

    /**
     * Verify when reservation placement count is larger than maximum allowed placement count, {@link
     * OperationFailedException} exception will be thrown.
     *
     * @throws Exception when failing to create reservation.
     */
    @Test(expected = OperationFailedException.class)
    public void testCreateReservationValidation() throws Exception {
        final DemandReservationApiInputDTO demandApiInputDTO =
                getDemandReservationApiInputDTO(MAXIMUM_PLACEMENT_COUNT + 1);
        reservationsService.createReservationForDemand(RESERVATION, demandApiInputDTO);
    }

    /**
     * Test deletion of non deployed reservation with forced deletion
     */
    @Test
    public void testDeleteReservationWithForceDelete() throws Exception {
        final DeleteReservationByIdRequest request = DeleteReservationByIdRequest.newBuilder()
                .setReservationId(123)
                .setDeployed(false)
                .build();

        boolean deletedResult = reservationsService.deleteReservationByID(String.valueOf(request.getReservationId()),request.getDeployed(),true);
        assertTrue(deletedResult);
    }

    /**
     * Test deletion of deployed reservation with forced deletion
     */
    @Test
    public void testDeleteReservationWithForceDeleteEnabledAndIsDeployed() throws Exception {
        final DeleteReservationByIdRequest request = DeleteReservationByIdRequest.newBuilder()
                .setReservationId(123)
                .setDeployed(true)
                .build();
        final GetReservationByIdRequest getRequest = GetReservationByIdRequest.newBuilder()
                .setReservationId(Long.valueOf(request.getReservationId()))
                .setApiCallBlock(false)
                .build();

        boolean deletedResult = reservationsService.deleteReservationByID(String.valueOf(request.getReservationId()),
                request.getDeployed(),true);
        assertTrue(deletedResult);
    }

    /**
     * Test deletion of non-deployed reservation with non-forced deletion
     */
    @Test
    public void testDeleteReservationThatIsNotDeployed() throws Exception {
        final DeleteReservationByIdRequest request = DeleteReservationByIdRequest.newBuilder()
                .setReservationId(123)
                .setDeployed(false)
                .build();
        boolean deletedResult = reservationsService.deleteReservationByID(String.valueOf(request.getReservationId()),
                request.getDeployed(),false);
        assertTrue(deletedResult);
    }

    /**
     * Test deletion of deployed reservation with non-forced deletion
     */
    @Test
    public void testDeleteReservationWithIsDeployed() throws Exception {
        final DeleteReservationByIdRequest request = DeleteReservationByIdRequest.newBuilder()
                .setReservationId(123)
                .setDeployed(false)
                .build();

        final GetReservationByIdRequest getRequest = GetReservationByIdRequest.newBuilder()
                .setReservationId(Long.valueOf(request.getReservationId()))
                .setApiCallBlock(false)
                .build();

        final Reservation returnedReservation = Reservation.newBuilder()
                .setDeployed(true)
                .build();

        Mockito.when(reservationServiceMole
                .getReservationById(getRequest)).thenReturn(returnedReservation);

        try{
            reservationsService.deleteReservationByID(String.valueOf(request.getReservationId()),
                    request.getDeployed(),false);
        } catch (OperationFailedException e){
            assertEquals("Delete failed reservation already deleted",e.getMessage());
        }

    }

    private DemandReservationApiInputDTO getDemandReservationApiInputDTO(final int count) {
        final DemandReservationApiInputDTO demandApiInputDTO = new DemandReservationApiInputDTO();
        final DemandReservationParametersDTO reservationParametersDTO =
                new DemandReservationParametersDTO();
        final PlacementParametersDTO placementParameters = new PlacementParametersDTO();
        placementParameters.setCount(count);
        reservationParametersDTO.setPlacementParameters(placementParameters);
        demandApiInputDTO.setParameters(ImmutableList.of(reservationParametersDTO));
        return demandApiInputDTO;
    }
}
