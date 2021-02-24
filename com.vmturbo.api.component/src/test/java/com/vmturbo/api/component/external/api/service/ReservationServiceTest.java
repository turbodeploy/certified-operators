package com.vmturbo.api.component.external.api.service;

import static com.vmturbo.api.enums.ReservationAction.RESERVATION;
import static org.junit.Assert.assertEquals;

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
import com.vmturbo.api.dto.BaseApiDTO;
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
import com.vmturbo.common.protobuf.plan.ReservationDTO.Reservation;
import com.vmturbo.common.protobuf.plan.ReservationDTOMoles.ReservationServiceMole;
import com.vmturbo.common.protobuf.plan.ReservationServiceGrpc;
import com.vmturbo.common.protobuf.plan.TemplateDTOMoles.TemplateServiceMole;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.common.ClassicEnumMapper.CommodityTypeUnits;

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
     * test getReservationAwareStats. 2 reservations. one is reserved and one is placement_failed.
     * Only the reserved reservation contribute to the entity stats.
     * @throws Exception
     */
    @Test
    public void testGetReservationAwareStats()  throws Exception {

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
        final long hostId = 123456L;
        final ApiId apiIdH = Mockito.mock(ApiId.class);
        List<StatSnapshotApiDTO> entityStatSnapshotH = createEntityStatSnapshot(2000f,
                CommodityTypeUnits.MEM_PROVISIONED.getMixedCase());
        Mockito.when(apiIdH.isGroup()).thenReturn(false);
        Mockito.when(apiIdH.oid()).thenReturn(hostId);
        Mockito.when(mockUuidMapper.fromUuid(String.valueOf(hostId))).thenReturn(apiIdH);
        Mockito.when(statsService.getStatsByEntityQuery(
                String.valueOf(hostId),statPeriodApiInputDTO)).thenReturn(entityStatSnapshotH);

        // Storage Stats to be returned from stats service
        final long storageId = 234567L;
        final ApiId apiIdS = Mockito.mock(ApiId.class);
        Mockito.when(mockUuidMapper.fromUuid(Mockito.any())).thenReturn(apiIdH);
        List<StatSnapshotApiDTO> entityStatSnapshotS = createEntityStatSnapshot(3000f,
                CommodityTypeUnits.STORAGE_PROVISIONED.getMixedCase());
        Mockito.when(apiIdS.isGroup()).thenReturn(false);
        Mockito.when(apiIdS.oid()).thenReturn(storageId);
        Mockito.when(mockUuidMapper.fromUuid(String.valueOf(storageId))).thenReturn(apiIdS);
        Mockito.when(statsService.getStatsByEntityQuery(
                String.valueOf(storageId),statPeriodApiInputDTO)).thenReturn(entityStatSnapshotS);


        // reservationApiDTO1 corresponds to reservation1 which is successful and has both
        // ComputeResources and StorageResources set.
        final DemandReservationApiDTO reservationApiDTO1 = new DemandReservationApiDTO();
        final List<DemandEntityInfoDTO> demandEntityInfoDTOS1 = new ArrayList<>();
        DemandEntityInfoDTO demandEntityInfoDTO1 = new DemandEntityInfoDTO();
        PlacementInfoDTO placementInfoApiDTO1 = new PlacementInfoDTO();
        final ResourceApiDTO resourceApiDTOC1 = createResourceApiDTO(200f,
                CommodityTypeUnits.MEM_PROVISIONED.getMixedCase(), "123456");
        placementInfoApiDTO1.setComputeResources(Arrays.asList(resourceApiDTOC1));
        final ResourceApiDTO resourceApiDTOS1 = createResourceApiDTO(300f,
                CommodityTypeUnits.STORAGE_PROVISIONED.getMixedCase(), "234567");
        placementInfoApiDTO1.setStorageResources(Arrays.asList(resourceApiDTOS1));
        demandEntityInfoDTO1.setPlacements(placementInfoApiDTO1);
        demandEntityInfoDTOS1.add(demandEntityInfoDTO1);
        reservationApiDTO1.setDemandEntities(demandEntityInfoDTOS1);
        final Reservation reservation1 = Reservation.newBuilder().setName("reservation1").build();
        Mockito.when(reservationMapper.convertReservationToApiDTO(reservation1))
                .thenReturn(reservationApiDTO1);

        // reservationApiDTO2 corresponds to reservation2 which is failed and has both
        // ComputeResources and StorageResources not set.
        final DemandReservationApiDTO reservationApiDTO2 = new DemandReservationApiDTO();
        final List<DemandEntityInfoDTO> demandEntityInfoDTOS2 = new ArrayList<>();
        DemandEntityInfoDTO demandEntityInfoDTO2 = new DemandEntityInfoDTO();
        PlacementInfoDTO placementInfoApiDTO2 = new PlacementInfoDTO();
        demandEntityInfoDTO2.setPlacements(placementInfoApiDTO2);
        demandEntityInfoDTOS2.add(demandEntityInfoDTO2);
        reservationApiDTO2.setDemandEntities(demandEntityInfoDTOS2);
        final Reservation reservation2 = Reservation.newBuilder().setName("reservation2").build();
        Mockito.when(reservationMapper.convertReservationToApiDTO(reservation2))
                .thenReturn(reservationApiDTO2);

        List<Reservation> reservationIterator = new ArrayList<>();
        reservationIterator.add(reservation1);
        reservationIterator.add(reservation2);
        Mockito.when(reservationServiceMole
                .getAllReservations(Mockito.any())).thenReturn(reservationIterator);

        // host has the mem provisioned values added up.
        List<StatApiDTO> resultH = reservationsService.getReservationAwareStats(String.valueOf(hostId));
        Assert.assertEquals(2200f, resultH.get(0).getValues().getAvg(), 0.001);

        // storage has the storage provisioned values added up.
        List<StatApiDTO> resultS = reservationsService.getReservationAwareStats(String.valueOf(storageId));
        Assert.assertEquals(3300f, resultS.get(0).getValues().getAvg(), 0.001);

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
        Mockito.when(reservationMapper.convertToReservation(Mockito.any())).thenReturn(reservation);
        Mockito.when(reservationServiceMole.createReservation(Mockito.any()))
                .thenReturn(reservation);
        Mockito.when(reservationMapper.convertReservationToApiDTO(Mockito.any()))
                .thenReturn(demandReservationApiDTO);
        final DemandReservationApiDTO result =
                reservationsService.createReservationForDemand(false, RESERVATION,
                        demandApiInputDTO);
        Mockito.verify(reservationServiceMole, Mockito.times(1))
                .createReservation(Mockito.any(), Mockito.any());
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
        reservationsService.createReservationForDemand(false, RESERVATION, demandApiInputDTO);
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
