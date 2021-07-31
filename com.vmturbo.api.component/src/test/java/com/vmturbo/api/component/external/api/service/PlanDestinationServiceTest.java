package com.vmturbo.api.component.external.api.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.annotation.Nonnull;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.vmturbo.api.component.external.api.mapper.MarketMapper;
import com.vmturbo.api.component.external.api.mapper.PlanDestinationMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.component.external.api.util.BusinessAccountRetriever;
import com.vmturbo.api.dto.businessunit.BusinessUnitApiDTO;
import com.vmturbo.api.dto.market.MarketApiDTO;
import com.vmturbo.api.dto.plandestination.PlanDestinationApiDTO;
import com.vmturbo.api.exceptions.InvalidOperationException;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.common.protobuf.plan.PlanDTO.OptionalPlanInstance;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanId;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance.PlanStatus;
import com.vmturbo.common.protobuf.plan.PlanDTOMoles.PlanServiceMole;
import com.vmturbo.common.protobuf.plan.PlanExportDTO.GetPlanDestinationResponse;
import com.vmturbo.common.protobuf.plan.PlanExportDTO.GetPlanDestinationsRequest;
import com.vmturbo.common.protobuf.plan.PlanExportDTO.PlanDestination;
import com.vmturbo.common.protobuf.plan.PlanExportDTO.PlanDestinationCriteria;
import com.vmturbo.common.protobuf.plan.PlanExportDTO.PlanDestinationID;
import com.vmturbo.common.protobuf.plan.PlanExportDTO.PlanExportRequest;
import com.vmturbo.common.protobuf.plan.PlanExportDTO.PlanExportResponse;
import com.vmturbo.common.protobuf.plan.PlanExportDTO.PlanExportStatus;
import com.vmturbo.common.protobuf.plan.PlanExportDTO.PlanExportStatus.PlanExportState;
import com.vmturbo.common.protobuf.plan.PlanExportDTOMoles.PlanExportServiceMole;
import com.vmturbo.common.protobuf.plan.PlanExportServiceGrpc;
import com.vmturbo.common.protobuf.plan.PlanServiceGrpc;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.common.utils.Strings;

/**
 * Unit tests for PlanDestinationService.
 */
public class PlanDestinationServiceTest {
    private BusinessAccountRetriever businessAccountRetriever = mock(BusinessAccountRetriever.class);
    private TestExportService planExportRpcService = spy(TestExportService.class);
    private PlanServiceMole planRpcService = spy(TestPlanService.class);
    private MarketMapper marketMapper = mock(MarketMapper.class);
    private UuidMapper uuidMapper = mock(UuidMapper.class);
    private PlanDestinationService planDestinationService;

    private static final long validPlanDestinationId = 4815162342L;
    private static final String destinationName = "AzureMigrateProject";
    private static final long validMarketId = 777;
    private static final long invalidMarketId = 888L;
    private static final long businessUnitId = 999L;
    private static final int percent = 42;

    /**
     * The grpc server.
     */
    @Rule
    public GrpcTestServer grpcTestServer = GrpcTestServer.newServer(planExportRpcService,
            planRpcService);

    /**
     * Unit test set up.
     *
     * @throws Exception if set up meets exceptions.
     */
    @Before
    public void init() throws Exception {
        planDestinationService = new PlanDestinationService(
                PlanExportServiceGrpc.newBlockingStub(grpcTestServer.getChannel()),
                uuidMapper,
                new PlanDestinationMapper(businessAccountRetriever, marketMapper,
                        PlanServiceGrpc.newBlockingStub(grpcTestServer.getChannel())));

        ApiId invalidApiId = mock(ApiId.class);
        when(invalidApiId.isPlan()).thenReturn(false);
        when(uuidMapper.fromUuid(Strings.toString(invalidMarketId))).thenReturn(invalidApiId);

        ApiId apiId = mock(ApiId.class);
        when(apiId.isPlan()).thenReturn(true);
        when(apiId.oid()).thenReturn(validMarketId);
        when(uuidMapper.fromUuid(Strings.toString(validMarketId))).thenReturn(apiId);

        MarketApiDTO marketApiDTO = new MarketApiDTO();
        marketApiDTO.setUuid(Strings.toString(validMarketId));
        when(marketMapper.dtoFromPlanInstance(anyObject())).thenReturn(marketApiDTO);
    }

    /**
     * Test getPlanDestinations with null or a given business unit id as parameter.
     *
     * @throws Exception if exception occurs.
     */
    @Test
    public void testGetPlanDestinations() throws Exception {
        List<PlanDestination> allDestinations = new ArrayList();
        long bu1Id = 444444;
        long destination1Id = 111110;
        PlanDestination destination1 = PlanDestination.newBuilder().setOid(destination1Id)
                .setStatus(PlanExportStatus.newBuilder().setProgress(100)
                        .setState(PlanExportState.SUCCEEDED))
                .setCriteria(PlanDestinationCriteria.newBuilder().setAccountId(bu1Id)).build();
        allDestinations.add(destination1);
        long destination2Id = 111112;
        long bu2Id = 444445;
        int percent = 50;
        PlanDestination destination2 = PlanDestination.newBuilder().setOid(destination2Id)
                .setStatus(PlanExportStatus.newBuilder().setProgress(percent)
                        .setState(PlanExportState.IN_PROGRESS))
                .setCriteria(PlanDestinationCriteria.newBuilder().setAccountId(bu2Id)).build();
        allDestinations.add(destination2);
        when(planExportRpcService.getPlanDestinations(
                GetPlanDestinationsRequest.newBuilder().build())).thenReturn(allDestinations);
        String bu1IdStr = String.valueOf(bu1Id);
        BusinessUnitApiDTO bu1Dto = new BusinessUnitApiDTO();
        bu1Dto.setUuid(bu1IdStr);
        when(businessAccountRetriever.getBusinessAccount(bu1IdStr)).thenReturn(bu1Dto);
        String bu2IdStr = String.valueOf(bu2Id);
        BusinessUnitApiDTO bu2Dto = new BusinessUnitApiDTO();
        bu2Dto.setUuid(bu2IdStr);
        when(businessAccountRetriever.getBusinessAccount(bu2IdStr)).thenReturn(bu2Dto);
        List<PlanDestinationApiDTO> allPlanDestinationList = planDestinationService
                .getPlanDestinations(null);
        assertEquals(2, allPlanDestinationList.size());
        assertNotEquals(allPlanDestinationList.get(0).getBusinessUnit().getUuid(),
                allPlanDestinationList.get(1).getBusinessUnit().getUuid());

        when(planExportRpcService.getPlanDestinations(GetPlanDestinationsRequest.newBuilder()
                .setAccountId(bu2Id).build())).thenReturn(Arrays.asList(destination2));
        List<PlanDestinationApiDTO> planDestination2 = planDestinationService
                .getPlanDestinations(String.valueOf(bu2Id));
        assertEquals(1, planDestination2.size());
        assertEquals(bu2Id, Long.parseLong(planDestination2.get(0).getBusinessUnit().getUuid()));
    }

    /**
     * Test getPlanDestinationById by providing a valid plan destination id.
     *
     * @throws Exception if exception occurs.
     */
    @Test
    public void testGetPlanDestinationById() throws Exception {
        String uuid = "1234567";
        String name = "Test0";
        long oid = Long.parseLong(uuid);
        long buId = 111111;
        int percent = 50;
        long mktId = 777777;
        PlanDestination destination = PlanDestination.newBuilder().setOid(oid)
                .setDisplayName(name).setStatus(PlanExportStatus.newBuilder().setState(
                        PlanExportState.IN_PROGRESS).setProgress(percent)
                        .setDescription(PlanExportState.IN_PROGRESS.toString())).setCriteria(
                        PlanDestinationCriteria.newBuilder().setAccountId(buId))
                .setMarketId(mktId).build();
        String buIdStr = String.valueOf(buId);
        BusinessUnitApiDTO buDto = new BusinessUnitApiDTO();
        buDto.setUuid(buIdStr);
        when(planExportRpcService.getPlanDestination(PlanDestinationID.newBuilder().setId(oid)
                .build())).thenReturn(GetPlanDestinationResponse.newBuilder()
                .setDestination(destination).build());
        when(businessAccountRetriever.getBusinessAccount(buIdStr)).thenReturn(buDto);
        PlanInstance plan = PlanInstance.newBuilder().setPlanId(mktId).setStatus(
                PlanStatus.SUCCEEDED).build();
        when(planRpcService.getPlan(PlanId.newBuilder()
                .setPlanId(mktId).build())).thenReturn(OptionalPlanInstance.newBuilder()
                .setPlanInstance(plan).build());
        MarketApiDTO mktDto = new MarketApiDTO();
        mktDto.setUuid(String.valueOf(mktId));
        when(marketMapper.dtoFromPlanInstance(plan)).thenReturn(mktDto);

        PlanDestinationApiDTO dto = planDestinationService.getPlanDestinationById(uuid);
        assertEquals(uuid, dto.getUuid());
        assertEquals(name, dto.getDisplayName());
        assertEquals(destination.getClass().getSimpleName(), dto.getClassName());
        assertEquals(PlanExportState.IN_PROGRESS.name(), dto.getExportState().name());
        assertEquals(percent, dto.getExportProgressPercentage().intValue());
        assertEquals(PlanExportState.IN_PROGRESS.toString(), dto.getExportDescription());
        assertEquals(buIdStr, dto.getBusinessUnit().getUuid());
        assertEquals(String.valueOf(mktId), dto.getMarket().getUuid());
    }

    /**
     * Test getPlanDestinations with invalid business unit id and expect an exception to occur.
     *
     * @throws Exception if exception occurs.
     */
    @Test(expected = InvalidOperationException.class)
    public void testGetPlanDestinationsInvalidBuId() throws Exception {
        String invalid = "npe111";
        planDestinationService.getPlanDestinations(invalid);
    }

    /**
     * Test getPlanDestinations with no object match with the plan destination id.
     *
     * @throws Exception if exception occurs.
     */
    @Test(expected = UnknownObjectException.class)
    public void testGetPlanDestinationsNoObject() throws Exception {
        String valid = "1111";
        when(planExportRpcService.getPlanDestination(
                PlanDestinationID.newBuilder().setId(Long.parseLong(valid)).build()))
                .thenReturn(GetPlanDestinationResponse.newBuilder().build());
        planDestinationService.getPlanDestinationById(valid);
    }

    /**
     * Test getPlanDestinationById with invalid plan destination id and expect an exception to occur.
     *
     * @throws Exception if exception occurs.
     */
    @Test(expected = InvalidOperationException.class)
    public void testGetPlanDestinationsInvalidPlanDestinationId() throws Exception {
        String invalid = "33@";
        planDestinationService.getPlanDestinationById(invalid);
    }

    /**
     * Test uploadToPlanDestination with unknown plan market id and expect an exception to occur.
     *
     * @throws Exception if exception occurs.
     */
    @Test(expected = UnknownObjectException.class)
    public void testUploadUnknownMarket() throws Exception {
        planDestinationService.uploadToPlanDestination(Strings.toString(invalidMarketId), "456");
    }

    /**
     * Test uploadToPlanDestination with invalid destination id and expect an exception to occur.
     *
     * @throws Exception if exception occurs.
     */
    @Test(expected = InvalidOperationException.class)
    public void testUploadInvalidDestination() throws Exception {
        planDestinationService.uploadToPlanDestination(Strings.toString(validMarketId),
            "Not a valid numeric oid");
    }

    /**
     * Test uploadToPlanDestination with unknown destination id and expect an exception to occur.
     *
     * @throws Exception if exception occurs.
     */
    @Test(expected = UnknownObjectException.class)
    public void testUploadUnknownDestination() throws Exception {
        long destinationId = 444L;

        planDestinationService.uploadToPlanDestination(Strings.toString(validMarketId),
            Strings.toString(destinationId));
    }


    /**
     * Test uploadToPlanDestination that results in an in-progress upload.
     *
     * @throws Exception if exception occurs.
     */
    @Test
    public void testUploadDestinationStarted() throws Exception {
        String buIdStr = Strings.toString(businessUnitId);
        BusinessUnitApiDTO buDto = new BusinessUnitApiDTO();
        buDto.setUuid(buIdStr);
        when(businessAccountRetriever.getBusinessAccount(buIdStr)).thenReturn(buDto);

        PlanDestinationApiDTO result = planDestinationService.uploadToPlanDestination(
            Strings.toString(validMarketId),
            Strings.toString(validPlanDestinationId));

        assertEquals(Strings.toString(validPlanDestinationId), result.getUuid());
        assertEquals(destinationName, result.getDisplayName());
        assertEquals(PlanDestination.class.getSimpleName(), result.getClassName());
        assertEquals(PlanExportState.IN_PROGRESS.name(), result.getExportState().name());
        assertEquals(percent, result.getExportProgressPercentage().intValue());
        assertEquals(PlanExportState.IN_PROGRESS.toString(), result.getExportDescription());
        assertEquals(buIdStr, result.getBusinessUnit().getUuid());
        assertEquals(String.valueOf(validMarketId), result.getMarket().getUuid());
    }

    /**
     * Test implementation of the export service that simulates one valid export destination.
     */
    public static class TestExportService extends PlanExportServiceMole {
        @Override
        public PlanExportResponse startPlanExport(@Nonnull PlanExportRequest input) {
            if (input.getDestinationId() == validPlanDestinationId) {
                return PlanExportResponse.newBuilder()
                    .setDestination(
                        PlanDestination.newBuilder().setOid(validPlanDestinationId)
                            .setDisplayName(destinationName)
                            .setStatus(PlanExportStatus.newBuilder()
                                .setState(PlanExportState.IN_PROGRESS)
                                .setProgress(percent)
                                .setDescription(PlanExportState.IN_PROGRESS.toString()))
                            .setCriteria(
                                PlanDestinationCriteria.newBuilder().setAccountId(businessUnitId))
                            .setMarketId(validMarketId).build()).build();
            } else {
                return PlanExportResponse.getDefaultInstance();
            }
        }
    }

    /**
     * Mock implementation of the plan service that can retreieve plan markets.
     */
    public static class TestPlanService extends PlanServiceMole {
        @Override
        public OptionalPlanInstance getPlan(PlanId request) {
            return OptionalPlanInstance.newBuilder()
                .setPlanInstance(PlanInstance.newBuilder()
                    .setPlanId(request.getPlanId())
                    .setStatus(PlanStatus.SUCCEEDED)
                    .build()).build();
        }
    }
}
