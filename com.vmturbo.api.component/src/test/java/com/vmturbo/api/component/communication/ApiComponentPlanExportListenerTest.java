package com.vmturbo.api.component.communication;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.vmturbo.api.PlanDestinationNotificationDTO.PlanDestinationNotification;
import com.vmturbo.api.PlanDestinationNotificationDTO.PlanDestinationStatusNotification;
import com.vmturbo.api.component.external.api.mapper.MarketMapper;
import com.vmturbo.api.component.external.api.mapper.PlanDestinationMapper;
import com.vmturbo.api.component.external.api.util.BusinessAccountRetriever;
import com.vmturbo.api.component.external.api.websocket.UINotificationChannel;
import com.vmturbo.api.dto.businessunit.BusinessUnitApiDTO;
import com.vmturbo.api.exceptions.ConversionException;
import com.vmturbo.common.protobuf.plan.PlanDTOMoles.PlanServiceMole;
import com.vmturbo.common.protobuf.plan.PlanExportDTO.PlanDestination;
import com.vmturbo.common.protobuf.plan.PlanExportDTO.PlanDestinationCriteria;
import com.vmturbo.common.protobuf.plan.PlanExportDTO.PlanExportStatus;
import com.vmturbo.common.protobuf.plan.PlanExportDTO.PlanExportStatus.PlanExportState;
import com.vmturbo.common.protobuf.plan.PlanServiceGrpc;
import com.vmturbo.common.protobuf.plan.PlanServiceGrpc.PlanServiceBlockingStub;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.common.utils.Strings;

/**
 * Test that received progress and status messages are converted and sent to a UI channel as
 * plan destination notifications.
 */
public class ApiComponentPlanExportListenerTest {
    private PlanServiceMole planServiceMole = Mockito.spy(new PlanServiceMole());

    /**
     * Test server for gRPC dependencies of {@link PlanServiceGrpc}.
     */
    @Rule
    public GrpcTestServer grpcTestServer = GrpcTestServer.newServer(planServiceMole);

    private final UINotificationChannel uiNotificationChannel = mock(UINotificationChannel.class);
    private PlanServiceBlockingStub planServiceMock;
    private PlanDestinationMapper mapper;
    private ApiComponentPlanExportListener exportListener;
    private BusinessAccountRetriever buRetriever;

    private final long accountId = 98765L;
    private final String accountName = "My Account";

    @Captor
    private ArgumentCaptor<PlanDestinationNotification> notificationCaptor;

    /**
     * Initialize for tests.
     *
     * @throws ConversionException should not happen
     * @throws InterruptedException should not happen
     */
    @Before
    public final void init() throws ConversionException, InterruptedException {
        MockitoAnnotations.initMocks(this);

        planServiceMock = PlanServiceGrpc.newBlockingStub(grpcTestServer.getChannel());
        buRetriever = mock(BusinessAccountRetriever.class);

        BusinessUnitApiDTO businessUnit = new BusinessUnitApiDTO();
        businessUnit.setDisplayName(accountName);
        when(buRetriever.getBusinessAccounts(any())).thenReturn(Collections.singleton(businessUnit));

        mapper = new PlanDestinationMapper(buRetriever, mock(MarketMapper.class), planServiceMock);
        exportListener = new ApiComponentPlanExportListener(uiNotificationChannel, mapper);
    }

    /**
     * Test that plan destination progress updates are sent as progress update notifications.
     */
    @Test
    public void testOnPlanDestinationProgress() {
        PlanExportStatus status = PlanExportStatus.newBuilder()
            .setState(PlanExportState.IN_PROGRESS)
            .setDescription("Whatever")
            .setProgress(42)
            .build();

        PlanDestination destination = PlanDestination.newBuilder()
            .setOid(4815162342L)
            .setDisplayName("destinationName")
            .setCriteria(PlanDestinationCriteria.newBuilder()
                .setAccountId(12345L))
            .setStatus(status)
            .setHasExportedData(true)
            .setMarketId(accountId)
            .build();

        exportListener.onPlanDestinationProgress(destination);

        verify(uiNotificationChannel).broadcastPlanDestinationNotification(notificationCaptor.capture());
        final PlanDestinationNotification notification = notificationCaptor.getValue();

        assertEquals(Strings.toString(destination.getOid()), notification.getPlanDestinationId());
        assertEquals(destination.getDisplayName(), notification.getPlanDestinationName());
        assertEquals(Strings.toString(destination.getCriteria().getAccountId()),
            notification.getPlanDestinationAccountId());
        assertEquals(destination.getHasExportedData(), notification.getPlanDestinationHasExportedData());
        assertEquals(accountName, notification.getPlanDestinationAccountName());
        assertEquals(Strings.toString(destination.getMarketId()), notification.getPlanDestinationMarketId());

        assertTrue(notification.hasPlanDestinationProgressNotification());
        PlanDestinationStatusNotification progress = notification.getPlanDestinationProgressNotification();
        assertEquals(status.getState().toString(), progress.getStatus().toString());
        assertEquals(status.getProgress(), progress.getProgressPercentage());
        assertEquals(status.getDescription(), progress.getDescription());
    }

    /**
     * Test that plan destination state updates are sent as status update notifications.
     */
    @Test
    public void testOnPlanDestinationStatus() {
        PlanExportStatus status = PlanExportStatus.newBuilder()
            .setState(PlanExportState.SUCCEEDED)
            .setDescription("Finished")
            .setProgress(100)
            .build();

        PlanDestination destination = PlanDestination.newBuilder()
            .setOid(4815162342L)
            .setDisplayName("destinationName")
            .setCriteria(PlanDestinationCriteria.newBuilder()
                .setAccountId(accountId))
            .setStatus(status)
            .setHasExportedData(false)
            .setMarketId(12345L)
            .build();

        exportListener.onPlanDestinationStateChanged(destination);

        verify(uiNotificationChannel).broadcastPlanDestinationNotification(notificationCaptor.capture());
        final PlanDestinationNotification notification = notificationCaptor.getValue();

        assertEquals(Strings.toString(destination.getOid()), notification.getPlanDestinationId());
        assertEquals(destination.getDisplayName(), notification.getPlanDestinationName());
        assertEquals(Strings.toString(destination.getCriteria().getAccountId()),
            notification.getPlanDestinationAccountId());
        assertEquals(destination.getHasExportedData(), notification.getPlanDestinationHasExportedData());
        assertEquals(accountName, notification.getPlanDestinationAccountName());
        assertEquals(Strings.toString(destination.getMarketId()), notification.getPlanDestinationMarketId());

        assertTrue(notification.hasPlanDestinationStatusNotification());
        PlanDestinationStatusNotification progress = notification.getPlanDestinationStatusNotification();
        assertEquals(status.getState().toString(), progress.getStatus().toString());
        assertEquals(status.getProgress(), progress.getProgressPercentage());
        assertEquals(status.getDescription(), progress.getDescription());
    }
}