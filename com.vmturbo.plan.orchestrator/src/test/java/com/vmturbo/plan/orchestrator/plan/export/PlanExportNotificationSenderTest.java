package com.vmturbo.plan.orchestrator.plan.export;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import com.vmturbo.common.protobuf.plan.PlanExportDTO.PlanDestination;
import com.vmturbo.common.protobuf.plan.PlanExportDTO.PlanExportNotification;
import com.vmturbo.common.protobuf.plan.PlanExportDTO.PlanExportNotification.DestinationUpdate;
import com.vmturbo.common.protobuf.plan.PlanExportDTO.PlanExportStatus;
import com.vmturbo.common.protobuf.plan.PlanExportDTO.PlanExportStatus.PlanExportState;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.components.api.server.IMessageSender;
import com.vmturbo.plan.orchestrator.plan.export.PlanExportListener.PlanExportListenerException;

/**
 * Test that PlanExportNotificationSender sends the right kind of notifications for state
 * and progress updates.
 */
public class PlanExportNotificationSenderTest {
    private TestMessageSender messageSender = new TestMessageSender();
    private PlanExportListener notificationSender = new PlanExportNotificationSender(messageSender);

    /**
     * Check that state changes are sent correctly.
     *
     * @throws PlanExportListenerException idicates the test failed.
     */
    @Test
    public void testOnPlanDestinationStateChanged() throws PlanExportListenerException {
        notificationSender.onPlanDestinationStateChanged(PlanDestination.newBuilder()
            .setStatus(PlanExportStatus.newBuilder()
                .setState(PlanExportState.SUCCEEDED)
                .setProgress(100)
                .setDescription(PlanExportState.SUCCEEDED.toString())
                .build())
            .build());

        PlanExportNotification sent = messageSender.getLastSent();
        assertTrue(sent.hasDestinationStateUpdate());
        DestinationUpdate update = sent.getDestinationStateUpdate();
        assertTrue(update.hasUpdatedDestination());
        PlanDestination destination = update.getUpdatedDestination();
        assertTrue(destination.hasStatus());
        PlanExportStatus status = destination.getStatus();
        assertEquals(PlanExportState.SUCCEEDED, status.getState());
        assertEquals(100, status.getProgress());
        assertEquals(PlanExportState.SUCCEEDED.toString(), status.getDescription());
    }

    /**
     * Check that progress changes are sent correctly.
     *
     * @throws PlanExportListenerException idicates the test failed.
     */
    @Test
    public void testOnPlanDestinationProgress() throws PlanExportListenerException {
        notificationSender.onPlanDestinationProgress(PlanDestination.newBuilder()
            .setStatus(PlanExportStatus.newBuilder()
                .setState(PlanExportState.IN_PROGRESS)
                .setProgress(42)
                .setDescription(PlanExportState.IN_PROGRESS.toString())
                .build())
            .build());

        PlanExportNotification sent = messageSender.getLastSent();
        assertTrue(sent.hasDestinationProgressUpdate());
        DestinationUpdate update = sent.getDestinationProgressUpdate();
        assertTrue(update.hasUpdatedDestination());
        PlanDestination destination = update.getUpdatedDestination();
        assertTrue(destination.hasStatus());
        PlanExportStatus status = destination.getStatus();
        assertEquals(PlanExportState.IN_PROGRESS, status.getState());
        assertEquals(42, status.getProgress());
        assertEquals(PlanExportState.IN_PROGRESS.toString(), status.getDescription());
    }

    /**
     * A simple mock message sender that records what was sent.
     */
    private class TestMessageSender implements IMessageSender<PlanExportNotification> {
        private PlanExportNotification lastSent = null;

        @Override
        public void sendMessage(@NotNull final PlanExportNotification serverMsg) throws CommunicationException, InterruptedException {
            lastSent = serverMsg;
        }

        @Override
        public int getMaxRequestSizeBytes() {
            return 0;
        }

        @Override
        public int getRecommendedRequestSizeBytes() {
            return 0;
        }

        private PlanExportNotification getLastSent() {
            return lastSent;
        }
    }
}
