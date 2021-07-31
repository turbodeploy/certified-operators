package com.vmturbo.topology.processor.operation.planexport;

import java.time.Clock;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.vmturbo.platform.sdk.common.MediationMessage.MediationClientMessage;
import com.vmturbo.platform.sdk.common.MediationMessage.PlanExportProgress;
import com.vmturbo.platform.sdk.common.MediationMessage.PlanExportResult;
import com.vmturbo.topology.processor.operation.IOperationManager.OperationCallback;
import com.vmturbo.topology.processor.operation.OperationMessageHandler;
import com.vmturbo.topology.processor.operation.action.Action;

/**
 * Handles action execution responses from probes for a {@link Action} operation.
 */
public class PlanExportMessageHandler extends OperationMessageHandler<PlanExport, PlanExportResult> {

    private final PlanExportOperationCallback callback;

    /**
     * Constructs action message handler.
     *
     * @param export operation to handle messages for
     * @param clock clock to use for time operations
     * @param timeoutMilliseconds timeout value
     * @param callback callback to execute when operation response or error arrives
     */
    public PlanExportMessageHandler(@Nonnull final PlanExport export,
                                    @Nonnull final Clock clock,
                                    final long timeoutMilliseconds,
                                    @Nonnull PlanExportOperationCallback callback) {
        super(export, clock, timeoutMilliseconds, callback);
        this.callback = Objects.requireNonNull(callback);
    }

    @Override
    @Nonnull
    public HandlerStatus onMessage(@Nonnull final MediationClientMessage receivedMessage) {
        switch (receivedMessage.getMediationClientMessageCase()) {
            case PLANEXPORTPROGRESS:
                callback.onPlanExportProgress(receivedMessage.getPlanExportProgress());
                return HandlerStatus.IN_PROGRESS;
            case PLANEXPORTRESULT:
                callback.onSuccess(receivedMessage.getPlanExportResult());
                return HandlerStatus.COMPLETE;
            default:
                return super.onMessage(receivedMessage);
        }
    }

    /**
     * Action operation callback is used to receive respobses from the action execution.
     */
    public interface PlanExportOperationCallback extends OperationCallback<PlanExportResult> {
        /**
         * Called when plan export progress is reported by a plan export operation.
         *
         * @param progress plan export progress
         */
        void onPlanExportProgress(@Nonnull PlanExportProgress progress);
    }
}
