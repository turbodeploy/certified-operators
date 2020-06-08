package com.vmturbo.topology.processor.operation;

import java.time.Clock;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Objects;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.platform.sdk.common.MediationMessage.MediationClientMessage;
import com.vmturbo.topology.processor.communication.BaseMessageHandler;
import com.vmturbo.topology.processor.operation.IOperationManager.OperationCallback;

/**
 * Message handler for operations.
 *
 * @param <O> type of operation to handle
 * @param <R> type of a result this operation returns.
 */
public abstract class OperationMessageHandler<O extends Operation, R> extends
        BaseMessageHandler implements IOperationMessageHandler<O> {

    protected final Logger logger = LogManager.getLogger(getClass());

    private final OperationCallback<R> callback;
    protected final O operation;

    /**
     * Constructs operation message handler.
     *
     * @param operation operation to handle messages for
     * @param clock clock to use for time operations
     * @param timeoutMilliseconds timeout value
     * @param callback callback to receive operation ressult
     */
    public OperationMessageHandler(@Nonnull final O operation,
            @Nonnull final Clock clock,
            final long timeoutMilliseconds,
            @Nonnull OperationCallback<R> callback) {
        super(clock, timeoutMilliseconds);
        this.operation = Objects.requireNonNull(operation);
        this.callback = Objects.requireNonNull(callback);
    }


    @Nonnull
    protected HandlerStatus onMessage(@Nonnull final MediationClientMessage receivedMessage) {
        switch (receivedMessage.getMediationClientMessageCase()) {
            case KEEPALIVE:
                return HandlerStatus.IN_PROGRESS;
            case TARGETOPERATIONERROR:
                notifyOperationError(receivedMessage.getTargetOperationError());
                return HandlerStatus.COMPLETE;
            default:
                logger.error("Unable to handle message of type " + receivedMessage.getMediationClientMessageCase());
                return HandlerStatus.IN_PROGRESS;
        }
    }

    @Override
    public void onExpiration() {
        logger.warn("Expiring message handler for operation {} after {} seconds of inactivity",
                operation, secondSinceStart());
        final String error = new StringBuilder()
                .append(operation.getClass().getSimpleName())
                .append(" ")
                .append(operation.getId())
                .append(" timed out after ")
                .append(secondSinceStart())
                .append(" seconds.")
                .toString();
        notifyOperationError(error);
    }

    private void notifyOperationError(@Nonnull String error) {
        callback.onFailure(error);
    }

    @Override
    public void onTransportClose() {
        notifyOperationError("Communication transport to remote probe closed.");
    }

    /**
     * Reports that target has been removed. Method is used to create an operation's specific
     * error message for the target removal.
     *
     * @param targetId target OID that has been removed
     */
    public void onTargetRemoved(long targetId) {
        notifyOperationError("Target " + targetId + " removed.");
    }

    /**
     * Get the {@link Operation} associated with this message.
     *
     * @return The {@link Operation} associated with this message.
     */
    @Override
    @Nonnull
    public O getOperation() {
        return operation;
    }

    private long secondSinceStart() {
        return ChronoUnit.SECONDS.between(operation.getStartTime(), LocalDateTime.now());
    }

    @Nonnull
    protected Logger getLogger() {
        return logger;
    }

    @Nonnull
    protected OperationCallback<R> getCallback() {
        return callback;
    }

    @Override
    public String toString() {
        return "MessageHandler[" + operation + ']';
    }
}
