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

/**
 * Message handler for operations.
 */
public abstract class OperationMessageHandler<O extends Operation> extends
        BaseMessageHandler implements IOperationMessageHandler<O> {

    protected final Logger logger = LogManager.getLogger(getClass());

    protected final O operation;

    protected final OperationManager manager;

    public OperationMessageHandler(@Nonnull final OperationManager manager,
            @Nonnull final O operation,
            @Nonnull final Clock clock,
            final long timeoutMilliseconds) {
        super(clock, timeoutMilliseconds);
        this.operation = Objects.requireNonNull(operation);
        this.manager = Objects.requireNonNull(manager);
    }


    @Nonnull
    protected HandlerStatus onMessage(@Nonnull final MediationClientMessage receivedMessage) {
        switch (receivedMessage.getMediationClientMessageCase()) {
            case KEEPALIVE:
                return HandlerStatus.IN_PROGRESS;
            default:
                logger.error("Unable to handle message of type " + receivedMessage.getMediationClientMessageCase());
                return HandlerStatus.IN_PROGRESS;
        }
    }

    @Override
    public void onExpiration() {
        logger.warn("Expiring message handler for operation {} after {} seconds of inactivity",
                operation, secondSinceStart());
        manager.notifyTimeout(operation, secondSinceStart());
    }

    @Override
    public void onTransportClose() {
        manager.notifyOperationCancelled(operation, "Communication transport to remote probe closed.");
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

    protected Logger getLogger() {
        return logger;
    }
}
