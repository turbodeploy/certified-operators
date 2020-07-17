package com.vmturbo.topology.processor.actions;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;

import javax.annotation.Nonnull;

import io.opentracing.SpanContext;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.action.orchestrator.dto.ActionMessages.ActionEvent;
import com.vmturbo.components.api.client.IMessageReceiver;
import com.vmturbo.components.common.RequiresDataInitialization;
import com.vmturbo.topology.processor.actions.data.context.ActionExecutionContextFactory;
import com.vmturbo.topology.processor.operation.IOperationManager;

/**
 * Action audit service is expected to accumulate simple action state transitions from Kafka topic
 * and send them periodically in batches to SDK probe.
 */
public class ActionAuditService implements RequiresDataInitialization {

    private final Logger logger = LogManager.getLogger(getClass());
    // TODO remove entries on target removal
    // TODO This spread of target-related messages will break kafka topics consumption
    // we have to put every target (ServiceNow, ActionScript) to a separate partition.
    private final Map<Long, TargetActionAuditService> targetSenders = new HashMap<>();
    private final ActionExecutionContextFactory contextFactory;
    private final IOperationManager operationManager;
    private final int batchSize;

    private final ScheduledExecutorService threadPool;
    private final long sendIntervalSec;
    private final int initializationPriority;
    private final AtomicBoolean initialized = new AtomicBoolean(false);

    /**
     * Constructs service.
     *
     * @param eventsReceiver message receiver to accept action audit events
     * @param operationManager operation manager to execute operations with SDK probe
     * @param contextFactory action context factory used to convert actions from XL to SDK
     *         format
     * @param scheduler scheduled thread pool for periodical execution of sending operations
     * @param batchSize maximum batch size of events to send to SDK probe
     * @param sendIntervalSec interval to send the events to SDK probe
     * @param initializationPriority initialization priority. Action events auditing will be
     *         only started after {@link #initialize()} is called
     * @see RequiresDataInitialization#priority()
     */
    protected ActionAuditService(@Nonnull IMessageReceiver<ActionEvent> eventsReceiver,
            @Nonnull IOperationManager operationManager,
            @Nonnull ActionExecutionContextFactory contextFactory,
            @Nonnull ScheduledExecutorService scheduler, long sendIntervalSec, int batchSize,
            int initializationPriority) {
        this.operationManager = Objects.requireNonNull(operationManager);
        this.contextFactory = Objects.requireNonNull(contextFactory);
        this.batchSize = validateValue(batchSize, value -> value > 0,
                "batchSize must be a positive value, found: ");
        this.sendIntervalSec = validateValue(sendIntervalSec, value -> value > 0,
                "sendIntervalSec must be a positive value, found: ");
        this.threadPool = scheduler;
        this.initializationPriority = initializationPriority;
        eventsReceiver.addListener(this::onNewEvent);
        logger.info("Constructed action audit service with batchSize {} and sendInterval {}sec",
                batchSize, sendIntervalSec);
    }

    @Nonnull
    private static <T> T validateValue(@Nonnull T value, @Nonnull Predicate<T> assumption,
            @Nonnull String errorMessage) {
        if (assumption.test(value)) {
            return value;
        } else {
            throw new IllegalArgumentException(errorMessage + value);
        }
    }

    private void onNewEvent(@Nonnull ActionEvent event, @Nonnull Runnable commit,
                            @Nonnull final SpanContext tracingContext) {
        if (!event.getActionRequest().hasTargetId()) {
            logger.error("Action event {} does not have target associated",
                    event.getActionRequest().getActionId());
        }
        final long targetId = event.getActionRequest().getTargetId();
        final TargetActionAuditService targetSvc = targetSenders.computeIfAbsent(targetId,
                key -> new TargetActionAuditService(targetId, operationManager, contextFactory,
                        threadPool, sendIntervalSec, batchSize, initialized));
        targetSvc.onNewEvent(event, commit);
    }

    @Override
    public void initialize() {
        initialized.set(true);
    }

    @Override
    public int priority() {
        return initializationPriority;
    }
}
