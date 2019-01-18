package com.vmturbo.action.orchestrator.api.impl;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

import javax.annotation.Nonnull;

import org.apache.kafka.common.errors.ApiException;

import com.vmturbo.action.orchestrator.api.ActionOrchestrator;
import com.vmturbo.action.orchestrator.api.ActionsListener;
import com.vmturbo.action.orchestrator.dto.ActionMessages.ActionOrchestratorNotification;
import com.vmturbo.components.api.client.ComponentNotificationReceiver;
import com.vmturbo.components.api.client.IMessageReceiver;

/**
 * Receiver to accept notifications from Action Orhcestrator.
 */
public class ActionOrchestratorNotificationReceiver extends
        ComponentNotificationReceiver<ActionOrchestratorNotification> implements
        ActionOrchestrator {

    /**
     * Kafka topic to receive action orchestrator actions notifications.
     */
    public static final String ACTIONS_TOPIC = "action-orchestrator-actions";

    private final Set<ActionsListener> actionsListenersSet =
            Collections.newSetFromMap(new ConcurrentHashMap<>());

    public ActionOrchestratorNotificationReceiver(
            @Nonnull final IMessageReceiver<ActionOrchestratorNotification> messageReceiver,
            @Nonnull final ExecutorService executorService) {
        super(messageReceiver, executorService);
    }

    @Override
    protected void processMessage(@Nonnull final ActionOrchestratorNotification message)
            throws ApiException {
        switch (message.getTypeCase()) {
            case ACTION_PLAN: // DEPRECATED
                doWithListeners(
                        listener -> listener.onActionsReceived(message.getActionPlan()),
                        message.getTypeCase());
                break;
            case ACTIONS_UPDATED:
                doWithListeners(
                    listener -> listener.onActionsUpdated(message.getActionsUpdated()),
                    message.getTypeCase());
                break;
            case ACTION_PROGRESS:
                doWithListeners(
                    listener -> listener.onActionProgress(message.getActionProgress()),
                    message.getTypeCase());
                break;
            case ACTION_SUCCESS:
                doWithListeners(
                    listener -> listener.onActionSuccess(message.getActionSuccess()),
                    message.getTypeCase());
                break;
            case ACTION_FAILURE:
                doWithListeners(
                    listener -> listener.onActionFailure(message.getActionFailure()),
                    message.getTypeCase());
                break;
            default:
                throw new ApiException("Message type unrecognized: " + message);
        }
    }

    private void doWithListeners(@Nonnull final Consumer<ActionsListener> command,
                                 @Nonnull final ActionOrchestratorNotification.TypeCase messageCase) {
        for (final ActionsListener listener : actionsListenersSet) {
            getExecutorService().submit(() -> {
                try {
                    command.accept(listener);
                } catch (RuntimeException e) {
                    getLogger().error(
                            "Error executing command (" + messageCase + ") for listener " +
                                    listener, e);
                }
            });
        }
    }

    @Override
    public void addActionsListener(@Nonnull final ActionsListener listener) {
        actionsListenersSet.add(Objects.requireNonNull(listener));
    }
}
