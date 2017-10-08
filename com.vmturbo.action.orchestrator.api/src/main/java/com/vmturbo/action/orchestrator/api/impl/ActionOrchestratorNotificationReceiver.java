package com.vmturbo.action.orchestrator.api.impl;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

import javax.annotation.Nonnull;

import com.vmturbo.action.orchestrator.api.ActionsListener;
import com.vmturbo.action.orchestrator.dto.ActionMessages.ActionOrchestratorNotification;
import com.vmturbo.components.api.client.ComponentNotificationReceiver;
import com.vmturbo.components.api.client.IMessageReceiver;

/**
 * The websocket client connecting to the Action Orchestrator.
 */
class ActionOrchestratorNotificationReceiver
        extends ComponentNotificationReceiver<ActionOrchestratorNotification> {

    private final Set<ActionsListener> actionsListenersSet =
            Collections.newSetFromMap(new ConcurrentHashMap<>());

    ActionOrchestratorNotificationReceiver(
            @Nonnull final IMessageReceiver<ActionOrchestratorNotification> messageReceiver,
            @Nonnull final ExecutorService executorService) {
        super(messageReceiver, executorService);
    }

    @Override
    protected void processMessage(@Nonnull final ActionOrchestratorNotification message)
            throws ActionOrchestratorException {
        switch (message.getTypeCase()) {
            case ACTION_PLAN:
                doWithListeners(
                    listener -> listener.onActionsReceived(message.getActionPlan()),
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
                throw new ActionOrchestratorException("Message type unrecognized: " + message);
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

    void addActionsListener(@Nonnull final ActionsListener listener) {
        actionsListenersSet.add(Objects.requireNonNull(listener));
    }
}
