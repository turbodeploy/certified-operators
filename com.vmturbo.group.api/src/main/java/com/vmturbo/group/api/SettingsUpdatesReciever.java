package com.vmturbo.group.api;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.components.api.client.ApiClientException;
import com.vmturbo.components.api.client.ComponentNotificationReceiver;
import com.vmturbo.components.api.client.IMessageReceiver;
import com.vmturbo.group.api.SettingMessages.SettingNotification;

/**
 * Class which receives the broadcast for a settings change.
 */
public class SettingsUpdatesReciever extends ComponentNotificationReceiver<SettingNotification> {

    private final Logger logger = LogManager.getLogger(getClass());

    private final Set<SettingsListener> settingsListenersSet =
            Collections.newSetFromMap(new ConcurrentHashMap<>());

    /**
     * The topic this receiver listens to.
     */
    public static final String SETTINGS_UPDATES_TOPIC = "settings-updates";

    public SettingsUpdatesReciever(@Nonnull final IMessageReceiver<SettingNotification> messageReceiver,
                                 @Nonnull final ExecutorService executorService) {
        super(messageReceiver, executorService);
    }

    /**
     * Processes the broadcasted message and forwards it to the correct listener.
     * @param message The received message.
     * @throws ApiClientException ApiClientException.
     * @throws InterruptedException InterruptedException.
     */
    @Override
    protected void processMessage(@Nonnull final SettingNotification message)
            throws ApiClientException, InterruptedException {
        logger.debug("Settings have been updated. BroadCast a Settings Updated Notification");
        doWithListeners(listener -> listener.onSettingsUpdated(message), message);
    }

    /**
     * Calls the setting listeners in different threads.
     * @param command command.
     * @param notification notification.
     */
    private void doWithListeners(@Nonnull final Consumer<SettingsListener> command,
                                 @Nonnull final SettingNotification notification) {
        for (final SettingsListener listener : settingsListenersSet) {
            getExecutorService().submit(() -> {
                try {
                    command.accept(listener);
                } catch (RuntimeException e) {
                    getLogger().error(
                            "Error while executing for listener " + listener, e);
                }
            });
        }
    }

    /**
     * Add listeners to listen to this broadcast message(SETTINGS_UPDATES_TOPIC).
     * @param listener The listener which listens to this broadcast.
     */
    public void addSettingsListener(@Nonnull final SettingsListener listener) {
        settingsListenersSet.add(listener);
    }
}
