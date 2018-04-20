package com.vmturbo.sample.component.notifications;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.vmturbo.components.api.test.SenderReceiverPair;
import com.vmturbo.sample.api.SampleNotifications.SampleNotification;

/**
 * This is the configuration for the spring context for {@link SampleNotificationsTest}.
 *
 * We want to test that websocket notifications flow from the sample component to clients.
 * Therefore we need to stand up a minimal websocket "server" with the
 * {@link SampleComponentNotificationSender} and any supporting beans.
 *
 * See {@link SampleNotificationsTest} for how we use this configuration.
 */
@Configuration
public class SampleNotificationsTestConfig {

    @Bean
    public SenderReceiverPair<SampleNotification> senderReceiverPair() {
        return new SenderReceiverPair<>();
    }

    /**
     * This is the "backend" that the echo component uses to send notifications to
     * listeners.
     */
    @Bean
    public SampleComponentNotificationSender echoNotificationsBackend() {
        return new SampleComponentNotificationSender(senderReceiverPair());
    }

    /**
     * This is the "backend" that the echo component uses to send notifications to
     * listeners.
     */
    @Bean
    public SampleComponentNotificationSender sampleComponentNotificationSender() {
        return new SampleComponentNotificationSender(senderReceiverPair());
    }

}
