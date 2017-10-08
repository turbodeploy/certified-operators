package com.vmturbo.components.test.utilities.communication;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.Nonnull;

import org.springframework.context.ApplicationContext;

import com.vmturbo.components.api.server.WebsocketNotificationSender;
import com.vmturbo.components.test.utilities.communication.ComponentStubHost.StubConfiguration;

/**
 * Abstract implementation of notification sender stub. It provides the ability to await for the
 * incoming clients, that will listen for notifications.
 *
 * @param <T> type of configuration
 */
public abstract class AbstractNotificationSenderStub<T extends StubConfiguration> implements
        NotificationSenderStub<T> {

    private WebsocketNotificationSender sender;

    @Override
    public void initialize(@Nonnull ApplicationContext context) {
        this.sender = context.getBeansOfType(WebsocketNotificationSender.class)
                .values()
                .iterator()
                .next();
    }

    @Override
    public void waitForEndpoints(int numOfEndpoints, long timeout, TimeUnit timeUnit)
            throws InterruptedException, TimeoutException {
        sender.waitForEndpoints(numOfEndpoints, timeout, timeUnit);
    }
}
