package com.vmturbo.components.test.utilities.communication;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.Nonnull;

import org.springframework.context.ApplicationContext;

import com.vmturbo.components.api.server.ComponentNotificationSender;
import com.vmturbo.components.test.utilities.communication.ComponentStubHost.StubConfiguration;

/**
 * A stub to allow sending notifications of a particular {@link ComponentNotificationSender}.
 * The purpose is for the testing framework to be able to imitate components that the component
 * under test depends upon.
 *
 * @param <ConfigClass> The class of the @Configuration-annotated class that creates the @Bean
 *                     definition for the component's particular
 *                     {@link ComponentNotificationSender}.
 */
public interface NotificationSenderStub<ConfigClass extends StubConfiguration> {

    /**
     * Initialize the notification sender with the application context that the {@link ConfigClass}
     * configuration got created in. The main purpose is to allow the stub to get a reference to
     * a bean in the context.
     *
     * @param context The context that the {@link ConfigClass} configuration got created in.
     */
    void initialize(@Nonnull final ApplicationContext context);

    /**
     * Get the configuration this stub defined.
     *
     * @return The class of the @Configuration-annotated class.
     */
    Class<ConfigClass> getConfiguration();

    /**
     * Waits for the clients to arrive and to begin consuming notifications from this stub.
     *
     * @param numOfEndpoints number of clients to await to arrive
     * @param timeout time to wait
     * @param timeUnit time unit to treat {@code time}
     * @throws InterruptedException if thread has been interrupted
     * @throws TimeoutException if endpoints did not arrive after the specified time passed.
     */
    void waitForEndpoints(int numOfEndpoints, long timeout, @Nonnull TimeUnit timeUnit)
            throws InterruptedException, TimeoutException;
}
