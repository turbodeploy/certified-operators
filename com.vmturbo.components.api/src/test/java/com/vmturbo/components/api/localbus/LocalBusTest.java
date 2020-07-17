package com.vmturbo.components.api.localbus;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import com.google.protobuf.Empty;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

import com.vmturbo.components.api.client.IMessageReceiver;
import com.vmturbo.components.api.server.IMessageSender;

/**
 * Unit tests for the {@link LocalBus}.
 */
public class LocalBusTest {

    private static final Logger logger = LogManager.getLogger();

    /**
     * Test the bus sending/receiving.
     *
     * @throws Exception If there is an error.
     */
    @Test
    public void testLocalBus() throws Exception {
        ExecutorService executorService = Executors.newCachedThreadPool();
        try {
            final LocalBus bus = new LocalBus(executorService);

            final IMessageReceiver<Empty> fooReceiver = bus.messageReceiver("foo", msg -> null);

            final IMessageReceiver<Empty> barReceiver = bus.messageReceiver("bar", msg -> null);

            final IMessageSender<Empty> fooSender = bus.messageSender("foo");

            final IMessageSender<Empty> barSender = bus.messageSender("bar");

            Semaphore fooSemaphore = new Semaphore(0);
            Semaphore barSemaphore = new Semaphore(0);
            fooReceiver.addListener((msg, commitCmd, tracingContext) -> {
                fooSemaphore.release();
            });
            // Add a failing listener too.
            fooReceiver.addListener((msg, commitCmd, tracingContext) -> {
                throw new RuntimeException("BOO!");
            });
            barReceiver.addListener((msg, commitCmd, tracingContext) -> {
                barSemaphore.release();
            });

            fooSender.sendMessage(Empty.getDefaultInstance());
            assertTrue(fooSemaphore.tryAcquire(10, TimeUnit.SECONDS));
            assertThat(fooSemaphore.availablePermits(), is(0));
            assertThat(barSemaphore.availablePermits(), is(0));

            barSender.sendMessage(Empty.getDefaultInstance());
            barSender.sendMessage(Empty.getDefaultInstance());
            assertTrue(barSemaphore.tryAcquire(10, TimeUnit.SECONDS));
            assertTrue(barSemaphore.tryAcquire(10, TimeUnit.SECONDS));
            assertThat(barSemaphore.availablePermits(), is(0));
            assertThat(fooSemaphore.availablePermits(), is(0));
        } finally {
            executorService.shutdownNow();
        }
    }

}