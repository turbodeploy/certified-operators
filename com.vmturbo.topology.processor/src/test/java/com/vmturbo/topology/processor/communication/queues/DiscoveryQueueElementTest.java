package com.vmturbo.topology.processor.communication.queues;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.LocalDateTime;
import java.util.concurrent.Executors;
import java.util.function.BiConsumer;
import java.util.function.Function;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.vmturbo.communication.ITransport;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryType;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationClientMessage;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationServerMessage;
import com.vmturbo.topology.processor.operation.discovery.Discovery;
import com.vmturbo.topology.processor.operation.discovery.DiscoveryBundle;
import com.vmturbo.topology.processor.targets.Target;

/**
 * Test the functionality of {@link DiscoveryQueueElement}.
 */
public class DiscoveryQueueElementTest {

    @Mock
    private Function<Runnable, DiscoveryBundle> discoveryMethodMock1;

    @Mock
    private  Function<Runnable, DiscoveryBundle> discoveryMethodMock2;

    @Mock
    private BiConsumer<Discovery, Exception> errorHandler;

    @Mock
    private ITransport<MediationServerMessage, MediationClientMessage> transport;

    private final DiscoveryBundle discoveryBundleMock1 = mock(DiscoveryBundle.class);

    private final Discovery discoveryMock1 = mock(Discovery.class);

    private final Target targetMock1 = mock(Target.class);

    private final Target targetMock2 = mock(Target.class);

    private IDiscoveryQueueElement discoveryQueueElement1;

    private IDiscoveryQueueElement discoveryQueueElement2;

    private final Runnable runnableMock = mock(Runnable.class);

    /**
     * Expected exception rule.
     */
    @Rule
    public ExpectedException expectedException = ExpectedException.none();


    /**
     * Setup the mocks and the DiscoveryQueueElements needed for tests.
     */
    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        final LocalDateTime before = LocalDateTime.now();
        discoveryQueueElement1 =
                new DiscoveryQueueElement(targetMock1, DiscoveryType.FULL, discoveryMethodMock1,
                        errorHandler, false);
        discoveryQueueElement2 =
                new DiscoveryQueueElement(targetMock2, DiscoveryType.FULL, discoveryMethodMock2,
                        errorHandler, true);
        final LocalDateTime after = LocalDateTime.now();
        assertEquals(targetMock1, discoveryQueueElement1.getTarget());
        assertEquals(targetMock2, discoveryQueueElement2.getTarget());
        assertTrue(before.compareTo(discoveryQueueElement1.getQueuedTime()) <= 0);
        assertTrue(after.compareTo(discoveryQueueElement1.getQueuedTime()) >= 0);
        assertTrue(before.compareTo(discoveryQueueElement2.getQueuedTime()) <= 0);
        assertTrue(after.compareTo(discoveryQueueElement2.getQueuedTime()) >= 0);
        assertFalse(discoveryQueueElement1.runImmediately());
        assertTrue(discoveryQueueElement2.runImmediately());
        assertEquals(1, discoveryQueueElement1.compareTo(discoveryQueueElement2));
        when(discoveryMethodMock1.apply(eq(runnableMock)))
                .thenReturn(discoveryBundleMock1);
        when(discoveryBundleMock1.getDiscovery()).thenReturn(discoveryMock1);
        when(discoveryMethodMock2.apply(eq(runnableMock)))
                .thenThrow(new RuntimeException("Test exception"));
    }

    /**
     * Test that standard path for discovery works.
     */
    @Test
    public void testPerformDiscovery() {
        final Discovery discovery = discoveryQueueElement1.performDiscovery(
                (bundle) -> bundle.getDiscovery(), runnableMock);
        assertEquals(discoveryMock1, discovery);
    }

    /**
     * Test that performDiscovery throws an exception if the discovery method passed in throws
     * one.
     */
    @Test
    public void testPerformDiscoveryException() {
        expectedException.expect(RuntimeException.class);
        expectedException.expectMessage("Test exception");
        discoveryQueueElement2.performDiscovery((bundle) -> bundle.getDiscovery(), runnableMock);
    }

    /**
     * Test that getDiscovery blocks and then returns the correct value.
     *
     * @throws InterruptedException if discoveryQueueElement1.getDiscovery() throws it.
     */
    @Test
    public void testGetDiscovery() throws InterruptedException {
        final long sleepTime = 100L;
        final long beforeWait = System.currentTimeMillis();
        Executors.newSingleThreadExecutor().submit(new Runnable() {
            public void run() {
                try {
                    Thread.sleep(sleepTime);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                discoveryQueueElement1.performDiscovery((bundle) -> bundle.getDiscovery(),
                        runnableMock);
            }
        });
        final Discovery discovery = discoveryQueueElement1.getDiscovery(sleepTime + 100L);
        final long afterWait = System.currentTimeMillis();
        // check that the discovery was empty due performDiscovery throwing an exception
        assertEquals(discoveryMock1, discovery);
        // check that getDiscovery waited until performDiscovery ran before returning
        assertTrue(afterWait - beforeWait > sleepTime - 1);
    }

    /**
     * Test that lock gets notified when performDiscovery is called even if discovery method throws
     * an exception.
     *
     * @throws InterruptedException if discoveryQueueElement2.getDiscovery() throws it.
     */
    @Test
    public void testNotifyOnException() throws InterruptedException {
        final long sleepTime = 100L;
        final long beforeWait = System.currentTimeMillis();
        Executors.newSingleThreadExecutor().submit(new Runnable() {
            public void run() {
                try {
                    Thread.sleep(sleepTime);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                discoveryQueueElement2.performDiscovery((bundle) -> bundle.getDiscovery(),
                        runnableMock);
            }
        });
        final Discovery discovery = discoveryQueueElement2.getDiscovery(sleepTime + 100L);
        final long afterWait = System.currentTimeMillis();
        // check that the discovery was empty due to performDiscovery throwing an exception
        assertNull(discovery);
        // check that getDiscovery waited until performDiscovery ran before returning
        assertTrue(afterWait - beforeWait > sleepTime - 1);
    }

    /**
     * Test that getDiscovery times out as it should when performDiscovery is not called.
     *
     * @throws InterruptedException if thread is interrupted while waiting.
     */
    @Test
    public void testGetDiscoveryTimesout() throws InterruptedException {
        final long waitTime = 100L;
        final long beforeWait = System.currentTimeMillis();
        final Discovery discovery = discoveryQueueElement2.getDiscovery(waitTime);
        final long afterWait = System.currentTimeMillis();
        // check that the discovery was empty due to performDiscovery not being called
        assertNull(discovery);
        // check that getDiscovery waited the amount of time it was supposed to
        assertTrue(afterWait - beforeWait >= waitTime);

    }
}
