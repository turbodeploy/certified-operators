package com.vmturbo.topology.processor.communication.queues;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.LocalDateTime;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.vmturbo.platform.common.dto.Discovery.DiscoveryType;
import com.vmturbo.topology.processor.targets.Target;

/**
 * Test the functionality of DiscoveryQueue.
 */
public class DiscoveryQueueTest {

    private final long targetId1 = 1111L;

    private final long targetId2 = 2222L;

    private final long targetId3 = 3333L;

    private final Target targetMock1 = mock(Target.class);

    private final Target targetMock2 = mock(Target.class);

    private final Target targetMock3 = mock(Target.class);

    private final long probeId1 = 1111L;

    private final long probeId2 = 2222L;

    private final IDiscoveryQueueElement discoveryQueueElement1 =
            mock(IDiscoveryQueueElement.class);

    private IDiscoveryQueueElement discoveryQueueElement2 =
            mock(IDiscoveryQueueElement.class);

    private IDiscoveryQueueElement discoveryQueueElement3 =
            mock(IDiscoveryQueueElement.class);

    private IDiscoveryQueueElement discoveryQueueElement4 =
            mock(IDiscoveryQueueElement.class);

    private IDiscoveryQueue discoveryQueue = new DiscoveryQueue(probeId1, DiscoveryType.FULL);

    /**
     * Expected exception rule.
     */
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    /**
     * Setup the preconditions for the tests.  4 different queue elements: the first two match the
     * probe type and discovery type for the discoveryQueue, but the third has the wrong probe type.
     * The fourth queue element is identical to the second except for queue time.
     */
    @Before
    public void setup() {
        LocalDateTime firstDate = LocalDateTime.now();
        LocalDateTime secondDate = firstDate.plusMinutes(10L);
        LocalDateTime thirdDate = firstDate.plusMinutes(20L);
        when(targetMock1.getProbeId()).thenReturn(probeId1);
        when(targetMock2.getProbeId()).thenReturn(probeId1);
        when(targetMock3.getProbeId()).thenReturn(probeId2);
        when(targetMock1.getId()).thenReturn(targetId1);
        when(targetMock2.getId()).thenReturn(targetId2);
        when(targetMock3.getId()).thenReturn(targetId3);
        when(discoveryQueueElement1.getQueuedTime()).thenReturn(firstDate);
        when(discoveryQueueElement2.getQueuedTime()).thenReturn(secondDate);
        when(discoveryQueueElement3.getQueuedTime()).thenReturn(secondDate);
        when(discoveryQueueElement4.getQueuedTime()).thenReturn(thirdDate);
        when(discoveryQueueElement1.runImmediately()).thenReturn(false);
        when(discoveryQueueElement2.runImmediately()).thenReturn(false);
        when(discoveryQueueElement3.runImmediately()).thenReturn(false);
        when(discoveryQueueElement4.runImmediately()).thenReturn(false);
        when(discoveryQueueElement1.getTarget()).thenReturn(targetMock1);
        when(discoveryQueueElement2.getTarget()).thenReturn(targetMock2);
        when(discoveryQueueElement3.getTarget()).thenReturn(targetMock3);
        when(discoveryQueueElement4.getTarget()).thenReturn(targetMock2);
        when(discoveryQueueElement1.getDiscoveryType()).thenReturn(DiscoveryType.FULL);
        when(discoveryQueueElement2.getDiscoveryType()).thenReturn(DiscoveryType.FULL);
        when(discoveryQueueElement3.getDiscoveryType()).thenReturn(DiscoveryType.FULL);
        when(discoveryQueueElement4.getDiscoveryType()).thenReturn(DiscoveryType.FULL);
    }

    /**
     * Test that add, peek, size, isEmpty, and remove work as expected.
     *
     * @throws DiscoveryQueueException if discoveryQueue.add throws it.
     */
    @Test
    public void testBasicFunctionality() throws DiscoveryQueueException {
        assertTrue(discoveryQueue.isEmpty());
        assertEquals(0, discoveryQueue.size());
        assertEquals(discoveryQueueElement1, discoveryQueue.add(discoveryQueueElement1));
        assertEquals(discoveryQueueElement1, discoveryQueue.peek().get());
        assertFalse(discoveryQueue.isEmpty());
        assertEquals(1, discoveryQueue.size());
        assertEquals(discoveryQueueElement2, discoveryQueue.add(discoveryQueueElement2));
        assertFalse(discoveryQueue.isEmpty());
        assertEquals(2, discoveryQueue.size());
        assertEquals(discoveryQueueElement1, discoveryQueue.peek().get());
        assertEquals(discoveryQueueElement1, discoveryQueue.remove().get());
        assertEquals(discoveryQueueElement2, discoveryQueue.peek().get());
        assertEquals(discoveryQueueElement2, discoveryQueue.remove().get());
        assertFalse(discoveryQueue.peek().isPresent());
        assertFalse(discoveryQueue.remove().isPresent());
    }

    /**
     * Test that when we add an element that doesn't have the right probe ID, and exception is
     * thrown.
     *
     * @throws DiscoveryQueueException when an incompatible element is added to the queue.
     */
    @Test
    public void testIllegalAddThrowsException() throws DiscoveryQueueException {
        expectedException.expect(DiscoveryQueueException.class);
        expectedException.expectMessage("Expected probe ID");
        discoveryQueue.add(discoveryQueueElement3);
    }

    /**
     * Test that sort orders the elements based on the timestamp in the DiscoveryQueueElement.
     *
     * @throws DiscoveryQueueException when we attempt to add a queue element that doesn't belong
     * in this queue.
     */
    @Test
    public void testSort() throws DiscoveryQueueException {
        when(discoveryQueueElement1.compareTo(eq(discoveryQueueElement2))).thenReturn(-1);
        when(discoveryQueueElement2.compareTo(eq(discoveryQueueElement1))).thenReturn(1);
        discoveryQueue.add(discoveryQueueElement2);
        discoveryQueue.add(discoveryQueueElement1);
        assertEquals(discoveryQueueElement2, discoveryQueue.peek().get());
        discoveryQueue.sort();
        assertEquals(discoveryQueueElement1, discoveryQueue.peek().get());
    }

    /**
     * Test that when an element is added with runImmediately set, it moves to the front of the
     * queue.
     *
     * @throws DiscoveryQueueException when we attempt to add a queue element that doesn't belong
     * in this queue.
     */
    @Test
    public void testAddRunImmediately() throws DiscoveryQueueException {
        // Change discoveryQueueElement2 so that it is marked runImmediately.  It should jump to the
        // head of the queue.
        when(discoveryQueueElement1.compareTo(eq(discoveryQueueElement2))).thenReturn(1);
        when(discoveryQueueElement2.compareTo(eq(discoveryQueueElement1))).thenReturn(-1);
        when(discoveryQueueElement2.runImmediately()).thenReturn(true);
        discoveryQueue.add(discoveryQueueElement1);
        assertEquals(discoveryQueueElement1, discoveryQueue.peek().get());
        discoveryQueue.add(discoveryQueueElement2);
        assertEquals(discoveryQueueElement2, discoveryQueue.peek().get());
        assertEquals(discoveryQueueElement2, discoveryQueue.remove().get());
        assertEquals(discoveryQueueElement1, discoveryQueue.peek().get());
        assertEquals(discoveryQueueElement1, discoveryQueue.remove().get());
    }

    /**
     * Test that when an element is added a second time, it has no effect.
     *
     * @throws DiscoveryQueueException when we attempt to add a queue element that doesn't belong
     * in this queue.
     */
    @Test
    public void testSecondAddDoesNothing() throws DiscoveryQueueException {
        discoveryQueue.add(discoveryQueueElement1);
        discoveryQueue.add(discoveryQueueElement2);
        assertEquals(2, discoveryQueue.size());
        // Check that when we add a different element for the same target, it is ignored.
        assertEquals(discoveryQueueElement2, discoveryQueue.add(discoveryQueueElement4));
        assertEquals(2, discoveryQueue.size());
    }

    /**
     * Test that when an element is added with runImmediately set, if it is already queued
     * it moves to the front of the queue.
     *
     * @throws DiscoveryQueueException when we attempt to add a queue element that doesn't belong
     * in this queue.
     */
    @Test
    public void testSecondAddRunImmediately() throws DiscoveryQueueException {
        discoveryQueue.add(discoveryQueueElement1);
        assertEquals(discoveryQueueElement1, discoveryQueue.peek().get());
        discoveryQueue.add(discoveryQueueElement2);
        assertEquals(discoveryQueueElement1, discoveryQueue.peek().get());
        // Change discoveryQueueElement2 so that it is marked runImmediately.  It should jump to the
        // head of the queue when added again.
        when(discoveryQueueElement4.runImmediately()).thenReturn(true);
        // since element2 and element4 both apply to target2, when we queue element4 the effect
        // should be to push its runImmediately value to element2 and return element2
        when(discoveryQueueElement1.compareTo(eq(discoveryQueueElement2))).thenReturn(1);
        when(discoveryQueueElement2.compareTo(eq(discoveryQueueElement1))).thenReturn(-1);
        assertEquals(discoveryQueueElement2, discoveryQueue.add(discoveryQueueElement4));
        assertEquals(discoveryQueueElement2, discoveryQueue.peek().get());
        assertEquals(discoveryQueueElement2, discoveryQueue.remove().get());
        assertEquals(discoveryQueueElement1, discoveryQueue.peek().get());
        assertEquals(discoveryQueueElement1, discoveryQueue.remove().get());
        assertTrue(discoveryQueue.isEmpty());
    }

    /**
     * Test that when a target is removed, its related DiscoveryQueueElement is deleted from the
     * queue.
     *
     * @throws DiscoveryQueueException when add method throws it.
     */
    @Test
    public void testTargetRemoved() throws DiscoveryQueueException {
        discoveryQueue.add(discoveryQueueElement1);
        assertEquals(discoveryQueueElement1, discoveryQueue.peek().get());
        discoveryQueue.add(discoveryQueueElement2);
        assertEquals(discoveryQueueElement1, discoveryQueue.peek().get());
        assertEquals(2, discoveryQueue.size());
        discoveryQueue.handleTargetRemoval(targetId1);
        assertEquals(discoveryQueueElement2, discoveryQueue.peek().get());
        assertEquals(1, discoveryQueue.size());
    }
}
