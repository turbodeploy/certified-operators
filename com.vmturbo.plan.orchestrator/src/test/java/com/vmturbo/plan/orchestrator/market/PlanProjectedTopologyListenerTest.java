package com.vmturbo.plan.orchestrator.market;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.TimeoutException;

import javax.annotation.Nonnull;

import io.opentracing.SpanContext;

import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.communication.chunking.RemoteIterator;

/**
 * Unit tests for the {@link PlanProjectedTopologyListener}.
 */
public class PlanProjectedTopologyListenerTest {

    private static final long PROJECTED_TOPO_ID = 101;

    private static final TopologyInfo TOPOLOGY_INFO = TopologyInfo.newBuilder()
        .setTopologyId(PROJECTED_TOPO_ID - 1)
        .build();

    /**
     * Test that we call the relevant processor when a topology it applies to is received.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testFoundProcessor() throws Exception {
        final PlanProjectedTopologyListener listener = new PlanProjectedTopologyListener();
        final TestIterator iterator = new TestIterator();
        final TestProjectedTopologyProcessor processor = spy(TestProjectedTopologyProcessor.class);
        listener.addProjectedTopologyProcessor(processor);

        listener.onProjectedTopologyReceived(PROJECTED_TOPO_ID, TOPOLOGY_INFO,
            iterator, Mockito.mock(SpanContext.class));

        // Verify that we called the "handle" method of the processor.
        verify(processor).handleProjectedTopology(PROJECTED_TOPO_ID, TOPOLOGY_INFO, iterator);
        // Verify that we drained the iterator - even though the processor didn't finish draining it.
        assertTrue(iterator.isDrained());
    }

    /**
     * Test that when more than one processor applies, just one gets called.
     */
    @Test
    public void testMoreThanOneProcessor() {
        final PlanProjectedTopologyListener listener = new PlanProjectedTopologyListener();
        final TestIterator iterator = new TestIterator();
        final TestProjectedTopologyProcessor processor1 = new TestProjectedTopologyProcessor();
        final TestProjectedTopologyProcessor processor2 = new TestProjectedTopologyProcessor();
        listener.addProjectedTopologyProcessor(processor1);
        listener.addProjectedTopologyProcessor(processor2);

        listener.onProjectedTopologyReceived(PROJECTED_TOPO_ID, TOPOLOGY_INFO,
            iterator, Mockito.mock(SpanContext.class));

        // Only one of the processors should have been called. The order doesn't matter.
        assertThat(processor1.numHandled + processor2.numHandled, is(1));
        // Verify that we drained the iterator - even though the processor didn't finish draining it.
        assertTrue(iterator.isDrained());
    }

    /**
     * Test that we still drain the iterator if the processor throws exceptions.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testFoundProcessorException() throws Exception {
        testWithException(new RuntimeException("ERROR"));
        testWithException(new CommunicationException("Comms broken."));
        testWithException(new TimeoutException("Time is broken."));
    }

    /**
     * Test that we still drain the iterator if there is no applicable processor found.
     */
    @Test
    public void testNoProcessorFound() {
        final TestIterator iterator = new TestIterator();
        final PlanProjectedTopologyListener listener = new PlanProjectedTopologyListener();
        listener.onProjectedTopologyReceived(PROJECTED_TOPO_ID, TOPOLOGY_INFO,
            iterator, Mockito.mock(SpanContext.class));
        // Make sure we still drain the iterator.
        assertTrue(iterator.isDrained());
    }

    private void testWithException(@Nonnull final Throwable exception)
        throws InterruptedException, TimeoutException, CommunicationException {
        final PlanProjectedTopologyListener listener = new PlanProjectedTopologyListener();
        final TestIterator iterator = new TestIterator();
        TestProjectedTopologyProcessor processor = spy(TestProjectedTopologyProcessor.class);
        listener.addProjectedTopologyProcessor(processor);
        doThrow(exception).when(processor).handleProjectedTopology(PROJECTED_TOPO_ID, TOPOLOGY_INFO, iterator);
        listener.onProjectedTopologyReceived(PROJECTED_TOPO_ID, TOPOLOGY_INFO,
            iterator, Mockito.mock(SpanContext.class));

        verify(processor).handleProjectedTopology(PROJECTED_TOPO_ID, TOPOLOGY_INFO, iterator);
        assertTrue(iterator.isDrained());
    }

    /**
     * Test iterator, with a utility method to check if it's been drained.
     */
    private static class TestIterator implements RemoteIterator<ProjectedTopologyEntity> {
        private final Iterator<ProjectedTopologyEntity> it = Collections.singletonList(ProjectedTopologyEntity.getDefaultInstance())
            .iterator();

        public boolean isDrained() {
            return !it.hasNext();
        }

        @Override
        public boolean hasNext() {
            return it.hasNext();
        }

        @Nonnull
        @Override
        public Collection<ProjectedTopologyEntity> nextChunk() {
            return Collections.singletonList(it.next());
        }
    }

    /**
     * Test {@link ProjectedTopologyProcessor}.
     */
    static class TestProjectedTopologyProcessor implements ProjectedTopologyProcessor {

        int numHandled = 0;

        @Override
        public boolean appliesTo(@Nonnull final TopologyInfo sourceTopologyInfo) {
            return sourceTopologyInfo.equals(TOPOLOGY_INFO);
        }

        @Override
        public void handleProjectedTopology(final long projectedTopologyId,
                                            @Nonnull final TopologyInfo sourceTopologyInfo,
                                            @Nonnull final RemoteIterator<ProjectedTopologyEntity> iterator) throws InterruptedException, TimeoutException, CommunicationException {
            numHandled++;
        }
    }
}