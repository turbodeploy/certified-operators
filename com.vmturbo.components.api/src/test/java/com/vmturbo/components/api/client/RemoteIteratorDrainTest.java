package com.vmturbo.components.api.client;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.stream.IntStream;

import javax.annotation.Nonnull;

import com.google.protobuf.Empty;

import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.communication.CommunicationException;
import com.vmturbo.communication.chunking.RemoteIterator;

/**
 * Tests for {@link RemoteIteratorDrain}.
 */
public class RemoteIteratorDrainTest {

    /**
     * Test that draining the iterator goes through all contents.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testDrain() throws Exception {
        TestIterator iterator = new TestIterator(50);
        RemoteIteratorDrain.drainIterator(iterator, "foo", false);
        assertTrue(iterator.isDrained());
    }

    /**
     * Test that draining the iterator swallows exceptions.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testDrainSwallowsException() throws Exception {
        RemoteIterator<Empty> iterator = Mockito.mock(RemoteIterator.class);
        when(iterator.hasNext()).thenReturn(true, true, false);
        when(iterator.nextChunk()).thenThrow(new CommunicationException("BOO!"));

        RemoteIteratorDrain.drainIterator(iterator, "foo", false);
        verify(iterator, times(2)).hasNext();
    }

    /**
     * Test draining an empty iterator.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testDrainEmptyIterator() throws Exception {
        TestIterator iterator = new TestIterator(0);
        RemoteIteratorDrain.drainIterator(iterator, "foo", false);

        assertTrue(iterator.isDrained());
    }


    /**
     * Test iterator, with a utility method to check if it's been drained.
     */
    private static class TestIterator implements RemoteIterator<Empty> {
        private final Iterator<Empty> it;

        TestIterator(final int numElements) {
            it = IntStream.range(0, numElements).mapToObj(i -> Empty.getDefaultInstance()).iterator();
        }

        public boolean isDrained() {
            return !it.hasNext();
        }

        @Override
        public boolean hasNext() {
            return it.hasNext();
        }

        @Nonnull
        @Override
        public Collection<Empty> nextChunk() {
            return Collections.singletonList(it.next());
        }
    }
}