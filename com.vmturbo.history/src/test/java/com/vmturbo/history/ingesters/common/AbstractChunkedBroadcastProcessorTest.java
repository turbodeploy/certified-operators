package com.vmturbo.history.ingesters.common;

import static com.vmturbo.history.ingesters.common.AbstractChunkedBroadcastProcessorTest.DummyChunkProcessor.SPECIAL_VALUE_SLEEP_5_SECS;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nonnull;

import org.junit.Test;
import org.mockito.stubbing.Answer;

import com.vmturbo.communication.CommunicationException;
import com.vmturbo.communication.chunking.RemoteIterator;

/**
 * Tests of {@link AbstractChunkedBroadcastProcessor} functionality.
 */
public class AbstractChunkedBroadcastProcessorTest {

    /**
     * Test that when an individual processor takes too long to handle an individual chunk, that
     * processor times out after the configured time limit, and then continues processing following
     * chunks.
     *
     * @throws InterruptedException   if interrupted
     * @throws TimeoutException       if operation times out
     * @throws CommunicationException if topology consumption fails
     */
    @Test
    public void testChunkTimeLimit()
            throws InterruptedException, TimeoutException, CommunicationException {
        final ImmutableChunkedBroadcastProcessorConfig config =
                ImmutableChunkedBroadcastProcessorConfig.builder()
                        .defaultChunkTimeLimitMsec(1000)
                        .threadPool(Executors.newSingleThreadExecutor())
                        .build();
        DummyBroadcastProcessor broadcastProcessor =
                new DummyBroadcastProcessor(
                        Collections.singletonList(new DummyChunkProcessor.Factory()),
                        config);
        final RemoteIterator<Integer> broadcast = createBroadcast(1, 2, 3, null,
                SPECIAL_VALUE_SLEEP_5_SECS, null,
                4, SPECIAL_VALUE_SLEEP_5_SECS, 5, 6, null,
                7, 8, 9);
        final Object result = broadcastProcessor.processBroadcast("Dummy", broadcast,
                new AtomicInteger());
        // we should have gotten (1, 2, 3) in first chunk () in second, (4) in third, and
        // (7, 8, 9) in last. Sum = 34.
        assertEquals(34, result);
    }

    @SafeVarargs
    private final <T> RemoteIterator<T> createBroadcast(T... values)
            throws InterruptedException, TimeoutException, CommunicationException {
        List<List<T>> chunks = new ArrayList<>();
        List<T> chunk = new ArrayList<>();
        for (T value : values) {
            if (value != null) {
                chunk.add(value);
            } else {
                chunks.add(chunk);
                chunk = new ArrayList<>();
            }
        }
        chunks.add(chunk);
        return mockRemoteIterator(chunks);
    }

    private <T> RemoteIterator<T> mockRemoteIterator(List<List<T>> chunks)
            throws InterruptedException, TimeoutException, CommunicationException {
        final RemoteIterator<T> mock = mock(RemoteIterator.class);
        when(mock.hasNext()).thenAnswer((Answer<Boolean>)invocation -> chunks.size() > 0);
        when(mock.nextChunk()).thenAnswer((Answer<List<T>>)invocation -> {
            if (chunks.isEmpty()) {
                throw new NoSuchElementException();
            } else {
                return chunks.remove(0);
            }
        });
        return mock;
    }

    /**
     * A broadcast processor that consumes chunks of integers.
     */
    public static class DummyBroadcastProcessor extends
            AbstractChunkedBroadcastProcessor<Integer, String, AtomicInteger, Integer> {

        /**
         * Creates an instance.
         *
         * @param iChunkProcessorFactories collection of factories that will create the chunk processors
         * @param config                   config information for the processor
         */
        public DummyBroadcastProcessor(
                @Nonnull final Collection<? extends IChunkProcessorFactory<
                        Integer, String, AtomicInteger>> iChunkProcessorFactories,
                @Nonnull final ChunkedBroadcastProcessorConfig config) {
            super(iChunkProcessorFactories, config);
        }

        @Override
        protected String summarizeInfo(final String info) {
            return info;
        }

        @Override
        protected Integer getProcessingResult(final String info, final AtomicInteger state) {
            return state.get();
        }
    }

    /**
     * A chunk processor that accepts chunks of Integers.
     *
     * <p>Specific integer values appearing in the stream cause special actions used by tests,
     * namely:</p>
     * <ul>
     *     <li>SPECIAL_VALUE_SLEEP_5_SECS: Sleep 5 seconds. This can be used in conjunction with
     *     a chunkMaxTime setting of < 5 seconds to test that timeouts are working. In that case,
     *     following integers in this chunk should not be processed, while previous integers
     *     should be.</li>
     * </ul>
     *
     * <p>The chunk processor returns the sum of the integers in the chunks that it processes.</p>
     */
    public static class DummyChunkProcessor implements IChunkProcessor<Integer> {

        /** Special value that causes processor to pause 5 seconds. */
        public static final int SPECIAL_VALUE_SLEEP_5_SECS = -1;

        private final AtomicInteger state;

        DummyChunkProcessor(AtomicInteger state) {
            this.state = state;
        }

        @Override
        public ChunkDisposition processChunk(@Nonnull final Collection<Integer> chunk,
                @Nonnull final String infoSummary) throws InterruptedException {
            for (int i : chunk) {
                switch (i) {
                    case SPECIAL_VALUE_SLEEP_5_SECS:
                        Thread.sleep(TimeUnit.SECONDS.toMillis(5));
                        break;
                    default:
                        state.getAndAdd(i);
                        break;
                }
            }
            return ChunkDisposition.SUCCESS;
        }


        /**
         * Create a new {@link DummyChunkProcessor}.
         */
        public static class Factory implements IChunkProcessorFactory<Integer, String, AtomicInteger> {
            @Override
            public Optional<IChunkProcessor<Integer>> getChunkProcessor(
                    final String info, final AtomicInteger state) {
                return Optional.of(new DummyChunkProcessor(state));
            }
        }
    }
}
