package com.vmturbo.history.ingesters.common;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isIn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import org.mockito.stubbing.Answer;

import com.vmturbo.communication.CommunicationException;
import com.vmturbo.communication.chunking.RemoteIterator;

/**
 * Tests of {@link AbstractChunkedBroadcastProcessor} functionality.
 */
public class AbstractChunkedBroadcastProcessorTest {
    private static final Logger logger = LogManager.getLogger();
    private static final int INTERRUPTION_SPECIAL_VALUE = Integer.MIN_VALUE + 1;
    private static final int EXCEPTION_SPECIAL_VALUE = Integer.MIN_VALUE + 2;
    private DummyBroadcastProcessor broadcastProcessor;
    private ChunkedBroadcastProcessorConfig config;

    /**
     * Set up for all testss. Create a braodcast processor managing two chunk procesors. The first
     * is subject to delays, exceptions and interrupts, as determined by "special" values that may
     * appear in chunks. The second ignores all such values and just processes the other chunks.
     * This way we can tell not only what was processed by the affected chunk in various scenarios,
     * but also what was processed by the unaffected chunk.
     */
    @Before
    public void before() {
        config = ImmutableChunkedBroadcastProcessorConfig.builder()
                .defaultChunkTimeLimitMsec(1000)
                .threadPool(Executors.newSingleThreadExecutor())
                .build();
        this.broadcastProcessor =
                new DummyBroadcastProcessor(
                        Arrays.asList(
                                new DummyChunkProcessor.Factory(true),
                                new DummyChunkProcessor.Factory(false)),
                        config);
    }

    /**
     * Make sure that when nothing bad happens when processing a broadcast, all chunks are fully
     * captured.
     *
     * @throws CommunicationException if we have a problem retrieving a chunk
     * @throws InterruptedException   if processing is interrupted
     * @throws TimeoutException       if chunkn processing times out
     */
    @Test
    public void testThatNormalProcessingWorks()
            throws CommunicationException, InterruptedException, TimeoutException {
        final RemoteIterator<Integer> broadcast = createBroadcast(1, 2, 3, null,
                4, 5, 6, null,
                7, 8, 9);
        final Object result = broadcastProcessor.processBroadcast("Dummy", broadcast,
                new AtomicInteger());
        // We should record all chunks -  1..9 twice (once in each processor)
        assertThat(result, is(90));
    }

    /**
     * Test that when a chunk processor fails on a chunk, it affects that processor but not the
     * others. The failing processor's state may reflect a portion of the chunk already processed
     * prior ot the failure. In this test rig, that is always the case.
     *
     * @throws CommunicationException if we have a problem retrieving a chunk
     * @throws InterruptedException   if processing is interrupted
     * @throws TimeoutException       if chunkn processing times out
     */
    @Test
    public void testThatFailingChunkIsDiscardsFollowingChunks()
            throws CommunicationException, InterruptedException, TimeoutException {
        final RemoteIterator<Integer> broadcast = createBroadcast(1, 2, 3, null,
                4, EXCEPTION_SPECIAL_VALUE, 5, 6, null,
                7, 8, 9);
        final Object result = broadcastProcessor.processBroadcast("Dummy", broadcast,
                new AtomicInteger());
        // First procesor will only get 1..4, but the other shoud not be affected (sees 1..9),
        // so togehter they record 55
        assertThat(result, is(55));
    }

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
        final RemoteIterator<Integer> broadcast = createBroadcast(1, 2, 3, null,
                -2, null,
                4, -2, 5, 6, null,
                7, 8, 9);
        final Object result = broadcastProcessor.processBroadcast("Dummy", broadcast,
                new AtomicInteger());
        // 2nd and 3rd chunks (after receiving value 4) should be discarded by first processor,
        // and second processor should get all values. So we get 34 from first and 45 from
        // second, for total 79
        assertThat(result, is(79));
    }

    /**
     * Test that an interruption of a chunk processor causes that processor to stop and receive no
     * more chunks. Furthermore, all other processors are canceled if they have not already
     * completed, and no processor will receive any later chunks. Any given processor may reflect
     * having partially processed the current chunk.
     *
     * @throws InterruptedException   if interrupted
     * @throws TimeoutException       if operation times out
     * @throws CommunicationException if topology consumption fails
     */
    @Test
    public void testThatInterruptionAbandonsLaterChunks()
            throws CommunicationException, InterruptedException, TimeoutException {
        final RemoteIterator<Integer> broadcast = createBroadcast(1, 2, 3, null,
                4, INTERRUPTION_SPECIAL_VALUE, 5, 6, null,
                7, 8, 9);
        final Object result = broadcastProcessor.processBroadcast("Dummy", broadcast,
                new AtomicInteger());
        // Our first processor (the one that handles special values) should see only 1..4, for a
        // total of 10. The other processor may or may not complete its second batch, depending on
        // scheduling. It should see none, all, or some prefix of the second chunk, so its total
        // should be 6, 10, 15, or 21. So grand total should be 16, 24, 35, or 31.
        assertThat(result, isIn(Arrays.asList(16, 24, 35, 31)));
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

        private final AtomicInteger state;
        private final boolean handleSpecials;

        DummyChunkProcessor(AtomicInteger state, boolean handleSpecials) {
            this.state = state;
            this.handleSpecials = handleSpecials;
        }

        @Override
        public ChunkDisposition processChunk(@Nonnull final Collection<Integer> chunk,
                @Nonnull final String infoSummary) throws InterruptedException {
            for (int i : chunk) {
                if (handleSpecials) {
                    switch (i) {
                        case INTERRUPTION_SPECIAL_VALUE:
                            throw new InterruptedException();
                        case EXCEPTION_SPECIAL_VALUE:
                            throw new RuntimeException();
                        default:
                            if (i < 0) {
                                Thread.sleep(TimeUnit.SECONDS.toMillis(-i));
                            } else {
                                state.getAndAdd(i);
                            }
                            break;
                    }
                } else if (i > 0) {
                    // if we're not supposed to handle specials, we only process non-negative
                    // values
                    state.getAndAdd(i);
                }
            }
            return ChunkDisposition.SUCCESS;
        }

        /**
         * Create a new {@link DummyChunkProcessor}.
         */
        public static class Factory
                implements IChunkProcessorFactory<Integer, String, AtomicInteger> {

            private final boolean handleSpecials;

            /**
             * Create a new chunk processor to be part of this broadcast processor.
             *
             * @param handleSpecials true if this chunk processor should handle special values in
             *                       chunks, else they are ignored
             */
            public Factory(boolean handleSpecials) {
                this.handleSpecials = handleSpecials;
            }

            @Override
            public Optional<IChunkProcessor<Integer>> getChunkProcessor(
                    final String info, final AtomicInteger state) {
                return Optional.of(new DummyChunkProcessor(state, handleSpecials));
            }
        }
    }
}
