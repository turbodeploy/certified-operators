package com.vmturbo.history.ingesters;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.annotation.Nonnull;

import org.junit.Test;

import com.vmturbo.communication.chunking.RemoteIterator;
import com.vmturbo.history.ingesters.common.AbstractChunkedBroadcastProcessor;
import com.vmturbo.history.ingesters.common.ChunkedBroadcastProcessorConfig;
import com.vmturbo.history.ingesters.common.IChunkProcessor;
import com.vmturbo.history.ingesters.common.IChunkProcessorFactory;
import com.vmturbo.history.ingesters.common.ImmutableChunkedBroadcastProcessorConfig;
import com.vmturbo.history.utils.MultiStageTimer;

/**
 * Tests of the Ingester framework.
 */
public class IngestersTest {

    /**
     * Test that the overall processing sequence of an ingester instance is correct, including
     * hooks and chunk processor invocations.
     *
     * <p>Note that while one might expect mocks to be usable here, they're not because mock
     * objects are not reliable when used by different threads.</p>
     *
     */
    @Test
    public void testIngesterProcessingSequence() {
        final SequenceCheckingIngester ingester = new SequenceCheckingIngester(3);
        int processed = ingester.processBroadcast("broadcast", new IntRemoteIterator(10, 3));
        assertEquals(10, processed);
        ingester.checkMessageSequence();
    }

    /**
     * Ingester implementation that provides a shared message board, where chunks and the
     * ingester itself can record events in the processing sequence.
     */
    private static class SequenceCheckingIngester
            extends AbstractChunkedBroadcastProcessor<Integer, String, List<String>, Integer> {
        private final int nProc;
        private int intCount = 0;
        private int chunkCount = 0;
        private List<String> messages = Collections.synchronizedList(new ArrayList<>());
        private static ChunkedBroadcastProcessorConfig ingesterConfig =
                ImmutableChunkedBroadcastProcessorConfig.builder()
                        .threadPool(Executors.newSingleThreadExecutor())
                        .defaultChunkTimeLimitMsec(TimeUnit.SECONDS.toMillis(60))
                        .build();

        SequenceCheckingIngester(int nProc) {
            super(createProcessorFactories(nProc), ingesterConfig);
            this.nProc = nProc;

        }

        private static Collection<IChunkProcessorFactory<Integer, String, List<String>>>
        createProcessorFactories(int n) {
            return IntStream.range(0, n)
                    .mapToObj(SequenceRecordingChunkProcessorFactory::new)
                    .collect(Collectors.toList());
        }

        @Override
        protected Integer getProcessingResult(final String s, final List<String> state) {
            return intCount;
        }

        @Override
        protected List<String> getSharedState(final String s) {
            return messages;
        }

        @Override
        protected String summarizeInfo(final String s) {
            // just something distinguishable from the info string
            return "[" + s + "]";
        }

        @Override
        protected void startBroadcastHook(final String s, final List<String> state, final MultiStageTimer timer) {
            messages.add("startBroadcastHook");
        }

        @Override
        protected void beforeChunkHook(final Collection<Integer> chunk, final int chunkNo, final String s, final List<String> state, final MultiStageTimer timer) {
            messages.add(String.format("beforeChunkHook chunk #%d[%d]", chunkNo, chunk.size()));
        }

        @Override
        protected void afterChunkHook(final Collection<Integer> chunk, final int chunkNo, final String s, final List<String> state, final MultiStageTimer timer) {
            messages.add(String.format("afterChunkHook chunk #%d[%d]", chunkNo, chunk.size()));
            intCount += chunk.size();
            chunkCount += 1;
        }

        @Override
        protected void beforeFinishHook(final String s, final List<String> state, final MultiStageTimer timer) {
            messages.add("beforeFinishHook");
        }

        @Override
        protected void afterFinishHook(final String s, final List<String> state, final int objectCount, final MultiStageTimer timer) {
            messages.add("afterFinishHook");
        }

        private static Pattern processPat = Pattern.compile("(\\d+) processChunk");
        private static Pattern finishPat = Pattern.compile("(\\d+) finish");

        void checkMessageSequence() {
            int i = 0;
            assertTrue(messages.size() > i && messages.get(i++).startsWith("startBroadcastHook"));
            for (int chunkNo = 0; chunkNo < chunkCount; chunkNo++) {
                i = checkBatchMessages(i, chunkNo);
            }
            assertTrue(messages.size() > i && messages.get(i++).startsWith("beforeFinishHook"));
            boolean[] finishSeen = new boolean[nProc];
            for (int p = 0; p < nProc; p++) {
                assertTrue(messages.size() > i);
                Matcher m = finishPat.matcher(messages.get(i++));
                assertTrue(m.matches());
                int id = Integer.parseInt(m.group(1));
                assertFalse(finishSeen[id]);
                finishSeen[id] = true;
            }
            assertTrue(messages.size() > i && messages.get(i++).startsWith("afterFinishHook"));
            assertEquals(i, messages.size());
        }

        private int checkBatchMessages(int index, int chunkNo) {
            boolean[] seen = new boolean[nProc];
            assertTrue(messages.size() > index && messages.get(index++).startsWith(
                    String.format("beforeChunkHook chunk #%d", chunkNo + 1)));
            for (int i = 0; i < nProc; i++) {
                assertTrue(messages.size() > index);
                Matcher m = processPat.matcher(messages.get(index++));
                assertTrue(m.matches());
                int id = Integer.parseInt(m.group(1));
                assertFalse(seen[id]);
                seen[id] = true;
            }
            assertTrue(messages.size() > index && messages.get(index++).startsWith(
                    String.format("afterChunkHook chunk #%d", chunkNo + 1)));
            return index;
        }
    }

    /**
     * ChunkProcessor implementation that simply posts messages to a shared message board when
     * its methods are inovked.
     */
    private static class SequenceRecordingChunkProcessor implements IChunkProcessor<Integer> {

        private final int id;
        private final List<String> messages;

        SequenceRecordingChunkProcessor(int id, List<String> messages) {
            this.id = id;
            this.messages = messages;
        }

        @Override
        public ChunkDisposition processChunk(@Nonnull final Collection<Integer> chunk,
                @Nonnull final String infoSummary) {
            messages.add(String.format("%d processChunk", id));
            return ChunkDisposition.SUCCESS;
        }

        @Override
        public void finish(final int objectCount, final boolean expedite, final String infoSummary) {
            messages.add(String.format("%d finish", id));
        }
    }


    /**
     * Create a new {@link SequenceRecordingChunkProcessor} instance.
     */
    public static class SequenceRecordingChunkProcessorFactory
            implements IChunkProcessorFactory<Integer, String, List<String>> {

        private final int id;

        SequenceRecordingChunkProcessorFactory(int id) {
            this.id = id;
        }

        @Override
        public Optional<IChunkProcessor<Integer>> getChunkProcessor(final String s, final List<String> state) {
            return Optional.of(new SequenceRecordingChunkProcessor(id, state));
        }
    }

    /**
     * {@link RemoteIterator} whose element type is Integer.
     */
    private static class IntRemoteIterator implements RemoteIterator<Integer> {

        private final int n;
        private final int chunkSize;
        int sent = 0;

        IntRemoteIterator(int n, int chunkSize) {
            this.n = n;
            this.chunkSize = chunkSize;
        }

        @Override
        public boolean hasNext() {
            return sent < n;
        }

        @Nonnull
        @Override
        public Collection<Integer> nextChunk() {
            if (hasNext()) {
                List<Integer> chunk = new ArrayList<>();
                while (sent < n && chunk.size() < chunkSize) {
                    chunk.add(sent++);
                }
                return chunk;
            } else {
                throw new NoSuchElementException();
            }
        }
    }

}
