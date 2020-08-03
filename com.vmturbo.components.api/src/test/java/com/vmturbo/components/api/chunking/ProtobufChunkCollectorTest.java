package com.vmturbo.components.api.chunking;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.vmturbo.communication.version.VersionNegotiation.NegotiationRequest;

/**
 * Unit tests for {@link ProtobufChunkCollectorTest}.
 */
public class ProtobufChunkCollectorTest {

    private static final NegotiationRequest PROTO = NegotiationRequest.newBuilder()
        .setProtocolVersion("oisentsr")
        .build();

    private static final int SIZE = PROTO.getSerializedSize();

    /**
     * JUnit rule to catch expected exceptions in tests.
     */
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    /**
     * Test that the memory size limit is enforced.
     *
     * @throws OversizedElementException To satisfy compiler.
     */
    @Test
    public void testTryAddSizeLimit() throws OversizedElementException {
        ProtobufChunkCollector<NegotiationRequest> chunk = new ProtobufChunkCollector<>(SIZE * 100, SIZE * 2);
        assertNull(chunk.addToCurrentChunk(PROTO));
        assertNull(chunk.addToCurrentChunk(PROTO));

        assertEquals(Arrays.asList(PROTO, PROTO), chunk.addToCurrentChunk(PROTO));

        assertEquals(Arrays.asList(PROTO), chunk.takeCurrentChunk());

        assertEquals(Collections.emptyList(), chunk.takeCurrentChunk());
    }

    /**
     * Test that the item count limit is enforced.
     *
     * @throws OversizedElementException To satisfy compiler.
     */
    @Test
    public void testTryAddDesiredSize() throws OversizedElementException {
        // Plenty of memory room
        final int maxCount = 10;
        ProtobufChunkCollector<NegotiationRequest> chunk = new ProtobufChunkCollector<>(SIZE * maxCount, SIZE * maxCount * 2);
        for (int i = 0; i < (maxCount - 1); ++i) {
            assertNull(chunk.addToCurrentChunk(PROTO));
        }
        // That's the final straw...
        Collection<NegotiationRequest> reqs = chunk.addToCurrentChunk(PROTO);

        assertEquals(maxCount, reqs.size());
        for (NegotiationRequest req : reqs) {
            assertEquals(PROTO, req);
        }

        assertEquals(Collections.emptyList(), chunk.takeCurrentChunk());
    }

    /**
     * Test that adding an overly large element to an empty chunk throws the expected exception.
     *
     * @throws OversizedElementException To satisfy compiler.
     */
    @Test
    public void testTryAddTooLarge() throws OversizedElementException {
        ProtobufChunkCollector<NegotiationRequest> chunk = new ProtobufChunkCollector<>(SIZE * 1000, SIZE - 1);
        expectedException.expect(OversizedElementException.class);
        chunk.addToCurrentChunk(PROTO);
    }

    /**
     * Test that the count method returns the accurate count.
     *
     * @throws OversizedElementException To satisfy compiler.
     */
    @Test
    public void testCount() throws OversizedElementException {
        ProtobufChunkCollector<NegotiationRequest> chunk = new ProtobufChunkCollector<>(SIZE * 100, SIZE * 100);
        assertNull(chunk.addToCurrentChunk(PROTO));
        assertNull(chunk.addToCurrentChunk(PROTO));
        assertEquals(chunk.count(), 2);
    }

    /**
     * Test that the stream returns the stream of expected elements.
     *
     * @throws OversizedElementException To satisfy compiler.
     */
    @Test
    public void testTake() throws OversizedElementException {
        ProtobufChunkCollector<NegotiationRequest> chunk = new ProtobufChunkCollector<>(SIZE * 100, SIZE * 100);
        assertNull(chunk.addToCurrentChunk(PROTO));
        assertNull(chunk.addToCurrentChunk(PROTO));
        assertEquals(Arrays.asList(PROTO, PROTO), chunk.takeCurrentChunk());
        assertEquals(Collections.emptyList(), chunk.takeCurrentChunk());
    }
}