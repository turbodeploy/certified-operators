package com.vmturbo.components.api.chunking;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.Test;

import com.vmturbo.communication.chunking.MessageChunker;
import com.vmturbo.communication.version.VersionNegotiation.NegotiationRequest;

/**
 * Unit tests for {@link MessageChunker}.
 */
public class ProtobufChunkIteratorTest {

    private static final NegotiationRequest PROTO = NegotiationRequest.newBuilder()
        .setProtocolVersion("oisentsr")
        .build();

    private static final int SIZE = PROTO.getSerializedSize();

    /**
     * Test that chunking a collection into multiple collections works and respects the
     * provided limit.
     *
     * @throws OversizedElementException To satisfy compiler.
     */
    @Test
    public void testChunkIterable() throws OversizedElementException {
        final int limit = 100;
        List<NegotiationRequest> list = IntStream.range(0, limit * 2)
            .mapToObj(i -> PROTO)
            .collect(Collectors.toList());
        // One more.
        list.add(PROTO);
        List<Collection<NegotiationRequest>> chunks = new ArrayList<>();
        ProtobufChunkIterator.partition(list, limit * SIZE, limit * SIZE * 2)
            .forEachRemaining(chunks::add);
        assertEquals(3, chunks.size());
        assertEquals(limit, chunks.get(0).size());
        assertEquals(limit, chunks.get(1).size());
        assertEquals(1, chunks.get(2).size());
    }

    /**
     * Test that chunking a collection into  a single collection works and respects the
     * provided limit.
     *
     * @throws OversizedElementException To satisfy compiler.
     */
    @Test
    public void testChunkIterableSmallChunk() throws OversizedElementException {
        // Use memory limits, because then we can get away with less entries in the test list :)
        List<NegotiationRequest> list = Collections.singletonList(PROTO);
        List<Collection<NegotiationRequest>> chunks = new ArrayList<>();
        ProtobufChunkIterator.partition(list.iterator(), SIZE * 100, SIZE * 100)
            .forEachRemaining(chunks::add);
        assertEquals(1, chunks.size());
        assertEquals(1, chunks.get(0).size());
        assertEquals(PROTO, chunks.get(0).iterator().next());
    }
}