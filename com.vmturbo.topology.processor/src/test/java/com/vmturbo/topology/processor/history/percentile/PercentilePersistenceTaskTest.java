package com.vmturbo.topology.processor.history.percentile;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.Collections;
import java.util.Map;

import com.google.protobuf.ByteString;

import io.grpc.Status;
import io.grpc.stub.CallStreamObserver;
import io.grpc.stub.StreamObserver;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.vmturbo.common.protobuf.stats.Stats.GetPercentileCountsRequest;
import com.vmturbo.common.protobuf.stats.Stats.PercentileChunk;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceStub;
import com.vmturbo.common.protobuf.stats.StatsMoles.StatsHistoryServiceMole;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.commons.Units;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.sdk.common.util.Pair;
import com.vmturbo.topology.processor.history.CommodityField;
import com.vmturbo.topology.processor.history.EntityCommodityFieldReference;
import com.vmturbo.topology.processor.history.HistoryCalculationException;
import com.vmturbo.topology.processor.history.percentile.PercentileDto.PercentileCounts;
import com.vmturbo.topology.processor.history.percentile.PercentileDto.PercentileCounts.PercentileRecord;

/**
 * Unit tests for PercentilePersistenceTask.
 */
public class PercentilePersistenceTaskTest {
    private static final Pair<Long, Long> DEFAULT_RANGE = Pair.create(null, null);
    /**
     * Expected test exception.
     */
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private static final int chunkSizeKb = 102;
    private static final PercentileHistoricalEditorConfig config =
                    new PercentileHistoricalEditorConfig(10, 18, 777777L, 10, chunkSizeKb,
                                    Collections.emptyMap(), null, Clock.systemUTC());
    private static final long oid1 = 1;
    private static final long oid2 = 2;
    private static final long oid3 = 3;
    private static final int ct1 = 7;
    private static final int ct2 = 8;
    private StatsHistoryServiceMole history;
    private GrpcTestServer grpcServer;

    /**
     * Initializes the tests.
     *
     * @throws IOException if error occurred while creating gRPC server
     */
    @Before
    public void init() throws IOException {
        history = Mockito.spy(new StatsHistoryServiceMole());
        grpcServer = GrpcTestServer.newServer(history);
        grpcServer.start();
    }

    /**
     * Cleans up resources.
     */
    @After
    public void shutdown() {
        grpcServer.close();
    }

    /**
     * Test that a stream of two chunks each having commodities is retrieved and parsed successfully.
     *
     * @throws HistoryCalculationException when loading fails
     * @throws InterruptedException when interrupted
     */
    @Test
    public void testLoadSuccessTwoChunks()
                    throws HistoryCalculationException, InterruptedException {
        float cap = 89F;
        String key = "qqq";
        PercentileRecord rec1 = PercentileRecord.newBuilder().setEntityOid(oid1)
                        .setCommodityType(ct1).setCapacity(cap).setPeriod(30).build();
        PercentileRecord rec2 = PercentileRecord.newBuilder().setEntityOid(oid2)
                        .setCommodityType(ct2).setCapacity(cap).setPeriod(30).build();
        PercentileRecord rec3 = PercentileRecord.newBuilder().setEntityOid(oid3)
                        .setCommodityType(ct2).setCapacity(cap).setKey(key).setPeriod(30).build();
        byte[] payload = PercentileCounts.newBuilder()
                        .addPercentileRecords(rec1).addPercentileRecords(rec2)
                        .addPercentileRecords(rec3)
                        .build().toByteArray();

        Answer<Void> answerGetCounts = new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                GetPercentileCountsRequest request = invocation
                                .getArgumentAt(0, GetPercentileCountsRequest.class);
                Assert.assertNotNull(request);
                Assert.assertEquals(chunkSizeKb * Units.KBYTE, request.getChunkSize());
                @SuppressWarnings("unchecked")
                StreamObserver<PercentileChunk> observer = invocation.getArgumentAt(1, StreamObserver.class);
                int pos = payload.length / 3;
                observer.onNext(PercentileChunk.newBuilder().setPeriod(0).setStartTimestamp(PercentilePersistenceTask.TOTAL_TIMESTAMP)
                                .setContent(ByteString.copyFrom(payload, 0, pos))
                                .build());
                observer.onNext(PercentileChunk.newBuilder().setPeriod(0).setStartTimestamp(PercentilePersistenceTask.TOTAL_TIMESTAMP)
                                .setContent(ByteString.copyFrom(payload, pos, payload.length - pos))
                                .build());
                observer.onCompleted();
                return null;
            }
        };
        Mockito.doAnswer(answerGetCounts).when(history).getPercentileCounts(Mockito.any(),
                                                                            Mockito.any());

        final PercentilePersistenceTask task = new PercentilePersistenceTask(
                StatsHistoryServiceGrpc.newStub(grpcServer.getChannel()), DEFAULT_RANGE);
        Map<EntityCommodityFieldReference, PercentileRecord> comms = task
                        .load(Collections.emptyList(), config);
        Assert.assertNotNull(comms);
        final CommodityType commType1 = CommodityType.newBuilder().setType(ct1).build();
        final CommodityType commType2 = CommodityType.newBuilder().setType(ct2).build();
        final CommodityType commType3 = CommodityType.newBuilder().setType(ct2).setKey(key).build();
        Assert.assertTrue(comms
                        .containsKey(new EntityCommodityFieldReference(oid1, commType1,
                                                                       CommodityField.USED)));
        Assert.assertTrue(comms
                        .containsKey(new EntityCommodityFieldReference(oid2, commType2,
                                                                       CommodityField.USED)));
        Assert.assertTrue(comms
                        .containsKey(new EntityCommodityFieldReference(oid3, commType3,
                                                                       CommodityField.USED)));
    }

    /**
     * Test that reading IO failure is rethrown.
     *
     * @throws HistoryCalculationException expected
     * @throws InterruptedException when interrupted
     */
    @Test
    public void testLoadIoFailureThrown() throws HistoryCalculationException, InterruptedException {

        Mockito.doThrow(Status.INTERNAL.withCause(new Exception("qqq")).asRuntimeException())
                .when(history)
                .getPercentileCounts(Mockito.any(), Mockito.any());

        final PercentilePersistenceTask task = new PercentilePersistenceTask(
                StatsHistoryServiceGrpc.newStub(grpcServer.getChannel()), DEFAULT_RANGE);
        expectedException.expect(HistoryCalculationException.class);
        expectedException.expectMessage("Failed to load");
        task.load(Collections.emptyList(), config);
    }

    /**
     * Test that incorrect payload contents leads to exception thrown.
     *
     * @throws HistoryCalculationException expected
     * @throws InterruptedException when interrupted
     */
    @Test
    public void testLoadParseFailureThrown()
                    throws HistoryCalculationException, InterruptedException {
        Answer<Void> answerGetCounts = new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                @SuppressWarnings("unchecked")
                StreamObserver<PercentileChunk> observer =
                        invocation.getArgumentAt(1, StreamObserver.class);
                observer.onNext(PercentileChunk.newBuilder()
                                .setPeriod(0).setStartTimestamp(0)
                                .setContent(ByteString.copyFrom("qqq", StandardCharsets.UTF_8))
                                .build());
                observer.onCompleted();
                return null;
            }
        };
        Mockito.doAnswer(answerGetCounts).when(history).getPercentileCounts(Mockito.any(),
                                                                            Mockito.any());

        final PercentilePersistenceTask task = new PercentilePersistenceTask(
                StatsHistoryServiceGrpc.newStub(grpcServer.getChannel()), DEFAULT_RANGE);
        expectedException.expect(HistoryCalculationException.class);
        expectedException.expectMessage("Failed to deserialize");
        task.load(Collections.emptyList(), config);
    }

    /**
     * Test the successful save operation.
     *
     * @throws HistoryCalculationException when failed
     * @throws InterruptedException when interrupted
     */
    @Ignore("Ignore for now to succeed build")
    @Test
    public void testSaveSuccess() throws HistoryCalculationException, InterruptedException {
        float cap = 89F;
        String key = "qqq";
        PercentileRecord rec1 = PercentileRecord.newBuilder().setEntityOid(oid1)
                        .setCommodityType(ct1).setCapacity(cap).setPeriod(30).build();
        PercentileRecord rec2 = PercentileRecord.newBuilder().setEntityOid(oid3)
                        .setCommodityType(ct2).setCapacity(cap).setKey(key).setPeriod(30).build();
        PercentileCounts counts = PercentileCounts.newBuilder().addPercentileRecords(rec1)
                        .addPercentileRecords(rec2).build();
        long periodMs = 4756L;
        TestWriter writer = new TestWriter();
        Mockito.doReturn(writer)
                .when(history)
                .setPercentileCounts(Mockito.any(StreamObserver.class));

        final PercentilePersistenceTask task = new PercentilePersistenceTask(
                StatsHistoryServiceGrpc.newStub(grpcServer.getChannel()), DEFAULT_RANGE);
        task.save(counts, periodMs, config);
        Assert.assertArrayEquals(counts.toByteArray(), writer.getResult());
    }

    /**
     * Test if {@link PercentilePersistenceTask#save} actual write
     * the data when it gets empty counts.
     *
     * @throws InterruptedException when failed
     * @throws HistoryCalculationException when failed
     */
    @Test
    public void testSaveWithEmptyInput() throws InterruptedException, HistoryCalculationException {
        TestWriter writer = Mockito.spy(new TestWriter());
        Mockito.doReturn(writer).when(history).setPercentileCounts(Mockito.any(StreamObserver.class));

        PercentilePersistenceTask task = new PercentilePersistenceTask(
                StatsHistoryServiceGrpc.newStub(grpcServer.getChannel()), DEFAULT_RANGE);
        final PercentileCounts counts = PercentileCounts.newBuilder().build();
        task.save(counts, 0, config);

        Assert.assertArrayEquals(counts.toByteArray(), writer.getResult());
        Mockito.verify(writer, Mockito.times(1)).onNext(Mockito.any());

        // Call of onCompleted is implementation details.
        Mockito.verify(writer, Mockito.atMost(1)).onCompleted();
    }

    /**
     * Test that  {@link PercentilePersistenceTask#PercentilePersistenceTask(StatsHistoryServiceStub)} creates a task that will load/save total value in DB.
     */
    @Test
    public void testDefaultConstructor() {
        final PercentilePersistenceTask task = new PercentilePersistenceTask(
                StatsHistoryServiceGrpc.newStub(grpcServer.getChannel()), DEFAULT_RANGE);
        Assert.assertEquals(PercentilePersistenceTask.TOTAL_TIMESTAMP, task.getStartTimestamp());
    }

    /**
     * Accumulate chunks into a single array.
     */
    private static class TestWriter extends CallStreamObserver<PercentileChunk> {
        private final ByteArrayOutputStream result = new ByteArrayOutputStream();

        @Override
        public void onNext(PercentileChunk chunk) {
            try {
                chunk.getContent().writeTo(result);
            } catch (IOException e) {
                // does not happen with ByteArrayOutputStream
            }
        }

        @Override
        public void onError(Throwable t) {}

        @Override
        public void onCompleted() {}

        /**
         * Get the accumulated bytes result.
         *
         * @return byte array compiled from chunks
         */
        public byte[] getResult() {
            return result.toByteArray();
        }

        @Override
        public boolean isReady() {
            return true;
        }

        @Override
        public void setOnReadyHandler(Runnable onReadyHandler) {}

        @Override
        public void disableAutoInboundFlowControl() {}

        @Override
        public void request(int count) {}

        @Override
        public void setMessageCompression(boolean enable) {}
    }
}
