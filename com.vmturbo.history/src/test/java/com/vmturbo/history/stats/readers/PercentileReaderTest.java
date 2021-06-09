/*
 * (C) Turbonomic 2019.
 */
package com.vmturbo.history.stats.readers;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;

import org.hamcrest.CoreMatchers;
import org.jooq.DSLContext;
import org.jooq.Result;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.jooq.tools.jdbc.MockConnection;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.vmturbo.common.protobuf.stats.Stats.GetPercentileCountsRequest;
import com.vmturbo.common.protobuf.stats.Stats.PercentileChunk;
import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.db.VmtDbException;
import com.vmturbo.history.schema.abstraction.tables.PercentileBlobs;
import com.vmturbo.history.schema.abstraction.tables.records.PercentileBlobsRecord;
import com.vmturbo.history.stats.TestDataProvider;
import com.vmturbo.platform.sdk.common.util.Pair;

/**
 * Checks that {@link PercentileReader} is working as expected.
 */
public class PercentileReaderTest {

    private static final PercentileBlobs PERCENTILE_BLOBS_TABLE = PercentileBlobs.PERCENTILE_BLOBS;
    private static final long GRPC_TIMEOUT_MS = 100000;
    private static final String SOURCE_DATA = "Some_long_more_than_chunk_size_item string for test bytes";
    private static final long START_TIMESTAMP = 20L;
    private static final long PERIOD = 3L;
    private static final String TIMEOUT_EXCEPTION_FORMAT =
            "Cannot read percentile data for start timestamp '%s' and period '%s' because of exceeding GRPC timeout '%s' ms";
    private static final int CHUNK_SIZE = 10;

    private HistorydbIO historyDbIo;
    private LinkedList<Pair<Pair<String, List<?>>, ?>> sqlRequestToResponse;
    private PercentileReader percentileReader;
    private Clock clock;

    /**
     * Initializes all resources required by tests.
     *
     * @throws VmtDbException in case of error while creating/closing DB
     *                 connection.
     */
    @Before
    public void before() throws VmtDbException {
        historyDbIo = Mockito.mock(HistorydbIO.class);
        sqlRequestToResponse = new LinkedList<>();
        clock = Mockito.mock(Clock.class);
        Mockito.when(clock.millis()).thenAnswer(invocation -> System.currentTimeMillis());
        percentileReader = new PercentileReader(CHUNK_SIZE, GRPC_TIMEOUT_MS, clock, historyDbIo);
        final Connection connection =
                        new MockConnection(new TestDataProvider(sqlRequestToResponse));
        Mockito.when(historyDbIo.transConnection()).thenReturn(connection);
        Mockito.when(historyDbIo.using(Mockito.any(Connection.class)))
                        .thenReturn(DSL.using(connection, SQLDialect.MARIADB));
    }

    /**
     * Checks that request to empty database will not fail with an exception.
     */
    @Test
    public void checkRequestToEmptyDatabase() {
        sqlRequestToResponse.add(Pair.create(
                        Pair.create("select `vmtdb`.`percentile_blobs`.`start_timestamp`",
                                        Collections.singletonList(0L)), null));
        final List<PercentileChunk> result = new ArrayList<>();
        final StreamObserver<PercentileChunk> responseObserver = createStreamObserver(result, true);
        percentileReader.processRequest(
                        GetPercentileCountsRequest.newBuilder().setChunkSize(CHUNK_SIZE).build(),
                        responseObserver);
        Assert.assertThat(result, CoreMatchers.is(Collections.emptyList()));
    }

    private static StreamObserver<PercentileChunk> createStreamObserver(
                    Collection<PercentileChunk> receivedChunks, boolean isReady) {
        @SuppressWarnings("unchecked")
        final ServerCallStreamObserver<PercentileChunk> result =
                        Mockito.mock(ServerCallStreamObserver.class);
        Mockito.doAnswer(invocation -> {
            receivedChunks.add(invocation.getArgumentAt(0, PercentileChunk.class));
            return null;
        }).when(result).onNext(Mockito.any());
        Mockito.when(result.isReady()).thenReturn(isReady);
        return result;
    }

    /**
     * Checks that in case SQL connection will throw {@link SQLException} while connection closing,
     * then request will fail with expected type and message.
     *
     * @throws VmtDbException in case of error while reading data from the
     *                 database.
     */
    @Test
    public void checkSqlException() throws VmtDbException {
        sqlRequestToResponse.add(Pair.create(
                        Pair.create("select `vmtdb`.`percentile_blobs`.`start_timestamp`",
                                        Collections.singletonList(0L)), null));
        Mockito.when(historyDbIo.transConnection()).thenAnswer((Answer<Connection>)invocation -> {
            final MockConnection connection = Mockito.spy(new MockConnection(
                            new TestDataProvider(sqlRequestToResponse)));
            Mockito.doThrow(new SQLException("Something wrong has happened")).when(connection)
                            .close();
            return connection;
        });
        final StreamObserver<PercentileChunk> streamObserver =
                        createStreamObserver(new ArrayList<>(), true);
        percentileReader.processRequest(
                        GetPercentileCountsRequest.newBuilder().setChunkSize(CHUNK_SIZE).build(),
                        streamObserver);
        Mockito.verify(streamObserver, Mockito.atLeastOnce()).onError(Mockito.any());
    }

    /**
     * Checks that data read from database will be split into chunks according to chunk size value
     * specified through the API request.
     *
     * @throws IOException in case union of the received data split into chunks
     *                 failed.
     */
    @Test
    public void checkBatchedResponse() throws IOException {
        final Result<PercentileBlobsRecord> result = createDbRecord(SOURCE_DATA);
        sqlRequestToResponse.add(Pair.create(
                        Pair.create("select `vmtdb`.`percentile_blobs`.`start_timestamp`",
                                        Collections.singletonList(START_TIMESTAMP)), result));
        final List<PercentileChunk> records = new ArrayList<>();
        percentileReader.processRequest(GetPercentileCountsRequest.newBuilder().setChunkSize(
                CHUNK_SIZE)
                        .setStartTimestamp(START_TIMESTAMP).build(), createStreamObserver(records, true));
        Assert.assertThat(records.size(), CoreMatchers.is(6));
        final ByteArrayOutputStream receivedData = new ByteArrayOutputStream();
        for (PercentileChunk chunk : records) {
            receivedData.write(chunk.getContent().toByteArray());
        }
        Assert.assertThat(receivedData.toString(), CoreMatchers.is(SOURCE_DATA));
    }

    /**
     * Checked that multiple blobs will be combined into one data blob that are going to be send
     * from the reader.
     *
     * @throws IOException in case of error during reading from the DB.
     */
    @Test
    public void checkChunkedBlob() throws IOException {
        final DSLContext context = DSL.using(SQLDialect.MARIADB);
        final Result<PercentileBlobsRecord> result = context.newResult(PERCENTILE_BLOBS_TABLE);
        final String[] chunks = new String[] {"Some_long_more_than_chunk_size_item",
                        " string",
                        " for",
                        " test",
                        " bytes"};
        for (int i = 0; i < chunks.length; i++) {
            final PercentileBlobsRecord values = context.newRecord(PERCENTILE_BLOBS_TABLE)
                            .values(START_TIMESTAMP, PERIOD, chunks[i].getBytes(StandardCharsets.UTF_8), i);
            result.add(values);
        }
        sqlRequestToResponse.add(Pair.create(
                        Pair.create("select `vmtdb`.`percentile_blobs`.`start_timestamp`",
                                        Collections.singletonList(START_TIMESTAMP)), result));
        final List<PercentileChunk> records = new ArrayList<>();
        percentileReader.processRequest(GetPercentileCountsRequest.newBuilder().setChunkSize(
                CHUNK_SIZE)
                        .setStartTimestamp(START_TIMESTAMP).build(), createStreamObserver(records, true));
        Assert.assertThat(records.size(), CoreMatchers.is(8));
        final ByteArrayOutputStream receivedData = new ByteArrayOutputStream();
        for (PercentileChunk chunk : records) {
            receivedData.write(chunk.getContent().toByteArray());
        }
        Assert.assertThat(receivedData.toString(), CoreMatchers.is(SOURCE_DATA));
    }

    private static Result<PercentileBlobsRecord> createDbRecord(String sourceData) {
        final DSLContext context = DSL.using(SQLDialect.MARIADB);
        final Result<PercentileBlobsRecord> result = context.newResult(PERCENTILE_BLOBS_TABLE);
        final PercentileBlobsRecord values = context.newRecord(PERCENTILE_BLOBS_TABLE)
                        .values(START_TIMESTAMP, PERIOD, sourceData.getBytes(), 0);
        result.add(values);
        return result;
    }

    /**
     * Checks that reading will be interrupted by GRPC timeout.
     */
    @Test
    public void checkTimeoutDueToGrpcNotReady() {
        final Result<PercentileBlobsRecord> result = createDbRecord(SOURCE_DATA);
        final Queue<Long> currentTimeValues = Stream.of(0L, GRPC_TIMEOUT_MS + 1L)
                        .collect(Collectors.toCollection(LinkedList::new));
        Mockito.when(clock.millis()).thenAnswer(invocation -> currentTimeValues.poll());
        sqlRequestToResponse.add(Pair.create(
                Pair.create("select `vmtdb`.`percentile_blobs`.`start_timestamp`",
                        Collections.singletonList(START_TIMESTAMP)), result));
        final StreamObserver<PercentileChunk> streamObserver =
                        createStreamObserver(Collections.emptyList(), false);
        percentileReader.processRequest(GetPercentileCountsRequest.newBuilder().setChunkSize(
                CHUNK_SIZE).setStartTimestamp(START_TIMESTAMP).build(), streamObserver);
        checkException(streamObserver,
                String.format(TIMEOUT_EXCEPTION_FORMAT, START_TIMESTAMP, PERIOD, GRPC_TIMEOUT_MS));
    }

    private void checkException(StreamObserver<PercentileChunk> streamObserver,
            String message) {
        final ArgumentCaptor<Throwable> errorCaptor = ArgumentCaptor.forClass(Throwable.class);
        Mockito.verify(streamObserver, Mockito.atLeastOnce()).onError(errorCaptor.capture());
        Assert.assertThat(errorCaptor.getValue().getMessage(), CoreMatchers.containsString(message));
    }

    /**
     * Checks the successful reading of data in parts when the ready state of the stream observer is
     * constantly switched.
     *
     * @throws IOException in case of error during reading from the DB.
     */
    @Test
    public void checkReadInChunksWhenStreamObserverStateChanges() throws IOException {
        final Result<PercentileBlobsRecord> result = createDbRecord(SOURCE_DATA);
        sqlRequestToResponse.add(Pair.create(
                Pair.create("select `vmtdb`.`percentile_blobs`.`start_timestamp`",
                        Collections.singletonList(START_TIMESTAMP)), result));
        final List<PercentileChunk> records = new ArrayList<>();
        final boolean isReady = true;
        final ServerCallStreamObserver<PercentileChunk> streamObserver =
                (ServerCallStreamObserver<PercentileChunk>)createStreamObserver(records, isReady);
        Mockito.when(streamObserver.isReady()).thenAnswer(new SwitcherAnswer(isReady));
        percentileReader.processRequest(GetPercentileCountsRequest.newBuilder()
                .setChunkSize(CHUNK_SIZE)
                .setStartTimestamp(START_TIMESTAMP)
                .build(), streamObserver);
        final ByteArrayOutputStream receivedData = new ByteArrayOutputStream();
        for (PercentileChunk chunk : records) {
            receivedData.write(chunk.getContent().toByteArray());
        }
        Mockito.verify(streamObserver, Mockito.times(1)).onCompleted();
        Assert.assertThat(receivedData.toString(), CoreMatchers.is(SOURCE_DATA));
    }

    /**
     * Checks that reading will be interrupted by GRPC timeout after successful reading one chunk.
     */
    @Test
    public void checkTimeoutWhenStreamObserverStateChanges() {
        final Result<PercentileBlobsRecord> result = createDbRecord(SOURCE_DATA);
        final Queue<Long> currentTimeValues = Stream.of(0L, GRPC_TIMEOUT_MS, GRPC_TIMEOUT_MS, GRPC_TIMEOUT_MS + 1L)
                .collect(Collectors.toCollection(LinkedList::new));
        Mockito.when(clock.millis()).thenAnswer(invocation -> currentTimeValues.poll());
        sqlRequestToResponse.add(Pair.create(
                Pair.create("select `vmtdb`.`percentile_blobs`.`start_timestamp`",
                        Collections.singletonList(START_TIMESTAMP)), result));
        final List<PercentileChunk> records = new ArrayList<>();
        final boolean isReady = true;
        final ServerCallStreamObserver<PercentileChunk> streamObserver =
                (ServerCallStreamObserver<PercentileChunk>)createStreamObserver(records, isReady);
        Mockito.when(streamObserver.isReady()).thenAnswer(new SwitcherAnswer(isReady));
        Mockito.doAnswer(invocation -> {
            Mockito.when(streamObserver.isReady()).thenReturn(false);
            return null;
        }).when(streamObserver).onNext(Mockito.any());
        percentileReader.processRequest(GetPercentileCountsRequest.newBuilder().setChunkSize(
                CHUNK_SIZE).setStartTimestamp(START_TIMESTAMP).build(), streamObserver);
        checkException(streamObserver,
                String.format(TIMEOUT_EXCEPTION_FORMAT, START_TIMESTAMP, PERIOD, GRPC_TIMEOUT_MS));
        Mockito.verify(streamObserver, Mockito.atLeastOnce()).onNext(Mockito.any());
    }

    /**
     * Class for changing the state of {@link StreamObserver}.
     */
    private static class SwitcherAnswer implements Answer<Boolean> {
        private boolean isReady;

        /**
         * Creates {@link SwitcherAnswer} instance.
         *
         * @param isReady the state of {@link StreamObserver}.
         */
        SwitcherAnswer(boolean isReady) {
            this.isReady = isReady;
        }

        @Override
        public Boolean answer(InvocationOnMock invocationOnMock) throws Throwable {
            isReady = !isReady;
            return isReady;
        }
    }
}
