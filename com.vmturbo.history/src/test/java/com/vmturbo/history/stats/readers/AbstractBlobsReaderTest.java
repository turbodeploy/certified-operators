/*
 * (C) Turbonomic 2019.
 */
package com.vmturbo.history.stats.readers;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.protobuf.ByteString;

import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;

import org.hamcrest.CoreMatchers;
import org.jooq.DSLContext;
import org.jooq.Result;
import org.jooq.SQLDialect;
import org.jooq.Table;
import org.jooq.conf.MappedSchema;
import org.jooq.conf.RenderMapping;
import org.jooq.conf.Settings;
import org.jooq.impl.DSL;
import org.jooq.tools.jdbc.MockConnection;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.db.VmtDbException;
import com.vmturbo.history.stats.TestDataProvider;
import com.vmturbo.platform.sdk.common.util.Pair;

/**
 * Checks that {@link AbstractBlobsReaderTest} is working as expected.
 *
 * @param <Q> type of the API request that will be processed and data from history will be returned.
 * @param <C> type of the record chunks that will be returned by the reader.
 * @param <R> type of the records returned from the database.
 */
public abstract class AbstractBlobsReaderTest<Q, C, R extends org.jooq.Record> {
    /**
     * Returns the database table of the parameterized records.
     *
     * @return the database table of the parameterized records
     */
    protected abstract Table<R> getBlobsTable();

    /**
     * Returns the aggregation period of the statistics, or 0 if no aggregation.
     *
     * @return the aggregation period of the statistics, or 0 if no aggregation
     */
    protected abstract long getPeriod();

    /**
     * Returns the record class name, for debug logging purpose.
     *
     * @return the record class name
     */
    protected abstract String getRecordClassName();

    /**
     * Returns the chunk class, for debug logging purpose.
     *
     * @return the chunk class
     */
    protected abstract Class<C> getChunkClass();

    /**
     * Returns the content of the given chunk.
     *
     * @param chunk the chunk
     * @return the content of the chunk
     */
    protected abstract ByteString getChunkContent(@Nonnull C chunk);

    /**
     * Returns a new {@link AbstractBlobsReader} constructed with the given parameters.
     *
     * @param timeToWaitNetworkReadinessMs time to wait network buffer readiness to send accept
     *                                     next chunk for sending over the network.
     * @param grpcTimeoutMs GRPC interaction timeout in milliseconds
     * @param clock provides information about current time.
     * @param historydbIO provides connection to database.
     * @return a new {@link AbstractBlobsReader}
     */
    protected abstract AbstractBlobsReader newReader(int timeToWaitNetworkReadinessMs,
            long grpcTimeoutMs, @Nonnull Clock clock, @Nonnull HistorydbIO historydbIO);

    /**
     * Returns a new database record constructed with the given parameters.
     *
     * @param context the {@link DSLContext} of the database
     * @param chunk the chunk
     * @param chunkIndex the index of the chunk
     * @param timestamp the timestamp of the chunk
     * @return a new database record
     */
    protected abstract R newRecord(@Nonnull DSLContext context, @Nonnull String chunk,
            int chunkIndex, long timestamp);

    /**
     * Returns a new request constructed with the given parameters.
     *
     * @param chunkSize the size of the chunk
     * @param timestamp the timestamp of the chunk
     * @return a new request
     */
    protected abstract Q newRequest(int chunkSize, long timestamp);

    /**
     * Adds a request-to-response pair with the given timestamp and result into the list to be used
     * in the mock SQL server.
     *
     * @param sqlRequestToResponse the holding list of request-to-response SQL statements used in
     *                             the mock SQL server
     * @param timestamp the timestamp used as a parameter in the SELECT statement to add
     * @param result the result to return for the request to add
     */
    protected abstract void addSqlResult(
            @Nonnull LinkedList<Pair<Pair<String, List<?>>, ?>> sqlRequestToResponse,
            long timestamp, @Nullable Result<R> result);

    static final String TEST_DB_SCHEMA_NAME = "test_vmtdb";
    private static final long GRPC_TIMEOUT_MS = 100000;
    private static final String SOURCE_DATA = "Some_long_more_than_chunk_size_item string for test bytes";
    private static final long START_TIMESTAMP = 20L;
    private static final String TIMEOUT_EXCEPTION_FORMAT =
            "Cannot read %s data for start timestamp '%s' and period '%s' because of exceeding GRPC timeout '%s' ms";
    private static final int CHUNK_SIZE = 10;

    private AbstractBlobsReader reader;
    private HistorydbIO historyDbIo;
    private LinkedList<Pair<Pair<String, List<?>>, ?>> sqlRequestToResponse;
    private Clock clock;

    /**
     * Initializes all resources required by tests.
     *
     * @throws VmtDbException in case of error while creating/closing DB connection.
     */
    @Before
    public void before() throws VmtDbException {
        historyDbIo = Mockito.mock(HistorydbIO.class);
        final Settings settings = new Settings().withRenderMapping(new RenderMapping().withSchemata(
                new MappedSchema().withInput("vmtdb").withOutput(TEST_DB_SCHEMA_NAME)));
        sqlRequestToResponse = new LinkedList<>();
        clock = Mockito.mock(Clock.class);
        Mockito.when(clock.millis()).thenAnswer(invocation -> System.currentTimeMillis());
        reader = newReader(CHUNK_SIZE, GRPC_TIMEOUT_MS, clock, historyDbIo);
        final Connection connection =
                        new MockConnection(new TestDataProvider(sqlRequestToResponse));
        Mockito.when(historyDbIo.transConnection()).thenReturn(connection);
        Mockito.when(historyDbIo.using(Mockito.any(Connection.class)))
                        .thenReturn(DSL.using(connection, SQLDialect.MARIADB, settings));
    }

    /**
     * Checks that request to empty database will not fail with an exception.
     */
    @Test
    public void checkRequestToEmptyDatabase() {
        addSqlResult(sqlRequestToResponse, 0L, null);
        final List<C> result = new ArrayList<>();
        final StreamObserver<C> responseObserver = createStreamObserver(result, true);
        reader.processRequest(newRequest(CHUNK_SIZE, 0L), responseObserver);
        Assert.assertThat(result, CoreMatchers.is(Collections.emptyList()));
    }

    private StreamObserver<C> createStreamObserver(@Nonnull Collection<C> receivedChunks, boolean isReady) {
        Objects.requireNonNull(receivedChunks);
        @SuppressWarnings("unchecked")
        final ServerCallStreamObserver<C> result = Mockito.mock(ServerCallStreamObserver.class);
        Mockito.doAnswer(invocation -> {
            receivedChunks.add(invocation.getArgumentAt(0, getChunkClass()));
            return null;
        }).when(result).onNext(Mockito.any());
        Mockito.when(result.isReady()).thenReturn(isReady);
        return result;
    }

    /**
     * Checks that in case SQL connection will throw {@link SQLException} while connection closing,
     * then request will fail with expected type and message.
     *
     * @throws VmtDbException in case of error while reading data from the database.
     */
    @Test
    public void checkSqlException() throws VmtDbException {
        addSqlResult(sqlRequestToResponse, 0L, null);
        Mockito.when(historyDbIo.transConnection()).thenAnswer((Answer<Connection>)invocation -> {
            final MockConnection connection = Mockito.spy(new MockConnection(
                            new TestDataProvider(sqlRequestToResponse)));
            Mockito.doThrow(new SQLException("Something wrong has happened")).when(connection)
                            .close();
            return connection;
        });
        final StreamObserver<C> streamObserver = createStreamObserver(new ArrayList<>(), true);
        reader.processRequest(newRequest(CHUNK_SIZE, 0L), streamObserver);
        Mockito.verify(streamObserver, Mockito.atLeastOnce()).onError(Mockito.any());
    }

    /**
     * Checks that data read from database will be split into chunks according to chunk size value
     * specified through the API request.
     *
     * @throws IOException in case union of the received data split into chunks failed.
     */
    @Test
    public void checkBatchedResponse() throws IOException {
        final Result<R> result = createDbRecord(SOURCE_DATA);
        addSqlResult(sqlRequestToResponse, START_TIMESTAMP, result);
        final List<C> records = new ArrayList<>();
        reader.processRequest(newRequest(CHUNK_SIZE, START_TIMESTAMP), createStreamObserver(records, true));
        Assert.assertThat(records.size(), CoreMatchers.is(6));
        final ByteArrayOutputStream receivedData = new ByteArrayOutputStream();
        for (C chunk : records) {
            receivedData.write(getChunkContent(chunk).toByteArray());
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
        final Result<R> result = context.newResult(getBlobsTable());
        final String[] chunks = new String[] {"Some_long_more_than_chunk_size_item",
                        " string",
                        " for",
                        " test",
                        " bytes"};
        for (int i = 0; i < chunks.length; i++) {
            final R values = newRecord(context, chunks[i], i, START_TIMESTAMP);
            result.add(values);
        }
        addSqlResult(sqlRequestToResponse, START_TIMESTAMP, result);
        final List<C> records = new ArrayList<>();
        reader.processRequest(newRequest(CHUNK_SIZE, START_TIMESTAMP), createStreamObserver(records, true));
        Assert.assertThat(records.size(), CoreMatchers.is(8));
        final ByteArrayOutputStream receivedData = new ByteArrayOutputStream();
        for (C chunk : records) {
            receivedData.write(getChunkContent(chunk).toByteArray());
        }
        Assert.assertThat(receivedData.toString(), CoreMatchers.is(SOURCE_DATA));
    }

    private Result<R> createDbRecord(@Nonnull String sourceData) {
        Objects.requireNonNull(sourceData);
        final DSLContext context = DSL.using(SQLDialect.MARIADB);
        final Result<R> result = context.newResult(getBlobsTable());
        final R values = newRecord(context, sourceData, 0, START_TIMESTAMP);
        result.add(values);
        return result;
    }

    /**
     * Checks that reading will be interrupted by GRPC timeout.
     */
    @Test
    public void checkTimeoutDueToGrpcNotReady() {
        final Result<R> result = createDbRecord(SOURCE_DATA);
        final Queue<Long> currentTimeValues = Stream.of(0L, GRPC_TIMEOUT_MS + 1L)
                        .collect(Collectors.toCollection(LinkedList::new));
        Mockito.when(clock.millis()).thenAnswer(invocation -> currentTimeValues.poll());
        addSqlResult(sqlRequestToResponse, START_TIMESTAMP, result);
        final StreamObserver<C> streamObserver = createStreamObserver(Collections.emptyList(), false);
        reader.processRequest(newRequest(CHUNK_SIZE, START_TIMESTAMP), streamObserver);
        checkException(streamObserver, String.format(getRecordClassName(),
                TIMEOUT_EXCEPTION_FORMAT, START_TIMESTAMP, getPeriod(), GRPC_TIMEOUT_MS));
    }

    private void checkException(StreamObserver<C> streamObserver, String message) {
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
        final Result<R> result = createDbRecord(SOURCE_DATA);
        addSqlResult(sqlRequestToResponse, START_TIMESTAMP, result);
        final List<C> records = new ArrayList<>();
        final boolean isReady = true;
        final ServerCallStreamObserver<C> streamObserver =
                (ServerCallStreamObserver<C>)createStreamObserver(records, isReady);
        Mockito.when(streamObserver.isReady()).thenAnswer(new SwitcherAnswer(isReady));
        reader.processRequest(newRequest(CHUNK_SIZE, START_TIMESTAMP), streamObserver);
        final ByteArrayOutputStream receivedData = new ByteArrayOutputStream();
        for (C chunk : records) {
            receivedData.write(getChunkContent(chunk).toByteArray());
        }
        Mockito.verify(streamObserver, Mockito.times(1)).onCompleted();
        Assert.assertThat(receivedData.toString(), CoreMatchers.is(SOURCE_DATA));
    }

    /**
     * Checks that reading will be interrupted by GRPC timeout after successful reading one chunk.
     */
    @Test
    public void checkTimeoutWhenStreamObserverStateChanges() {
        final Result<R> result = createDbRecord(SOURCE_DATA);
        final Queue<Long> currentTimeValues = Stream.of(0L, GRPC_TIMEOUT_MS, GRPC_TIMEOUT_MS, GRPC_TIMEOUT_MS + 1L)
                .collect(Collectors.toCollection(LinkedList::new));
        Mockito.when(clock.millis()).thenAnswer(invocation -> currentTimeValues.poll());
        addSqlResult(sqlRequestToResponse, START_TIMESTAMP, result);
        final List<C> records = new ArrayList<>();
        final boolean isReady = true;
        final ServerCallStreamObserver<C> streamObserver =
                (ServerCallStreamObserver<C>)createStreamObserver(records, isReady);
        Mockito.when(streamObserver.isReady()).thenAnswer(new SwitcherAnswer(isReady));
        Mockito.doAnswer(invocation -> {
            Mockito.when(streamObserver.isReady()).thenReturn(false);
            return null;
        }).when(streamObserver).onNext(Mockito.any());
        reader.processRequest(newRequest(CHUNK_SIZE, START_TIMESTAMP), streamObserver);
        checkException(streamObserver, String.format(TIMEOUT_EXCEPTION_FORMAT,
                getRecordClassName(), START_TIMESTAMP, getPeriod(), GRPC_TIMEOUT_MS));
        Mockito.verify(streamObserver, Mockito.atLeastOnce()).onNext(Mockito.any());
    }

    /**
     * Class for alternating a boolean state as the answer.
     */
    private static class SwitcherAnswer implements Answer<Boolean> {
        private boolean booleanAnswer;

        /**
         * Creates {@link SwitcherAnswer} instance.
         *
         * @param booleanAnswer initial state.
         */
        SwitcherAnswer(boolean booleanAnswer) {
            this.booleanAnswer = booleanAnswer;
        }

        @Override
        public Boolean answer(@Nullable InvocationOnMock invocationOnMock) throws Throwable {
            booleanAnswer = !booleanAnswer;
            return booleanAnswer;
        }
    }
}
