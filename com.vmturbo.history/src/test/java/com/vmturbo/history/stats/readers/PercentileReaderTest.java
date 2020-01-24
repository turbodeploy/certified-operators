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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
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
    private static final int GRPC_TIMEOUT_MS = 100000;
    private static final String SOURCE_DATA = "Some string for test bytes";

    /**
     * Helps to check that testing code is throwing exceptions of the desired type with desired
     * causes and desired messages.
     */
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

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
        percentileReader = new PercentileReader(10, GRPC_TIMEOUT_MS, clock, historyDbIo);
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
                        GetPercentileCountsRequest.newBuilder().setChunkSize(10).build(),
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
                        GetPercentileCountsRequest.newBuilder().setChunkSize(10).build(),
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
                                        Collections.singletonList(20L)), result));
        final List<PercentileChunk> records = new ArrayList<>();
        percentileReader.processRequest(GetPercentileCountsRequest.newBuilder().setChunkSize(10)
                        .setStartTimestamp(20).build(), createStreamObserver(records, true));
        Assert.assertThat(records.size(), CoreMatchers.is(3));
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
                        .values(20L, 3L, sourceData.getBytes());
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
                                        Collections.singletonList(0L)), result));
        final StreamObserver<PercentileChunk> streamObserver =
                        createStreamObserver(Collections.emptyList(), false);
        percentileReader.processRequest(GetPercentileCountsRequest.newBuilder().setChunkSize(10)
                        .setStartTimestamp(0).build(), streamObserver);
        final ArgumentCaptor<Throwable> errorCaptor = ArgumentCaptor.forClass(Throwable.class);
        Mockito.verify(streamObserver, Mockito.atLeastOnce()).onError(errorCaptor.capture());
        Assert.assertThat(errorCaptor.getValue().getMessage(),
                        CoreMatchers.containsString("because of exceeding GRPC timeout"));
    }

}
