/*
 * (C) Turbonomic 2019.
 */
package com.vmturbo.history.stats.readers;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

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

    /**
     * Helps to check that testing code is throwing exceptions of the desired type with desired
     * causes and desired messages.
     */
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private HistorydbIO historyDbIo;
    private LinkedList<Pair<Pair<String, List<?>>, ?>> sqlRequestToResponse;
    private PercentileReader percentileReader;

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
        percentileReader = new PercentileReader(historyDbIo);
        final Connection connection = new MockConnection(
                        new TestDataProvider(sqlRequestToResponse));
        Mockito.when(historyDbIo.transConnection()).thenReturn(connection);
        Mockito.when(historyDbIo.using(Mockito.any(Connection.class)))
                        .thenReturn(DSL.using(connection, SQLDialect.MARIADB));
    }

    /**
     * Checks that request to empty database will not fail with an exception.
     *
     * @throws VmtDbException in case of error while reading data from the
     *                 database.
     */
    @Test
    public void checkRequestToEmptyDatabase() throws VmtDbException {
        sqlRequestToResponse.add(Pair.create(
                        Pair.create("select `vmtdb`.`percentile_blobs`.`start_timestamp`",
                                        Collections.singletonList(new Timestamp(0))), null));
        final List<PercentileChunk> result = new ArrayList<>();
        final StreamObserver<PercentileChunk> responseObserver = createStreamObserver(result);
        percentileReader.processRequest(
                        GetPercentileCountsRequest.newBuilder().setChunkSize(10).build(),
                        responseObserver);
        Assert.assertThat(result, CoreMatchers.is(Collections.emptyList()));
    }

    private StreamObserver<PercentileChunk> createStreamObserver(
                    List<PercentileChunk> result) {
        return new StreamObserver<PercentileChunk>() {
            @Override
            public void onNext(PercentileChunk percentileChunk) {
                result.add(percentileChunk);
            }

            @Override
            public void onError(Throwable throwable) {

            }

            @Override
            public void onCompleted() {

            }
        };
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
                                        Collections.singletonList(new Timestamp(0))), null));
        Mockito.when(historyDbIo.transConnection()).thenAnswer((Answer<Connection>)invocation -> {
            final MockConnection connection = Mockito.spy(new MockConnection(
                            new TestDataProvider(sqlRequestToResponse)));
            Mockito.doThrow(new SQLException("Something wrong has happened")).when(connection)
                            .close();
            return connection;
        });
        final StreamObserver<PercentileChunk> streamObserver =
                        Mockito.spy(createStreamObserver(new ArrayList<>()));
        percentileReader.processRequest(
                        GetPercentileCountsRequest.newBuilder().setChunkSize(10).build(),
                        streamObserver);
        Mockito.verify(streamObserver, Mockito.atLeastOnce()).onError(Mockito.any());
    }

    /**
     * Checks that data read from database will be split into chunks according to chunk size value
     * specified through the API request.
     *
     * @throws VmtDbException in case reading data from the database failed
     * @throws IOException in case union of the received data split into chunks
     *                 failed.
     */
    @Test
    public void checkBatchedResponse() throws VmtDbException, IOException {
        final DSLContext context = DSL.using(SQLDialect.MARIADB);
        final Result<PercentileBlobsRecord> result = context.newResult(PERCENTILE_BLOBS_TABLE);
        final String sourceData = "Some string for test bytes";
        final PercentileBlobsRecord values = context.newRecord(PERCENTILE_BLOBS_TABLE)
                        .values(new Timestamp(20), 3L, sourceData.getBytes());
        result.add(values);
        sqlRequestToResponse.add(Pair.create(
                        Pair.create("select `vmtdb`.`percentile_blobs`.`start_timestamp`",
                                        Collections.singletonList(new Timestamp(20))), result));
        final List<PercentileChunk> records = new ArrayList<>();
        percentileReader.processRequest(GetPercentileCountsRequest.newBuilder().setChunkSize(10)
                        .setStartTimestamp(20).build(), createStreamObserver(records));
        Assert.assertThat(records.size(), CoreMatchers.is(3));
        final ByteArrayOutputStream receivedData = new ByteArrayOutputStream();
        for (PercentileChunk chunk : records) {
            receivedData.write(chunk.getContent().toByteArray());
        }
        Assert.assertThat(receivedData.toString(), CoreMatchers.is(sourceData));
    }

}
