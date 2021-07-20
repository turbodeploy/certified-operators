/*
 * (C) Turbonomic 2019.
 */
package com.vmturbo.history.stats.writers;

import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.protobuf.ByteString;

import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;

import org.apache.commons.io.IOUtils;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Matchers;
import org.jooq.SQLDialect;
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
import org.mockito.stubbing.Answer;

import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.db.VmtDbException;
import com.vmturbo.history.stats.TestDataProvider;
import com.vmturbo.platform.sdk.common.util.Pair;

/**
 * Checks that {@link AbstractBlobsWriter} implementation is working as expected.
 *
 * @param <P> type of the API response to return to the write request.
 * @param <C> type of the record chunks from the write request.
 */
public abstract class AbstractBlobsWriterTest<P, C> {
    /**
     * Returns a new {@link AbstractBlobsWriter} constructed with the given parameters.
     *
     * @param responseObserver the {@link StreamObserver} for the SQL response
     * @param historydbIO provides connection to database.
     * @return a new {@link AbstractBlobsWriter}
     */
    protected abstract AbstractBlobsWriter newWriter(@Nonnull StreamObserver<P> responseObserver,
            @Nonnull HistorydbIO historydbIO);

    /**
     * Returns a new chunk of the statistics constructed with the given parameters.
     *
     * @param period the aggregation period of the statistics
     * @param startTimestamp the timestamp of the chunk
     * @param data the data of the chunk
     * @return the constructed new chunk
     */
    protected abstract C newChunk(@Nullable Long period, long startTimestamp, @Nonnull ByteString data);

    /**
     * Returns the delete statement to be registered in the mock SQL server.
     *
     * @return the delete statement to be registered in the mock SQL server
     */
    protected abstract String deleteStatement();

    /**
     * Registers an insert statement in the mock SQL server.
     *
     * @return the insert statement to be registered in the mock SQL server
     */
    protected abstract String addInsertStatement();

    /**
     * Returns the aggregation period of the statistics.
     *
     * @return the aggregation period of the statistics
     */
    protected abstract Long getPeriod();

    static final String TEST_DB_SCHEMA_NAME = "test_vmtdb";
    private static final String SQL_EXCEPTION_MESSAGE = "Something wrong has happened";
    private static final String TEST_DATA = "Test data bytes to be stored in database";
    static final long START_TIMESTAMP = 21L;

    private HistorydbIO historyDbIo;
    protected LinkedList<Pair<Pair<String, List<?>>, ?>> sqlRequestToResponse;
    private StreamObserver<P> streamObserver;
    private MockConnection connection;

    /**
     * Initializes all resources required by tests.
     */
    @Before
    public void before() {
        historyDbIo = Mockito.mock(HistorydbIO.class);
        final Settings settings = new Settings().withRenderMapping(new RenderMapping().withSchemata(
                new MappedSchema().withInput("vmtdb").withOutput(TEST_DB_SCHEMA_NAME)));
        Mockito.when(historyDbIo.JooqBuilder())
                        .thenReturn(DSL.using(connection, SQLDialect.MARIADB, settings));
        sqlRequestToResponse = new LinkedList<>();
        connection = Mockito.spy(new MockConnection(
                        new TestDataProvider(sqlRequestToResponse)));
        streamObserver = mockStreamObserver();
    }

    private static <V> StreamObserver<V> mockStreamObserver() {
        @SuppressWarnings("unchecked")
        final StreamObserver<V> result = (StreamObserver<V>)Mockito.mock(StreamObserver.class);
        return result;
    }

    /**
     * Checks that data writing should be done successfully.
     *
     * @throws VmtDbException in case of error while creating/closing DB connection.
     * @throws IOException in case of error while manipulating piped streams(connecting, reading, writing).
     * @throws SQLException in case of error while executing SQL expression.
     */
    @Test
    public void checkDataUpdated() throws VmtDbException, IOException, SQLException {
        sqlRequestToResponse.add(Pair.create(Pair.create(deleteStatement(),
                        Collections.singletonList(START_TIMESTAMP)), null));
        addInsertStatement();
        final AtomicReference<InputStream> writingData = new AtomicReference<>();
        Mockito.when(connection.prepareStatement(Mockito.anyString()))
                        .thenAnswer((Answer<PreparedStatement>)invocation -> {
                            final PreparedStatement result =
                                            Mockito.spy((PreparedStatement)invocation
                                                            .callRealMethod());
                            Mockito.doAnswer((Answer<Object>)invocation1 -> {
                                writingData.set(invocation1.getArgumentAt(1, InputStream.class));
                                return null;
                            }).when(result).setBinaryStream(Mockito.anyInt(),
                                            Mockito.any(InputStream.class));
                            return result;
                        });
        Mockito.when(historyDbIo.transConnection()).thenReturn(connection);
        final StreamObserver<C> writer = newWriter(streamObserver, historyDbIo);
        writer.onNext(newChunk(getPeriod(), START_TIMESTAMP, ByteString.copyFrom(TEST_DATA, StandardCharsets.UTF_8)));
        final String actual = getDataToWriteInDb(writingData);
        writer.onCompleted();
        Assert.assertThat(actual, CoreMatchers.is(TEST_DATA));
    }

    /**
     * Checks the case when reader(prepared statement thread) failed due to some {@link
     * SQLException} and piped buffer became full. In this case it is expected that we will print a
     * comprehensive description about what have failed.
     *
     * @throws VmtDbException in case of error while creating/closing DB connection.
     * @throws SQLException in case of error while executing SQL expression.
     */
    @Test
    public void checkPreparedStatementFailure() throws SQLException, VmtDbException {
        sqlRequestToResponse.add(Pair.create(Pair.create(deleteStatement(),
                        Collections.singletonList(START_TIMESTAMP)), null));
        final String insertStatement = addInsertStatement();
        final AtomicReference<InputStream> writingData = new AtomicReference<>();
        Mockito.when(connection.prepareStatement(Mockito.anyString())).thenAnswer(
                (Answer<PreparedStatement>)invocation -> {
                    final PreparedStatement result = Mockito.spy(
                            (PreparedStatement)invocation.callRealMethod());
                    Mockito.doAnswer((Answer<Object>)invocation1 -> {
                        writingData.set(invocation1.getArgumentAt(1, InputStream.class));
                        return null;
                    }).when(result).setBinaryStream(Mockito.anyInt(),
                            Mockito.any(InputStream.class));
                    if (invocation.getArgumentAt(0, String.class).startsWith(insertStatement)) {
                        Mockito.doAnswer((answer) -> {
                            final InputStream inputStream = writingData.get();
                            IOUtils.readFully(inputStream, TEST_DATA.length());
                            throw new SQLException(SQL_EXCEPTION_MESSAGE);
                        }).when(result).execute();
                    }
                    return result;
                });
        Mockito.when(historyDbIo.transConnection()).thenReturn(connection);
        final StreamObserver<C> writer = newWriter(streamObserver, historyDbIo);
        writer.onNext(newChunk(getPeriod(), START_TIMESTAMP, ByteString.copyFrom(TEST_DATA, StandardCharsets.UTF_8)));
        final byte[] bytes = new byte[1026];
        Arrays.fill(bytes, (byte)1);
        writer.onNext(newChunk(getPeriod(), START_TIMESTAMP, ByteString.copyFrom(bytes)));
        writer.onNext(newChunk(getPeriod(), START_TIMESTAMP, ByteString.copyFrom(bytes)));
        final ArgumentCaptor<Throwable> errorCaptor = ArgumentCaptor.forClass(Throwable.class);
        Mockito.verify(streamObserver, Mockito.only()).onError(errorCaptor.capture());
        final Throwable error = errorCaptor.getAllValues().get(0);
        checkThrowable(error, Status.INTERNAL.getCode().name(), StatusException.class);
        final Throwable ioCause = error.getCause();
        checkThrowable(ioCause, SQL_EXCEPTION_MESSAGE, SQLException.class);
    }

    /**
     * Checks the case when reader(prepared statement thread) died because of interrupted exception
     * or something unexpected and piped buffer became full. In this case it is expected that we
     * will print a comprehensive description about what have failed.
     *
     * @throws VmtDbException in case of error while creating/closing DB connection.
     * @throws SQLException in case of error while executing SQL expression.
     */
    @Test
    public void checkPreparedStatementInterrupted() throws SQLException, VmtDbException {
        sqlRequestToResponse.add(Pair.create(Pair.create(deleteStatement(),
                        Collections.singletonList(START_TIMESTAMP)), null));
        final String insertStatement = addInsertStatement();
        final AtomicReference<InputStream> writingData = new AtomicReference<>();
        Mockito.when(connection.prepareStatement(Mockito.anyString())).thenAnswer(
                (Answer<PreparedStatement>)invocation -> {
                    final PreparedStatement result = Mockito.spy(
                            (PreparedStatement)invocation.callRealMethod());
                    Mockito.doAnswer((Answer<Object>)invocation1 -> {
                        writingData.set(invocation1.getArgumentAt(1, InputStream.class));
                        return null;
                    }).when(result).setBinaryStream(Mockito.anyInt(),
                            Mockito.any(InputStream.class));
                    if (invocation.getArgumentAt(0, String.class).startsWith(insertStatement)) {
                        Mockito.doAnswer((answer) -> {
                            final InputStream inputStream = writingData.get();
                            IOUtils.readFully(inputStream, TEST_DATA.length());
                            throw new InterruptedIOException();
                        }).when(result).execute();
                    }
                    return result;
                });
        Mockito.when(historyDbIo.transConnection()).thenReturn(connection);
        final StreamObserver<C> writer = newWriter(streamObserver, historyDbIo);
        writer.onNext(newChunk(getPeriod(), START_TIMESTAMP, ByteString.copyFrom(TEST_DATA, StandardCharsets.UTF_8)));
        final byte[] bytes = new byte[1026];
        Arrays.fill(bytes, (byte)1);
        writer.onNext(newChunk(getPeriod(), START_TIMESTAMP, ByteString.copyFrom(bytes)));
        final ArgumentCaptor<Throwable> errorCaptor = ArgumentCaptor.forClass(Throwable.class);
        Mockito.verify(streamObserver, Mockito.only()).onError(errorCaptor.capture());
        final Throwable error = errorCaptor.getValue();
        checkThrowable(error, Status.INTERNAL.getCode().name(), StatusException.class);
        final Throwable originalException = error.getCause();
        checkThrowable(originalException, null, InterruptedIOException.class);
    }

    private static <V extends Throwable> void checkThrowable(@Nonnull Throwable cause,
            @Nullable String message, @Nullable Class<V> exceptionType) {
        Objects.requireNonNull(cause);
        Assert.assertThat(cause, CoreMatchers.instanceOf(exceptionType));
        Assert.assertThat(cause.getMessage(), CoreMatchers.is(message));
    }

    private static String getDataToWriteInDb(@Nonnull AtomicReference<InputStream> writingData)
                    throws IOException {
        final InputStream input = Objects.requireNonNull(writingData).get();
        final int available = input.available();
        final byte[] data = new byte[available];
        input.read(data, 0, available);
        return new String(data, StandardCharsets.UTF_8);
    }

    /**
     * Checks that {@link VmtDbException} is throwing in case of exception while closing DB
     * connection.
     *
     * @throws VmtDbException in case of exception while closing/opening DB connection.
     */
    @Test
    public void checkSqlExceptionOnCommitConnection() throws VmtDbException {
        sqlRequestToResponse.add(Pair.create(Pair.create(deleteStatement(),
                        Collections.singletonList(new Timestamp(START_TIMESTAMP))), null));
        addInsertStatement();
        Mockito.when(historyDbIo.transConnection()).thenAnswer((Answer<Connection>)invocation -> {
            final PreparedStatement mockedStatement = Mockito.mock(PreparedStatement.class);
            Mockito.when(connection.prepareStatement(Mockito.anyString()))
                            .thenReturn(mockedStatement);
            Mockito.doThrow(new SQLException(SQL_EXCEPTION_MESSAGE)).when(connection).commit();
            return connection;
        });
        Mockito.doAnswer((Answer<Object>)invocation -> {
            final Throwable exception = invocation.getArgumentAt(0, Throwable.class);
            Assert.assertThat(exception.getCause().getMessage(),
                            Matchers.containsString(SQL_EXCEPTION_MESSAGE));
            return null;
        }).when(streamObserver).onError(Mockito.any());
        final StreamObserver<C> writer = newWriter(streamObserver, historyDbIo);
        writer.onNext(newChunk(getPeriod(), START_TIMESTAMP, ByteString.EMPTY));
        writer.onCompleted();
        Mockito.verify(streamObserver, Mockito.times(1)).onError(Mockito.any());
    }

    /**
     * Checks that {@link StreamObserver} will provide information about error in case {@link
     * SQLException} will happen while data reading process.
     *
     * @throws VmtDbException in case of exception while closing/opening DB connection.
     */
    @Test
    public void checkSqlExceptionOnConnectionCreation() throws VmtDbException {
        sqlRequestToResponse.add(Pair.create(Pair.create(deleteStatement(),
                        Collections.singletonList(new Timestamp(START_TIMESTAMP))), null));
        addInsertStatement();
        Mockito.when(historyDbIo.transConnection())
                        .thenThrow(new VmtDbException(VmtDbException.CONN_POOL_STARTUP,
                                        SQL_EXCEPTION_MESSAGE));
        Mockito.doAnswer((Answer<Object>)invocation -> {
            final Throwable exception = invocation.getArgumentAt(0, Throwable.class);
            Assert.assertThat(exception.getCause().getMessage(),
                            Matchers.containsString("Error initializing connection pool"));
            return null;
        }).when(streamObserver).onError(Mockito.any());
        final StreamObserver<C> writer = newWriter(streamObserver, historyDbIo);
        writer.onNext(newChunk(getPeriod(), START_TIMESTAMP, ByteString.EMPTY));
        Mockito.verify(streamObserver, Mockito.times(1)).onError(Mockito.any());
    }
}
