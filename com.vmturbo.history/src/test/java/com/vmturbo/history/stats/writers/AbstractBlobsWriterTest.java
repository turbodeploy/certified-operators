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
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.sql.DataSource;

import com.google.protobuf.ByteString;

import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;

import org.apache.commons.io.IOUtils;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Matchers;
import org.jooq.exception.DataAccessException;
import org.jooq.tools.jdbc.MockConnection;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import com.vmturbo.history.stats.TestDataProvider;
import com.vmturbo.history.stats.TestDataProvider.SqlWithResponse;

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
     * @param dataSource provides connection to database.
     * @return a new {@link AbstractBlobsWriter}
     */
    protected abstract AbstractBlobsWriter newWriter(@Nonnull StreamObserver<P> responseObserver,
            @Nonnull DataSource dataSource);

    /**
     * Returns a new chunk of the statistics constructed with the given parameters.
     *
     * @param period the aggregation period of the statistics
     * @param startTimestamp the timestamp of the chunk
     * @param data the data of the chunk
     * @return the constructed new chunk
     */
    protected abstract C newChunk(@Nullable Long period, long startTimestamp,
            @Nonnull ByteString data);

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

    protected final Queue<SqlWithResponse> sqlWithResponses = new ArrayDeque<>();
    private final StreamObserver<P> streamObserver = mockStreamObserver();
    private final DataSource dataSource = Mockito.mock(DataSource.class);
    private final Connection spiedConnection = Mockito.spy(
            new MockConnection(new TestDataProvider(sqlWithResponses)));

    /**
     * Initializes all resources required by tests.
     * @throws SQLException required by mock
     */
    @Before
    public void before() throws SQLException {
        Mockito.when(dataSource.getConnection()).thenReturn(spiedConnection);
    }

    private static <V> StreamObserver<V> mockStreamObserver() {
        @SuppressWarnings("unchecked") final StreamObserver<V> result =
                (StreamObserver<V>)Mockito.mock(StreamObserver.class);
        return result;
    }

    /**
     * Checks that data writing should be done successfully.
     *
     * @throws IOException in case of error while manipulating piped streams(connecting,
     *         reading, writing).
     * @throws DataAccessException in case of error while executing SQL expression.
     * @throws SQLException if there's a problem creating a prepared statemnt
     */
    @Test
    public void checkDataUpdated() throws IOException, DataAccessException, SQLException {
        sqlWithResponses.add(new SqlWithResponse(
                deleteStatement(), Collections.singletonList(START_TIMESTAMP), null));
        addInsertStatement();
        final AtomicReference<InputStream> writingData = new AtomicReference<>();
        Mockito.when(spiedConnection.prepareStatement(Mockito.anyString()))
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
        final StreamObserver<C> writer = newWriter(streamObserver, dataSource);
        writer.onNext(newChunk(getPeriod(), START_TIMESTAMP,
                ByteString.copyFrom(TEST_DATA, StandardCharsets.UTF_8)));
        final String actual = getDataToWriteInDb(writingData);
        writer.onCompleted();
        Assert.assertThat(actual, CoreMatchers.is(TEST_DATA));
    }

    /**
     * Checks the case when reader(prepared statement thread) failed due to some {@link
     * SQLException} and piped buffer became full. In this case it is expected that we will print a
     * comprehensive description about what have failed.
     *
     * @throws SQLException in case of error while executing SQL expression.
     */
    @Test
    public void checkPreparedStatementFailure() throws SQLException {
        sqlWithResponses.add(new SqlWithResponse(
                deleteStatement(), Collections.singletonList(START_TIMESTAMP), null));
        final String insertStatement = addInsertStatement();
        final AtomicReference<InputStream> writingData = new AtomicReference<>();
        Mockito.when(spiedConnection.prepareStatement(Mockito.anyString())).thenAnswer(
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
        final StreamObserver<C> writer = newWriter(streamObserver, dataSource);
        writer.onNext(newChunk(getPeriod(), START_TIMESTAMP,
                ByteString.copyFrom(TEST_DATA, StandardCharsets.UTF_8)));
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
     * @throws SQLException in case of error while executing SQL expression.
     */
    @Test
    public void checkPreparedStatementInterrupted() throws SQLException {
        sqlWithResponses.add(new SqlWithResponse(
                deleteStatement(), Collections.singletonList(START_TIMESTAMP), null));
        final String insertStatement = addInsertStatement();
        final AtomicReference<InputStream> writingData = new AtomicReference<>();
        Mockito.when(spiedConnection.prepareStatement(Mockito.anyString())).thenAnswer(
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
        final StreamObserver<C> writer = newWriter(streamObserver, dataSource);
        writer.onNext(newChunk(getPeriod(), START_TIMESTAMP,
                ByteString.copyFrom(TEST_DATA, StandardCharsets.UTF_8)));
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

    /**
     * Checks that {@link DataAccessException} is throwing in case of exception while closing DB
     * connection.
     *
     * @throws SQLException required by mocks
     */
    @Test
    public void checkSqlExceptionOnCommitConnection() throws SQLException {
        sqlWithResponses.add(new SqlWithResponse(deleteStatement(),
                Collections.singletonList(new Timestamp(START_TIMESTAMP)), null));
        addInsertStatement();
        final PreparedStatement mockedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(spiedConnection.prepareStatement(Mockito.anyString()))
                .thenReturn(mockedStatement);
        Mockito.doThrow(new SQLException(SQL_EXCEPTION_MESSAGE)).when(spiedConnection).commit();
        Mockito.doAnswer((Answer<Object>)invocation -> {
            final Throwable exception = invocation.getArgumentAt(0, Throwable.class);
            Assert.assertThat(exception.getCause().getMessage(),
                    Matchers.containsString(SQL_EXCEPTION_MESSAGE));
            return null;
        }).when(streamObserver).onError(Mockito.any());
        final StreamObserver<C> writer = newWriter(streamObserver, dataSource);
        writer.onNext(newChunk(getPeriod(), START_TIMESTAMP, ByteString.EMPTY));
        writer.onCompleted();
        Mockito.verify(streamObserver, Mockito.times(1)).onError(Mockito.any());
    }

    /**
     * Checks that {@link StreamObserver} will provide information about error in case {@link
     * SQLException} will happen while data reading process.
     *
     * @throws SQLException reqauired by mocks
     */
    @Test
    public void checkSqlExceptionOnConnectionCreation() throws SQLException {
        sqlWithResponses.add(new SqlWithResponse(
                deleteStatement(), Collections.singletonList(new Timestamp(START_TIMESTAMP)),
                null));
        addInsertStatement();
        Mockito.when(dataSource.getConnection())
                .thenThrow(new SQLException(SQL_EXCEPTION_MESSAGE));
        Mockito.doAnswer((Answer<Object>)invocation -> {
            final Throwable exception = invocation.getArgumentAt(0, Throwable.class);
            Assert.assertThat(exception.getCause().getMessage(),
                    Matchers.containsString(SQL_EXCEPTION_MESSAGE));
            return null;
        }).when(streamObserver).onError(Mockito.any());
        final StreamObserver<C> writer = newWriter(streamObserver, dataSource);
        writer.onNext(newChunk(getPeriod(), START_TIMESTAMP, ByteString.EMPTY));
        Mockito.verify(streamObserver, Mockito.times(1)).onError(Mockito.any());
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
}
