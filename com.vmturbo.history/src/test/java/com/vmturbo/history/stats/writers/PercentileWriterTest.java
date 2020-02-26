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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import com.google.protobuf.ByteString;

import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;

import org.apache.commons.io.IOUtils;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Matchers;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.jooq.tools.jdbc.MockConnection;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import com.vmturbo.common.protobuf.stats.Stats.PercentileChunk;
import com.vmturbo.common.protobuf.stats.Stats.SetPercentileCountsResponse;
import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.db.VmtDbException;
import com.vmturbo.history.stats.TestDataProvider;
import com.vmturbo.platform.sdk.common.util.Pair;

/**
 * Checks that {@link PercentileWriter} implementation is working as expected.
 */
public class PercentileWriterTest {

    private static final String DELETE_FROM_VMTDB_PERCENTILE_BLOBS =
                    "delete from `vmtdb`.`percentile_blobs`";
    private static final String INSERT_INTO_VMTDB_PERCENTILE_BLOBS =
                    "insert into `vmtdb`.`percentile_blobs`";
    private static final String SQL_EXCEPTION_MESSAGE = "Something wrong has happened";
    private static final String TEST_DATA = "Test data bytes to be stored in database";
    private static final String PIPE_CLOSED = "Pipe closed";

    private HistorydbIO historyDbIo;
    private LinkedList<Pair<Pair<String, List<?>>, ?>> sqlRequestToResponse;
    private StreamObserver<SetPercentileCountsResponse> streamObserver;
    private ExecutorService statsWritersPool;
    private MockConnection connection;

    /**
     * Initializes all resources required by tests.
     */
    @Before
    public void before() {
        historyDbIo = Mockito.mock(HistorydbIO.class);
        Mockito.when(historyDbIo.JooqBuilder())
                        .thenReturn(DSL.using(connection, SQLDialect.MARIADB));
        sqlRequestToResponse = new LinkedList<>();
        connection = Mockito.spy(new MockConnection(
                        new TestDataProvider(sqlRequestToResponse)));
        streamObserver = mockStreamObserver();
        statsWritersPool = Executors.newCachedThreadPool();
    }

    /**
     * Releases all resources occupied by tests.
     */
    @After
    public void after() {
        statsWritersPool.shutdownNow();
    }

    private static <V> StreamObserver<V> mockStreamObserver() {
        @SuppressWarnings("unchecked")
        final StreamObserver<V> result = (StreamObserver<V>)Mockito.mock(StreamObserver.class);
        return result;
    }

    /**
     * Checks that data writing should be done successfully.
     *
     * @throws VmtDbException in case of error while creating/closing DB
     *                 connection.
     * @throws IOException in case of error while manipulating piped
     *                 streams(connecting, reading, writing).
     * @throws SQLException in case of error while executing SQL expression.
     */
    @Test
    public void checkDataUpdated() throws VmtDbException, IOException, SQLException {
        sqlRequestToResponse.add(Pair.create(Pair.create(DELETE_FROM_VMTDB_PERCENTILE_BLOBS,
                        Collections.singletonList(10L)), null));
        sqlRequestToResponse.add(Pair.create(Pair.create(INSERT_INTO_VMTDB_PERCENTILE_BLOBS,
                        Arrays.asList(10L, 3L)), null));
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
        final StreamObserver<PercentileChunk> percentileWriter =
                        new PercentileWriter(streamObserver, historyDbIo, statsWritersPool);
        percentileWriter.onNext(PercentileChunk.newBuilder().setStartTimestamp(10)
                        .setContent(ByteString.copyFrom(TEST_DATA, StandardCharsets.UTF_8))
                        .setPeriod(3).build());
        final String actual = getDataToWriteInDb(writingData);
        percentileWriter.onCompleted();
        Assert.assertThat(actual, CoreMatchers.is(TEST_DATA));
    }

    /**
     * Checks the case when reader(prepared statement thread) failed due to some {@link
     * SQLException} and piped buffer became full. In this case it is expected that we will print a
     * comprehensive description about what have failed.
     *
     * @throws VmtDbException in case of error while creating/closing DB
     *                 connection.
     * @throws SQLException in case of error while executing SQL expression.
     */
    @Test
    public void checkPreparedStatementFailure() throws SQLException, VmtDbException {
        sqlRequestToResponse.add(Pair.create(Pair.create(DELETE_FROM_VMTDB_PERCENTILE_BLOBS,
                        Collections.singletonList(10L)), null));
        sqlRequestToResponse.add(Pair.create(Pair.create(INSERT_INTO_VMTDB_PERCENTILE_BLOBS,
                        Arrays.asList(10L, 3L)), null));
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
                            if (invocation.getArgumentAt(0, String.class)
                                            .startsWith(INSERT_INTO_VMTDB_PERCENTILE_BLOBS)) {
                                Mockito.doAnswer((answer) -> {
                                    final InputStream inputStream = writingData.get();
                                    IOUtils.readFully(inputStream, TEST_DATA.length());
                                    throw new SQLException(SQL_EXCEPTION_MESSAGE);
                                }).when(result).execute();
                            }
                            return result;
                        });
        Mockito.when(historyDbIo.transConnection()).thenReturn(connection);
        final StreamObserver<PercentileChunk> percentileWriter =
                        new PercentileWriter(streamObserver, historyDbIo, statsWritersPool);
        percentileWriter.onNext(PercentileChunk.newBuilder().setStartTimestamp(10)
                        .setContent(ByteString.copyFrom(TEST_DATA, StandardCharsets.UTF_8))
                        .setPeriod(3).build());
        final byte[] bytes = new byte[1026];
        Arrays.fill(bytes, (byte)1);
        percentileWriter.onNext(PercentileChunk.newBuilder().setStartTimestamp(10)
                        .setContent(ByteString.copyFrom(bytes))
                        .setPeriod(3).build());
        percentileWriter.onNext(PercentileChunk.newBuilder().setStartTimestamp(10)
                        .setContent(ByteString.copyFrom(bytes))
                        .setPeriod(3).build());
        final ArgumentCaptor<Throwable> errorCaptor = ArgumentCaptor.forClass(Throwable.class);
        Mockito.verify(streamObserver, Mockito.only()).onError(errorCaptor.capture());
        final Throwable error = errorCaptor.getValue();
        checkThrowable(error, Status.INTERNAL.getCode().name(), StatusException.class);
        final Throwable rpcFailureCause = error.getCause();
        checkThrowable(rpcFailureCause, PIPE_CLOSED, IOException.class);
        final Throwable ioCause = rpcFailureCause.getCause();
        checkThrowable(ioCause, SQL_EXCEPTION_MESSAGE, SQLException.class);
    }

    /**
     * Checks the case when reader(prepared statement thread) died because of interrupted exception
     * or something unexpected and piped buffer became full. In this case it is expected that we
     * will print a comprehensive description about what have failed.
     *
     * @throws VmtDbException in case of error while creating/closing DB
     *                 connection.
     * @throws SQLException in case of error while executing SQL expression.
     */
    @Test
    public void checkPreparedStatementInterrupted() throws SQLException, VmtDbException {
        sqlRequestToResponse.add(Pair.create(Pair.create(DELETE_FROM_VMTDB_PERCENTILE_BLOBS,
                        Collections.singletonList(10L)), null));
        sqlRequestToResponse.add(Pair.create(Pair.create(INSERT_INTO_VMTDB_PERCENTILE_BLOBS,
                        Arrays.asList(10L, 3L)), null));
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
                            if (invocation.getArgumentAt(0, String.class)
                                            .startsWith(INSERT_INTO_VMTDB_PERCENTILE_BLOBS)) {
                                Mockito.doAnswer((answer) -> {
                                    statsWritersPool.shutdownNow();
                                    final InputStream inputStream = writingData.get();
                                    IOUtils.readFully(inputStream, TEST_DATA.length());
                                    throw new InterruptedIOException();
                                }).when(result).execute();
                            }
                            return result;
                        });
        Mockito.when(historyDbIo.transConnection()).thenReturn(connection);
        final StreamObserver<PercentileChunk> percentileWriter =
                        new PercentileWriter(streamObserver, historyDbIo, statsWritersPool);
        percentileWriter.onNext(PercentileChunk.newBuilder().setStartTimestamp(10)
                        .setContent(ByteString.copyFrom(TEST_DATA, StandardCharsets.UTF_8))
                        .setPeriod(3).build());
        final byte[] bytes = new byte[1026];
        Arrays.fill(bytes, (byte)1);
        percentileWriter.onNext(PercentileChunk.newBuilder().setStartTimestamp(10)
                        .setContent(ByteString.copyFrom(bytes))
                        .setPeriod(3).build());
        final ArgumentCaptor<Throwable> errorCaptor = ArgumentCaptor.forClass(Throwable.class);
        Mockito.verify(streamObserver, Mockito.only()).onError(errorCaptor.capture());
        final Throwable error = errorCaptor.getValue();
        checkThrowable(error, Status.INTERNAL.getCode().name(), StatusException.class);
        final Throwable rpcCause = error.getCause();
        checkThrowable(rpcCause, "Read end dead", IOException.class);
        final Throwable originalException = rpcCause.getCause();
        checkThrowable(originalException, null, InterruptedIOException.class);
    }

    private static <V extends Throwable> void checkThrowable(Throwable cause, String message,
                    Class<V> exceptionType) {
        Assert.assertThat(cause, CoreMatchers.instanceOf(exceptionType));
        Assert.assertThat(cause.getMessage(), CoreMatchers.is(message));
    }

    private static String getDataToWriteInDb(AtomicReference<InputStream> writingData)
                    throws IOException {
        final InputStream input = writingData.get();
        final int available = input.available();
        final byte[] data = new byte[available];
        input.read(data, 0, available);
        return new String(data, StandardCharsets.UTF_8);
    }

    /**
     * Checks that {@link VmtDbException} is throwing in case of exception while closing DB
     * connection.
     *
     * @throws VmtDbException in case of exception while closing/opening DB
     *                 connection.
     */
    @Test
    public void checkSqlExceptionOnCommitConnection() throws VmtDbException {
        sqlRequestToResponse.add(Pair.create(Pair.create(DELETE_FROM_VMTDB_PERCENTILE_BLOBS,
                        Collections.singletonList(new Timestamp(10))), null));
        sqlRequestToResponse.add(Pair.create(Pair.create(INSERT_INTO_VMTDB_PERCENTILE_BLOBS,
                        Arrays.asList(new Timestamp(10), 3L, ByteString.EMPTY.toByteArray())),
                        null));
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
        final StreamObserver<PercentileChunk> percentileWriter =
                        new PercentileWriter(streamObserver, historyDbIo, statsWritersPool);
        percentileWriter.onNext(PercentileChunk.newBuilder().setStartTimestamp(10)
                        .setContent(ByteString.EMPTY).setPeriod(3).build());
        percentileWriter.onCompleted();
        Mockito.verify(streamObserver, Mockito.times(1)).onError(Mockito.any());
    }

    /**
     * Checks that {@link StreamObserver} will provide information about error in case {@link
     * SQLException} will happen while data reading process.
     *
     * @throws VmtDbException in case of exception while closing/opening DB
     *                 connection.
     */
    @Test
    public void checkSqlExceptionOnConnectionCreation() throws VmtDbException {
        sqlRequestToResponse.add(Pair.create(Pair.create(DELETE_FROM_VMTDB_PERCENTILE_BLOBS,
                        Collections.singletonList(new Timestamp(10))), null));
        sqlRequestToResponse.add(Pair.create(Pair.create(INSERT_INTO_VMTDB_PERCENTILE_BLOBS,
                        Arrays.asList(new Timestamp(10), 3L, ByteString.EMPTY.toByteArray())),
                        null));
        Mockito.when(historyDbIo.transConnection())
                        .thenThrow(new VmtDbException(VmtDbException.CONN_POOL_STARTUP,
                                        SQL_EXCEPTION_MESSAGE));
        Mockito.doAnswer((Answer<Object>)invocation -> {
            final Throwable exception = invocation.getArgumentAt(0, Throwable.class);
            Assert.assertThat(exception.getCause().getMessage(),
                            Matchers.containsString("Error initializing connection pool"));
            return null;
        }).when(streamObserver).onError(Mockito.any());
        final StreamObserver<PercentileChunk> percentileWriter =
                        new PercentileWriter(streamObserver, historyDbIo, statsWritersPool);
        percentileWriter.onNext(PercentileChunk.newBuilder().setStartTimestamp(10)
                        .setContent(ByteString.EMPTY).setPeriod(3).build());
        Mockito.verify(streamObserver, Mockito.times(1)).onError(Mockito.any());
    }
}
