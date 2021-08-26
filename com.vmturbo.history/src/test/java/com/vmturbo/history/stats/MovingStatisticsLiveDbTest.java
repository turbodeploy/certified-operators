/*
 * (C) Turbonomic 2021.
 */
package com.vmturbo.history.stats;

import static com.vmturbo.history.schema.abstraction.Tables.MOVING_STATISTICS_BLOBS;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import com.google.protobuf.ByteString;

import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.vmturbo.common.protobuf.stats.Stats.GetMovingStatisticsRequest;
import com.vmturbo.common.protobuf.stats.Stats.MovingStatisticsChunk;
import com.vmturbo.common.protobuf.stats.Stats.MovingStatisticsChunk.Builder;
import com.vmturbo.common.protobuf.stats.Stats.SetMovingStatisticsResponse;
import com.vmturbo.history.db.DBConnectionPool;
import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.db.SchemaUtil;
import com.vmturbo.history.db.VmtDbException;
import com.vmturbo.history.stats.readers.MovingStatisticsReader;
import com.vmturbo.history.stats.writers.MovingStatisticsWriter;

/**
 * Checks that {@link MovingStatisticsWriter} works as expected with live db configuration.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {DbTestConfig.class})
@DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_CLASS)
public class MovingStatisticsLiveDbTest {
    private static final String OLD_CHUNK_DATA = "old chunk data";
    private static final long OLD_START_TIMESTAMP = 11L;
    private static final long NEW_START_TIMESTAMP = OLD_START_TIMESTAMP + 10;
    private static final List<String> NEW_CHUNK_DATA = Arrays.asList("first chunk", "second chunk");

    @Autowired
    private DbTestConfig dbTestConfig;
    private static String testDbName;
    private static HistorydbIO historydbIO;

    /**
     * Set up and populate live database for tests, and create required mocks.
     *
     * @throws Exception if any problem occurs
     */
    @Before
    public void before() throws Exception {
        testDbName = dbTestConfig.testDbName();
        historydbIO = dbTestConfig.historydbIO();
        HistorydbIO.setSharedInstance(historydbIO);
        historydbIO.setSchemaForTests(testDbName);
        // we don't need to recreate the database for each test
        historydbIO.init(false, null, testDbName, Optional.empty());
    }

    /**
     * Clear the moving statistics table after each test.
     *
     * @throws SQLException sql error
     * @throws VmtDbException db error
     */
    @After
    public void after() throws SQLException, VmtDbException {
        try (Connection conn = historydbIO.connection()) {
            historydbIO.using(conn).truncateTable(MOVING_STATISTICS_BLOBS).execute();
        }
    }

    /**
     * Tear down our database when tests are complete.
     *
     * @throws Throwable if there's a problem
     */
    @AfterClass
    public static void afterClass() throws Throwable {
        DBConnectionPool.instance.getInternalPool().close();
        try (Connection conn = historydbIO.getRootConnection()) {
            SchemaUtil.dropDb(testDbName, conn);
            SchemaUtil.dropUser(historydbIO.getUserName(), conn);
        }
    }

    /**
     * Checks that writing multiple chunks of data is successful, and the returned data is of the
     * more recent timestamp.
     */
    @Test
    public void testMultipleChunks() {
        final StreamObserver<SetMovingStatisticsResponse> responseObserver = mockStreamObserver();
        final StreamObserver<MovingStatisticsChunk> writer = new MovingStatisticsWriter(responseObserver, historydbIO);
        writer.onNext(newChunk(OLD_START_TIMESTAMP, ByteString.copyFrom(OLD_CHUNK_DATA, StandardCharsets.UTF_8)));
        writer.onNext(newChunk(NEW_START_TIMESTAMP, ByteString.copyFrom(NEW_CHUNK_DATA.get(0), StandardCharsets.UTF_8)));
        writer.onNext(newChunk(NEW_START_TIMESTAMP, ByteString.copyFrom(NEW_CHUNK_DATA.get(1), StandardCharsets.UTF_8)));
        writer.onCompleted();

        final MovingStatisticsReader reader = new MovingStatisticsReader(
                (int)TimeUnit.SECONDS.toMillis(30), TimeUnit.SECONDS.toMillis(30),
                Clock.systemUTC(), historydbIO);
        final GetMovingStatisticsRequest request = GetMovingStatisticsRequest.newBuilder().setChunkSize(100).build();
        final Collection<MovingStatisticsChunk> retrievedChunks = new ArrayList<>();
        final ServerCallStreamObserver<MovingStatisticsChunk> output = createServerCallStreamObserver(retrievedChunks);
        reader.processRequest(request, output);
        Assert.assertEquals("Number of chunks retrieved is wrong", NEW_CHUNK_DATA.size(), retrievedChunks.size());
        int chunkIndex = 0;
        for (final MovingStatisticsChunk chunk : retrievedChunks) {
            Assert.assertEquals("Unexpected start timestamp in the retrieved chunk" + chunk, NEW_START_TIMESTAMP, chunk.getStartTimestamp());
            Assert.assertEquals("Unexpected contents in the retrieved chunk" + chunk, NEW_CHUNK_DATA.get(chunkIndex), chunk.getContent().toStringUtf8());
            chunkIndex++;
        }
    }

    /**
     * Returns a new chunk of the statistics constructed with the given parameters.
     *
     * @param startTimestamp the timestamp of the chunk
     * @param data the data of the chunk
     * @return the constructed new chunk
     */
    private MovingStatisticsChunk newChunk(long startTimestamp, @Nonnull ByteString data) {
        Objects.requireNonNull(data);
        final Builder chunkBuilder = MovingStatisticsChunk.newBuilder();
        chunkBuilder.setStartTimestamp(startTimestamp);
        chunkBuilder.setContent(data);
        return chunkBuilder.build();
    }

    private static <V> StreamObserver<V> mockStreamObserver() {
        @SuppressWarnings("unchecked")
        final StreamObserver<V> result = (StreamObserver<V>)Mockito.mock(StreamObserver.class);
        return result;
    }

    private ServerCallStreamObserver<MovingStatisticsChunk> createServerCallStreamObserver(
            @Nonnull Collection<MovingStatisticsChunk> receivedChunks) {
        Objects.requireNonNull(receivedChunks);
        @SuppressWarnings("unchecked")
        final ServerCallStreamObserver<MovingStatisticsChunk> result = Mockito.mock(ServerCallStreamObserver.class);
        Mockito.doAnswer(invocation -> {
            receivedChunks.add(invocation.getArgumentAt(0, MovingStatisticsChunk.class));
            return null;
        }).when(result).onNext(Mockito.any());
        Mockito.when(result.isReady()).thenReturn(true);
        return result;
    }
}
