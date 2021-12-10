/*
 * (C) Turbonomic 2021.
 */
package com.vmturbo.history.stats;

import static com.vmturbo.history.schema.abstraction.Tables.MOVING_STATISTICS_BLOBS;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;
import javax.sql.DataSource;

import com.google.protobuf.ByteString;

import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;

import org.jooq.DSLContext;
import org.jooq.exception.DataAccessException;
import org.junit.After;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.stats.Stats.GetMovingStatisticsRequest;
import com.vmturbo.common.protobuf.stats.Stats.MovingStatisticsChunk;
import com.vmturbo.common.protobuf.stats.Stats.MovingStatisticsChunk.Builder;
import com.vmturbo.common.protobuf.stats.Stats.SetMovingStatisticsResponse;
import com.vmturbo.history.schema.abstraction.Vmtdb;
import com.vmturbo.history.stats.readers.MovingStatisticsReader;
import com.vmturbo.history.stats.writers.MovingStatisticsWriter;
import com.vmturbo.sql.utils.DbCleanupRule;
import com.vmturbo.sql.utils.DbConfigurationRule;

/**
 * Checks that {@link MovingStatisticsWriter} works as expected with live db configuration.
 */
public class MovingStatisticsLiveDbTest {
    private static final String OLD_CHUNK_DATA = "old chunk data";
    private static final long OLD_START_TIMESTAMP = 11L;
    private static final long NEW_START_TIMESTAMP = OLD_START_TIMESTAMP + 10;
    private static final List<String> NEW_CHUNK_DATA = Arrays.asList("first chunk", "second chunk");

    /**
     * Provision and provide access to a test database.
     */
    @ClassRule
    public static DbConfigurationRule dbConfig = new DbConfigurationRule(Vmtdb.VMTDB);

    /**
     * Clean up tables in the test database before each test.
     */
    @Rule
    public DbCleanupRule dbCleanup = dbConfig.cleanupRule();

    private final DSLContext dsl = dbConfig.getDslContext();
    private final DataSource dataSource = dbConfig.getDataSource();

    /**
     * Clear the moving statistics table after each test.
     *
     * @throws DataAccessException db error
     */
    @After
    public void after() throws DataAccessException {
        dsl.truncateTable(MOVING_STATISTICS_BLOBS).execute();
    }

    /**
     * Checks that writing multiple chunks of data is successful, and the returned data is of the
     * more recent timestamp.
     */
    @Test
    public void testMultipleChunks() {
        final StreamObserver<SetMovingStatisticsResponse> responseObserver = mockStreamObserver();
        final StreamObserver<MovingStatisticsChunk> writer = new MovingStatisticsWriter(responseObserver, dataSource);
        writer.onNext(newChunk(OLD_START_TIMESTAMP, ByteString.copyFrom(OLD_CHUNK_DATA, StandardCharsets.UTF_8)));
        writer.onNext(newChunk(NEW_START_TIMESTAMP, ByteString.copyFrom(NEW_CHUNK_DATA.get(0), StandardCharsets.UTF_8)));
        writer.onNext(newChunk(NEW_START_TIMESTAMP, ByteString.copyFrom(NEW_CHUNK_DATA.get(1), StandardCharsets.UTF_8)));
        writer.onCompleted();

        final MovingStatisticsReader reader = new MovingStatisticsReader(
                (int)TimeUnit.SECONDS.toMillis(30), TimeUnit.SECONDS.toMillis(30),
                Clock.systemUTC(), dsl);
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
