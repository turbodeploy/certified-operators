package com.vmturbo.history.stats.writers;

import static org.mockito.Mockito.when;

import java.sql.Connection;

import javax.annotation.Nonnull;
import javax.sql.DataSource;

import com.google.protobuf.ByteString;

import io.grpc.stub.StreamObserver;

import org.jooq.DSLContext;
import org.jooq.TableField;
import org.jooq.impl.TableImpl;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.vmturbo.common.protobuf.stats.Stats.PercentileChunk;
import com.vmturbo.common.protobuf.stats.Stats.SetPercentileCountsResponse;
import com.vmturbo.history.schema.abstraction.tables.records.PercentileBlobsRecord;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.proactivesupport.DataMetricTimer;

/**
 * Tests the edge case when onNext is never called resulting in one of the NPEs from OM-89320.
 */
public class AbstractBlobsWriterEdgeCaseTest {

    @Mock
    private StreamObserver<SetPercentileCountsResponse> mockedResponseObserver;

    @Mock
    private DataSource mockedDataSource;

    @Mock
    private DataMetricSummary mockedDataMetricSummary;

    @Mock
    private DataMetricTimer mockDataMetricTimer;

    @Mock TableImpl<PercentileBlobsRecord> mockTableImpl;

    private FakeAbstractWriter fakeAbstractWriter;

    /**
     * Sets up the mocks.
     */
    @Before
    public void before() {
        MockitoAnnotations.initMocks(this);
        when(mockedDataMetricSummary.startTimer()).thenReturn(mockDataMetricTimer);
        fakeAbstractWriter = new FakeAbstractWriter(mockedResponseObserver, mockedDataSource, mockedDataMetricSummary, mockTableImpl);
    }

    /**
     * Never calling onNext() should not result in an NPE when calling onCompleted().
     */
    @Test
    public void testZeroOnNextThenOnCompleted() {
        fakeAbstractWriter.onCompleted();
    }

    /**
     * Implements a fake AbstractWriter so that we can create the edge case scenario that
     * reproduces one of the NPEs from OM-89320.
     */
    private static class FakeAbstractWriter extends AbstractBlobsWriter<SetPercentileCountsResponse,
            PercentileChunk, PercentileBlobsRecord> {


        FakeAbstractWriter(
                @Nonnull StreamObserver<SetPercentileCountsResponse> responseObserver,
                @Nonnull DataSource dataSource,
                @Nonnull DataMetricSummary metric,
                @Nonnull TableImpl<PercentileBlobsRecord> blobsTable) {
            super(responseObserver, dataSource, metric, blobsTable);
        }

        @Override
        protected byte[] writeChunk(@Nonnull Connection connection, @Nonnull DSLContext context,
                int processedChunks, @Nonnull PercentileChunk chunk) {
            throw new UnsupportedOperationException("This method is not implemented");
        }

        @Override
        protected long getStartTimestamp(@Nonnull PercentileChunk chunk) {
            throw new UnsupportedOperationException("This method is not implemented");
        }

        @Override
        protected ByteString getContent(@Nonnull PercentileChunk chunk) {
            throw new UnsupportedOperationException("This method is not implemented");
        }

        @Nonnull
        @Override
        protected TableField<PercentileBlobsRecord, Long> getStartTimestampField() {
            throw new UnsupportedOperationException("This method is not implemented");
        }

        @Override
        protected String getRecordSimpleName() {
            return this.getClass().getSimpleName();
        }

        @Override
        protected SetPercentileCountsResponse newResponse() {
            return SetPercentileCountsResponse.newBuilder().build();
        }
    }
}

