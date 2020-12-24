package com.vmturbo.topology.processor.stitching.journal;

import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.grpc.stub.StreamObserver;

import org.junit.Test;

import com.vmturbo.common.protobuf.topology.Stitching.FilteredJournalRequest;
import com.vmturbo.common.protobuf.topology.Stitching.IncludeAllFilter;
import com.vmturbo.common.protobuf.topology.Stitching.JournalEntry;
import com.vmturbo.common.protobuf.topology.Stitching.OutputOptions;
import com.vmturbo.stitching.journal.JournalRecorder.LoggerRecorder;
import com.vmturbo.stitching.journal.JournalRecorder.StreamObserverRecorder;
import com.vmturbo.topology.processor.scheduling.Scheduler;
import com.vmturbo.topology.processor.stitching.journal.StitchingJournalFactory.ConfigurableStitchingJournalFactory;
import com.vmturbo.topology.processor.topology.TopologyHandler;

/**
 * Tests for the {@link StitchingJournalRpcService}.
 */
public class StitchingJournalRpcServiceTest {
    final TopologyHandler topologyHandler = mock(TopologyHandler.class);
    final Scheduler scheduler = mock(Scheduler.class);
    final JournalFilterFactory journalFilterFactory = mock(JournalFilterFactory.class);
    final FilteredJournalRequest.Builder request = FilteredJournalRequest.newBuilder()
        .setIncludeAllFilter(IncludeAllFilter.getDefaultInstance());
    @SuppressWarnings("unchecked")
    final StreamObserver<JournalEntry> observer = (StreamObserver<JournalEntry>)mock(StreamObserver.class);
    final ConfigurableStitchingJournalFactory journalFactory = mock(ConfigurableStitchingJournalFactory.class);

    final StitchingJournalRpcService rpcService = new StitchingJournalRpcService(topologyHandler,
        scheduler, journalFilterFactory);

    /**
     * Test that the ReturnToCaller option records to a grpc StreamObserverRecorder.
     */
    @Test
    public void testReturnToCallerOutput() {
        rpcService.captureJournalEntries(request.setOutputOptions(OutputOptions.RETURN_TO_CALLER).build(),
            observer, journalFactory);
        verify(journalFactory).addRecorder(isA(StreamObserverRecorder.class));
    }

    /**
     * Test that the Logger option records to a LoggerRecorder.
     */
    @Test
    public void testLoggerOutput() {
        rpcService.captureJournalEntries(request.setOutputOptions(OutputOptions.LOGGER).build(),
            observer, journalFactory);
        verify(journalFactory).addRecorder(isA(LoggerRecorder.class));
    }

    /**
     * Test that the LogAndReturnToCaller option records to a grpc StreamObserverRecorder and LogRecorder.
     */
    @Test
    public void testLogAndReturnToCallerOutput() {
        rpcService.captureJournalEntries(request.setOutputOptions(OutputOptions.LOG_AND_RETURN_TO_CALLER).build(),
            observer, journalFactory);
        verify(journalFactory, times(1)).addRecorder(isA(StreamObserverRecorder.class));
        verify(journalFactory, times(1)).addRecorder(isA(LoggerRecorder.class));
    }
}