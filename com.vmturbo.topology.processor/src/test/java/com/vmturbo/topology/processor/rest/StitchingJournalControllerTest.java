package com.vmturbo.topology.processor.rest;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Clock;

import org.junit.Before;
import org.junit.Test;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import com.vmturbo.common.protobuf.topology.Stitching.FilteredJournalRequest;
import com.vmturbo.common.protobuf.topology.Stitching.IncludeAllFilter;
import com.vmturbo.common.protobuf.topology.Stitching.JournalOptions;
import com.vmturbo.common.protobuf.topology.Stitching.OutputOptions;
import com.vmturbo.stitching.journal.JournalFilter;
import com.vmturbo.stitching.journal.JournalRecorder.LoggerRecorder;
import com.vmturbo.stitching.journal.JournalRecorder.OutputStreamRecorder;
import com.vmturbo.topology.processor.scheduling.Scheduler;
import com.vmturbo.topology.processor.stitching.journal.JournalFilterFactory;
import com.vmturbo.topology.processor.stitching.journal.StitchingJournalFactory.ConfigurableStitchingJournalFactory;
import com.vmturbo.topology.processor.topology.TopologyHandler;

/**
 * Tests for {@link StitchingJournalController}.
 */
public class StitchingJournalControllerTest {
    final TopologyHandler topologyHandler = mock(TopologyHandler.class);
    final Scheduler scheduler = mock(Scheduler.class);
    final JournalFilterFactory journalFilterFactory = mock(JournalFilterFactory.class);
    final FilteredJournalRequest.Builder request = FilteredJournalRequest.newBuilder()
        .setIncludeAllFilter(IncludeAllFilter.getDefaultInstance());
    final ConfigurableStitchingJournalFactory journalFactory = mock(ConfigurableStitchingJournalFactory.class);
    final Clock clock = mock(Clock.class);

    final StitchingJournalController controller = new StitchingJournalController(topologyHandler,
        scheduler, journalFilterFactory, clock);

    /**
     * Setup the tests.
     */
    @Before
    public void setup() {
        when(journalFilterFactory.filterFor(any(FilteredJournalRequest.class)))
            .thenReturn(mock(JournalFilter.class));
    }

    /**
     * Test that the ReturnToCaller option records to a grpc OutputStreamRecorder.
     *
     * @throws IOException when something goes wrong.
     */
    @Test
    public void testReturnToCallerOutput() throws IOException {
        final StreamingResponseBody body = controller.streamTextJournal(
            request.setOutputOptions(OutputOptions.RETURN_TO_CALLER).build(), journalFactory);
        body.writeTo(new ByteArrayOutputStream(1000));
        verify(journalFactory).setFilter(isA(JournalFilter.class));
        verify(journalFactory).setJournalOptions(isA(JournalOptions.class));
        verify(journalFactory).addRecorder(isA(OutputStreamRecorder.class));
    }

    /**
     * Test that the Logger option records to a LoggerRecorder.
     *
     * @throws IOException when something goes wrong.
     */
    @Test
    public void testLoggerOutput() throws IOException {
        final StreamingResponseBody body = controller.streamTextJournal(
            request.setOutputOptions(OutputOptions.LOGGER).build(), journalFactory);
        body.writeTo(new ByteArrayOutputStream(1000));
        verify(journalFactory).setFilter(isA(JournalFilter.class));
        verify(journalFactory).setJournalOptions(isA(JournalOptions.class));
        verify(journalFactory).addRecorder(isA(LoggerRecorder.class));
    }

    /**
     * Test that the LogAndReturnToCaller option records to a grpc OutputStreamRecorder and LogRecorder.
     *
     * @throws IOException when something goes wrong.
     */
    @Test
    public void testLogAndReturnToCallerOutput() throws IOException {
        final StreamingResponseBody body = controller.streamTextJournal(
            request.setOutputOptions(OutputOptions.LOG_AND_RETURN_TO_CALLER).build(), journalFactory);
        body.writeTo(new ByteArrayOutputStream(1000));
        verify(journalFactory).setFilter(isA(JournalFilter.class));
        verify(journalFactory).setJournalOptions(isA(JournalOptions.class));
        verify(journalFactory, times(1)).addRecorder(isA(OutputStreamRecorder.class));
        verify(journalFactory, times(1)).addRecorder(isA(LoggerRecorder.class));
    }
}