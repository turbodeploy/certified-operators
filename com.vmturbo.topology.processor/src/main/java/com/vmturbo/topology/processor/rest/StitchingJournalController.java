package com.vmturbo.topology.processor.rest;

import java.io.IOException;
import java.io.OutputStream;
import java.time.Clock;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import com.vmturbo.common.protobuf.topology.Stitching;
import com.vmturbo.common.protobuf.topology.StitchingREST.FilteredJournalRequest;
import com.vmturbo.stitching.journal.JournalRecorder.LoggerRecorder;
import com.vmturbo.stitching.journal.JournalRecorder.OutputStreamRecorder;
import com.vmturbo.topology.processor.scheduling.Scheduler;
import com.vmturbo.topology.processor.stitching.journal.JournalFilterFactory;
import com.vmturbo.topology.processor.stitching.journal.StitchingJournalFactory;
import com.vmturbo.topology.processor.stitching.journal.StitchingJournalFactory.ConfigurableStitchingJournalFactory;
import com.vmturbo.topology.processor.topology.TopologyHandler;

/**
 * Controller for the REST interface for capturing the stitching journal.
 * Uses Spring annotations.
 */
@Api(value = "/stitchingJournal")
@RequestMapping(value = "/stitchingJournal")
@RestController
public class StitchingJournalController {
    private static final Logger logger = LogManager.getLogger();

    private final TopologyHandler topologyHandler;
    private final Scheduler scheduler;
    private final JournalFilterFactory filterFactory;
    private final Clock clock;

    public StitchingJournalController(@Nonnull final TopologyHandler topologyHandler,
                                      @Nonnull final Scheduler scheduler,
                                      @Nonnull final JournalFilterFactory filterFactory,
                                      @Nonnull final Clock clock) {
        this.topologyHandler = Objects.requireNonNull(topologyHandler);
        this.scheduler = Objects.requireNonNull(scheduler);
        this.filterFactory = Objects.requireNonNull(filterFactory);
        this.clock = Objects.requireNonNull(clock);
    }

    @RequestMapping(
        method = RequestMethod.POST,
        consumes = {MediaType.APPLICATION_JSON_UTF8_VALUE},
        produces = {"text/plain"})
    @ApiOperation(value = "Get a human-readable text representation of the stitching journal. " +
        "The text journal is streamed to prevent massive memory consumption in constructing " +
        "or receiving the journal. Note that there is a separate gRPC interface to stream a more " +
        "machine-parsable representation of the journal. This call will not work from the swagger-ui. " +
        "Instead you can form your query in the swagger-ui and copy the curl command into the command line.")
    public ResponseEntity<StreamingResponseBody>
    streamTextJournal(@ApiParam(value = "Specification of how to filter the information entered into " +
        "the journal and options for grouping and formatting that information.", required = true)
                      @RequestBody final FilteredJournalRequest request) {
        try {
            final com.vmturbo.common.protobuf.topology.Stitching.FilteredJournalRequest protoRequest =
                request.toProto();

            final ConfigurableStitchingJournalFactory journalFactory =
                StitchingJournalFactory.configurableStitchingJournalFactory(clock);
            final StreamingResponseBody body = streamTextJournal(protoRequest, journalFactory);

            return new ResponseEntity<>(body, HttpStatus.OK);
        } catch (Exception e) {
            logger.error("Unable to broadcast topology and capture stitching log due to error: ", e);
            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @VisibleForTesting
    @Nonnull
    StreamingResponseBody streamTextJournal(@Nonnull final Stitching.FilteredJournalRequest protoRequest,
                                            @Nonnull final ConfigurableStitchingJournalFactory journalFactory)
        throws IOException {
        journalFactory.setFilter(filterFactory.filterFor(protoRequest));
        journalFactory.setJournalOptions(protoRequest.getJournalOptions());

        // Use a streaming response body to allow the response to be streamed.
        // This prevents a massive buildup in memory of the log as stitching occurs.
        // It also allows the caller not to have to receive the entire log at once.
        return outputStream -> {
            addRecorders(protoRequest, journalFactory, outputStream);

            scheduler.resetBroadcastSchedule();
            try {
                topologyHandler.broadcastLatestTopology(journalFactory);
            } catch (Exception e) {
                outputStream.write(e.getMessage().getBytes());
            }
        };
    }

    private void addRecorders(@Nonnull final Stitching.FilteredJournalRequest request,
                              @Nonnull final ConfigurableStitchingJournalFactory journalFactory,
                              @Nonnull final OutputStream outputStream) {
        switch (request.getOutputOptions()) {
            case RETURN_TO_CALLER:
                journalFactory.addRecorder(new OutputStreamRecorder(outputStream));
                break;
            case LOGGER:
                journalFactory.addRecorder(new LoggerRecorder(logger,
                    LoggerRecorder.DEFAULT_FLUSHING_CHARACTER_LENGTH));
                break;
            case LOG_AND_RETURN_TO_CALLER:
                journalFactory.addRecorder(new LoggerRecorder(logger,
                    LoggerRecorder.DEFAULT_FLUSHING_CHARACTER_LENGTH));
                journalFactory.addRecorder(new OutputStreamRecorder(outputStream));
                break;
        }
    }
}
