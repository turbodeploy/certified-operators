package com.vmturbo.topology.processor.stitching.journal;

import java.time.Clock;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.topology.Stitching.FilteredJournalRequest;
import com.vmturbo.common.protobuf.topology.Stitching.JournalEntry;
import com.vmturbo.common.protobuf.topology.StitchingJournalServiceGrpc.StitchingJournalServiceImplBase;
import com.vmturbo.stitching.journal.JournalRecorder.LoggerRecorder;
import com.vmturbo.stitching.journal.JournalRecorder.StreamObserverRecorder;
import com.vmturbo.topology.processor.scheduling.Scheduler;
import com.vmturbo.topology.processor.stitching.journal.StitchingJournalFactory.ConfigurableStitchingJournalFactory;
import com.vmturbo.topology.processor.topology.TopologyHandler;

/**
 * Implements the StitchingJournalService.
 */
public class StitchingJournalRpcService extends StitchingJournalServiceImplBase {

    private static final Logger logger = LogManager.getLogger();

    private final TopologyHandler topologyHandler;
    private final Scheduler scheduler;
    private final JournalFilterFactory filterFactory;

    public StitchingJournalRpcService(@Nonnull final TopologyHandler topologyHandler,
                                      @Nonnull final Scheduler scheduler,
                                      @Nonnull final JournalFilterFactory filterFactory) {
        this.topologyHandler = Objects.requireNonNull(topologyHandler);
        this.scheduler = Objects.requireNonNull(scheduler);
        this.filterFactory = Objects.requireNonNull(filterFactory);
    }

    @Override
    public void captureJournalEntries(FilteredJournalRequest request,
                                      StreamObserver<JournalEntry> responseObserver) {
        final ConfigurableStitchingJournalFactory journalFactory =
            StitchingJournalFactory.configurableStitchingJournalFactory(Clock.systemUTC());
        captureJournalEntries(request, responseObserver, journalFactory);
    }

    @VisibleForTesting
    void captureJournalEntries(@Nonnull final FilteredJournalRequest request,
                                       @Nonnull final StreamObserver<JournalEntry> responseObserver,
                                       @Nonnull final ConfigurableStitchingJournalFactory journalFactory) {
        addRecorders(request, journalFactory, responseObserver);
        journalFactory.setFilter(filterFactory.filterFor(request));
        journalFactory.setJournalOptions(request.getJournalOptions());

        try {
            scheduler.resetBroadcastSchedule();
            topologyHandler.broadcastLatestTopology(journalFactory);
            responseObserver.onCompleted();
        } catch (Exception e) {
            logger.error("Unable to broadcast topology and capture stitching log due to error: ", e);
            responseObserver.onError(Status.INTERNAL
                    .withDescription(e.getMessage())
                    .withCause(e)
                    .asException()
            );
        }
    }

    private void addRecorders(@Nonnull final FilteredJournalRequest request,
                              @Nonnull final ConfigurableStitchingJournalFactory journalFactory,
                              @Nonnull final StreamObserver<JournalEntry> responseObserver) {
        switch (request.getOutputOptions()) {
            case RETURN_TO_CALLER:
                journalFactory.addRecorder(new StreamObserverRecorder(responseObserver));
                break;
            case LOGGER:
                journalFactory.addRecorder(new LoggerRecorder(logger,
                    LoggerRecorder.DEFAULT_FLUSHING_CHARACTER_LENGTH));
                break;
            case LOG_AND_RETURN_TO_CALLER:
                journalFactory.addRecorder(new LoggerRecorder(logger,
                    LoggerRecorder.DEFAULT_FLUSHING_CHARACTER_LENGTH));
                journalFactory.addRecorder(new StreamObserverRecorder(responseObserver));
                break;
            default:
                logger.error("Unknown output option: {}", request.getOutputOptions());
        }
    }
}
