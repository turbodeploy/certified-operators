package com.vmturbo.cost.component.savings;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyEventDTO.EntityEvents.TopologyEvent;
import com.vmturbo.common.protobuf.topology.TopologyEventDTO.EntityEvents.TopologyEvent.EntityStateChangeDetails;
import com.vmturbo.common.protobuf.topology.TopologyEventDTO.EntityEvents.TopologyEvent.ProviderChangeDetails;
import com.vmturbo.common.protobuf.topology.TopologyEventDTO.EntityEvents.TopologyEvent.ResourceDeletionDetails;
import com.vmturbo.common.protobuf.topology.TopologyEventDTO.EntityEvents.TopologyEvent.TopologyEventInfo;
import com.vmturbo.common.protobuf.topology.TopologyEventDTO.EntityEvents.TopologyEvent.TopologyEventType;
import com.vmturbo.cost.component.savings.EntityEventsJournal.ActionEvent;
import com.vmturbo.cost.component.savings.EntityEventsJournal.ActionEvent.ActionEventType;
import com.vmturbo.cost.component.savings.EntityEventsJournal.SavingsEvent;
import com.vmturbo.cost.component.savings.EntityEventsJournal.SavingsEvent.Builder;
import com.vmturbo.platform.common.dto.CommonDTOREST.EntityDTO.EntityType;

/**
 * Support for injecting events into the event queue.  When enabled, this will run every 10 seconds
 * to scan for injected events.
 *
 * <p/>The source of the events is a scenario script
 * file that is used by the test data generator.  To prepare a script for use by this class, invoke
 * the generate-test-data.py tool with the '--events' option.  This will produce JSON output that
 * should be placed at '/tmp/injected-events.json'.  After that file is placed, an empty file named
 * '/tmp/injected-events.json.available' must be created.
 *
 * <p/>Events can be directly injected into the cluster targeted by the current Kubernetes context
 * by also including '--inject' on the event generation command.  This will place the two files
 * mentioned above into the active cluster's cost component.
 *
 * <p/>NOTE: The intent is for this class to only exist during feature development.  It will be
 * removed before final release.
 */
public class EventInjector implements Runnable {
    /**
     * Logger.
     */
    private static final Logger logger = LogManager.getLogger();
    private static final String SCRIPT_FILE = "/tmp/injected-events.json";
    private final EntitySavingsTracker entitySavingsTracker;
    private final EntitySavingsProcessor entitySavingsProcessor;
    private final EntityEventsJournal entityEventsJournal;

    /**
     * Action lifetimes.
     */
    private final EntitySavingsRetentionConfig entitySavingsRetentionConfig;

    /**
     * Event format passed between the data generator and the event injector.
     */
    public static class ScriptEvent {
        long timestamp;
        long expirationTimestamp;
        String eventType;
        long uuid;
        boolean state;
        double destTier;
        double sourceTier;
        boolean purgeState;

        /**
         * Return string representation of event.
         * @return string representation of event
         */
        @Override
        public String toString() {
            return String.format("%s@%d", eventType, timestamp);
        }
    }

    /**
     * Constructor.
     * @param entitySavingsTracker entity savings tracker to inject actions into.
     * @param entitySavingsProcessor savings processor
     * @param entityEventsJournal events journal to populate.
     * @param entitySavingsRetentionConfig savings action retention configuration.
     */
    EventInjector(EntitySavingsTracker entitySavingsTracker,
            EntitySavingsProcessor entitySavingsProcessor, EntityEventsJournal entityEventsJournal,
            @Nonnull final EntitySavingsRetentionConfig entitySavingsRetentionConfig) {
        this.entitySavingsTracker = entitySavingsTracker;
        this.entitySavingsProcessor = entitySavingsProcessor;
        this.entityEventsJournal = entityEventsJournal;
        this.entitySavingsRetentionConfig = entitySavingsRetentionConfig;
    }


    /**
     * Start the event injector.
     */
    public void start() {
        (new Thread(new EventInjector(entitySavingsTracker, entitySavingsProcessor,
                entityEventsJournal,
                entitySavingsRetentionConfig))).start();
    }

    /**
     * Start the event injector file watcher.
     */
    @Override
    public void run() {
        final Path monitoredPath = FileSystems.getDefault().getPath("/tmp");
        File file = new File(SCRIPT_FILE + ".available");
        Path availablePath = file.toPath().getFileName();
        WatchService watchService = null;
        while (true) {
            try {
                watchService = FileSystems.getDefault().newWatchService();
                monitoredPath.register(watchService, StandardWatchEventKinds.ENTRY_MODIFY);
                WatchKey wk;
                while (true) {
                    try {
                        wk = watchService.take();
                    } catch (InterruptedException e) {
                        continue;
                    }
                    for (WatchEvent<?> event : wk.pollEvents()) {
                        // we only register "ENTRY_MODIFY" so the context is always a Path.
                        final Path changed = (Path)event.context();
                        if (changed.equals(availablePath)) {
                            injectEvents();
                        }
                        wk.reset();
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
                if (watchService != null) {
                    try {
                        watchService.close();
                    } catch (IOException ioException) {
                        ioException.printStackTrace();
                    }
                }
            }
            // We only arrive here after an exception.  Restart, but first delay a bit in order to
            // avoid consuming excessive CPU.
            try {
                Thread.sleep(1000L);
            } catch (InterruptedException e) {
            }
        }
    }

    /**
     * Inject script events into the event journal, if available.
     */
    private void injectEvents() {
        // Check for script available
        File availableFile = new File(SCRIPT_FILE + ".available");
        if (!availableFile.exists()) {
            return;
        }
        logger.debug("Injecting events from scenario events");
        // Open the script file, convert the events to SavingsEvents, and add them to the event
        // journal.
        Gson gson = new Gson();
        JsonReader reader;
        List<ScriptEvent> events = new ArrayList<>();
        AtomicBoolean purgePreviousTestState = new AtomicBoolean(false);
        try {
            reader = new JsonReader(new FileReader(SCRIPT_FILE));
            events = Arrays.asList(gson.fromJson(reader, ScriptEvent[].class));
            events.forEach(event -> addEvent(event, entityEventsJournal, purgePreviousTestState));
        } catch (FileNotFoundException e) {
            logger.error("Cannot inject events: {}", e.toString());
        } finally {
            // Remove the events available file.
            availableFile.delete();
        }
        // Determine the scope of the scenario: participating UUIDs and the time period.
        Long earliestEventTime = Long.MAX_VALUE;
        Long latestEventTime = Long.MIN_VALUE;
        Set<Long> participatingUuids = new HashSet<>();
        for (ScriptEvent event : events) {
            earliestEventTime = Math.min(earliestEventTime, event.timestamp);
            latestEventTime = Math.max(latestEventTime, event.timestamp);
            if (event.uuid != 0L) {
                participatingUuids.add(event.uuid);
            }
        }

        if (earliestEventTime > latestEventTime) {
            logger.warn("No events in script file - not running savings tracker");
            return;
        }
        LocalDateTime startTime = makeLocalDateTime(earliestEventTime, false);
        LocalDateTime endTime = makeLocalDateTime(latestEventTime, true);
        if (purgePreviousTestState.get()) {
            entitySavingsTracker.purgeState(participatingUuids);
        }
        entitySavingsTracker.processEvents(startTime, endTime, participatingUuids);
    }

    @VisibleForTesting
    @Nonnull
    static LocalDateTime makeLocalDateTime(Long timestamp, boolean roundUp) {
        LocalDateTime trueTime = Instant.ofEpochMilli(timestamp).atZone(ZoneId.of("UTC"))
                .toLocalDateTime();
        LocalDateTime truncated = trueTime.truncatedTo(ChronoUnit.HOURS);
        if (roundUp && !truncated.equals(trueTime)) {
            // Round up to the next whole hour.
            truncated = truncated.plusHours(1L);
        }
        return truncated;
    }

    /**
     * Convert a script event to a SavingsEvent and add it to the journal.
     * @param event event generated by the script
     * @param entityEventsJournal event journal to populate
     * @param purgePreviousTestState true if the stats and entity for the entities in the UUID list
     *          should be deleted before processing the events.
     */
     public static void addEvent(ScriptEvent event, EntityEventsJournal entityEventsJournal,
             AtomicBoolean purgePreviousTestState) {
        logger.debug("Adding event: " + event);
        Builder result = new SavingsEvent.Builder()
                .entityId(event.uuid)
                .timestamp(event.timestamp)
                .expirationTime(event.expirationTimestamp);
         if ("RECOMMENDATION_ADDED".equals(event.eventType)) {
            EntityPriceChange entityPriceChange =  new EntityPriceChange.Builder()
                    .sourceCost(event.sourceTier)
                    .destinationCost(event.destTier)
                    .sourceOid((long)event.sourceTier)
                    .destinationOid((long)event.destTier)
                    .build();
            ActionEvent actionEvent = new ActionEvent.Builder()
                    .actionId(event.uuid)
                    .eventType(ActionEventType.RECOMMENDATION_ADDED)
                    .build();
            result.actionEvent(actionEvent).entityPriceChange(entityPriceChange);
        } else if ("RECOMMENDATION_REMOVED".equals(event.eventType)) {
            EntityPriceChange dummyPriceChange =  new EntityPriceChange.Builder()
                    .sourceCost(0d).destinationCost(0d)
                    .sourceOid(0L)
                    .destinationOid(0L)
                    .build();
            ActionEvent actionEvent = new ActionEvent.Builder()
                    .actionId(event.uuid)
                    .eventType(ActionEventType.RECOMMENDATION_REMOVED)
                    .build();
            result.actionEvent(actionEvent).entityPriceChange(dummyPriceChange);
        } else if ("POWER_STATE".equals(event.eventType)) {
            result.topologyEvent(createTopologyEvent(TopologyEventType.STATE_CHANGE,
                    event.timestamp)
              .setEventInfo(TopologyEventInfo.newBuilder()
                      .setStateChange(EntityStateChangeDetails.newBuilder()
                              .setSourceState(event.state ? EntityState.POWERED_OFF
                                      : EntityState.POWERED_ON)
                              .setDestinationState(event.state ? EntityState.POWERED_ON
                                      : EntityState.POWERED_OFF)
                              .build()))
              .build());
        } else if ("RESIZE_EXECUTED".equals(event.eventType)) {
            EntityPriceChange entityPriceChange =  new EntityPriceChange.Builder()
                    .sourceCost(event.sourceTier)
                    .destinationCost(event.destTier)
                    .sourceOid((long)event.sourceTier)
                    .destinationOid((long)event.destTier)
                    .build();
            ActionEvent actionEvent = new ActionEvent.Builder()
                    .actionId(event.uuid)
                    .eventType(ActionEventType.SCALE_EXECUTION_SUCCESS)
                    .build();
            result.actionEvent(actionEvent).entityPriceChange(entityPriceChange);
        } else if ("DELETE_EXECUTED".equals(event.eventType)) {
            EntityPriceChange entityPriceChange =  new EntityPriceChange.Builder()
                    .sourceCost(event.sourceTier)
                    .destinationCost(0d)
                    .sourceOid((long)event.sourceTier)
                    .destinationOid((long)event.destTier)
                    .build();
            ActionEvent actionEvent = new ActionEvent.Builder()
                    .actionId(event.uuid)
                    .eventType(ActionEventType.DELETE_EXECUTION_SUCCESS)
                    .build();
            result.actionEvent(actionEvent).entityPriceChange(entityPriceChange);
        } else if ("ENTITY_REMOVED".equals(event.eventType)) {
            result.topologyEvent(createTopologyEvent(TopologyEventType.RESOURCE_DELETION,
                    event.timestamp)
                .setEventInfo(TopologyEventInfo.newBuilder()
                        .setResourceDeletion(ResourceDeletionDetails.newBuilder()))
                .build());
        } else if ("PROVIDER_CHANGE".equals(event.eventType)) {
            result.topologyEvent(createTopologyEvent(TopologyEventType.PROVIDER_CHANGE,
                    event.timestamp).setEventInfo(TopologyEventInfo.newBuilder()
                    .setProviderChange(ProviderChangeDetails.newBuilder()
                            .setDestinationProviderOid((long)event.destTier)
                            .setProviderType(EntityType.VIRTUAL_MACHINE.getValue()))
                    .build()).build());
        } else if ("STOP".equals(event.eventType)) {
            purgePreviousTestState.set(event.purgeState);
            return;
        } else {
            logger.error("Invalid injected event type '{}' - ignoring", event.eventType);
            return;
        }
        entityEventsJournal.addEvent(result.build());
    }

    private static TopologyEvent.Builder createTopologyEvent(TopologyEventType eventType,
            long timestamp) {
        return TopologyEvent.newBuilder()
                .setType(eventType)
                .setEventTimestamp(timestamp);
    }
}
