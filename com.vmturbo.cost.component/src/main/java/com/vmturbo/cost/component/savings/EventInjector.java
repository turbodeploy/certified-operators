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
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;

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
    private final EntityEventsJournal entityEventsJournal;

    /**
     * Event format passed between the data generator and the event injector.
     */
    public static class ScriptEvent {
        long timestamp;
        String eventType;
        long uuid;
        boolean state;
        double destTier;
        double sourceTier;

        /**
         * Return string representation of event.
         * @return string representation of event
         */
        public String toString() {
            return String.format("%s@%d", eventType, timestamp);
        }
    }

    /**
     * Constructor.
     *
     * @param entitySavingsTracker entity savings tracker to inject actions into.
     * @param entityEventsJournal events journal to populate.
     */
    EventInjector(EntitySavingsTracker entitySavingsTracker,
                  EntityEventsJournal entityEventsJournal) {
        this.entitySavingsTracker = entitySavingsTracker;
        this.entityEventsJournal = entityEventsJournal;
    }

    /**
     * Start the event injector.
     */
    public void start() {
        (new Thread(new EventInjector(this.entitySavingsTracker, this.entityEventsJournal))).start();
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
                WatchKey wk = null;
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
                    }
                    wk.reset();
                }
            } catch (IOException e) {
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
        JsonReader reader = null;
        try {
            reader = new JsonReader(new FileReader(SCRIPT_FILE));
            List<ScriptEvent> events = Arrays.asList(gson.fromJson(reader, ScriptEvent[].class));
            events.forEach(event -> addEvent(event, entityEventsJournal));
        } catch (FileNotFoundException e) {
            logger.error("Cannot inject events: " + e.toString());
        } finally {
            // Remove the events available file.
            availableFile.delete();
        }
        // Call the tracker here to handle the injected events.  Always process all events. This
        // prevents the savings tracker discarding the injected events due to their timestamps
        // potentially occurring before the previous processing period.
        entitySavingsTracker.processEvents(LocalDateTime.MIN, LocalDateTime.now().truncatedTo(ChronoUnit.HOURS));
    }

    /**
     * Convert a script event to a SavingsEvent and add it to the journal.
     * @param event event generated by the script
     * @param entityEventsJournal event journal to populate
     */
     public static void addEvent(ScriptEvent event, EntityEventsJournal entityEventsJournal) {
        logger.debug("Adding event: " + event);
        Builder result = new SavingsEvent.Builder()
                .entityId(event.uuid)
                .timestamp(event.timestamp);
        if ("RESIZE_RECOMMENDATION".equals(event.eventType)) {
            EntityPriceChange entityPriceChange =  new EntityPriceChange.Builder()
                    .sourceCost(event.sourceTier)
                    .destinationCost(event.destTier)
                    .build();
            ActionEvent actionEvent = new ActionEvent.Builder()
                    .actionId(event.uuid)
                    .eventType(ActionEventType.RECOMMENDATION_ADDED)
                    .build();
            result.actionEvent(actionEvent).entityPriceChange(entityPriceChange);
        } else if ("CANCEL_RECOMMENDATION".equals(event.eventType)) {
            EntityPriceChange dummyPriceChange =  new EntityPriceChange.Builder()
                    .sourceCost(0d).destinationCost(0d).build();
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
                    .build();
            ActionEvent actionEvent = new ActionEvent.Builder()
                    .actionId(event.uuid)
                    .eventType(ActionEventType.EXECUTION_SUCCESS)
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
                    event.timestamp)
                .setEventInfo(TopologyEventInfo.newBuilder()
                        .setProviderChange(ProviderChangeDetails.newBuilder()
                                .setSourceProviderOid(1L)
                                .setDestinationProviderOid(2L)
                                .setProviderType(EntityType.VIRTUAL_MACHINE.getValue()))
                        .build())
                .build());
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
