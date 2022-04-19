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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.search.Search;
import com.vmturbo.common.protobuf.search.Search.LogicalOperator;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter;
import com.vmturbo.common.protobuf.search.Search.SearchEntitiesRequest;
import com.vmturbo.common.protobuf.search.Search.SearchEntitiesResponse;
import com.vmturbo.common.protobuf.search.Search.SearchFilter;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.Search.SearchQuery;
import com.vmturbo.common.protobuf.search.SearchProtoUtil;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.Type;

/**
 * Support for injecting script scenarios into the billing based savings feature.
 */
public class DataInjectionMonitor implements Runnable {
    /**
     * Logger.
     */
    private static final Logger logger = LogManager.getLogger();

    private final String scriptFile;

    /**
     * Handler to process injected events.
     */
    private final ScenarioDataInjector scenarioDataInjector;

    /**
     * SavingsTracker to inject data into.
     */
    private final ScenarioDataHandler scenarioDataHandler;

    /**
     * Search service client, used for name to OID resolution.
     */
    private final SearchServiceBlockingStub searchServiceStub;

    /**
     * Event format passed between the data generator and the event injector.
     */
    public static class ScriptEvent {
        /**
         * Timestamp of event.
         */
        public Long timestamp;
        /**
         * Event type.
         */
        public String eventType;
        /**
         * Event's target UUID.
         */
        public String uuid;
    }

    /**
     * Constructor.
     *
     * @param scriptFilename filename to watch for
     * @param scenarioDataInjector test data injector
     * @param scenarioDataHandler test data handler to inject actions into.
     * @param searchServiceStub search service client
     */
    public DataInjectionMonitor(@Nonnull String scriptFilename,
            @Nonnull ScenarioDataInjector scenarioDataInjector,
            @Nonnull ScenarioDataHandler scenarioDataHandler,
            @Nonnull SearchServiceBlockingStub searchServiceStub) {
        this.scriptFile = Objects.requireNonNull(scriptFilename);
        this.scenarioDataInjector = Objects.requireNonNull(scenarioDataInjector);
        this.scenarioDataHandler = Objects.requireNonNull(scenarioDataHandler);
        this.searchServiceStub = Objects.requireNonNull(searchServiceStub);
    }


    /**
     * Start the event injector.
     */
    public void start() {
        logger.info("Enabling savings data injection");
        new Thread(new DataInjectionMonitor(scriptFile, scenarioDataInjector, scenarioDataHandler,
                searchServiceStub)).start();
    }

    /**
     * Start the event injector file watcher.
     */
    @Override
    public void run() {
        final Path monitoredPath = FileSystems.getDefault().getPath("/tmp");
        File file = new File(scriptFile + ".available");
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
        File availableFile = new File("/tmp/" + scriptFile + ".available");
        if (!availableFile.exists()) {
            return;
        }
        logger.debug("Injecting data from scenario events");
        Gson gson = new Gson();
        JsonReader reader;
        List<ScriptEvent> scriptEvents = new ArrayList<>();
        AtomicBoolean purgePreviousTestState = new AtomicBoolean(false);
        @Nonnull Map<String, Long> uuidMap = new HashMap<>();
        try {
            reader = new JsonReader(new FileReader("/tmp/" + scriptFile));
            scriptEvents = Arrays.asList(gson.fromJson(reader,
                    scenarioDataInjector.getScriptEventClass()));
            uuidMap = resolveEntities(scriptEvents);
            for (ScriptEvent event : scriptEvents) {
                Long uuid = uuidMap.getOrDefault(event.uuid, 0L);
                scenarioDataInjector.handleScriptEvent(event, uuid, purgePreviousTestState);
            }
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
        for (ScriptEvent event : scriptEvents) {
            earliestEventTime = Math.min(earliestEventTime, event.timestamp);
            latestEventTime = Math.max(latestEventTime, event.timestamp);
            if (uuidIsValid(event.uuid)) {
                participatingUuids.add(uuidMap.get(event.uuid));
            }
        }
        if (earliestEventTime > latestEventTime) {
            logger.warn("No events in script file - not running savings tracker");
            return;
        }
        LocalDateTime startTime = makeLocalDateTime(earliestEventTime, false);
        LocalDateTime endTime = makeLocalDateTime(latestEventTime, true);
        if (purgePreviousTestState.get()) {
            scenarioDataHandler.purgeState(participatingUuids);
        }
        scenarioDataHandler.processStates(participatingUuids, startTime, endTime);
    }

    /**
     * Return whether a UUID is valid.  Valid UUIDs are non-null, not empty, and not "0".
     *
     * @param uuid uuid to check
     * @return true if the event's UUID is valid.
     */
    private static boolean uuidIsValid(@Nullable String uuid) {
        return uuid != null && !uuid.isEmpty() && !uuid.equals("0");
    }

    @Nonnull
    private Map<String, Long> resolveEntities(List<ScriptEvent> events) {
        SearchQuery.Builder searchQueryBuilder = SearchQuery.newBuilder()
                .setLogicalOperator(LogicalOperator.OR);
        Set<String> entityNames = events.stream()
                .map(e -> e.uuid)
                // If the event is not associated with an entity, skip it.
                .filter(DataInjectionMonitor::uuidIsValid)
                .collect(Collectors.toSet());
        for (String entityName : entityNames) {
            SearchParameters.Builder parametersBuilder = SearchParameters.newBuilder();

            // Starting filter
            List<String> entityTypes = ImmutableList.of(
                    "VirtualMachine",
                    "VirtualVolume",
                    "Database",
                    "DatabaseServer");
            PropertyFilter propertyFilter = SearchProtoUtil.entityTypeFilter(entityTypes);
            parametersBuilder.setStartingFilter(propertyFilter);

            // Search filter
            propertyFilter = SearchProtoUtil.stringPropertyFilterRegex("displayName", entityName);
            SearchFilter searchFilter = SearchProtoUtil.searchFilterProperty(propertyFilter);
            parametersBuilder.addSearchFilter(searchFilter);
            searchQueryBuilder.addSearchParameters(parametersBuilder);
        }
        Search.SearchEntitiesRequest searchRequest = SearchEntitiesRequest.newBuilder()
                .setSearch(searchQueryBuilder)
                .setReturnType(Type.MINIMAL)
                .build();
        SearchEntitiesResponse rsp = searchServiceStub.searchEntities(searchRequest);
        Map<String, Long> resolvedEntities = new Hashtable<>();
        Map<String, Long> results = new Hashtable<>();
        if (rsp != null && rsp.getEntitiesList() != null) {
            for (PartialEntity partialEntity : rsp.getEntitiesList()) {
                MinimalEntity entity = partialEntity.getMinimal();
                resolvedEntities.put(entity.getDisplayName(), entity.getOid());
            }
        }
        // For any unresolved entity name, use a dummy OID base on the name as the resolved OID
        for (String entityName : entityNames) {
            Long oid = resolvedEntities.get(entityName);
            if (oid == null) {
                oid = makeDummyOid(entityName);
                logger.warn("Cannot determine OID for entity {} - using dummy OID {}",
                        entityName, oid);
            }
            results.put(entityName, oid);
        }
        return results;
    }

    /**
     * Create a dummy OID for entities that are not resolvable.
     *
     * @param entityName entity display name
     * @return dummy OID.  If the entity name can be parsed as a long, then that is returned, else
     * the hash code of the name is returned.
     */
    private static long makeDummyOid(String entityName) {
        try {
            return Long.valueOf(entityName);
        } catch (NumberFormatException e) {
            return entityName.hashCode();
        }
    }

    /**
     * Create a LocalDateTime from a millisecond timestamp with optional rounding.
     *
     * @param timestamp timestamp in currentTimeMillis
     * @param roundUp whether to round up to the next hour
     * @return LocalDateTime
     */
    @VisibleForTesting
    @Nonnull
    public static LocalDateTime makeLocalDateTime(Long timestamp, boolean roundUp) {
        LocalDateTime trueTime = Instant.ofEpochMilli(timestamp).atZone(ZoneId.of("UTC"))
                .toLocalDateTime();
        LocalDateTime truncated = trueTime.truncatedTo(ChronoUnit.HOURS);
        if (roundUp && !truncated.equals(trueTime)) {
            // Round up to the next whole hour.
            truncated = truncated.plusHours(1L);
        }
        return truncated;
    }
}
