package com.vmturbo.repository.topology;

import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.arangodb.ArangoDBException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Collections2;

import javaslang.Function1;

import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.components.common.diagnostics.Diagnosable;
import com.vmturbo.components.common.diagnostics.DiagnosticsException;
import com.vmturbo.repository.dto.ServiceEntityRepoDTO;
import com.vmturbo.repository.exception.GraphDatabaseExceptions.CollectionOperationException;
import com.vmturbo.repository.exception.GraphDatabaseExceptions.EdgeOperationException;
import com.vmturbo.repository.exception.GraphDatabaseExceptions.GraphDatabaseException;
import com.vmturbo.repository.exception.GraphDatabaseExceptions.VertexOperationException;
import com.vmturbo.repository.graph.GraphDefinition;
import com.vmturbo.repository.graph.driver.GraphDatabaseDriver;
import com.vmturbo.repository.graph.driver.GraphDatabaseDriverBuilder;
import com.vmturbo.repository.graph.executor.GraphDBExecutor;
import com.vmturbo.repository.graph.operator.TopologyGraphCreator;
import com.vmturbo.repository.listener.realtime.LiveTopologyStore;
import com.vmturbo.repository.listener.realtime.ProjectedRealtimeTopology.ProjectedTopologyBuilder;
import com.vmturbo.repository.listener.realtime.SourceRealtimeTopology.SourceRealtimeTopologyBuilder;
import com.vmturbo.repository.topology.TopologyID.TopologyType;
import com.vmturbo.repository.topology.protobufs.TopologyProtobufHandler;
import com.vmturbo.repository.topology.protobufs.TopologyProtobufWriter;
import com.vmturbo.repository.topology.protobufs.TopologyProtobufsManager;

/**
 * The {@link TopologyLifecycleManager} is the interface for both recording/writing new topologies
 * to all the underlying stores, for cleaning up topologies that are no longer necessary, and for
 * keeping track of the existing topologies. All interactions with topologies should be
 * performed via the methods on this class.
 * <p>
 * The general topology “lifecycle” is - arrive to the repository, get written to the graph
 * database, maybe get written as a blob, and eventually get removed when a newer topology comes in.
 * <p>
 * Topologies may also be removed (e.g. when a plan is deleted) via
 * {@link TopologyLifecycleManager#deleteTopology(TopologyID)}.
 */
public class TopologyLifecycleManager implements Diagnosable {

    private static final Logger LOGGER = LogManager.getLogger();

    private Map<Long, Map<TopologyType, TopologyID>> topologyIdByContextAndType = new HashMap<>();

    private final GraphDatabaseDriverBuilder graphDatabaseDriverBuilder;

    private final GraphDefinition graphDefinition;

    private final TopologyProtobufsManager topologyProtobufsManager;

    private final long realtimeTopologyContextId;

    // optional scheduled executor to perform delayed database drops
    private final ScheduledExecutorService scheduler;

    // how many seconds to delay realtime topology drops by
    private final long realtimeTopologyDropDelaySecs;

    // how many expected real time "Source" DB to be kept with full clean up?
    private final int numberOfExpectedRealtimeSourceDB;

    // how many expected real time "Projected" DB to be kept with full clean up?
    private final int numberOfExpectedRealtimeProjectedDB;

    private final LiveTopologyStore liveTopologyStore;

    private final boolean realtimeInMemory;

    private final GlobalSupplyChainManager globalSupplyChainManager;

    private final GraphDBExecutor graphDbExecutor;

    public TopologyLifecycleManager(@Nonnull final GraphDatabaseDriverBuilder graphDatabaseDriverBuilder,
                                    @Nonnull final GraphDefinition graphDefinition,
                                    @Nonnull final TopologyProtobufsManager topologyProtobufsManager,
                                    final long realtimeTopologyContextId,
                                    @Nonnull final ScheduledExecutorService scheduler,
                                    @Nonnull final LiveTopologyStore liveTopologyStore,
                                    final boolean realtimeInMemory,
                                    final long realtimeTopologyDropDelaySecs,
                                    final int numberOfExpectedRealtimeSourceDB,
                                    final int numberOfExpectedRealtimeProjectedDB,
                                    @Nonnull final GlobalSupplyChainManager globalSupplyChainManager,
                                    @Nonnull final GraphDBExecutor graphDBExecutor) {
        this(graphDatabaseDriverBuilder, graphDefinition, topologyProtobufsManager, realtimeTopologyContextId,
            scheduler, liveTopologyStore, realtimeInMemory, realtimeTopologyDropDelaySecs, numberOfExpectedRealtimeSourceDB,
            numberOfExpectedRealtimeProjectedDB, globalSupplyChainManager, graphDBExecutor, true);
    }

    @VisibleForTesting
    TopologyLifecycleManager(@Nonnull final GraphDatabaseDriverBuilder graphDatabaseDriverBuilder,
            @Nonnull final GraphDefinition graphDefinition,
            @Nonnull final TopologyProtobufsManager topologyProtobufsManager,
            final long realtimeTopologyContextId,
            @Nonnull final ScheduledExecutorService scheduler,
            @Nonnull final LiveTopologyStore liveTopologyStore,
            final boolean realtimeInMemory,
            final long realtimeTopologyDropDelaySecs,
            final int numberOfExpectedRealtimeSourceDB,
            final int numberOfExpectedRealtimeProjectedDB,
            @Nonnull final GlobalSupplyChainManager globalSupplyChainManager,
            @Nonnull final GraphDBExecutor graphDbExecutor,
            final boolean loadExisting) {
        this.graphDatabaseDriverBuilder = graphDatabaseDriverBuilder;
        this.graphDefinition = graphDefinition;
        this.topologyProtobufsManager = topologyProtobufsManager;
        this.realtimeTopologyContextId = realtimeTopologyContextId;
        this.scheduler = scheduler;
        this.liveTopologyStore = liveTopologyStore;
        this.realtimeInMemory = realtimeInMemory;
        this.realtimeTopologyDropDelaySecs = realtimeTopologyDropDelaySecs;
        this.numberOfExpectedRealtimeSourceDB = numberOfExpectedRealtimeSourceDB;
        this.numberOfExpectedRealtimeProjectedDB = numberOfExpectedRealtimeProjectedDB;
        this.globalSupplyChainManager = Objects.requireNonNull(globalSupplyChainManager);
        this.graphDbExecutor = Objects.requireNonNull(graphDbExecutor);

        if (realtimeTopologyDropDelaySecs <= 0) {
            LOGGER.info("realtimeTopologyDropDelaySecs is set to {} -- disabling delayed drop behavior.", realtimeTopologyDropDelaySecs);
        }

        if (loadExisting) {
            // TODO (roman, July 17 2017): This would be cleaner if the health monitor system supported
            // queueing operations until the dependency is available, so consider extending it to do
            // that.
            Executors.newSingleThreadExecutor().execute(
                    new RegisteredTopologyLoader(TimeUnit.MILLISECONDS.convert(10, TimeUnit.SECONDS),
                            graphDatabaseDriverBuilder, this, globalSupplyChainManager,
                            realtimeTopologyContextId));
        }
    }

    @Nonnull
    @Override
    public List<String> collectDiags() throws DiagnosticsException {
        return topologyIdByContextAndType.values().stream()
                .flatMap(idsByType -> idsByType.values().stream())
                // Save the database names in the diags.
                .map(TopologyID::toDatabaseName)
                .collect(Collectors.toList());
    }

    @Override
    public void restoreDiags(@Nonnull final List<String> collectedDiags) throws DiagnosticsException {
        // Overwrite whatever is in the lifecycle manager with the restored diags.
        // We rely on the diags handler to actually do the restoration of the database
        // to arangodb.
        collectedDiags.stream()
                .map(TopologyID::fromDatabaseName)
                .filter(Optional::isPresent).map(Optional::get)
                // Register, overwriting any existing TID for the same context and topology type.
                // This will drop any database associated with the existing TID.
                .forEach(tid -> registerTopology(tid, true));
    }

    /**
     * Re-load available topologies from the database in a separate thread.
     * Using a separate thread because we don't want the operation to fail if the database
     * is down when the manager is created.
     */
    @VisibleForTesting
    static class RegisteredTopologyLoader implements Runnable {
        private final long pollingIntervalMs;
        private final long realtimeTopologyContextId;
        private GraphDatabaseDriverBuilder driverBuilder;
        private TopologyLifecycleManager lifecycleManager;
        private GlobalSupplyChainManager globalSupplyChainManager;

        @VisibleForTesting
        RegisteredTopologyLoader(final long pollingIntervalMs,
                                 @Nonnull final GraphDatabaseDriverBuilder driverBuilder,
                                 @Nonnull final TopologyLifecycleManager lifecycleManager,
                                 @Nonnull final GlobalSupplyChainManager globalSupplyChainManager,
                                 final long realtimeTopologyContextId) {
            this.pollingIntervalMs = pollingIntervalMs;
            this.driverBuilder = Objects.requireNonNull(driverBuilder);
            this.lifecycleManager = Objects.requireNonNull(lifecycleManager);
            this.globalSupplyChainManager = Objects.requireNonNull(globalSupplyChainManager);
            this.realtimeTopologyContextId = realtimeTopologyContextId;
        }

        @Override
        public void run() {
            Set<String> databases = null;
            while (databases == null) {
                try {
                    databases = driverBuilder.listDatabases();
                } catch (GraphDatabaseException e) {
                    try {
                        Thread.sleep(pollingIntervalMs);
                    } catch (InterruptedException ie) {
                        LOGGER.error("Database re-loading thread interrupted.", ie);
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            }

            databases
                .stream()
                .map(TopologyID::fromDatabaseName)
                .filter(Optional::isPresent)
                .map(Optional::get)
                // This means if there are multiple databases for the same context and topology type,
                // one of them will get dropped.
                // Don't overwrite any new topologies that came in though.
                .forEach(tid -> {
                    final boolean registered = lifecycleManager.registerTopology(tid, false);
                    // Only clean up non-realtime (plan) topologies, real time topologies are clean up in
                    // deleteObsoletedRealtimeDB method
                    if (!registered && tid.getContextId() != realtimeTopologyContextId ) {
                        driverBuilder.build(tid.toDatabaseName()).dropDatabase();
                    }
                });

            Optional<TopologyID> realtimeId = lifecycleManager.getRealtimeTopologyId();
            if (realtimeId.isPresent()) {
                globalSupplyChainManager.loadGlobalSupplyChainFromDb(realtimeId.get());
            }
        }
    }

    /**
     * The method provides a "full" clean up on realtime topologies in DB. Obsoleted real time topologies
     * can happened when DB (Arango) crashed during inserting real time topologies.
     * Details:
     * 1. Delete the realtime topology associated with passed in topology id.
     * 2. Retrieve all realtime topologies from DB and sort the "Source" and "Projected" topologies separately
     * based on topology id with ascending order.
     * 3. If the size of each realtime topologies are bigger than expected numbers
     * ("numberOfExpectedRealtimeSourceDB" and "numberOfExpectedRealtimeProjectedDB"), delete the earlier ones.
     *
     * @param topologyID new realtime toplogyid that to be deleted.
     * @param driverBuilder Graph database driver builder to interact with DB.
     * @param realtimeTopologyContextId real time topology context id.
     * @param numberOfExpectedRealtimeSourceDB  number of expected real time "Source" DB, injected from environment.
     * @param numberOfExpectedRealtimeProjectedDB number of expected real time "Projected" DB, injected from environment.
     */
    @VisibleForTesting
    static void deleteObsoletedRealtimeDB(@Nonnull final TopologyID topologyID,
                                          @Nonnull final GraphDatabaseDriverBuilder driverBuilder,
                                          @Nonnull final GlobalSupplyChainManager globalSupplyChainManager,
                                          final long realtimeTopologyContextId,
                                          final int numberOfExpectedRealtimeSourceDB,
                                          final int numberOfExpectedRealtimeProjectedDB) {
        // explicitly deleting DB associated to the topologyID, so if deletion failed, it will provide hints that
        // this DB was deleted externally.
        try {
            LOGGER.info("Dropping database {}.", topologyID.toDatabaseName());
            globalSupplyChainManager.removeGlobalSupplyChain(topologyID);
            driverBuilder.build(topologyID.toDatabaseName()).dropDatabase();
        } catch (ArangoDBException e) {
            if (e.getResponseCode().intValue() == 404) {
                // ignore database not found errors since we are trying to drop these anyways.
                LOGGER.warn("Database for previous topology {} was not found -- skipping drop.", topologyID);
            } else {
                LOGGER.error("Failed to clean up topology {}. ", topologyID);
            }
        }
        Set<String> databases;
        try {
            databases = driverBuilder.listDatabases();
        } catch (GraphDatabaseException e) {
            LOGGER.error("Failed to listing databases during full realtime topology clean up: ", e);
            return;
        }

        final List<TopologyID> realtimeTopologyIds = databases
            .stream()
            .map(TopologyID::fromDatabaseName)
            .filter(Optional::isPresent)
            .map(Optional::get)
            // only clean real time topologies
            .filter(tid -> tid.getContextId() == realtimeTopologyContextId)
            .collect(Collectors.toList());
        deleteObsoletedRealtimeDBHelper(driverBuilder, realtimeTopologyIds,
            TopologyType.PROJECTED, numberOfExpectedRealtimeSourceDB);
        deleteObsoletedRealtimeDBHelper(driverBuilder, realtimeTopologyIds,
            TopologyType.SOURCE, numberOfExpectedRealtimeProjectedDB);
    }

    /* internal helper method to delete obsoleted realtime topologies */
    private static void deleteObsoletedRealtimeDBHelper(@Nonnull GraphDatabaseDriverBuilder driverBuilder,
                                                        @Nonnull final List<TopologyID> realtimeTopologyIds,
                                                        @Nonnull final TopologyType topologyType,
                                                        final int number) {
        final List<TopologyID> tobeRemovedIds = realtimeTopologyIds.stream()
            .filter(topologyID -> topologyID.getType() == topologyType)
            // assume newer topologies always have bigger topologyId,
            // see com.vmturbo.commons.idgen.IdentityGenerator for details.
            .sorted(Comparator.comparingLong(topologyID -> topologyID.getTopologyId()))
            .collect(Collectors.toList());

        if (tobeRemovedIds.size() > number) {
            tobeRemovedIds.stream()
                .limit(tobeRemovedIds.size() - number)
                .forEach(tid -> {
                        try {
                            driverBuilder.build(tid.toDatabaseName()).dropDatabase();
                            LOGGER.warn("Dropping obsoleted real time database {}.",
                                tid.toDatabaseName());
                        } catch (ArangoDBException e) {
                            LOGGER.error("Failed to drop obsoleted real time database {} with exception:",
                                tid.toDatabaseName(), e.getMessage());
                        }
                    }
                );
        }
    }

    public TopologyCreator<TopologyEntityDTO> newSourceTopologyCreator(@Nonnull final TopologyID topologyID,
                                                                       @Nonnull final TopologyInfo topologyInfo) {
        if (realtimeInMemory && topologyID.getContextId() == realtimeTopologyContextId) {
            return new InMemorySourceTopologyCreator(liveTopologyStore.newRealtimeTopology(topologyInfo));
        } else {
            return new ArangoSourceTopologyCreator(
                topologyID,
                graphDatabaseDriverBuilder,
                this::registerTopology,
                graphDriver -> new TopologyGraphCreator(graphDriver, graphDefinition),
                TopologyEntityDTOConverter::convertToServiceEntityRepoDTOs,
                globalSupplyChainManager,
                graphDbExecutor,
                realtimeTopologyContextId,
                numberOfExpectedRealtimeSourceDB,
                numberOfExpectedRealtimeProjectedDB);
        }
    }

    public TopologyCreator<ProjectedTopologyEntity> newProjectedTopologyCreator(@Nonnull final TopologyID topologyID,
                                                                                @Nonnull final TopologyInfo originalTopologyInfo) {
        if (realtimeInMemory && topologyID.getContextId() == realtimeTopologyContextId) {
            return new InMemoryProjectedTopologyCreator(
                liveTopologyStore.newProjectedTopology(topologyID.getTopologyId(), originalTopologyInfo));
        } else {
            return new ArangoProjectedTopologyCreator(
                topologyID,
                graphDatabaseDriverBuilder,
                topologyProtobufsManager,
                this::registerTopology,
                graphDriver -> new TopologyGraphCreator(graphDriver, graphDefinition),
                TopologyEntityDTOConverter::convertToServiceEntityRepoDTOs,
                globalSupplyChainManager,
                graphDbExecutor,
                realtimeTopologyContextId,
                numberOfExpectedRealtimeSourceDB,
                numberOfExpectedRealtimeProjectedDB);
        }
    }

    /**
     * Register a new topology ID. See
     * {@link TopologyLifecycleManager#registerTopology(TopologyID)}.
     *
     * @param tid The {@link TopologyID} to register.
     * @param overwrite Whether to overwrite an already-registered {@link TopologyID} with the
     *                  same type and context. If true, drop the database associated with the
     *                  conflicting {@link TopologyID}.
     * @return true if the topology was registered, false otherwise. If overwrite is true,
     *         the result will always be true.
     */
    @VisibleForTesting
    boolean registerTopology(@Nonnull final TopologyID tid, final boolean overwrite) {
        final Map<TopologyType, TopologyID> idByTypeInContext =
                topologyIdByContextAndType.computeIfAbsent(tid.getContextId(), k -> new HashMap<>());

        if (overwrite) {
            // Replace the previous one.
            TopologyID topologyID = idByTypeInContext.put(tid.getType(), tid);
            if (topologyID != null) {
                Runnable dropTask = () -> {
                        deleteObsoletedRealtimeDB(topologyID, graphDatabaseDriverBuilder,
                            globalSupplyChainManager,
                            realtimeTopologyContextId, numberOfExpectedRealtimeSourceDB,
                            numberOfExpectedRealtimeProjectedDB);
                };
                // determine if we need to schedule a delayed drop of the existing database
                long delaySeconds = realtimeTopologyDropDelaySecs;
                if (scheduler != null && delaySeconds > 0) {
                    // schedule for delayed drop
                    LOGGER.info("Scheduling drop of database {} in {} seconds.", topologyID.toDatabaseName(), delaySeconds);
                    scheduler.schedule(dropTask, delaySeconds, TimeUnit.SECONDS);
                } else {
                    // run immediately
                    dropTask.run();
                }
            }
            return true;
        } else {
            final TopologyID existing = idByTypeInContext.get(tid.getType());
            if (existing != null) {
                LOGGER.warn("Not registering topology {} because it would overwrite " +
                    "the existing topology {}.", tid, existing);
            } else {
                idByTypeInContext.put(tid.getType(), tid);
            }
            return existing == null;
        }
    }

    /**
     * Register a new topology ID. This will make the ID "visible" to all user requests.
     * The database and graphs associated with the Topology ID should already be written, or else
     * we will be accepting requests to topologies with no data.
     * <p>
     * The {@link TopologyLifecycleManager} only keeps a single topology for each {context,type}
     * tuple. If there is an existing topology with the same context and type as the input,
     * all data associated with it will be deleted.
     *
     * @param tid The {@link TopologyID} to register.
     */
    void registerTopology(@Nonnull final TopologyID tid) {
        registerTopology(tid, true);
    }


    // This is ugly. Arango doesn't have an api for getting the
    // error code. so have to hardcode it here.
    // There is ErroNums class in 3.x but not in 4.x
    // http://arangodb.github.io/arangodb-java-driver/javadoc-3_0/com/arangodb/ErrorNums.html
    // http://arangodb.github.io/arangodb-java-driver/javadoc-4_0/index.html?overview-summary.html
    public static final int ERROR_ARANGO_DATABASE_NOT_FOUND = 1228;

    /**
     * Delete a topology from the repository. If the {@link TopologyID} does not exist,
     * this has no effect.
     *
     * TODO (roman, Nov 7 2017): We probably don't need topology-level granularity, and only
     * want to clear a whole context. This would make bookeeping simpler. But we need to convert
     * topology protobuf manager to operate on context IDs before we can do that.
     *
     * @param tid The ID of the topology to remove.
     * @throws TopologyDeletionException If there are errors deleting the topology.
     */
    public void deleteTopology(@Nonnull final TopologyID tid) throws TopologyDeletionException {
        final List<String> errors = new LinkedList<>();
        try {
            topologyProtobufsManager.createTopologyProtobufReader(tid.getTopologyId(),
                    Optional.empty()).delete();
        } catch (NoSuchElementException e) {
            // This is not considered an error, since the requested topology is "deleted" now :)
            LOGGER.warn("No topology protobufs to delete for topology {}", tid);
        } catch (ArangoDBException e) {
            errors.add(e.getErrorMessage());
        }

        try {
            graphDatabaseDriverBuilder.build(tid.toDatabaseName()).dropDatabase();

            // Once we dropped the database, get rid of it in the internal map (if it exists).
            final Map<TopologyType, TopologyID> idByTypeInContext =
                    topologyIdByContextAndType.get(tid.getContextId());
            if (idByTypeInContext != null) {
                final TopologyID id = idByTypeInContext.get(tid.getType());
                if (id.equals(tid)) {
                    idByTypeInContext.remove(tid.getType());
                }
            }
        } catch (ArangoDBException e) {
            if (e.getErrorNum() == ERROR_ARANGO_DATABASE_NOT_FOUND) {
                // This is not considered an error, since the requested db is "deleted" now :)
                LOGGER.warn("No database to delete for topology {}", tid);
            } else {
                errors.add(e.getErrorMessage());
            }
        }

        if (!errors.isEmpty()) {
            throw new TopologyDeletionException(errors);
        }
    }

    /**
     * Get the {@link TopologyID} of the latest realtime topology. This is the last topology
     * received from a Topology Processor broadcast.
     *
     * @return The {@link TopologyID} of the latest realtime topology, or an empty optional
     *         if there has been no broadcast.
     */
    @Nonnull
    public Optional<TopologyID> getRealtimeTopologyId() {
        return getRealtimeTopologyId(TopologyType.SOURCE);
    }

    @Nonnull
    public Optional<TopologyID> getRealtimeTopologyId(@Nonnull final TopologyType topologyType) {
        RealtimeTopologyID realtimeTopologyID
                = new RealtimeTopologyID(realtimeTopologyContextId, topologyType);
        // return an empty optional if there is no topology id registered yet, otherwise return
        // an optional containing a dynamic reference to the realtime topology database id.
        return realtimeTopologyID.isPresent() ? Optional.of(realtimeTopologyID) : Optional.empty();
    }

    /**
     * A convenience method - the equivalent of calling {@link TopologyID#database()} on the result
     * of {@link TopologyLifecycleManager#getRealtimeTopologyId()}.
     *
     * @return The {@link TopologyDatabase} of the latest realtime topology, or an empty optional
     *         if there has been no broadcast.
     */
    public Optional<TopologyDatabase> getRealtimeDatabase() {
        return getRealtimeTopologyId().map(TopologyID::database);
    }

    /**
     * Get the {@link TopologyID} representing the latest topology with a particular topology
     * context and type.
     *
     * @param contextId The topology context ID - for example, the realtime context, or a plan
     *                  context.
     * @param type The {@link TopologyType} of the topology.
     * @return The {@link TopologyID} of the latest topology for the desired context and type,
     *         or an empty optional if there has been no broadcast of a matching topology.
     */
    public Optional<TopologyID> getTopologyId(final long contextId,
                                               @Nonnull final TopologyType type) {
        final Map<TopologyType, TopologyID> idByTypeInContext =
                topologyIdByContextAndType.get(contextId);
        if (idByTypeInContext != null) {
            final TopologyID id = idByTypeInContext.get(type);
            if (id != null) {
                return Optional.of(id);
            }
        }
        return Optional.empty();
    }

    /**
     * A convenience method - the equivalent of calling {@link TopologyID#database()} on
     * the result of {@link TopologyLifecycleManager#getTopologyId(long, TopologyType)}.
     *
     * @param contextId See {@link TopologyLifecycleManager#getTopologyId(long, TopologyType)}.
     * @param type See {@link TopologyLifecycleManager#getTopologyId(long, TopologyType)}.
     * @return The {@link TopologyDatabase} of the latest topology for the desired context and type,
     *         or an empty optional if there has been no broadcast of a matching topology.
     */
    public Optional<TopologyDatabase> databaseOf(final long contextId,
                                                 @Nonnull final TopologyType type) {
        return getTopologyId(contextId, type)
            .map(TopologyID::database);
    }

    /**
     * A factory purely to make unit testing of {@link TopologyCreator} easier.
     */
    @FunctionalInterface
    @VisibleForTesting
    interface TopologyGraphCreatorFactory {
        TopologyGraphCreator newGraphCreator(GraphDatabaseDriver driver);
    }

    /**
     * A converter to make unit testing of {@link TopologyCreator} easier.
     */
    @FunctionalInterface
    @VisibleForTesting
    interface EntityConverter {
        Set<ServiceEntityRepoDTO> convert(Collection<TopologyEntityDTO> entities);
    }

    public interface TopologyCreator<ENTITY_DTO_TYPE> {
        void initialize() throws GraphDatabaseException;

        void addEntities(final Collection<ENTITY_DTO_TYPE> entities) throws TopologyEntitiesException;

        /**
         * Finalize the topology. To be called after all the entities are added.
         *
         * @throws GraphDatabaseException If there is an issue with the underlying graph database.
         */
        void complete() throws GraphDatabaseException, TopologyEntitiesException;

        /**
         * Roll back the topology, deleting any entities written so far.
         */
        void rollback();

    }

    public static class InMemorySourceTopologyCreator implements TopologyCreator<TopologyEntityDTO> {
        private final SourceRealtimeTopologyBuilder sourceRealtimeTopologyBuilder;

        public InMemorySourceTopologyCreator(@Nonnull final SourceRealtimeTopologyBuilder sourceRealtimeTopologyBuilder) {
            this.sourceRealtimeTopologyBuilder = sourceRealtimeTopologyBuilder;
        }

        @Override
        public void initialize() throws GraphDatabaseException {
            // Nothing to initialize.
        }

        @Override
        public void addEntities(final Collection<TopologyEntityDTO> entities) throws TopologyEntitiesException {
            sourceRealtimeTopologyBuilder.addEntities(entities);
        }

        @Override
        public void complete() throws GraphDatabaseException {
            sourceRealtimeTopologyBuilder.finish();
        }

        @Override
        public void rollback() {
            // Nothing to roll back - keep the previous topology.
        }
    }

    public static class InMemoryProjectedTopologyCreator implements TopologyCreator<ProjectedTopologyEntity> {
        private final ProjectedTopologyBuilder projectedTopologyBuilder;

        public InMemoryProjectedTopologyCreator(@Nonnull final ProjectedTopologyBuilder projectedTopologyBuilder) {
            this.projectedTopologyBuilder = projectedTopologyBuilder;
        }

        @Override
        public void initialize() throws GraphDatabaseException {
            // Nothing to initialize.
        }

        @Override
        public void addEntities(final Collection<ProjectedTopologyEntity> entities) throws TopologyEntitiesException {
            projectedTopologyBuilder.addEntities(entities);
        }

        @Override
        public void complete() throws GraphDatabaseException {
            projectedTopologyBuilder.finish();
        }

        @Override
        public void rollback() {
            // Nothing to roll back - keep the previous topology.
        }
    }
    /**
     * A {@link ArangoTopologyCreator} is responsible for storing a topology (i.e. a set of
     * {@link TopologyEntityDTO}s) in ArangoDB.
     * <p>
     * The user obtains an instance via
     * {@link TopologyLifecycleManager#newSourceTopologyCreator(TopologyID, TopologyInfo)} or
     * {@link TopologyLifecycleManager#newProjectedTopologyCreator(TopologyID, TopologyInfo)}, and is responsible
     * for calling the methods in the following order:
     *
     *  {@link TopologyCreator#initialize()}
     *     |
     *  {@link TopologyCreator#addEntities(Collection)} as many times as necessary
     *     |
     *  {@link TopologyCreator#complete()} or {@link TopologyCreator#rollback()}.
     */
    public abstract static class ArangoTopologyCreator<ENTITY_DTO_TYPE> implements TopologyCreator<ENTITY_DTO_TYPE> {

        private final GraphDatabaseDriver graphDatabaseDriver;

        final TopologyGraphCreator topologyGraphCreator;

        private final TopologyID topologyID;

        final EntityConverter entityConverter;

        final Optional<TopologyProtobufWriter> topologyProtobufWriter;

        private final Consumer<TopologyID> onComplete;
        private final GraphDatabaseDriverBuilder graphDatabaseDriverBuilder;
        private final GlobalSupplyChainManager globalSupplyChainManager;
        private final GraphDBExecutor graphDbExecutor;
        private final long realtimeTopologyContextId;
        private final int numberOfExpectedRealtimeSourceDB;
        private final int numberOfExpectedRealtimeProjectedDB;

        ArangoTopologyCreator(@Nonnull final TopologyID topologyID,
                        @Nonnull final GraphDatabaseDriverBuilder graphDatabaseDriverBuilder,
                        @Nonnull final Optional<TopologyProtobufWriter> topologyProtobufWriter,
                        @Nonnull final Consumer<TopologyID> onComplete,
                        @Nonnull final TopologyGraphCreatorFactory topologyGraphCreatorFactory,
                        @Nonnull final EntityConverter entityConverter,
                        @Nonnull final GlobalSupplyChainManager globalSupplyChainManager,
                        @Nonnull final GraphDBExecutor graphDbExecutor,
                        final long realtimeTopologyContextId,
                        final int numberOfExpectedRealtimeSourceDB,
                        final int numberOfExpectedRealtimeProjectedDB) {
            this.topologyID = Objects.requireNonNull(topologyID);
            this.graphDatabaseDriverBuilder = Objects.requireNonNull(graphDatabaseDriverBuilder);
            this.graphDatabaseDriver = Objects.requireNonNull(graphDatabaseDriverBuilder.build(
                    topologyID.toDatabaseName()));
            this.onComplete = onComplete;
            this.topologyProtobufWriter = topologyProtobufWriter;

            topologyGraphCreator = topologyGraphCreatorFactory.newGraphCreator(graphDatabaseDriver);
            this.entityConverter = entityConverter;
            this.globalSupplyChainManager = Objects.requireNonNull(globalSupplyChainManager);
            this.graphDbExecutor = Objects.requireNonNull(graphDbExecutor);
            this.realtimeTopologyContextId = realtimeTopologyContextId;
            this.numberOfExpectedRealtimeSourceDB = numberOfExpectedRealtimeSourceDB;
            this.numberOfExpectedRealtimeProjectedDB = numberOfExpectedRealtimeProjectedDB;
        }


        /**
         * Perform all initialization required to store the topology.
         * The caller should only call this method once.
         *
         * @throws GraphDatabaseException If there is an issue with the underlying graph database.
         */
        @Override
        public void initialize() throws GraphDatabaseException {
            topologyGraphCreator.init();
        }

        /**
         * Finalize the topology. To be called after all the entities are added.
         *
         * @throws GraphDatabaseException If there is an issue with the underlying graph database.
         */
        @Override
        public void complete() throws GraphDatabaseException, TopologyEntitiesException {
            onComplete.accept(topologyID);
        }

        /**
         * Roll back the topology, deleting any entities written so far.
         */
        @Override
        public void rollback() {
            deleteObsoletedRealtimeDB(topologyID, graphDatabaseDriverBuilder,
                    globalSupplyChainManager, realtimeTopologyContextId,
                    numberOfExpectedRealtimeSourceDB, numberOfExpectedRealtimeProjectedDB);
            topologyProtobufWriter.ifPresent(TopologyProtobufHandler::delete);
        }
    }

    /**
     * The {@link TopologyCreator} to use for the source topology.
     */
    @VisibleForTesting
    static class ArangoSourceTopologyCreator extends ArangoTopologyCreator<TopologyEntityDTO> {

        private final GlobalSupplyChain globalSupplyChain;

        ArangoSourceTopologyCreator(@Nonnull final TopologyID topologyID,
                              @Nonnull final GraphDatabaseDriverBuilder graphDatabaseDriverBuilder,
                              @Nonnull final Consumer<TopologyID> onComplete,
                              @Nonnull final TopologyGraphCreatorFactory topologyGraphCreatorFactory,
                              @Nonnull final EntityConverter entityConverter,
                              @Nonnull final GlobalSupplyChainManager globalSupplyChainManager,
                              @Nonnull final GraphDBExecutor graphDBExecutor,
                              final long realtimeTopologyContextId,
                              final int numberOfExpectedRealtimeSourceDB,
                              final int numberOfExpectedRealtimeProjectedDB) {
            super(topologyID, graphDatabaseDriverBuilder, Optional.empty(), onComplete,
                    topologyGraphCreatorFactory, entityConverter, globalSupplyChainManager,
                    graphDBExecutor, realtimeTopologyContextId, numberOfExpectedRealtimeSourceDB,
                    numberOfExpectedRealtimeProjectedDB);
            Preconditions.checkArgument(!topologyProtobufWriter.isPresent());
            this.globalSupplyChain = new GlobalSupplyChain(topologyID, graphDBExecutor);
        }

        /**
         * Add a collection of entities to the topology.
         * The caller can call this method as many times as necessary.
         *
         * @param entities The entities to add.
         * @throws TopologyEntitiesException If there is any issue writing the entities.
         */
        @Override
        public void addEntities(final Collection<TopologyEntityDTO> entities)
                throws TopologyEntitiesException {
            try {
                topologyGraphCreator.updateTopologyToDb(entityConverter.convert(entities));
                globalSupplyChain.processEntities(entities);
            } catch (VertexOperationException | EdgeOperationException | CollectionOperationException e) {
                throw new TopologyEntitiesException(e);
            }
        }

        /**
         * Finalize the topology. To be called after all the entities are added.
         *
         * @throws GraphDatabaseException If there is an issue with the underlying graph database.
         */
        @Override
        public void complete() throws GraphDatabaseException, TopologyEntitiesException {
            globalSupplyChain.seal();
            globalSupplyChain.store();
            super.globalSupplyChainManager.addNewGlobalSupplyChain(super.topologyID, globalSupplyChain);
            super.onComplete.accept(super.topologyID);
        }
    }

    /**
     * The {@link TopologyCreator} to use for the projected topology.
     */
    @VisibleForTesting
    static class ArangoProjectedTopologyCreator extends ArangoTopologyCreator<ProjectedTopologyEntity> {
        ArangoProjectedTopologyCreator(@Nonnull final TopologyID topologyID,
                                 @Nonnull final GraphDatabaseDriverBuilder graphDatabaseDriverBuilder,
                                 @Nonnull final TopologyProtobufsManager protobufsManager,
                                 @Nonnull final Consumer<TopologyID> onComplete,
                                 @Nonnull final TopologyGraphCreatorFactory topologyGraphCreatorFactory,
                                 @Nonnull final EntityConverter entityConverter,
                                 @Nonnull final GlobalSupplyChainManager globalSupplyChainManager,
                                 @Nonnull final GraphDBExecutor graphDBExecutor,
                                 final long realtimeTopologyContextId,
                                 final int numberOfExpectedRealtimeSourceDB,
                                 final int numberOfExpectedRealtimeProjectedDB) {
            super(topologyID, graphDatabaseDriverBuilder,
                TopologyUtil.isPlanProjectedTopology(topologyID, realtimeTopologyContextId) ?
                    Optional.of(protobufsManager.createTopologyProtobufWriter(topologyID.getTopologyId())) : Optional.empty(),
                    onComplete, topologyGraphCreatorFactory, entityConverter, globalSupplyChainManager,
                    graphDBExecutor, realtimeTopologyContextId, numberOfExpectedRealtimeSourceDB,
                    numberOfExpectedRealtimeProjectedDB);
        }

        /**
         * Add a collection of entities to the topology.
         * The caller can call this method as many times as necessary.
         *
         * @param entities The entities to add.
         * @throws TopologyEntitiesException If there is any issue writing the entities.
         */
        @Override
        public void addEntities(final Collection<ProjectedTopologyEntity> entities)
                throws TopologyEntitiesException {
            try {
                topologyGraphCreator.updateTopologyToDb(
                    entityConverter.convert(Collections2.transform(entities,
                            ProjectedTopologyEntity::getEntity)));
                topologyProtobufWriter.ifPresent(writer -> writer.storeChunk(entities));
            } catch (VertexOperationException | EdgeOperationException | CollectionOperationException e) {
                throw new TopologyEntitiesException(e);
            }
        }
    }

    /**
     * An exception thrown when the {@link TopologyCreator} fails to persist a some entities.
     */
    public static class TopologyEntitiesException extends Exception {
        public TopologyEntitiesException(Throwable cause) {
            super(cause);
        }
    }

    public static class TopologyDeletionException extends Exception {

        public TopologyDeletionException(@Nonnull final List<String> errors) {
            super(StringUtils.join(errors, ", "));
        }
    }

    /**
     * A {@link TopologyDatabase} variant that will be dynamically re-evaluated each time it is
     * matched. We can use this to dynamically provide the current "realtime" topology database
     * name, for example.
     */
    public static class RealtimeTopologyDatabase extends TopologyDatabase {

        private Supplier<TopologyDatabase> databaseSupplier;

        /**
         *
         * @param databaseSupplier A function that will supply the database to use.
         */
        RealtimeTopologyDatabase(Supplier<TopologyDatabase> databaseSupplier) {
            this.databaseSupplier = databaseSupplier;
        }

        /**
         * @return true, if the database supplier function detects a value. false, if it would return
         * null.
         */
        public boolean hasValue() {
            return (databaseSupplier.get() != null);
        }

        private TopologyDatabase eval() {
            return databaseSupplier.get();
        }

        @Override
        public <R> R match(Function1<String, R> dbName) {
            return eval().match(dbName);
        }

        @Override
        public String toString() {
            return this.eval().toString();
        }
    }

    /**
     * A dynamic topology ID differs from a "regular" topology id in that the "topology id" component
     * of the "topologyID" (arrgh!!) is dynamic and will point to the current topology id for the
     * specified (static) topology context id. This means the database name it resolves to will also
     * be dynamic.
     */
    public class RealtimeTopologyID extends TopologyID {

        public RealtimeTopologyID(final long contextId, final TopologyType type) {
            super(contextId, 0, type);
        }

        @Override
        public long getTopologyId() {
            Optional<TopologyID> topologyID = TopologyLifecycleManager.this.getTopologyId(getContextId(), getType());
            // even though this is an optional, we are going to use the get() regardless of whether
            // the value exists. If the value turns out not to exist, we will propagate a
            // NoSuchElementException to indicate that the topology id doesn't exist.
            return topologyID.get().getTopologyId();
        }

        /**
         *
         * @return a {@link RealtimeTopologyDatabase} the dynamically recalculates the database name when
         *      * evaluated.
         */
        @Override
        public TopologyDatabase database() {
            return new RealtimeTopologyDatabase(()
                    -> TopologyLifecycleManager.this.getTopologyId(getContextId(), getType())
                    .map(TopologyID::database)
                    .orElse(null));
        }

        /**
         *
         * @return true, if there is a valid topology id present for this topology
         */
        public boolean isPresent() {
            return TopologyLifecycleManager.this.getTopologyId(getContextId(), getType()).isPresent();
        }

        @Override
        public int hashCode() {
            return Objects.hash(getContextId(), getTopologyId(), getType());
        }

        @Override
        public boolean equals(final Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (!(obj instanceof TopologyID)) {
                return false;
            }
            TopologyID other = (TopologyID) obj;
            return Objects.equals(getContextId(), other.getContextId()) &&
                    Objects.equals(getTopologyId(), other.getTopologyId()) &&
                    Objects.equals(getType(), other.getType());
        }
    }

}
