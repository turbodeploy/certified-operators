package com.vmturbo.repository.topology;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.arangodb.ArangoDBException;
import com.google.common.annotations.VisibleForTesting;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.components.common.diagnostics.Diagnosable;
import com.vmturbo.repository.dto.ServiceEntityRepoDTO;
import com.vmturbo.repository.exception.GraphDatabaseExceptions.CollectionOperationException;
import com.vmturbo.repository.exception.GraphDatabaseExceptions.EdgeOperationException;
import com.vmturbo.repository.exception.GraphDatabaseExceptions.GraphDatabaseException;
import com.vmturbo.repository.exception.GraphDatabaseExceptions.VertexOperationException;
import com.vmturbo.repository.graph.GraphDefinition;
import com.vmturbo.repository.graph.driver.GraphDatabaseDriver;
import com.vmturbo.repository.graph.driver.GraphDatabaseDriverBuilder;
import com.vmturbo.repository.graph.operator.TopologyGraphCreator;
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

    public TopologyLifecycleManager(@Nonnull final GraphDatabaseDriverBuilder graphDatabaseDriverBuilder,
                                    @Nonnull final GraphDefinition graphDefinition,
                                    @Nonnull final TopologyProtobufsManager topologyProtobufsManager,
                                    final long realtimeTopologyContextId) {
        this(graphDatabaseDriverBuilder, graphDefinition,
                topologyProtobufsManager, realtimeTopologyContextId, true);
    }

    @VisibleForTesting
    TopologyLifecycleManager(@Nonnull final GraphDatabaseDriverBuilder graphDatabaseDriverBuilder,
                                    @Nonnull final GraphDefinition graphDefinition,
                                    @Nonnull final TopologyProtobufsManager topologyProtobufsManager,
                                    final long realtimeTopologyContextId,
                                    final boolean loadExisting) {
        this.graphDatabaseDriverBuilder = graphDatabaseDriverBuilder;
        this.graphDefinition = graphDefinition;
        this.topologyProtobufsManager = topologyProtobufsManager;
        this.realtimeTopologyContextId = realtimeTopologyContextId;

        if (loadExisting) {
            // TODO (roman, July 17 2017): This would be cleaner if the health monitor system supported
            // queueing operations until the dependency is available, so consider extending it to do
            // that.
            Executors.newSingleThreadExecutor().execute(
                    new RegisteredTopologyLoader(TimeUnit.MILLISECONDS.convert(10, TimeUnit.SECONDS),
                            graphDatabaseDriverBuilder, this));
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
        private GraphDatabaseDriverBuilder driverBuilder;
        private TopologyLifecycleManager lifecycleManager;

        @VisibleForTesting
        RegisteredTopologyLoader(final long pollingIntervalMs,
                                 @Nonnull final GraphDatabaseDriverBuilder driverBuilder,
                                 @Nonnull final TopologyLifecycleManager lifecycleManager) {
            this.pollingIntervalMs = pollingIntervalMs;
            this.driverBuilder = Objects.requireNonNull(driverBuilder);
            this.lifecycleManager = Objects.requireNonNull(lifecycleManager);
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
                    if (!registered) {
                        driverBuilder.build(tid.toDatabaseName()).dropDatabase();
                    }
                });
        }
    }

    public TopologyCreator newTopologyCreator(@Nonnull final TopologyID topologyID) {
        return new TopologyCreator(
                topologyID,
                graphDatabaseDriverBuilder,
                topologyProtobufsManager,
                this::registerTopology,
                graphDriver -> new TopologyGraphCreator(graphDriver, graphDefinition),
                TopologyConverter::convert,
                realtimeTopologyContextId
        );
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
                graphDatabaseDriverBuilder.build(topologyID.toDatabaseName()).dropDatabase();
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
        return getTopologyId(realtimeTopologyContextId, topologyType);
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

    /**
     * A {@link TopologyCreator} is responsible for storing a topology (i.e. a set of
     * {@link TopologyEntityDTO}s) in the repository.
     * <p>
     * The user obtains an instance via
     * {@link TopologyLifecycleManager#newTopologyCreator(TopologyID)}, and is responsible
     * for calling the methods in the following order:
     *
     *  {@link TopologyCreator#initialize()}
     *     |
     *  {@link TopologyCreator#addEntities(Collection)} as many times as necessary
     *     |
     *  {@link TopologyCreator#complete()} or {@link TopologyCreator#rollback()}.
     */
    public static class TopologyCreator {

        private final GraphDatabaseDriver graphDatabaseDriver;

        private final TopologyGraphCreator topologyGraphCreator;

        private final TopologyID topologyID;

        private final EntityConverter entityConverter;

        private final Consumer<TopologyID> onComplete;

        private final Optional<TopologyProtobufWriter> topologyProtobufWriter;

        TopologyCreator(@Nonnull final TopologyID topologyID,
                        @Nonnull final GraphDatabaseDriverBuilder graphDatabaseDriverBuilder,
                        @Nonnull final TopologyProtobufsManager protobufsManager,
                        @Nonnull final Consumer<TopologyID> onComplete,
                        @Nonnull final TopologyGraphCreatorFactory topologyGraphCreatorFactory,
                        @Nonnull final EntityConverter entityConverter,
                        final long realtimeTopologyContextId) {
            this.topologyID = Objects.requireNonNull(topologyID);
            this.graphDatabaseDriver = Objects.requireNonNull(graphDatabaseDriverBuilder.build(
                    topologyID.toDatabaseName()));
            this.onComplete = onComplete;

            // persist raw topology protobuf only for plan projected topologies.
            // realtime (both source and projected) and plan source raw topologies are not used yet.
            if (TopologyUtil.isPlanProjectedTopology(topologyID, realtimeTopologyContextId)) {
                topologyProtobufWriter = Optional.of(protobufsManager.createTopologyProtobufWriter(topologyID.getTopologyId()));
            } else {
                topologyProtobufWriter = Optional.empty();
            }

            topologyGraphCreator = topologyGraphCreatorFactory.newGraphCreator(graphDatabaseDriver);
            this.entityConverter = entityConverter;
        }


        /**
         * Perform all initialization required to store the topology.
         * The caller should only call this method once.
         *
         * @throws GraphDatabaseException If there is an issue with the underlying graph database.
         */
        public void initialize() throws GraphDatabaseException {
            topologyGraphCreator.init();
        }

        /**
         * Add a collection of entities to the topology.
         * The caller can call this method as many times as necessary.
         *
         * @param entities The entities to add.
         * @throws TopologyEntitiesException If there is any issue writing the entities.
         */
        public void addEntities(final Collection<TopologyEntityDTO> entities)
                throws TopologyEntitiesException {
            try {
                topologyGraphCreator.updateTopologyToDb(entityConverter.convert(entities));
                topologyProtobufWriter.ifPresent(writer -> writer.storeChunk(entities));
            } catch (VertexOperationException | EdgeOperationException | CollectionOperationException e) {
                throw new TopologyEntitiesException(e);
            }
        }

        /**
         * Finalize the topology. To be called after all the entities are added.
         *
         * @throws GraphDatabaseException If there is an issue with the underlying graph database.
         */
        public void complete() throws GraphDatabaseException {
            onComplete.accept(topologyID);
        }

        /**
         * Roll back the topology, deleting any entities written so far.
         */
        public void rollback() {
            graphDatabaseDriver.dropDatabase();
            topologyProtobufWriter.ifPresent(TopologyProtobufHandler::delete);
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

}
