package com.vmturbo.repository.topology;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.components.common.diagnostics.DiagnosticsAppender;
import com.vmturbo.components.common.diagnostics.DiagnosticsException;
import com.vmturbo.components.common.diagnostics.DiagsRestorable;
import com.vmturbo.components.common.diagnostics.DiagsZipReader;
import com.vmturbo.repository.exception.GraphDatabaseExceptions.GraphDatabaseException;
import com.vmturbo.repository.listener.realtime.LiveTopologyStore;
import com.vmturbo.repository.listener.realtime.ProjectedRealtimeTopology.ProjectedTopologyBuilder;
import com.vmturbo.repository.listener.realtime.SourceRealtimeTopology.SourceRealtimeTopologyBuilder;
import com.vmturbo.repository.plan.db.SQLPlanEntityStore;
import com.vmturbo.repository.topology.TopologyID.TopologyType;

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
public class TopologyLifecycleManager implements DiagsRestorable<Void> {

    /**
     * The file name for the state of the {@link TopologyLifecycleManager}. It's a string file,
     * so the "diags" extension is required for compatibility with {@link DiagsZipReader}.
     */
    public static final String ID_MGR_FILE = "database-id-metadata";

    private static final Logger LOGGER = LogManager.getLogger();

    /**
     * Maps from topologyContextId + topologyType to a {@link TopologyID}.
     */
    private Map<Long, Map<TopologyType, TopologyID>> topologyIdByContextAndType = new HashMap<>();

    /**
     * Maps from a primitive topologyId field to a {@link TopologyID}.
     */
    private Map<Long, TopologyID> topologyIdByPrimitiveId = new HashMap<>();

    private final long realtimeTopologyContextId;

    private final SQLPlanEntityStore sqlPlanEntityStore;

    // optional scheduled executor to perform delayed database drops
    private final ScheduledExecutorService scheduler;

    // how many seconds to delay realtime topology drops by
    private final long realtimeTopologyDropDelaySecs;

    // how many expected real time "Source" DB to be kept with full clean up?
    private final int numberOfExpectedRealtimeSourceDB;

    // how many expected real time "Projected" DB to be kept with full clean up?
    private final int numberOfExpectedRealtimeProjectedDB;

    // the number of replicas of each collection to request from the graph database.
    private final int collectionReplicaCount;

    private final LiveTopologyStore liveTopologyStore;

    private final boolean useSqlForPlans;

    /**
     * Topology collection name prefix used to construct collection name with the name suffix when
     * collecting diags.
     */
    private static final String TOPOLOGY_COLLECTION_NAME_PREFIX =  "topology";

    public TopologyLifecycleManager(final long realtimeTopologyContextId,
            @Nonnull final ScheduledExecutorService scheduler,
            @Nonnull final LiveTopologyStore liveTopologyStore,
            final long realtimeTopologyDropDelaySecs,
            final int numberOfExpectedRealtimeSourceDB,
            final int numberOfExpectedRealtimeProjectedDB,
            final int collectionReplicaCount,
            @Nonnull final SQLPlanEntityStore sqlPlanEntityStore,
             final boolean useSqlForPlans) {
        this.realtimeTopologyContextId = realtimeTopologyContextId;
        this.scheduler = scheduler;
        this.liveTopologyStore = liveTopologyStore;
        this.realtimeTopologyDropDelaySecs = realtimeTopologyDropDelaySecs;
        this.numberOfExpectedRealtimeSourceDB = numberOfExpectedRealtimeSourceDB;
        this.numberOfExpectedRealtimeProjectedDB = numberOfExpectedRealtimeProjectedDB;
        this.collectionReplicaCount = collectionReplicaCount;
        this.sqlPlanEntityStore = sqlPlanEntityStore;
        this.useSqlForPlans = useSqlForPlans;

        if (realtimeTopologyDropDelaySecs <= 0) {
            LOGGER.info("realtimeTopologyDropDelaySecs is set to {} -- disabling delayed drop behavior.", realtimeTopologyDropDelaySecs);
        }

    }

    @Override
    public void collectDiags(@Nonnull DiagnosticsAppender appender) throws DiagnosticsException {
        for ( Map<TopologyType, TopologyID> map: topologyIdByContextAndType.values()) {
            for (TopologyID topologyID: map.values()) {
                final String string = TOPOLOGY_COLLECTION_NAME_PREFIX + topologyID.toCollectionNameSuffix();
                appender.appendString(string);
            }
        }
    }

    @Override
    public void restoreDiags(@Nonnull final List<String> collectedDiags,
                             @Nullable Void context) throws DiagnosticsException {
        // Overwrite whatever is in the lifecycle manager with the restored diags.
        // We rely on the diags handler to actually do the restoration of the database
        // to arangodb.
        collectedDiags.stream()
                .map(TopologyID::fromCollectionName)
                .filter(Optional::isPresent).map(Optional::get)
                // Register, overwriting any existing TID for the same context and topology type.
                // This will drop any database associated with the existing TID.
                .forEach(tid -> registerTopology(tid, true));
    }

    @Nonnull
    @Override
    public String getFileName() {
        return ID_MGR_FILE;
    }

    public TopologyCreator<TopologyEntityDTO> newSourceTopologyCreator(@Nonnull final TopologyID topologyID,
                                                                       @Nonnull final TopologyInfo topologyInfo) {
        if (topologyID.getContextId() == realtimeTopologyContextId) {
            return new InMemorySourceTopologyCreator(liveTopologyStore.newRealtimeSourceTopology(topologyInfo));
        } else {
            return sqlPlanEntityStore.newSourceTopologyCreator(topologyInfo);
        }
    }

    public TopologyCreator<ProjectedTopologyEntity> newProjectedTopologyCreator(@Nonnull final TopologyID topologyID,
                                                                                @Nonnull final TopologyInfo originalTopologyInfo) {
        if (topologyID.getContextId() == realtimeTopologyContextId) {
            return new InMemoryProjectedTopologyCreator(
                liveTopologyStore.newProjectedTopology(topologyID.getTopologyId(), originalTopologyInfo));
        } else {
            return sqlPlanEntityStore.newProjectedTopologyCreator(topologyID.getTopologyId(), originalTopologyInfo);
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
            topologyIdByPrimitiveId.put(tid.getTopologyId(), tid);
            return true;
        } else {
            final TopologyID existing = idByTypeInContext.get(tid.getType());
            if (existing != null) {
                // It is an expected case to have existing TopologyID here given that the TopologyID
                // type as the key of idByTypeInContext map is either SOURCE or PROJECTED. Multiple
                // collections in ArangoDB have common suffix with topology ID, so we'll try to
                // register the same TopologyID multiple times here from the list of Arango collections.
                // Use debug level message to avoid polluting the log.
                LOGGER.debug("Not registering topology {} because it would overwrite " +
                    "the existing topology {}.", tid, existing);
            } else {
                // Update the lookup maps
                idByTypeInContext.put(tid.getType(), tid);
                topologyIdByPrimitiveId.put(tid.getTopologyId(), tid);
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

    /**
     * Delete a topology from the repository. If the {@link TopologyID} does not exist,
     * this has no effect.
     *
     * TODO (roman, Nov 7 2017): We probably don't need topology-level granularity, and only
     * want to clear a whole context. This would make book keeping simpler. But we need to convert
     * topology protobuf manager to operate on context IDs before we can do that.
     *
     * @param tid The ID of the topology to remove.
     * @throws TopologyDeletionException If there are errors deleting the topology.
     */
    public void deleteTopology(@Nonnull final TopologyID tid) throws TopologyDeletionException {
        // There will be two separate deletion operations. Collect any associated errors to be
        // reported after both attempts.
        final List<String> errors = new LinkedList<>();

        final long topologyId = tid.getTopologyId();

        final Map<TopologyType, TopologyID> idByTypeInContext =
                topologyIdByContextAndType.get(tid.getContextId());
        if (idByTypeInContext != null) {
            final TopologyID id = idByTypeInContext.get(tid.getType());
            if (id.equals(tid)) {
                idByTypeInContext.remove(tid.getType());
            }
        }
        // Cleanup the other lookup map
        topologyIdByPrimitiveId.remove(topologyId);

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

    /**
     * List the ids of all registered contexts in the manager (including realtime context).
     *
     * @return The set of ids.
     */
    @Nonnull
    public Set<Long> listRegisteredContexts() {
        return Sets.newHashSet(topologyIdByContextAndType.keySet());
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
     * Get the {@link TopologyID} representing the topology with a particular topology ID.
     *
     * @param topologyId the primitive ID of the topology
     * @return The {@link TopologyID} of the latest topology for the desired topology ID,
     *         or an empty optional if there has been no broadcast of a matching topology.
     */
    public Optional<TopologyID> getTopologyId(final long topologyId) {
        return Optional.ofNullable(topologyIdByPrimitiveId.get(topologyId));
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

    public interface TopologyCreator<ENTITY_DTO_TYPE> {
        void initialize() throws GraphDatabaseException;

        /**
         * Add entities to the topology.
         *
         * @param entities To be added
         * @param tid The {@link TopologyID} being referenced
         * @throws TopologyEntitiesException If there is any issue writing the entities.
         */
        void addEntities(Collection<ENTITY_DTO_TYPE> entities, TopologyID tid) throws TopologyEntitiesException;

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
        public void addEntities(final Collection<TopologyEntityDTO> entities, final TopologyID tid) throws TopologyEntitiesException {
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

        public void addEntities(final Collection<ProjectedTopologyEntity> entities) throws TopologyEntitiesException {
            projectedTopologyBuilder.addEntities(entities);
        }

        @Override
        public void addEntities(final Collection<ProjectedTopologyEntity> entities, final TopologyID tid) throws TopologyEntitiesException {
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
