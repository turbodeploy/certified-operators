package com.vmturbo.repository.topology;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.gson.Gson;

import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.components.common.diagnostics.Diagnosable;

/**
 * Manage the <b>current</b> ID of the real-time topology.
 */
public class TopologyIDManager implements Diagnosable {
    private static final Logger LOGGER = LoggerFactory.getLogger(TopologyIDManager.class);

    private static final Optional<TopologyID> DEFAULT_REALTIME_TOPOLOGY_ID = Optional.empty();

    private volatile Optional<TopologyID> currentRealTimeTopologyId = DEFAULT_REALTIME_TOPOLOGY_ID;

    /**
     * This map is used internally to map from a topology context ID of long type
     * to a topology ID of {@link TopologyID} type.
     */
    private Multimap<Long, TopologyID> contextIdToTopologyId = HashMultimap.create();

    private static final TopologyType DEFAULT_TOPOLOGY_TYPE = TopologyType.SOURCE;

    public TopologyIDManager() {
    }

    public Optional<TopologyID> getCurrentRealTimeTopologyId() {
        return currentRealTimeTopologyId;
    }

    /**
     * Sets the current {@link TopologyID} for real-time scenario.
     *
     * @param tid The {@link TopologyID} instance to set
     */
    public void setCurrentRealTimeTopologyId(TopologyID tid) {
        LOGGER.info("Setting the current real time topology to {}", tid);
        currentRealTimeTopologyId = Optional.of(tid);
    }

    /**
     * Registers a topology ID of type {@link TopologyID}.
     * @param tid The {@link TopologyID} instance
     * @return true if tid was not registered yet and false otherwise
     */
    public boolean register(TopologyID tid) {
        return contextIdToTopologyId.put(tid.getContextId(), tid);
    }

    /**
     * De-registers a topology ID of type {@link TopologyID}.
     * @param tid The {@link TopologyID} instance
     * @return true if tid was indeed registered and false otherwise
     */
    public boolean  deRegister(TopologyID tid) {
        return contextIdToTopologyId.remove(tid.getContextId(), tid);
    }

    public Optional<TopologyDatabase> currentRealTimeDatabase() {
        return currentRealTimeTopologyId.map(tid -> TopologyDatabase.from(databaseName(tid)));
    }

    /**
     * Returns the database name associated to a given {@link TopologyID} instance.
     *
     * @param tid The {@link TopologyID} instance
     * @return The database name string
     */
    public String databaseName(TopologyID tid) {
        return "topology-" + tid.getContextId() + "-" + tid.getTopologyId();
    }

    /**
     * Returns the {@link TopologyDatabase} instance associated to a given topology context ID.
     *
     * @param contextId The topology context ID
     * @return the database associated with the context ID
     */
    public TopologyDatabase databaseOf(String contextId) {
        final long cid = Long.parseLong(contextId);
        // Find the TopologyID objects associated to the context id with the default type
        final List<TopologyID> tids = contextIdToTopologyId.get(cid)
                        .stream().filter(t -> DEFAULT_TOPOLOGY_TYPE.equals(t.getType()))
                        .collect(Collectors.toList());

        if (tids == null || tids.isEmpty()) {
            final String msg = "No topologies with context id " +  contextId + " found";
            LOGGER.warn(msg);
            throw new RuntimeException(msg);
        }

        // For now, assuming there is only one topology if no topology id specified
        return TopologyDatabase.from(databaseName(tids.get(0)));
    }

    @Nonnull
    @Override
    public List<String> collectDiags() {
        if (!currentRealTimeTopologyId.isPresent()) {
            return Collections.emptyList();
        } else {
            final Gson gson = ComponentGsonFactory.createGsonNoPrettyPrint();
            return Collections.singletonList(gson.toJson(currentRealTimeTopologyId.get()));
        }
    }

    @Override
    public void restoreDiags(@Nonnull final List<String> collectedDiags) {
        if (collectedDiags.isEmpty()) {
            currentRealTimeTopologyId = Optional.empty();
        } else {
            final Gson gson = ComponentGsonFactory.createGsonNoPrettyPrint();
            final TopologyID realtimeId = gson.fromJson(collectedDiags.get(0), TopologyID.class);
            currentRealTimeTopologyId = Optional.of(realtimeId);
        }
    }

    /**
     * Class for identifier to the topologies stored in database.
     */
    public static class TopologyID implements Serializable {
        private final long contextId;
        private final long topologyId;
        private final TopologyType type;

        public TopologyID(final long contextId, final long topologyId, final TopologyType type) {
            this.contextId = contextId;
            this.topologyId = topologyId;
            this.type = type;
        }

        public long getContextId() {
            return contextId;
        }

        public long getTopologyId() {
            return topologyId;
        }

        public TopologyType getType() {
            return type;
        }

        @Override
        public String toString() {
            return "TopologyID [contextId=" + contextId
                           + ", topologyId=" + topologyId
                           + ", type=" + type
                           + "]";
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + (int)(contextId ^ (contextId >>> 32));
            result = prime * result + (int)(topologyId ^ (topologyId >>> 32));
            result = prime * result + ((type == null) ? 0 : type.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            TopologyID other = (TopologyID)obj;
            if (contextId != other.contextId) {
                return false;
            }
            if (topologyId != other.topologyId) {
                return false;
            }
            if (type != other.type) {
                return false;
            }
            return true;
        }
    }

    /**
     * The enum for topology types.
     */
    public enum TopologyType {
        SOURCE,
        PROJECTED;
    }
}
