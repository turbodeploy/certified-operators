package com.vmturbo.repository.topology;

import java.io.Serializable;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The {@link TopologyID} is meant to contain identity-related properties of a topology, and
 * allow comparing and grouping topologies. It's basically a container for topology ID, context ID,
 * and topology type, which are the three important pieces of metadata for a topology. It's also
 * what the database name gets derived from (via {@link TopologyID#toDatabaseName()}, or,
 * inversely, {@link TopologyID#fromDatabaseName(String)}).
 */
public class TopologyID implements Serializable {
    /**
     * The format for the database name for a {@link TopologyID}.
     * This is really:
     *      topology-{contextId}-{type}-{topologyId}
     * but basic string formatting in Java doesn't allow named parameters.
     */
    private static final String DB_NAME_FORMAT = "topology-%d-%s-%d";

    /**
     * The pattern that can be used to convert a database name to a {@link TopologyID}.
     * Should match {@link TopologyID#DB_NAME_FORMAT}.
     */
    private static final Pattern DB_NAME_PATTERN = Pattern.compile(
            "topology-(?<contextId>\\d+)-(?<type>SOURCE|PROJECTED)-(?<topologyId>\\d+)");

    private final long contextId;
    private final long topologyId;
    private final TopologyType type;

    public TopologyID(final long contextId, final long topologyId, final TopologyType type) {
        this.contextId = contextId;
        this.topologyId = topologyId;
        this.type = type;
    }

    /**
     * Extract the {@link TopologyID} from the name of a database that stores information for
     * that topology. It is the inverse of {@link TopologyID#toDatabaseName()}.
     *
     * @param name The name of the database.
     * @return An optional containing the {@link TopologyID} representing the database, or
     *         an empty optional if the database name doesn't represent a {@link TopologyID}.
     */
    public static Optional<TopologyID> fromDatabaseName(String name) {
        final Matcher matcher = DB_NAME_PATTERN.matcher(name);
        if (matcher.find()) {
            final String contextId = matcher.group("contextId");
            final String type = matcher.group("type");
            final String topologyId = matcher.group("topologyId");
            if (contextId != null && type != null && topologyId != null) {
                return Optional.of(new TopologyID(Long.parseLong(contextId),
                        Long.parseLong(topologyId), TopologyType.valueOf(type)));
            }
        }
        return Optional.empty();
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

    /**
     * Get the database name to use to represent the topology identified by this {@link TopologyID}.
     * It is the inverse of {@link TopologyID#fromDatabaseName(String)}.
     *
     * @return The name to use for the database.
     */
    public String toDatabaseName() {
        return String.format(DB_NAME_FORMAT, getContextId(), getType(), getTopologyId());
    }

    public TopologyDatabase database() {
        return TopologyDatabase.from(toDatabaseName());
    }

    @Override
    public String toString() {
        return "TopologyID [contextId=" + getContextId()
                       + ", topologyId=" + getTopologyId()
                       + ", type=" + getType()
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
        if (contextId != other.getContextId()) {
            return false;
        }
        if (topologyId != other.getTopologyId()) {
            return false;
        }
        if (type != other.getType()) {
            return false;
        }
        return true;
    }

    /**
     * The enum for topology types.
     */
    public enum TopologyType {
        /**
         * A source topology is stitched from the entities discovered by the probes, and represents
         * the topology as it is at a particular point in time (i.e. at the time of broadcast).
         */
        SOURCE,

        /**
         * A projected topology is the expected topology after applying all actions recommended by
         * the market's analysis of a source topology.
         */
        PROJECTED;
    }

}
