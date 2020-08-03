package com.vmturbo.repository.topology;

import java.io.Serializable;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.collect.ImmutableBiMap;

import com.vmturbo.common.protobuf.repository.RepositoryDTO;

/**
 * The {@link TopologyID} is meant to contain identity-related properties of a topology, and
 * allow comparing and grouping topologies. It's basically a container for topology ID, context ID,
 * and topology type, which are the three important pieces of metadata for a topology. It's also
 * what the collection name suffix gets derived from (via {@link TopologyID#toCollectionNameSuffix()},
 * or, inversely, {@link TopologyID#fromCollectionName(String)}).
 */
public class TopologyID implements Serializable {
    /**
     * The format for the ArangoDB collection name for a {@link TopologyID}.
     * This is really:
     *      -{contextId}-{type}-{topologyId}
     * but basic string formatting in Java doesn't allow named parameters.
     */
    private static final String COLLECTION_SUFFIX_FORMAT = "-%d-%s-%d";

    /**
     * The pattern that can be used to convert a collection name to a {@link TopologyID}.
     * Should match {@link TopologyID#COLLECTION_SUFFIX_FORMAT}.
     */
    private static final Pattern COLLECTION_NAME_PATTERN = Pattern.compile(
        "([a-zA-Z]+)-(?<contextId>\\d+)-(?<collectionSuffix>[SP])-(?<topologyId>\\d+)");

    private final long contextId;
    private final long topologyId;
    private final TopologyType type;

    /**
     * TopologyID containing identity-related properties of a topology, and allow comparing and
     * grouping topologies.
     *
     * @param contextId               For a plan it is the plan ID, for real time topology it is a
     *                                number that uniquely identifies the source topology processor.
     * @param topologyId              Topology ID.
     * @param type                    Topology type, either SOURCE or PROJECTED.
     */
    public TopologyID(final long contextId, final long topologyId, final TopologyType type) {
        this.contextId = contextId;
        this.topologyId = topologyId;
        this.type = type;
    }

    /**
     * Extract the {@link TopologyID} from the name of a collection that stores information for
     * that topology. It is the inverse of {@link TopologyID#toCollectionNameSuffix()}.
     *
     * @param name The name of the collection.
     * @return An optional containing the {@link TopologyID} representing the collection, or
     *         an empty optional if the collection name doesn't represent a {@link TopologyID}.
     */
    public static Optional<TopologyID> fromCollectionName(String name) {
        final Matcher matcher = COLLECTION_NAME_PATTERN.matcher(name);
        if (matcher.find()) {
            final String contextId = matcher.group("contextId");
            final String collectionSuffix = matcher.group("collectionSuffix");
            final String topologyId = matcher.group("topologyId");
            if (contextId != null && collectionSuffix != null && topologyId != null) {
                return Optional.of(new TopologyID(Long.parseLong(contextId),
                    Long.parseLong(topologyId), TopologyType.getTopologyTypeFromPrefix(collectionSuffix)));
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
     * Get the collection name to use to represent the topology identified by this {@link TopologyID}.
     * It is the inverse of {@link TopologyID#fromCollectionName(String)}.
     *
     * @return Collection name suffix.
     */
    public String toCollectionNameSuffix() {
        return String.format(COLLECTION_SUFFIX_FORMAT, getContextId(),
            TopologyType.getPrefixFromTopologyType(type), getTopologyId());
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
        return Objects.hash(contextId, topologyId, type);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof TopologyID)) {
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

        /**
         * BiMap where we can get topology type prefix from the given type or get topology type from
         * the given type prefix.
         */
        private static final ImmutableBiMap<TopologyType, String> topologyTypeToPrefixMap = ImmutableBiMap
            .<TopologyType, String>builder()
            .put(SOURCE, "S")
            .put(PROJECTED, "P")
            .build();

        /**
         * Map a {@link RepositoryDTO.TopologyType} to a {@link TopologyType}
         *
         * <p>{@link RepositoryDTO.TopologyType} is used to represent the type of a topology in
         * protobuf messages used in gRPC requests between components. {@link TopologyType} is
         * used to represent the same thing within the repository as part of the identification
         * of a topology in the database.</p>
         *
         * @param dtoTopologyType the {@link RepositoryDTO.TopologyType} to convert
         * @return a {@link TopologyType} representing the same topology type
         */
        public static TopologyType mapTopologyType(RepositoryDTO.TopologyType dtoTopologyType) {
            switch (dtoTopologyType) {
                case SOURCE: return SOURCE;
                case PROJECTED: return PROJECTED;
                default: throw new IllegalArgumentException("TopologyType " + dtoTopologyType.name()
                    + " not recognized");
            }
        }

        /**
         * Get the topology type prefix from the given {@link TopologyType} to be constructed to Arango
         * collection name.
         *
         * @param topologyType Given {@link TopologyType}, either SOURCE or PROJECTED.
         * @return Topology type prefix to be constructed to Arango collection name.
         */
        public static String getPrefixFromTopologyType(TopologyType topologyType) {
            String topologyTypePrefix = topologyTypeToPrefixMap.get(topologyType);
            if (topologyTypePrefix == null) {
                throw new IllegalArgumentException("TopologyType " + topologyType.name()
                    + " not recognized");
            }
            return topologyTypePrefix;
        }

        /**
         * Get the {@link TopologyType} from the given topology type prefix.
         *
         * @param topologyTypePrefix Given topology type prefix.
         * @return {@link TopologyType} from the given topology type prefix.
         */
        public static TopologyType getTopologyTypeFromPrefix(String topologyTypePrefix) {
            TopologyType topologyType = topologyTypeToPrefixMap.inverse().get(topologyTypePrefix);
            if (topologyType == null) {
                throw new IllegalArgumentException("TopologyType prefix " + topologyTypePrefix
                    + " not recognized");
            }
            return topologyType;
        }
    }

}
