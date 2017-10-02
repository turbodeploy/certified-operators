package com.vmturbo.repository.constant;

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;

/**
 * Contains enum values for object states (now, it's only for entities).
 * Also provides a method to convert the object states from the one used in TopologyDTO
 * to the one used in the repository. One main purpose of such conversion is to match the
 * state strings hard-coded in the UI side.
 */
public class RepoObjectState {
    public static enum RepoEntityState {
        ACTIVE("ACTIVE"),
        IDLE("IDLE"),
        SUSPENDED("SUSPENDED"),
        MAINTENANCE("MAINTENANCE"),
        FAILOVER("FAILOVER"),
        UNKNOWN("UNKNOWN");

        private final String value;

        private RepoEntityState(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }

        /**
         * Converts state from a string to the enum state.
         *
         * @param state The state string to convert.
         * @return The enum state converted.
         */
        public static RepoEntityState fromString(String state) {
            if (state != null) {
                for (RepoEntityState t : RepoEntityState.values()) {
                    if (state.equals(t.value)) {
                        return t;
                    }
                }
            }

            throw new IllegalArgumentException(
                       "No RepoEntityState constant with state " + state + " found");
        }
    }

    /**
     * Mappings between entityState enum values in TopologyEntityDTO to strings that the UI expects.
     */
    private static final BiMap<EntityState, RepoEntityState> ENTITY_STATE_MAPPINGS =
       new ImmutableBiMap.Builder<EntityState, RepoEntityState>()
            .put(EntityState.POWERED_ON,     RepoEntityState.ACTIVE)
            .put(EntityState.POWERED_OFF,    RepoEntityState.IDLE)
            .put(EntityState.FAILOVER,       RepoEntityState.FAILOVER)
            .put(EntityState.MAINTENANCE,    RepoEntityState.MAINTENANCE)
            .put(EntityState.SUSPENDED,      RepoEntityState.SUSPENDED)
            .put(EntityState.UNKNOWN,        RepoEntityState.UNKNOWN)
            .build();

    /**
     * Maps the entity state used in TopologyEntityDTO to strings of entity states used
     * in the repository.
     *
     * @param topologyEntityState The entity state used in the TopologyEntityDTO
     * @return     The corresponding entity state string in the repository
     */
    public static String mapEntityType(EntityState topologyEntityState) {
        final RepoEntityState repoEntityState = ENTITY_STATE_MAPPINGS.get(topologyEntityState);

        if (repoEntityState == null) {
            return topologyEntityState.name();
        }

        return repoEntityState.getValue();
    }

    /**
     * Maps the entity state used in UI to the integer used in TopologyEntityDTO.
     *
     * @param repoState The entity state string to convert
     * @return The entity state integer converted
     */
    public static int toTopologyEntityState(String repoState) {
        final EntityState topologyEntityState = ENTITY_STATE_MAPPINGS.inverse()
                        .get(RepoEntityState.fromString(repoState));

        return topologyEntityState != null ? topologyEntityState.getNumber()
                                           : EntityState.UNKNOWN.getNumber();
    }
}
