package com.vmturbo.history.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;

/**
 * Record information about a Topology snapshot: the toplogyContextId, the topologyId,
 * the snapshot_time and a lookaside map for OID -> entyty type int for that OID.
 */
public class TopologyOrganizer {

    private final long snapshotTime;
    private final long topologyContextId;
    private final long topologyId;
    // map OID -> entity type
    private final Map<Long, Integer> topologyEntityTypeMap;

    /**
     * Initialize the {@link TopologyOrganizer}, recording the topologyContextId, the
     * topologyId, and snapshotTime.
     *
     * @param topologyContextId the long-running ID for a context
     * @param topologyId the unique id for this topology
     * @param snapshotTime the time of topology created
     */
    public TopologyOrganizer(long topologyContextId, long topologyId, long snapshotTime) {
        this.snapshotTime = snapshotTime;
        this.topologyId = topologyId;
        this.topologyContextId = topologyContextId;

        topologyEntityTypeMap = new HashMap<>();
    }

    /**
     * Initialize the {@link TopologyOrganizer}, recording the topologyContextId, the
     * topologyId, and also snapshotTime will be current time.
     *
     * @param topologyContextId the long-running ID for a context
     * @param topologyId the unique id for this topology
     */
    public TopologyOrganizer(long topologyContextId, long topologyId) {
        this(topologyContextId, topologyId, System.currentTimeMillis());
    }


    /**
     * Record the entityType for the {@link TopologyEntityDTO}. The type information is needed when
     * retrieving price index information.
     * @param dto the DTO to record the entity type for
     */
    public void addEntityType(TopologyEntityDTO dto) {
        topologyEntityTypeMap.put(dto.getOid(), dto.getEntityType());
    }

    /**
     * Look up the entityType for an entityId in the topologyEntityTypeMap. Return
     * Optional.empty() if not found in the map.
     *
     * @param entityId the ID of the entity for whose type we are looking
     * @return an Optional of the entityType for this entityId, or Optional.empty() if not found
     */
    public Optional<Integer> getEntityType(long entityId) {
        return Optional.ofNullable(topologyEntityTypeMap.get(entityId));
    }

    /**
     * The snapshot_time is the local time when this TopologyOrganizer was created.
     *
     * @return the time when this topology was created.
     */
    public long getSnapshotTime() {
        return snapshotTime;
    }

    public long getTopologyContextId() {
        return topologyContextId;
    }

    public long getTopologyId() {
        return topologyId;
    }
}
