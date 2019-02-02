package com.vmturbo.group.group;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.group.GroupDTO.ClusterInfo;

/**
 *  This class maintains an inverted in-memory index from
 *  HostId or StorageId to ClusterId. Discovered Clusters have only
 *  hosts or storage as their members.
 */
public class EntityToClusterMapping {

    private final Logger logger = LoggerFactory.getLogger(EntityToClusterMapping.class);

    /**
     * Mapping from HostOrStorageId -> ClusterId.
     * The cluster the entity belongs to.
     */
    @GuardedBy("mapLock")
    private Map<Long, Long> hostOrStorageIdToClusterIdMap =
            new HashMap<>();

    /**
     *  Lock to guard the map.
     */
    private final Object mapLock = new Object();

    public EntityToClusterMapping() {
    }

    /**
     * Return the Id of the Cluster this entity belongs to.
     * @param entityId The ID of the entity whose cluster needs to be determined.
     * @return ClusterId if one exists or 0 if the entity doesn't belong to any cluster.
     */
    public long getClusterForEntity(long entityId) {
        long clusterId;
        synchronized (mapLock) {
            clusterId = hostOrStorageIdToClusterIdMap.getOrDefault(entityId, 0L);
        }
        return clusterId;
    }

    /**
     *  Update the mapping from HostId or StorageId to ClusterId.
     *
     * @param updatedClusters The clusterId->clusterInfo mapping of the clusters discovered
     *                        during this broadcast.
     */
    public void updateEntityClusterMapping(@Nonnull Map<Long, ClusterInfo> updatedClusters) {

        synchronized (mapLock) {
            // Remove all the clusterIds which were not updated during this broadcast.
            hostOrStorageIdToClusterIdMap.clear();
            updatedClusters.entrySet()
                    .forEach(entry ->
                            entry.getValue().getMembers().getStaticMemberOidsList()
                                    .stream()
                                    .forEach(entityId -> {
                                    // assume unique entityId -> clusterId mapping.
                                    hostOrStorageIdToClusterIdMap.put(entityId, entry.getKey());
                                    })
                    );
        }
    }
}
