package com.vmturbo.topology.processor.group.discovery;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.group.GroupDTO.ClusterInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.ClusterInfo.Type;
import com.vmturbo.common.protobuf.topology.DiscoveredGroup.DiscoveredGroupInfo;
import com.vmturbo.topology.processor.stitching.TopologyStitchingEntity;

/**
 * A simple cache that permits finding the name of the compute cluster (if any) that a host
 * belongs to.
 *
 * Note that this cache assumes a host will never belong to more than one compute cluster. If a host does
 * belong to multiple compute clusters, which cluster the cache returns is not guaranteed.
 */
public class ComputeClusterMemberCache {
    /**
     * Maps targetId -> List of {@link ClusterMembers} for clusters discovered by that target.
     */
    private final Map<Long, List<ClusterMembers>> cache;

    /**
     * A small cache that is able to quickly determine the name of a compute cluster that the host belongs to.
     */
    public ComputeClusterMemberCache(@Nonnull final Map<Long, List<DiscoveredGroupInfo>> groupInfoByTarget) {
        cache = new HashMap<>();

        // Populate the cache
        groupInfoByTarget.forEach((targetId, groups) -> cache.put(targetId, groups.stream()
            .filter(DiscoveredGroupInfo::hasInterpretedCluster)
            .filter(group -> group.getInterpretedCluster().getClusterType() == Type.COMPUTE)
            .map(group -> new ClusterMembers(group.getInterpretedCluster()))
            .collect(Collectors.toList())));
    }

    /**
     * Determine the name of the compute cluster that a host belongs to.
     *
     * @param host The host whose compute cluster membership should be looked up.
     * @return The name of the compute cluster membership that the host belongs to.
     *         If no compute cluster is found, returns {@link Optional#empty()}
     */
    public Optional<String> clusterNameForHost(@Nonnull final TopologyStitchingEntity host) {
        return Optional.ofNullable(cache.get(host.getTargetId()))
            .flatMap(clusterMemberses -> clusterMemberses.stream()
                .filter(clusterMembers -> clusterMembers.hasMember(host.getOid()))
                .map(ClusterMembers::getClusterName)
                .findFirst());
    }

    /**
     * Determine the name of the compute cluster that a host belongs to. Get the name in the format
     * that the group component expects (it appends the entity type to the actual name).
     *
     * @param host The host whose compute cluster membership should be looked up.
     * @return The name of the compute cluster membership that the host belongs to.
     *         If no compute cluster is found, returns {@link Optional#empty()}
     */
    public Optional<String> groupComponentClusterNameForHost(@Nonnull final TopologyStitchingEntity host) {
        return Optional.ofNullable(cache.get(host.getTargetId()))
            .flatMap(clusterMemberses -> clusterMemberses.stream()
                .filter(clusterMembers -> clusterMembers.hasMember(host.getOid()))
                .map(clusterMembers -> GroupProtoUtil.discoveredIdFromName(
                        clusterMembers.getClusterInfo(), host.getTargetId()))
                .findFirst());
    }

    /**
     * A simple class containing the name of a cluster and a set of its member OIDs.
     */
    private static class ClusterMembers {
        private final Set<Long> memberOids;
        private final ClusterInfo clusterInfo;

        public ClusterMembers(@Nonnull final ClusterInfo clusterInfo) {
            this.clusterInfo = Objects.requireNonNull(clusterInfo);
            this.memberOids = new HashSet<>(clusterInfo.getMembers().getStaticMemberOidsList());
        }

        /**
         * Determine whether this cluster has a member with the given OID.
         *
         * @param memberOid The oid of the member we should look up.
         * @return True if the cluster contains the member, false otherwise.
         */
        public boolean hasMember(final long memberOid) {
            return memberOids.contains(memberOid);
        }

        /**
         * Get the name of the cluster.
         *
         * @return The name of the cluster.
         */
        public String getClusterName() {
            return clusterInfo.getName();
        }

        /**
         * Get the cluster info for the cluster.
         *
         * @return The cluster info for the cluster.
         */
        public ClusterInfo getClusterInfo() {
            return clusterInfo;
        }
    }
}
