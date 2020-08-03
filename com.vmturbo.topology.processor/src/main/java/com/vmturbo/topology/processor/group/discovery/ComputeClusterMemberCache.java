package com.vmturbo.topology.processor.group.discovery;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.group.GroupDTO.DiscoveredGroupsPoliciesSettings.UploadedGroup;
import com.vmturbo.common.protobuf.topology.DiscoveredGroup.DiscoveredGroupInfo;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;
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
            .filter(DiscoveredGroupInfo::hasUploadedGroup)
            .filter(group -> GroupType.COMPUTE_HOST_CLUSTER ==
                    group.getUploadedGroup().getDefinition().getType())
            .map(group -> new ClusterMembers(group.getUploadedGroup()))
            .collect(Collectors.toList())));
    }

    /**
     * Determine the Info DTO of the compute cluster that a host belongs to.
     *
     * @param host The host whose compute cluster membership should be looked up.
     * @return Info DTO of the compute cluster membership that the host belongs to.
     *         If no compute cluster is found, returns {@link Optional#empty()}
     */
    public Optional<UploadedGroup> clusterInfoForHost(@Nonnull final TopologyStitchingEntity host) {
        return Optional.ofNullable(cache.get(host.getTargetId()))
            .flatMap(clusterMembersList -> clusterMembersList.stream()
                .filter(clusterMembers -> clusterMembers.hasMember(host.getOid()))
                .map(ClusterMembers::getClusterInfo)
                .findFirst());
    }

    /**
     * A simple class containing the name of a cluster and a set of its member OIDs.
     */
    private static class ClusterMembers {
        private final Set<Long> memberOids;
        private final UploadedGroup cluster;

        ClusterMembers(@Nonnull final UploadedGroup cluster) {
            this.cluster = Objects.requireNonNull(cluster);
            this.memberOids = GroupProtoUtil.getAllStaticMembers(cluster.getDefinition());
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
         * Get the cluster info for the cluster.
         *
         * @return The cluster info for the cluster.
         */
        public UploadedGroup getClusterInfo() {
            return cluster;
        }
    }
}
