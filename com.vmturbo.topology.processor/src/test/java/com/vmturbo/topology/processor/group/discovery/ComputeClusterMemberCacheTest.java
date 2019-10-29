package com.vmturbo.topology.processor.group.discovery;

import static com.vmturbo.platform.common.builders.CommodityBuilders.cpuMHz;
import static com.vmturbo.platform.common.builders.EntityBuilders.physicalMachine;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.Arrays;
import java.util.Collections;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;

import org.junit.Test;

import com.vmturbo.common.protobuf.topology.DiscoveredGroup.DiscoveredGroupInfo;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;
import com.vmturbo.topology.processor.stitching.TopologyStitchingEntity;
import com.vmturbo.topology.processor.util.GroupTestUtils;

/**
 * Tests for {@link ComputeClusterMemberCache}.
 */
public class ComputeClusterMemberCacheTest {

    private static final String COMPUTE_CLUSTER_NAME = "compute-cluster";
    private static final String STORAGE_CLUSTER_NAME = "storage-cluster";

    final DiscoveredGroupInfo computeCluster = DiscoveredGroupInfo.newBuilder()
        .setUploadedGroup(GroupTestUtils.createUploadedCluster(COMPUTE_CLUSTER_NAME,
                GroupType.COMPUTE_HOST_CLUSTER, Collections.singletonList(HOST_ID)))
        .build();

    final DiscoveredGroupInfo storageCluster = DiscoveredGroupInfo.newBuilder()
        .setUploadedGroup(GroupTestUtils.createUploadedCluster(STORAGE_CLUSTER_NAME,
                GroupType.STORAGE_CLUSTER, Collections.singletonList(STORAGE_ID)))
        .build();

    private static final long TARGET_ID = 1L;
    private static final long HOST_ID = 100L;
    private static final long STORAGE_ID = 101L;

    final ComputeClusterMemberCache cache = new ComputeClusterMemberCache(
        ImmutableMap.of(TARGET_ID, Arrays.asList(computeCluster, storageCluster)));

    @Test
    public void testFound() {
        assertEquals(COMPUTE_CLUSTER_NAME, cache.clusterInfoForHost(host(HOST_ID, TARGET_ID)).get()
                .getSourceIdentifier());
    }

    @Test
    public void testStorageClusterNotUsed() {
        assertFalse(cache.clusterInfoForHost(host(STORAGE_ID, TARGET_ID)).isPresent());
    }

    @Test
    public void testNotFoundByTargetId() {
        assertFalse(cache.clusterInfoForHost(host(HOST_ID, TARGET_ID + 1)).isPresent());
    }

    @Test
    public void testNotFoundByOid() {
        assertFalse(cache.clusterInfoForHost(host(HOST_ID + 999, TARGET_ID + 1)).isPresent());
    }

    @Nonnull
    private static TopologyStitchingEntity host(final long oid, final long targetId) {
        return new TopologyStitchingEntity(physicalMachine("pm")
            .selling(cpuMHz().capacity(100.0))
            .build().toBuilder(),
            oid, targetId, 0);
    }
}