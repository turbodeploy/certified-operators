package com.vmturbo.stitching;

import java.util.List;

import javax.annotation.concurrent.Immutable;

import com.google.common.collect.ImmutableList;

import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.stitching.poststitching.CpuCapacityPostStitchingOperation;
import com.vmturbo.stitching.poststitching.PropagateStorageAccessAndLatencyPostStitchingOperation;
import com.vmturbo.stitching.poststitching.OverprovisionCapacityPostStitchingOperation.CpuProvisionedPostStitchingOperation;
import com.vmturbo.stitching.poststitching.IndependentStorageAccessPostStitchingOperation;
import com.vmturbo.stitching.poststitching.OverprovisionCapacityPostStitchingOperation.MemoryAllocationPostStitchingOperation;
import com.vmturbo.stitching.poststitching.OverprovisionCapacityPostStitchingOperation.MemoryProvisionedPostStitchingOperation;
import com.vmturbo.stitching.poststitching.OverprovisionCapacityPostStitchingOperation.PmCpuAllocationPostStitchingOperation;
import com.vmturbo.stitching.poststitching.SetCommodityMaxQuantityPostStitchingOperation;
import com.vmturbo.stitching.poststitching.StorageAccessPostStitchingOperation.DiskArrayStorageAccessPostStitchingOperation;
import com.vmturbo.stitching.poststitching.StorageAccessPostStitchingOperation.LogicalPoolStorageAccessPostStitchingOperation;
import com.vmturbo.stitching.poststitching.StorageAccessPostStitchingOperation.StorageControllerStorageAccessPostStitchingOperation;
import com.vmturbo.stitching.poststitching.StorageLatencyPostStitchingOperation.DiskArrayLatencyPostStitchingOperation;
import com.vmturbo.stitching.poststitching.StorageLatencyPostStitchingOperation.LogicalPoolLatencyPostStitchingOperation;
import com.vmturbo.stitching.poststitching.StorageLatencyPostStitchingOperation.StorageControllerLatencyPostStitchingOperation;
import com.vmturbo.stitching.poststitching.StorageLatencyPostStitchingOperation.StorageEntityLatencyPostStitchingOperation;
import com.vmturbo.stitching.poststitching.StorageProvisionedPostStitchingOperation.DiskArrayStorageProvisionedPostStitchingOperation;
import com.vmturbo.stitching.poststitching.StorageProvisionedPostStitchingOperation.LogicalPoolStorageProvisionedPostStitchingOperation;
import com.vmturbo.stitching.poststitching.StorageProvisionedPostStitchingOperation.StorageEntityStorageProvisionedPostStitchingOperation;
import com.vmturbo.stitching.poststitching.VirtualDatacenterCpuAllocationPostStitchingOperation;

/**
 * A library of {@link PostStitchingOperation}s. Maintains the known topology preStitching operations
 * so that they can be run at the appropriate phases of the stitching lifecycle.
 *
 * {@link PostStitchingOperation}s are maintained in the order that they are run.
 */
@Immutable
public class PostStitchingOperationLibrary {
    final ImmutableList<PostStitchingOperation> postStitchingOperations;

    /**
     * Create a new calculation library.
     * Note: these operations are executed in order.
     * todo (2018-2-20): consider removing the need for execution in order.
     *
     * Operations which depend on order:
     *  - CpuCapacityPostStitchingOperation must be executed before
     *    CpuProvisionedPostStitchingOperation and PmCpuAllocationPostStitchingOperation.
     *  - DiskArrayStorageAccessPostStitchingOperation must be executed before
     *    LogicalPoolStorageAccessPostStitchingOperation.
     *
     * @param statsServiceClient Stats/History client
     */
    public PostStitchingOperationLibrary(StatsHistoryServiceBlockingStub statsServiceClient) {
        postStitchingOperations = ImmutableList.of(
            new PropagateStorageAccessAndLatencyPostStitchingOperation(),
            new MemoryProvisionedPostStitchingOperation(),
            new CpuCapacityPostStitchingOperation(),
            new CpuProvisionedPostStitchingOperation(),
            new PmCpuAllocationPostStitchingOperation(),
            new VirtualDatacenterCpuAllocationPostStitchingOperation(),
            new StorageControllerLatencyPostStitchingOperation(),
            new StorageEntityLatencyPostStitchingOperation(),
            new LogicalPoolLatencyPostStitchingOperation(),
            new DiskArrayLatencyPostStitchingOperation(),
            new DiskArrayStorageProvisionedPostStitchingOperation(),
            new StorageEntityStorageProvisionedPostStitchingOperation(),
            new LogicalPoolStorageProvisionedPostStitchingOperation(),
            new MemoryAllocationPostStitchingOperation(),
            new IndependentStorageAccessPostStitchingOperation(),
            new StorageControllerStorageAccessPostStitchingOperation(),
            new DiskArrayStorageAccessPostStitchingOperation(),
            new LogicalPoolStorageAccessPostStitchingOperation(),
            new SetCommodityMaxQuantityPostStitchingOperation(statsServiceClient)
        );
    }

    /**
     * Get the list of {@link PostStitchingOperation}s to run after the main {@link StitchingOperation}s
     * during post-stitching.
     *
     * @return the list of {@link PostStitchingOperation}s to after the main {@link StitchingOperation}s.
     */
    public List<PostStitchingOperation> getPostStitchingOperations() {
        return postStitchingOperations;
    }
}
