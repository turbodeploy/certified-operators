package com.vmturbo.stitching;

import java.util.List;

import javax.annotation.concurrent.Immutable;

import com.google.common.collect.ImmutableList;

import com.vmturbo.stitching.poststitching.CpuAllocationPostStitchingOperation;
import com.vmturbo.stitching.poststitching.CpuProvisionedPostStitchingOperation;
import com.vmturbo.stitching.poststitching.MemoryProvisionedPostStitchingOperation;

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
     */
    public PostStitchingOperationLibrary() {
        postStitchingOperations = ImmutableList.of(new MemoryProvisionedPostStitchingOperation(),
            new CpuProvisionedPostStitchingOperation(), new CpuAllocationPostStitchingOperation());
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
