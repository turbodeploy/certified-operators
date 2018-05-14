package com.vmturbo.stitching;

import java.util.List;

import javax.annotation.concurrent.Immutable;

import com.google.common.collect.ImmutableList;

import com.vmturbo.stitching.prestitching.MergeSharedDatacentersPreStitchingOperation;
import com.vmturbo.stitching.prestitching.RemoveNonMarketEntitiesPreStitchingOperation;
import com.vmturbo.stitching.prestitching.SharedStoragePreStitchingOperation;

/**
 * A library of {@link PreStitchingOperation}s. Maintains the known topology preStitching operations
 * so that they can be run at the appropriate phases of the stitching lifecycle.
 *
 * {@link PreStitchingOperation}s are maintained in the order that they are run.
 */
@Immutable
public class PreStitchingOperationLibrary {
    final ImmutableList<PreStitchingOperation> preStitchingOperations;

    /**
     * Create a new {@link PreStitchingOperation} library.
     */
    public PreStitchingOperationLibrary() {
        preStitchingOperations = ImmutableList.of(
                new RemoveNonMarketEntitiesPreStitchingOperation(),
                new SharedStoragePreStitchingOperation(),
                new MergeSharedDatacentersPreStitchingOperation()
        );
    }

    /**
     * Get the list of {@link PreStitchingOperation} to run prior to the main {@link StitchingOperation}s.
     *
     * @return the list of {@link PreStitchingOperation} to run prior to the main {@link StitchingOperation}s.
     */
    public List<PreStitchingOperation> getPreStitchingOperations() {
        return preStitchingOperations;
    }
}
