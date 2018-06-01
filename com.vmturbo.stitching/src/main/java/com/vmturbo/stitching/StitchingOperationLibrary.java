package com.vmturbo.stitching;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import javax.annotation.Nonnull;

import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.stitching.compute.VMStitchingOperation;
import com.vmturbo.stitching.fabric.FabricChassisStitchingOperation;
import com.vmturbo.stitching.fabric.FabricPMStitchingOperation;
import com.vmturbo.stitching.storage.StorageStitchingOperation;

/**
 * A library of stitching operations. Maps probe type and category to operations that to be used for
 * stitching targets discovered by that probe.
 */
public class StitchingOperationLibrary {
    /**
     * Find an operation for the given probe type and category.
     *
     * @param probeType The probe type of the probe whose {@link StitchingOperation} should be looked up.
     * @param probeCategory The probe category of the probe whose {@link StitchingOperation} should be looked up.
     * @return The {@link StitchingOperation} to use for the associated probe type and category.
     * @throws StitchingUnknownProbeException If no {@link StitchingOperation} is known for the
     *         type/category combination.
     */
    public List<StitchingOperation<?, ?>> stitchingOperationsFor(@Nonnull final String probeType,
                                                                 @Nonnull final ProbeCategory probeCategory)
        throws StitchingUnknownProbeException {

        switch (probeCategory) {
            case STORAGE:
                return Collections.singletonList(new StorageStitchingOperation());
            case FABRIC:
                return Arrays.asList(new FabricChassisStitchingOperation(),
                        new FabricPMStitchingOperation());
            case HYPERVISOR:                    // Fall through
            case CLOUD_MANAGEMENT:              // Fall through
            case LOAD_BALANCER:                 // Fall through
            case NETWORK:                       // Fall through
            case OPERATION_MANAGER_APPLIANCE:   // Fall through
            case APPLICATION_SERVER:            // Fall through
            case DATABASE_SERVER:               // Fall through
            case CLOUD_NATIVE:
            case STORAGE_BROWSING:              // Fall through
            case ORCHESTRATOR:                  // Fall through
            case HYPERCONVERGED:                // Fall through
            case PAAS:                          // Fall through
            case GUEST_OS_PROCESSES:            // Fall through
            case CUSTOM:
                return Collections.emptyList();
            default:
            throw new StitchingUnknownProbeException(probeType, probeCategory);
        }
    }

    /**
     * An exception thrown if a probe type and category is not known.
     */
    public class StitchingUnknownProbeException extends Exception {
        public StitchingUnknownProbeException(@Nonnull final String probeType,
                                              @Nonnull final ProbeCategory probeCategory) {
            super("Unkown probe of type \"" + probeType + "\" in category " + probeCategory);
        }
    }
}
