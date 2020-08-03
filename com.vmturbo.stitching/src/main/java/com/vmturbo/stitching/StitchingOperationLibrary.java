package com.vmturbo.stitching;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.stitching.billing.AwsBillingBusinessAccountStitchingOperation;
import com.vmturbo.stitching.billing.AwsBillingStitchingOperation;
import com.vmturbo.stitching.cloudfoundry.CloudFoundryVMStitchingOperation;
import com.vmturbo.stitching.compute.IaasVMStitchingOperation;
import com.vmturbo.stitching.fabric.FabricChassisStitchingOperation;
import com.vmturbo.stitching.fabric.FabricPMStitchingOperation;
import com.vmturbo.stitching.vcd.ElasticVDCStitchingOperation;
import com.vmturbo.stitching.vcd.VcdVMStitchingOperation;
import com.vmturbo.stitching.vdi.DesktopPoolMasterImageStitchingOperation;
import com.vmturbo.stitching.vdi.VDIPMStitchingOperation;
import com.vmturbo.stitching.vdi.VDIStorageStitchingOperation;
import com.vmturbo.stitching.vdi.VDIVDCStitchingOperation;
import com.vmturbo.stitching.vdi.VDIVMStitchingOperation;

/**
 * A library of stitching operations. Maps probe type and category to operations that to be used for
 * stitching targets discovered by that probe.
 */
public class StitchingOperationLibrary {

    private static final Logger logger = LogManager.getLogger();

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
                return Collections.emptyList();
            case FABRIC:
                return Arrays.asList(new FabricChassisStitchingOperation(),
                        new FabricPMStitchingOperation());
            case HYPERVISOR:
                return Collections.emptyList();
            case CLOUD_MANAGEMENT:
                if (probeType.equals(SDKProbeType.VCD.getProbeType())) {
                    return Arrays.asList(new VcdVMStitchingOperation(),
                            new ElasticVDCStitchingOperation());
                }
                return Collections.emptyList();
            case LOAD_BALANCER:
                return Collections.emptyList();
            case NETWORK:
                return Collections.emptyList();
            case OPERATION_MANAGER_APPLIANCE:
                return Collections.emptyList();
            case APPLICATION_SERVER:
                return Collections.emptyList();
            case DATABASE_SERVER:
                return Collections.emptyList();
            case CLOUD_NATIVE:
                return Collections.singletonList(new IaasVMStitchingOperation());
            case STORAGE_BROWSING:
                return Collections.emptyList();
            case ORCHESTRATOR:
                return Collections.emptyList();
            case HYPERCONVERGED:
                if (probeType.equals(SDKProbeType.INTERSIGHT_UCS.getProbeType())) {
                    return Arrays.asList(new FabricChassisStitchingOperation(),
                            new FabricPMStitchingOperation());
                }
                return Collections.emptyList();
            case PAAS:
                if (probeType.equals(SDKProbeType.CLOUD_FOUNDRY.getProbeType())) {
                    return Collections.singletonList(
                            new CloudFoundryVMStitchingOperation());
                }
                return Collections.singletonList(new IaasVMStitchingOperation());
            case GUEST_OS_PROCESSES:
                return Collections.emptyList();
            case CUSTOM:
                return Collections.emptyList();
            case BILLING:
                if (probeType.equals(SDKProbeType.AWS_BILLING.getProbeType())) {
                    return Arrays.asList(new AwsBillingStitchingOperation(),
                        new AwsBillingBusinessAccountStitchingOperation());
                }
                return Collections.emptyList();
            case VIRTUAL_DESKTOP_INFRASTRUCTURE:
                return ImmutableList.of(
                        new VDIVMStitchingOperation(),
                        new VDIStorageStitchingOperation(),
                        new VDIPMStitchingOperation(),
                        new VDIVDCStitchingOperation(),
                        new DesktopPoolMasterImageStitchingOperation());
            default:
                logger.warn("Unknown probe type {} and category {}.", probeType, probeCategory);
                return Collections.emptyList();
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
