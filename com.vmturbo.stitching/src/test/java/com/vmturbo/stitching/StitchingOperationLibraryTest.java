package com.vmturbo.stitching;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Collectors;

import org.junit.Test;

import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.stitching.StitchingOperationLibrary.StitchingUnknownProbeException;
import com.vmturbo.stitching.cloudfoundry.CloudFoundryVMStitchingOperation;
import com.vmturbo.stitching.fabric.FabricPMStitchingOperation;
import com.vmturbo.stitching.vcd.ElasticVDCStitchingOperation;
import com.vmturbo.stitching.vcd.VcdVMStitchingOperation;

/**
 * Tests for {@link StitchingOperationLibrary}.
 */
public class StitchingOperationLibraryTest {

    private final StitchingOperationLibrary library = new StitchingOperationLibrary();

    @Test
    public void testStorageProbeCategory() throws StitchingUnknownProbeException {
        assertEquals(
                Collections.<Class>emptyList(),
                library.stitchingOperationsFor(SDKProbeType.NETAPP.getProbeType(),
                        ProbeCategory.STORAGE).stream()
                        .map(Object::getClass)
                        .collect(Collectors.toList())
        );
    }

    @Test
    public void testFabricProbeCategory() throws StitchingUnknownProbeException {
        assertEquals(Collections.singletonList(FabricPMStitchingOperation.class),
                library.stitchingOperationsFor(SDKProbeType.UCS.getProbeType(),
                        ProbeCategory.FABRIC).stream()
                        .map(Object::getClass)
                        .collect(Collectors.toList())
        );
    }

    @Test
    public void testHypervisorProbeCategory() throws StitchingUnknownProbeException {
        assertEquals(
            Collections.<Class>emptyList(),
            library.stitchingOperationsFor(SDKProbeType.VCENTER.getProbeType(),
                    ProbeCategory.HYPERVISOR).stream()
                .map(Object::getClass)
                .collect(Collectors.toList())
        );
    }

    @Test
    public void testVCDProbeCategory() throws StitchingUnknownProbeException {
        assertEquals(
                Arrays.asList(VcdVMStitchingOperation.class, ElasticVDCStitchingOperation.class),
                library.stitchingOperationsFor(SDKProbeType.VCD.getProbeType(),
                        ProbeCategory.CLOUD_MANAGEMENT).stream()
                        .map(Object::getClass)
                        .collect(Collectors.toList())
        );
    }

    @Test
    public void testCloudFoundryProbeCategory() throws StitchingUnknownProbeException {
        assertEquals(
                Arrays.asList(CloudFoundryVMStitchingOperation.class),
                library.stitchingOperationsFor(SDKProbeType.CLOUD_FOUNDRY.getProbeType(),
                        ProbeCategory.PAAS).stream()
                        .map(Object::getClass)
                        .collect(Collectors.toList())
        );
    }

    @Test
    public void testUnknownProbeCategory() throws StitchingUnknownProbeException {
        assertEquals(Collections.<StitchingOperation<?, ?>>emptyList(),
            library.stitchingOperationsFor("Unknown", ProbeCategory.UNKNOWN));

    }
}
