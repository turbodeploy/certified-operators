package com.vmturbo.stitching;

import static org.junit.Assert.assertEquals;

import java.util.Collections;
import java.util.stream.Collectors;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.stitching.StitchingOperationLibrary.StitchingUnknownProbeException;
import com.vmturbo.stitching.storage.StorageStitchingOperation;

/**
 * Tests for {@link StitchingOperationLibrary}.
 */
public class StitchingOperationLibraryTest {

    private final StitchingOperationLibrary library = new StitchingOperationLibrary();

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testStorageProbeCategory() throws StitchingUnknownProbeException {
        assertEquals(
            Collections.singletonList(StorageStitchingOperation.class),
            library.stitchingOperationsFor("NetApp", ProbeCategory.STORAGE).stream()
                .map(Object::getClass)
                .collect(Collectors.toList())
        );
    }

    @Test
    public void testHypervisorProbeCategory() throws StitchingUnknownProbeException {
        assertEquals(
            Collections.<Class>emptyList(),
            library.stitchingOperationsFor("VCenter", ProbeCategory.HYPERVISOR).stream()
                .map(Object::getClass)
                .collect(Collectors.toList())
        );
    }

    @Test
    public void testUnknownProbeCategory() throws StitchingUnknownProbeException {
        expectedException.expect(StitchingUnknownProbeException.class);

        library.stitchingOperationsFor("Unknown", ProbeCategory.UNKNOWN);
    }
}