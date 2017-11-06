package com.vmturbo.topology.processor.stitching;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import com.vmturbo.platform.sdk.common.MediationMessage;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.stitching.StitchingOperation;
import com.vmturbo.stitching.StitchingOperationLibrary;
import com.vmturbo.stitching.StitchingOperationLibrary.StitchingUnknownProbeException;
import com.vmturbo.topology.processor.probes.ProbeException;

public class StitchingOperationStoreTest {

    final StitchingOperationLibrary library = Mockito.mock(StitchingOperationLibrary.class);

    final StitchingOperationStore store = new StitchingOperationStore(library);

    final StitchingOperation<?, ?> firstOperation = mock(StitchingOperation.class);
    final StitchingOperation<?, ?> secondOperation = mock(StitchingOperation.class);
    final StitchingOperation<?, ?> thirdOperation = mock(StitchingOperation.class);

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testAddOperationsViaLibrary() throws Exception {
        when(library.stitchingOperationsFor(eq("some-hypervisor-probe"), eq(ProbeCategory.HYPERVISOR)))
            .thenReturn(Collections.singletonList(firstOperation));
        final MediationMessage.ProbeInfo probeInfo = MediationMessage.ProbeInfo.newBuilder()
            .setProbeCategory(ProbeCategory.HYPERVISOR.name())
            .setProbeType("some-hypervisor-probe")
            .build();

        store.setOperationsForProbe(1234L, probeInfo);

        assertEquals(1, store.probeCount());
        assertEquals(Collections.singletonList(firstOperation), store.getOperationsForProbe(1234L).get());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testAddUnknownProbe() throws StitchingUnknownProbeException, ProbeException {
        when(library.stitchingOperationsFor(eq("unknown-probe"), eq(ProbeCategory.HYPERVISOR)))
            .thenThrow(StitchingUnknownProbeException.class);

        final MediationMessage.ProbeInfo probeInfo = MediationMessage.ProbeInfo.newBuilder()
            .setProbeCategory(ProbeCategory.HYPERVISOR.name())
            .setProbeType("unknown-probe")
            .build();

        expectedException.expect(ProbeException.class);
        store.setOperationsForProbe(1234L, probeInfo);
    }

    @Test
    public void testGetAllOperations() {
        store.setOperationsForProbe(1234, Arrays.asList(thirdOperation, firstOperation));
        store.setOperationsForProbe(5678, Collections.singletonList(secondOperation));

        assertThat(store.getAllOperations().stream()
            .map(pso -> pso.stitchingOperation)
            .collect(Collectors.toList()), containsInAnyOrder(firstOperation, secondOperation, thirdOperation));
    }

    @Test
    public void testProbeCount() {
        store.setOperationsForProbe(1234, Arrays.asList(thirdOperation, firstOperation));
        store.setOperationsForProbe(5678, Collections.singletonList(secondOperation));

        assertEquals(2, store.probeCount());
    }

    @Test
    public void testOperationCount() {
        store.setOperationsForProbe(1234, Arrays.asList(thirdOperation, firstOperation));
        store.setOperationsForProbe(5678, Collections.singletonList(secondOperation));

        assertEquals(2, store.probeCount());
    }

    @Test
    public void testRemoveOperationsForProbe() {
        store.setOperationsForProbe(1234, Arrays.asList(thirdOperation, firstOperation));
        store.setOperationsForProbe(5678, Collections.singletonList(secondOperation));
        assertEquals(3, store.operationCount());

        store.removeOperationsForProbe(1234);
        assertEquals(1, store.operationCount());
        assertEquals(Optional.<List<StitchingOperation>>empty(), store.getOperationsForProbe(1234));
    }
}