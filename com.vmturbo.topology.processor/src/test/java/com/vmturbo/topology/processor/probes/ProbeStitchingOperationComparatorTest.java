package com.vmturbo.topology.processor.probes;

import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.stitching.StitchingOperation;
import com.vmturbo.topology.processor.stitching.StitchingOperationStore.ProbeStitchingOperation;

/**
 * Tests for {@link ProbeStitchingOperationComparator}.
 */
public class ProbeStitchingOperationComparatorTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Nonnull
    final ProbeStore probeStore = Mockito.mock(ProbeStore.class);

    @Nonnull
    final ProbeStitchingOperation vcenterOp =
        new ProbeStitchingOperation(1L, Mockito.mock(StitchingOperation.class));
    @Nonnull
    final ProbeStitchingOperation azureOp =
        new ProbeStitchingOperation(2L, Mockito.mock(StitchingOperation.class));
    @Nonnull
    final ProbeStitchingOperation hypervOp =
        new ProbeStitchingOperation(3L, Mockito.mock(StitchingOperation.class));
    @Nonnull
    final ProbeStitchingOperation unknownCategoryOp =
        new ProbeStitchingOperation(4L, Mockito.mock(StitchingOperation.class));
    @Nonnull
    final ProbeStitchingOperation unknownTypeOp =
        new ProbeStitchingOperation(5L, Mockito.mock(StitchingOperation.class));

    @Nonnull
    final ProbeInfo vcenterInfo = ProbeInfo.newBuilder()
        .setProbeType(SDKProbeType.VCENTER.getProbeType())
        .setProbeCategory(ProbeCategory.HYPERVISOR.getCategory())
        .setUiProbeCategory(ProbeCategory.HYPERVISOR.getCategory())
        .build();

    @Nonnull
    final ProbeInfo azureInfo = ProbeInfo.newBuilder()
        .setProbeType(SDKProbeType.AZURE.getProbeType())
        .setProbeCategory(ProbeCategory.CLOUD_MANAGEMENT.getCategory())
        .setUiProbeCategory(ProbeCategory.PUBLIC_CLOUD.getCategory())
        .build();

    @Nonnull
    final ProbeInfo hypervInfo = ProbeInfo.newBuilder()
        .setProbeType(SDKProbeType.HYPERV.getProbeType())
        .setProbeCategory(ProbeCategory.HYPERVISOR.getCategory())
        .setUiProbeCategory(ProbeCategory.HYPERVISOR.getCategory())
        .build();

    @Nonnull ProbeInfo unknownCategoryInfo = ProbeInfo.newBuilder()
        .setProbeType(SDKProbeType.HYPERV.getProbeType())
        .setProbeCategory("foo")
        .setUiProbeCategory("doo")
        .build();

    @Nonnull
    final ProbeInfo unknownTypeInfo = ProbeInfo.newBuilder()
        .setProbeType("bar")
        .setProbeCategory(ProbeCategory.CLOUD_MANAGEMENT.getCategory())
        .setUiProbeCategory(ProbeCategory.CLOUD_MANAGEMENT.getCategory())
        .build();

    @Before
    public void setup() {
        when(probeStore.getProbe(1L)).thenReturn(Optional.of(vcenterInfo));
        when(probeStore.getProbe(2L)).thenReturn(Optional.of(azureInfo));
        when(probeStore.getProbe(3L)).thenReturn(Optional.of(hypervInfo));
        when(probeStore.getProbe(4L)).thenReturn(Optional.of(unknownCategoryInfo));
        when(probeStore.getProbe(5L)).thenReturn(Optional.of(unknownTypeInfo));
    }

    @Test
    public void testSimpleCycle() throws Exception {
        final Map<ProbeCategory, Set<ProbeCategory>> categoryDependencies = new HashMap<>();
        categoryDependencies.put(ProbeCategory.CLOUD_MANAGEMENT, Collections.singleton(ProbeCategory.CLOUD_NATIVE));
        categoryDependencies.put(ProbeCategory.CLOUD_NATIVE, Collections.singleton(ProbeCategory.CLOUD_MANAGEMENT));

        expectedException.expect(ProbeException.class);
        new ProbeStitchingOperationComparator(probeStore, categoryDependencies, Collections.emptyMap());
    }

    @Test
    public void testSimpleDirectedAcyclicGraph() throws ProbeException {
        final Map<ProbeCategory, Set<ProbeCategory>> categoryDependencies = new HashMap<>();
        categoryDependencies.put(ProbeCategory.CLOUD_MANAGEMENT, Collections.singleton(ProbeCategory.HYPERVISOR));

        final ProbeStitchingOperationComparator comparator =
            new ProbeStitchingOperationComparator(probeStore, categoryDependencies, Collections.emptyMap());

        // Cloud management should be before hypervisors
        assertEquals(-1, comparator.compare(vcenterOp, azureOp));
        assertEquals(1, comparator.compare(azureOp, vcenterOp));
        assertEquals(-1, comparator.compare(hypervOp, azureOp));
        assertEquals(1, comparator.compare(azureOp, hypervOp));
    }

    @Test
    public void testSortWithSimpleDependency() throws ProbeException {
        final Map<ProbeCategory, Set<ProbeCategory>> categoryDependencies = new HashMap<>();
        categoryDependencies.put(ProbeCategory.CLOUD_MANAGEMENT, Collections.singleton(ProbeCategory.HYPERVISOR));
        categoryDependencies.put(ProbeCategory.STORAGE,
            new HashSet<>(Arrays.asList(ProbeCategory.HYPERVISOR, ProbeCategory.CLOUD_MANAGEMENT)));

        final ProbeStitchingOperationComparator comparator =
            new ProbeStitchingOperationComparator(probeStore, categoryDependencies, Collections.emptyMap());

        long index = 0;
        final List<ProbeStitchingOperation> operations = new ArrayList<>();
        for (ProbeCategory cat : ProbeCategory.values()) {
            operations.add(new ProbeStitchingOperation(index, Mockito.mock(StitchingOperation.class)));
            final ProbeInfo info = ProbeInfo.newBuilder()
                .setProbeCategory(cat.getCategory())
                .setUiProbeCategory(cat.getCategory())
                .setProbeType("foo")
                .build();
            when(probeStore.getProbe(index)).thenReturn(Optional.of(info));
            index++;
        }

        Collections.sort(operations, comparator);

        assertThat(opIndex(operations, probeStore, ProbeCategory.HYPERVISOR),
            lessThan(opIndex(operations, probeStore, ProbeCategory.STORAGE)));
        assertThat(opIndex(operations, probeStore, ProbeCategory.CLOUD_MANAGEMENT),
            lessThan(opIndex(operations, probeStore, ProbeCategory.STORAGE)));
        assertThat(opIndex(operations, probeStore, ProbeCategory.HYPERVISOR),
            lessThan(opIndex(operations, probeStore, ProbeCategory.CLOUD_MANAGEMENT)));
    }

    @Test
    public void testTypeOrderingForSameCategories() throws ProbeException {
        final Map<ProbeCategory, Set<ProbeCategory>> categoryDependencies = new HashMap<>();
        categoryDependencies.put(ProbeCategory.CLOUD_MANAGEMENT, Collections.singleton(ProbeCategory.HYPERVISOR));

        final Map<SDKProbeType, Set<SDKProbeType>> typeDependencies = new HashMap<>();
        typeDependencies.put(SDKProbeType.VCENTER, Collections.singleton(SDKProbeType.HYPERV));

        final ProbeStitchingOperationComparator comparator =
            new ProbeStitchingOperationComparator(probeStore, categoryDependencies, typeDependencies);

        // Cloud management should be before hypervisors
        assertEquals(-1, comparator.compare(vcenterOp, azureOp));
        assertEquals(1, comparator.compare(azureOp, vcenterOp));
        assertEquals(-1, comparator.compare(hypervOp, azureOp));
        assertEquals(1, comparator.compare(azureOp, hypervOp));

        // VCenter should be before HyperV
        assertEquals(1, comparator.compare(vcenterOp, hypervOp));
        assertEquals(-1, comparator.compare(hypervOp, vcenterOp));
    }

    @Test
    public void testUnknownCategory() throws ProbeException {
        // Unknown category should be last
        final Map<ProbeCategory, Set<ProbeCategory>> categoryDependencies = new HashMap<>();
        categoryDependencies.put(ProbeCategory.CLOUD_MANAGEMENT, Collections.singleton(ProbeCategory.HYPERVISOR));

        final Map<SDKProbeType, Set<SDKProbeType>> typeDependencies = new HashMap<>();
        typeDependencies.put(SDKProbeType.VCENTER, Collections.singleton(SDKProbeType.HYPERV));

        final ProbeStitchingOperationComparator comparator =
            new ProbeStitchingOperationComparator(probeStore, categoryDependencies, typeDependencies);

        assertEquals(-1, comparator.compare(vcenterOp, unknownCategoryOp));
        assertEquals(-1, comparator.compare(azureOp, unknownCategoryOp));
        assertEquals(-1, comparator.compare(hypervOp, unknownCategoryOp));

        assertEquals(1, comparator.compare(unknownCategoryOp, vcenterOp));
        assertEquals(1, comparator.compare(unknownCategoryOp, azureOp));
        assertEquals(1, comparator.compare(unknownCategoryOp, hypervOp));
    }

    @Test
    public void testUnknownType() throws ProbeException {
        // Unknown category should be last
        final Map<ProbeCategory, Set<ProbeCategory>> categoryDependencies = new HashMap<>();
        categoryDependencies.put(ProbeCategory.CLOUD_MANAGEMENT, Collections.singleton(ProbeCategory.HYPERVISOR));

        final Map<SDKProbeType, Set<SDKProbeType>> typeDependencies = new HashMap<>();
        typeDependencies.put(SDKProbeType.VCENTER, Collections.singleton(SDKProbeType.HYPERV));

        final ProbeStitchingOperationComparator comparator =
            new ProbeStitchingOperationComparator(probeStore, categoryDependencies, typeDependencies);

        assertEquals(-1, comparator.compare(azureOp, unknownTypeOp));
        assertEquals(-1, comparator.compare(vcenterOp, unknownTypeOp));
        assertEquals(-1, comparator.compare(hypervOp, unknownTypeOp));

        assertEquals(1, comparator.compare(unknownTypeOp, azureOp));
        assertEquals(1, comparator.compare(unknownTypeOp, vcenterOp));
        assertEquals(1, comparator.compare(unknownTypeOp, hypervOp));
    }

    private int opIndex(@Nonnull final List<ProbeStitchingOperation> operations,
                                            @Nonnull final ProbeStore probeStore,
                                            final ProbeCategory category) {
        for (int i = 0; i < operations.size(); i++) {
            final ProbeStitchingOperation op = operations.get(i);
            if (probeStore.getProbe(op.probeId).get().getProbeCategory().equals(category.getCategory())) {
                return i;
            }
        }

        return -1;
    }
}
