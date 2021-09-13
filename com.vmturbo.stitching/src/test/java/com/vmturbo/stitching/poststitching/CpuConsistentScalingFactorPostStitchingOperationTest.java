package com.vmturbo.stitching.poststitching;

import static com.vmturbo.stitching.poststitching.PostStitchingTestUtilities.makeTopologyEntityBuilder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.Collections;
import java.util.stream.Stream;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.AnalysisSettings;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfo;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.EntitySettingsCollection;
import com.vmturbo.stitching.TopologicalChangelog.EntityChangesBuilder;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.journal.IStitchingJournal;
import com.vmturbo.stitching.poststitching.CpuConsistentScalingFactorPostStitchingOperation.CloudNativeEntityConsistentScalingFactorPostStitchingOperation;
import com.vmturbo.stitching.poststitching.CpuConsistentScalingFactorPostStitchingOperation.VirtualMachineConsistentScalingFactorPostStitchingOperation;
import com.vmturbo.stitching.poststitching.PostStitchingTestUtilities.UnitTestResultBuilder;

/**
 * Tests for {@link CpuConsistentScalingFactorPostStitchingOperation}.
 */
public class CpuConsistentScalingFactorPostStitchingOperationTest {
    private final CpuConsistentScalingFactorPostStitchingOperation vmOperation =
        new VirtualMachineConsistentScalingFactorPostStitchingOperation(true);
    private final CpuConsistentScalingFactorPostStitchingOperation cnEntityOperation =
        new CloudNativeEntityConsistentScalingFactorPostStitchingOperation(true);

    private EntityChangesBuilder<TopologyEntity> resultBuilder = new UnitTestResultBuilder();

    private final EntitySettingsCollection settingsMock = mock(EntitySettingsCollection.class);

    private TopologyEntity.Builder vm = makeTopologyEntityBuilder(1L,
        EntityType.VIRTUAL_MACHINE_VALUE,
        Collections.emptyList(),
        Collections.emptyList());

    private TopologyEntity.Builder ns = makeTopologyEntityBuilder(2L,
        EntityType.NAMESPACE_VALUE,
        Collections.emptyList(),
        Collections.emptyList());

    private TopologyEntity.Builder pod = makeTopologyEntityBuilder(3L,
        EntityType.CONTAINER_POD_VALUE,
        Collections.emptyList(),
        Collections.emptyList());

    private TopologyEntity.Builder wc = makeTopologyEntityBuilder(4L,
        EntityType.WORKLOAD_CONTROLLER_VALUE,
        Collections.emptyList(),
        Collections.emptyList());

    @SuppressWarnings("unchecked")
    private final IStitchingJournal<TopologyEntity> journal =
        (IStitchingJournal<TopologyEntity>)mock(IStitchingJournal.class);

    /**
     * testMissingVmInfo.
     */
    @Test
    public void testMissingVmInfo() {
        vmOperation.performOperation(Stream.of(vm.build()), settingsMock, resultBuilder)
            .getChanges().forEach(change -> change.applyChange(journal));
        assertFalse(vm.getEntityBuilder().getAnalysisSettings().hasConsistentScalingFactor());
    }

    /**
     * testVmMissingNumCpus.
     */
    @Test
    public void testVmMissingNumCpus() {
        vm.getEntityBuilder()
            .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                .setVirtualMachine(VirtualMachineInfo.newBuilder().build()));
        vmOperation.performOperation(Stream.of(vm.build()), settingsMock, resultBuilder)
            .getChanges().forEach(change -> change.applyChange(journal));
        assertFalse(vm.getEntityBuilder().getAnalysisSettings().hasConsistentScalingFactor());
    }

    /**
     * testVmMissingVcpuCommodity.
     */
    @Test
    public void testVmMissingVcpuCommodity() {
        vm.getEntityBuilder()
            .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                .setVirtualMachine(VirtualMachineInfo.newBuilder().setNumCpus(2).build()));
        vmOperation.performOperation(Stream.of(vm.build()), settingsMock, resultBuilder)
            .getChanges().forEach(change -> change.applyChange(journal));
        assertFalse(vm.getEntityBuilder().getAnalysisSettings().hasConsistentScalingFactor());
    }

    /**
     * testVmConsistentScalingFactorSet.
     */
    @Test
    public void testVmConsistentScalingFactorSet() {
        vm.getEntityBuilder()
            .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                .setVirtualMachine(VirtualMachineInfo.newBuilder().setNumCpus(4).build()));
        vm.getEntityBuilder().addCommoditySoldList(CommoditySoldDTO.newBuilder()
            .setCommodityType(CommodityType.newBuilder().setType(CommodityDTO.CommodityType.VCPU_VALUE))
            .setCapacity(1000.0)
            .setScalingFactor(2.0f)
            .build());

        vmOperation.performOperation(Stream.of(vm.build()), settingsMock, resultBuilder)
            .getChanges().forEach(change -> change.applyChange(journal));
        assertTrue(vm.getEntityBuilder().getAnalysisSettings().hasConsistentScalingFactor());
        assertEquals(2.0f, vm.getEntityBuilder().getAnalysisSettings().getConsistentScalingFactor(), 0);
    }

    /**
     * testNsMissingNamespaceInfo.
     */
    @Test
    public void testNsMissingNamespaceInfo() {
        cnEntityOperation.performOperation(Stream.of(ns.build()), settingsMock, resultBuilder)
            .getChanges().forEach(change -> change.applyChange(journal));
        assertFalse(ns.getEntityBuilder().getAnalysisSettings().hasConsistentScalingFactor());
    }

    /**
     * testCloudNativeEntityConsistentScalingFactorSet.
     */
    @Test
    public void testCloudNativeEntityConsistentScalingFactorSet() {
        ns.getEntityBuilder().addCommoditySoldList(CommoditySoldDTO.newBuilder()
            .setCommodityType(CommodityType.newBuilder().setType(CommodityDTO.CommodityType.VCPU_LIMIT_QUOTA_VALUE))
            .setCapacity(2000.0)
            .setScalingFactor(2.0f)
            .build());
        wc.getEntityBuilder().addCommoditySoldList(CommoditySoldDTO.newBuilder()
            .setCommodityType(CommodityType.newBuilder().setType(CommodityDTO.CommodityType.VCPU_LIMIT_QUOTA_VALUE))
            .setCapacity(2000.0)
            .setScalingFactor(2.0f)
            .build());
        pod.getEntityBuilder().addCommoditySoldList(CommoditySoldDTO.newBuilder()
            .setCommodityType(CommodityType.newBuilder().setType(CommodityDTO.CommodityType.VCPU_LIMIT_QUOTA_VALUE))
            .setCapacity(2000.0)
            .setScalingFactor(2.0f)
            .build());

        cnEntityOperation.performOperation(Stream.of(ns.build(), wc.build(), pod.build()), settingsMock, resultBuilder)
            .getChanges().forEach(change -> change.applyChange(journal));
        assertTrue(ns.getEntityBuilder().getAnalysisSettings().hasConsistentScalingFactor());
        // 1 / scalingFactor = 1 / 2 = 0.5
        assertEquals(0.5f, ns.getEntityBuilder().getAnalysisSettings().getConsistentScalingFactor(), 0);
        assertEquals(0.5f, wc.getEntityBuilder().getAnalysisSettings().getConsistentScalingFactor(), 0);
        assertEquals(0.5f, pod.getEntityBuilder().getAnalysisSettings().getConsistentScalingFactor(), 0);
    }

    /**
     * testNsConsistentScalingFactorNotSet.
     */
    @Test
    public void testNsConsistentScalingFactorNotSet() {
        ns.getEntityBuilder().addCommoditySoldList(CommoditySoldDTO.newBuilder()
            .setCommodityType(CommodityType.newBuilder().setType(CommodityDTO.CommodityType.VCPU_LIMIT_QUOTA_VALUE))
            .setCapacity(2000.0)
            .setScalingFactor(0)
            .build());

        cnEntityOperation.performOperation(Stream.of(ns.build()), settingsMock, resultBuilder)
            .getChanges().forEach(change -> change.applyChange(journal));
        assertFalse(ns.getEntityBuilder().getAnalysisSettings().hasConsistentScalingFactor());
        // Default consistentScalingFactor is 1 if not set.
        assertEquals(1, ns.getEntityBuilder().getAnalysisSettings().getConsistentScalingFactor(), 0);
    }

    /**
     * Test that we can correctly compute the consistent scaling factor for millicores.
     */
    @Test
    public void testComputeMillicoreCSF() {
        final VirtualMachineInfo vmInfo = VirtualMachineInfo.newBuilder()
            .setNumCpus(4)
            .build();
        final CommoditySoldDTO vcpu = CommoditySoldDTO.newBuilder()
            .setCommodityType(CommodityType.newBuilder().setType(CommodityDTO.CommodityType.VCPU_VALUE))
            .setCapacity(8000.0)
            .setScalingFactor(1.5)
            .build();

        final double millicoresPerMHz = 4000.0 / (vcpu.getCapacity() * vcpu.getScalingFactor());
        Assert.assertEquals(millicoresPerMHz,
            CpuConsistentScalingFactorPostStitchingOperation.computeMillicoreConsistentScalingFactor(vmInfo, vcpu), 0);
    }

    /**
     * testUnableToComputeMillicoreCSF.
     */
    @Test
    public void testUnableToComputeMillicoreCSF() {
        final VirtualMachineInfo vmInfo = VirtualMachineInfo.newBuilder()
            .setNumCpus(4)
            .build();
        final CommoditySoldDTO vcpu = CommoditySoldDTO.newBuilder()
            .setCommodityType(CommodityType.newBuilder().setType(CommodityDTO.CommodityType.VCPU_VALUE))
            .setCapacity(0)
            .setScalingFactor(1.5)
            .build();

        Assert.assertEquals(AnalysisSettings.newBuilder().getConsistentScalingFactor(),
            CpuConsistentScalingFactorPostStitchingOperation.computeMillicoreConsistentScalingFactor(vmInfo, vcpu), 0);
    }
}