package com.vmturbo.stitching.poststitching;

import static com.vmturbo.stitching.poststitching.PostStitchingTestUtilities.makeCommodityBought;
import static com.vmturbo.stitching.poststitching.PostStitchingTestUtilities.makeCommoditySold;
import static com.vmturbo.stitching.poststitching.PostStitchingTestUtilities.makeTopologyEntityBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.PhysicalMachineInfo;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.EntitySettingsCollection;
import com.vmturbo.stitching.TopologicalChangelog;
import com.vmturbo.stitching.TopologicalChangelog.EntityChangesBuilder;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.cpucapacity.CpuCapacityStore;
import com.vmturbo.stitching.journal.IStitchingJournal;
import com.vmturbo.stitching.poststitching.PostStitchingTestUtilities.UnitTestResultBuilder;

public class CpuScalingFactorPostStitchingOperationTest {

    private static final long PM_OID = 4L;
    private static final double CPU_CAPACITY = 123D;
    private static final double CPU_PROVISIONED_CAPACITY = 456D;
    private static final long VM_OID = 5L;
    private static final long APP_OID = 6L;
    private static final double CPU_SCALE_FACTOR = 1.3;
    private static final String TEST_CPU_MODEL = "cpu-model-1";


    private CpuCapacityStore cpuCapacityStore = mock(CpuCapacityStore.class);
    private final CpuScalingFactorPostStitchingOperation operation =
            new CpuScalingFactorPostStitchingOperation(cpuCapacityStore);
    private EntityChangesBuilder<TopologyEntity> resultBuilder;

    private final EntitySettingsCollection settingsMock = mock(EntitySettingsCollection.class);

    // Set up the CPU and CPU_PROVISIONED commodities for the test PM
    final CommoditySoldDTO cpuSoldCommodity = makeCommoditySold(CommodityType.CPU, CPU_CAPACITY);
    final CommoditySoldDTO cpuProvisionedSoldCommodity = makeCommoditySold(CommodityType.CPU_PROVISIONED,
            CPU_PROVISIONED_CAPACITY);
    final List<CommoditySoldDTO> pmCommoditiesSoldList = Lists.newArrayList(
            cpuSoldCommodity, cpuProvisionedSoldCommodity);
    private TopologyEntity.Builder pm1 = makeTopologyEntityBuilder(PM_OID,
            EntityType.PHYSICAL_MACHINE.getNumber(),
            pmCommoditiesSoldList,
            Collections.emptyList());

    // Set up the VCPU commodity for the test VM
    final CommodityBoughtDTO cpuBoughtCommodity = makeCommodityBought(CommodityType.CPU);
    final CommodityBoughtDTO cpuProvisionedBoughtCommodity = makeCommodityBought(
            CommodityType.CPU_PROVISIONED);
    final List<CommodityBoughtDTO> vmCommoditiesBoughtList = Lists.newArrayList(
            cpuBoughtCommodity, cpuProvisionedBoughtCommodity);
    final CommoditySoldDTO vcpuSoldCommodity = makeCommoditySold(CommodityType.VCPU, CPU_CAPACITY);
    final List<CommoditySoldDTO> vmCommoditiesSoldList = Lists.newArrayList(
            vcpuSoldCommodity);
    private TopologyEntity.Builder vm1 = makeTopologyEntityBuilder(VM_OID,
            EntityType.VIRTUAL_MACHINE.getNumber(),
            vmCommoditiesSoldList,
            vmCommoditiesBoughtList);

    // Set up the VCPU commodity for the test App
    final CommodityBoughtDTO vcpuBoughtCommodity = makeCommodityBought(CommodityType.VCPU);
    final List<CommodityBoughtDTO> appCommoditiesBoughtList = Lists.newArrayList(
            vcpuBoughtCommodity);
    private TopologyEntity.Builder app1 = makeTopologyEntityBuilder(APP_OID,
            EntityType.APPLICATION_COMPONENT.getNumber(),
            Collections.emptyList(),
            appCommoditiesBoughtList);

    @SuppressWarnings("unchecked")
    private final IStitchingJournal<TopologyEntity> journal =
            (IStitchingJournal<TopologyEntity>)mock(IStitchingJournal.class);

    @Before
    public void setup() {
        resultBuilder = new UnitTestResultBuilder();
    }

    /**
     * Test that the scaleFactor for a given PM's CPU model is propagated to
     * <ul>
     *     <li>the CPU commodity sold by the PM</li>
     *     <li>the CPU_PROVISIONED commodity sold by the PM</li>
     *     <li>the VCPU commodity sold by any VM that buys from the PM</li>
     * </ul>
     */
    @Test
    public void testScaleFactorFromCpuModel() {
        // Arrange
        // prepare the PM with CPU capacity and selling to VM
        pm1.getEntityBuilder().setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                .setPhysicalMachine(PhysicalMachineInfo.newBuilder()
                        .setCpuModel(TEST_CPU_MODEL)
                        .build())
                .build());

        // link the VM to the PM as a consumer
        pm1.addConsumer(vm1);
        // link the App to the VM as a consumer.
        vm1.addConsumer(app1);

        // Ensure we can handle scenario where have consumer loop, and don't have
        // innfinite recursion.
        app1.addConsumer(vm1);

        // Set up the mock for the cpuCapacityStore
        when(cpuCapacityStore.getScalingFactor(TEST_CPU_MODEL))
                .thenReturn(Optional.of(CPU_SCALE_FACTOR));

        // The expected commodity values will each have the scaleFactor added
        final CommoditySoldDTO expectedCpuSoldCommodity = cpuSoldCommodity.toBuilder()
                .setScalingFactor(CPU_SCALE_FACTOR)
                .build();
        final CommoditySoldDTO expectedCpuProvisionedSoldCommodity = cpuProvisionedSoldCommodity
                .toBuilder()
                .setScalingFactor(CPU_SCALE_FACTOR)
                .build();
        final CommodityBoughtDTO expectedVmCpuCommodity = cpuBoughtCommodity.toBuilder()
                .setScalingFactor(CPU_SCALE_FACTOR)
                .build();
        final CommodityBoughtDTO expectedVmCpuProvisionedCommodity = cpuProvisionedBoughtCommodity
                .toBuilder()
                .setScalingFactor(CPU_SCALE_FACTOR)
                .build();
        final CommoditySoldDTO expectedvCpuSoldCommodity = vcpuSoldCommodity.toBuilder()
                .setScalingFactor(CPU_SCALE_FACTOR)
                .build();
        final CommodityBoughtDTO expectedvCpuBoughtCommodity = vcpuBoughtCommodity.toBuilder()
                .setScalingFactor(CPU_SCALE_FACTOR)
                .build();

        // Act
        final TopologicalChangelog<TopologyEntity> changeLog =
                operation.performOperation(Stream.of(pm1.build(), vm1.build(), app1.build()),
                        settingsMock, resultBuilder);
        changeLog.getChanges().forEach(change -> change.applyChange(journal));
        // Assert
        final List<CommoditySoldDTO> soldPmCommodities = pm1.getEntityBuilder()
                .getCommoditySoldListList();
        assertEquals(2, soldPmCommodities.size());
        assertThat(soldPmCommodities, containsInAnyOrder(expectedCpuSoldCommodity,
                expectedCpuProvisionedSoldCommodity));
        // check the related VM, as well
        final List<CommoditiesBoughtFromProvider> boughtVmCommoditiesFrom = vm1.getEntityBuilder()
                .getCommoditiesBoughtFromProvidersList();
        assertEquals(1, boughtVmCommoditiesFrom.size());
        final List<CommodityBoughtDTO> boughtVmCommodities = boughtVmCommoditiesFrom
                .iterator().next().getCommodityBoughtList();
        assertThat(boughtVmCommodities, containsInAnyOrder(expectedVmCpuCommodity,
                expectedVmCpuProvisionedCommodity));

        // App-VM relationship Assertion.
        final List<CommoditySoldDTO> soldVmCommodities = vm1.getEntityBuilder()
                .getCommoditySoldListList();
        assertEquals(1, soldVmCommodities.size());
        assertThat(soldVmCommodities, containsInAnyOrder(expectedvCpuSoldCommodity));
        final List<CommoditiesBoughtFromProvider> boughtAppCommoditiesFrom = app1.getEntityBuilder()
                .getCommoditiesBoughtFromProvidersList();
        assertEquals(1, boughtAppCommoditiesFrom.size());
        final List<CommodityBoughtDTO> boughtAppCommodities = boughtAppCommoditiesFrom
                .iterator().next().getCommodityBoughtList();
        assertThat(boughtAppCommodities, containsInAnyOrder(expectedvCpuBoughtCommodity));
    }

}