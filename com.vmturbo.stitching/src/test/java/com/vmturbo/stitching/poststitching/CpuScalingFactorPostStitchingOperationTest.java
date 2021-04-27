package com.vmturbo.stitching.poststitching;

import static com.vmturbo.stitching.poststitching.PostStitchingTestUtilities.makeCommodityBought;
import static com.vmturbo.stitching.poststitching.PostStitchingTestUtilities.makeCommoditySold;
import static com.vmturbo.stitching.poststitching.PostStitchingTestUtilities.makeTopologyEntityBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.collect.Lists;

import it.unimi.dsi.fastutil.longs.LongOpenHashSet;

import org.junit.Before;
import org.junit.Test;

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
            new CpuScalingFactorPostStitchingOperation(cpuCapacityStore, true);
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

    /**
     * Test updateScalingFactorForEntity on a container. Only container VCPU commodity has CPU_SCALE_FACTOR,
     * while the commodity of the provider of the container, which is a pod, doesn't have CPU_SCALE_FACTOR
     * propagated.
     */
    @Test
    public void testUpdateScalingFactorForEntityWithoutProviderUpdated() {
        final CommoditySoldDTO vcpuComm = makeCommoditySold(CommodityType.VCPU, CPU_CAPACITY);

        final TopologyEntity.Builder container = makeTopologyEntityBuilder(1L,
            EntityType.CONTAINER_VALUE,
            Collections.singletonList(vcpuComm),
            Collections.emptyList());
        final TopologyEntity.Builder pod = makeTopologyEntityBuilder(2L,
            EntityType.CONTAINER_POD_VALUE,
            Collections.singletonList(vcpuComm),
            Collections.emptyList());
        container.addProvider(pod);
        pod.addConsumer(container);

        final CommoditySoldDTO expectedContainerVcpuComm = vcpuComm.toBuilder()
            .setScalingFactor(CPU_SCALE_FACTOR)
            .build();
        final CommoditySoldDTO expectedPodVcpuComm = vcpuComm.toBuilder()
            .build();

        operation.updateScalingFactorForEntity(container.build(), CPU_SCALE_FACTOR, new LongOpenHashSet());

        List<CommoditySoldDTO> containerCommSoldList = container.getEntityBuilder().getCommoditySoldListList();
        assertEquals(1, containerCommSoldList.size());
        assertThat(containerCommSoldList, containsInAnyOrder(expectedContainerVcpuComm));

        // Here pod VCPU comm doesn't have CPU_SCALE_FACTOR propagate from container
        List<CommoditySoldDTO> podCommSoldList = pod.getEntityBuilder().getCommoditySoldListList();
        assertEquals(1, podCommSoldList.size());
        assertThat(podCommSoldList, containsInAnyOrder(expectedPodVcpuComm));
    }

    /**
     * Test updateScalingFactorForEntity on a pod. We'll propagate the CPU_SCALE_FACTOR to its consumer
     * -- container, and to its provider -- WorkloadController, and recursively update to the provider of
     * WorkloadController -- Namespace.
     *
     */
    @Test
    public void testUpdateScalingFactorForEntityWithProvidersUpdated() {
        final CommoditySoldDTO vcpuComm = makeCommoditySold(CommodityType.VCPU, CPU_CAPACITY);
        final CommoditySoldDTO vcpuLimitQuotaComm = makeCommoditySold(CommodityType.VCPU_LIMIT_QUOTA,
            CPU_CAPACITY);
        final CommoditySoldDTO vcpuRequestComm = makeCommoditySold(CommodityType.VCPU_REQUEST, CPU_CAPACITY);
        final CommoditySoldDTO vcpuRequestQuotaComm = makeCommoditySold(CommodityType.VCPU_REQUEST_QUOTA,
            CPU_CAPACITY);

        final CommodityBoughtDTO vcpuBoughtComm = vcpuBoughtCommodity;
        final CommodityBoughtDTO vcpuRequestBoughtComm = makeCommodityBought(CommodityType.VCPU_REQUEST);
        final CommodityBoughtDTO vcpuLimitQuotaBoughtComm = makeCommodityBought(CommodityType.VCPU_LIMIT_QUOTA);
        final CommodityBoughtDTO vcpuRequestQuotaBoughtComm = makeCommodityBought(CommodityType.VCPU_REQUEST_QUOTA);

        final TopologyEntity.Builder container = makeTopologyEntityBuilder(1L,
            EntityType.CONTAINER_VALUE,
            Arrays.asList(vcpuComm, vcpuRequestComm),
            // Buying VCPU & Request from pod.
            Arrays.asList(vcpuBoughtComm, vcpuRequestBoughtComm));
        final TopologyEntity.Builder pod = makeTopologyEntityBuilder(2L,
            EntityType.CONTAINER_POD_VALUE,
            Arrays.asList(vcpuComm, vcpuRequestComm),
            // Buying VCPU & Request bought from workload controller.
            Arrays.asList(vcpuBoughtComm, vcpuRequestBoughtComm));
        final TopologyEntity.Builder wc = makeTopologyEntityBuilder(3L,
            EntityType.WORKLOAD_CONTROLLER_VALUE,
            Arrays.asList(vcpuLimitQuotaComm, vcpuRequestQuotaComm),
            // Buying VCPUQuota & RequestQuota bought from namespace.
            Arrays.asList(vcpuLimitQuotaBoughtComm, vcpuRequestQuotaBoughtComm));
        final TopologyEntity.Builder ns = makeTopologyEntityBuilder(4L,
            EntityType.NAMESPACE_VALUE,
            Arrays.asList(vcpuLimitQuotaComm, vcpuRequestQuotaComm),
            Collections.emptyList());

        container.addProvider(pod);

        pod.addConsumer(container);
        pod.addProvider(wc);

        wc.addConsumer(pod);
        wc.addProvider(ns);

        ns.addConsumer(wc);

        final CommoditySoldDTO expectedVcpuComm = vcpuComm.toBuilder()
            .setScalingFactor(CPU_SCALE_FACTOR)
            .build();
        final CommoditySoldDTO expectedVcpuLimitQuotaComm = vcpuLimitQuotaComm.toBuilder()
            .setScalingFactor(CPU_SCALE_FACTOR)
            .build();
        final CommoditySoldDTO expectedVcpuRequestComm = vcpuRequestComm.toBuilder()
            .setScalingFactor(CPU_SCALE_FACTOR)
            .build();
        final CommoditySoldDTO expectedVcpuRequestQuotaComm = vcpuRequestQuotaComm.toBuilder()
            .setScalingFactor(CPU_SCALE_FACTOR)
            .build();

        final CommodityBoughtDTO expectedVcpuBoughtComm = vcpuBoughtComm.toBuilder()
                .setScalingFactor(CPU_SCALE_FACTOR)
                .build();
        final CommodityBoughtDTO expectedLimitVcpuQuotaBoughtComm = vcpuLimitQuotaBoughtComm.toBuilder()
                .setScalingFactor(CPU_SCALE_FACTOR)
                .build();
        final CommodityBoughtDTO expectedVcpuRequestBoughtComm = vcpuRequestBoughtComm.toBuilder()
                .setScalingFactor(CPU_SCALE_FACTOR)
                .build();
        final CommodityBoughtDTO expectedVcpuRequestQuotaBoughtComm = vcpuRequestQuotaBoughtComm.toBuilder()
                .setScalingFactor(CPU_SCALE_FACTOR)
                .build();

        operation.updateScalingFactorForEntity(pod.build(), CPU_SCALE_FACTOR, new LongOpenHashSet());

        assertSelling(container, expectedVcpuComm, expectedVcpuRequestComm);
        assertBuying(container, expectedVcpuBoughtComm, expectedVcpuRequestBoughtComm);

        assertSelling(pod, expectedVcpuComm, expectedVcpuRequestComm);
        assertBuying(pod, expectedVcpuBoughtComm, expectedVcpuRequestBoughtComm);

        assertSelling(wc, expectedVcpuLimitQuotaComm, expectedVcpuRequestQuotaComm);
        assertBuying(wc, expectedLimitVcpuQuotaBoughtComm, expectedVcpuRequestQuotaBoughtComm);

        assertSelling(ns, expectedVcpuLimitQuotaComm, expectedVcpuRequestQuotaComm);
    }

    /**
     * Some entities like namespaces may be connected to multiple hosts.
     * In these cases, we would like the namespace to get the largest scalingFactor of all hosts
     * that it is connected to.
     */
    @Test
    public void testUpdateMaxScalingFactor() {
        final CommoditySoldDTO vcpuLimitQuotaComm = makeCommoditySold(CommodityType.VCPU_LIMIT_QUOTA,
            CPU_CAPACITY);
        final CommodityBoughtDTO vcpuLimitQuotaBoughtComm =
            makeCommodityBought(CommodityType.VCPU_LIMIT_QUOTA);

        final TopologyEntity.Builder wc1 = makeTopologyEntityBuilder(3L,
            EntityType.WORKLOAD_CONTROLLER_VALUE,
            Collections.singletonList(vcpuLimitQuotaComm),
            // Buying VCPUQuota & RequestQuota bought from namespace.
            Collections.singletonList(vcpuLimitQuotaBoughtComm));
        final TopologyEntity.Builder wc2 = makeTopologyEntityBuilder(4L,
            EntityType.WORKLOAD_CONTROLLER_VALUE,
            Collections.singletonList(vcpuLimitQuotaComm),
            // Buying VCPUQuota & RequestQuota bought from namespace.
            Collections.singletonList(vcpuLimitQuotaBoughtComm));
        final TopologyEntity.Builder wc3 = makeTopologyEntityBuilder(5L,
            EntityType.WORKLOAD_CONTROLLER_VALUE,
            Collections.singletonList(vcpuLimitQuotaComm),
            // Buying VCPUQuota & RequestQuota bought from namespace.
            Collections.singletonList(vcpuLimitQuotaBoughtComm));

        final TopologyEntity.Builder ns = makeTopologyEntityBuilder(4L,
            EntityType.NAMESPACE_VALUE,
            Collections.singletonList(vcpuLimitQuotaComm),
            Collections.emptyList());
        Stream.of(wc1, wc2, wc3).forEach(wc -> {
            ns.addConsumer(wc);
            wc.addProvider(ns);
        });

        final LongOpenHashSet visited = new LongOpenHashSet();
        operation.updateScalingFactorForEntity(wc1.build(), 2.0, visited);
        operation.updateScalingFactorForEntity(wc2.build(), 3.0, visited);
        operation.updateScalingFactorForEntity(wc3.build(), 1.0, visited);
        assertSelling(ns, vcpuLimitQuotaComm.toBuilder().setScalingFactor(3.0).build());
    }

    /**
     * Some entities like namespaces may be connected to multiple hosts.
     * In these cases, we would like the namespace to get the largest scalingFactor of all hosts
     * that it is connected to. With heterogeneous scaling disabled we should just take
     * the first update to the namespace.
     */
    @Test
    public void testUpdateMaxScalingFactorHeterogeneousScalingDisabled() {
        final CommoditySoldDTO vcpuLimitQuotaComm = makeCommoditySold(CommodityType.VCPU_LIMIT_QUOTA,
            CPU_CAPACITY);
        final CommodityBoughtDTO vcpuLimitQuotaBoughtComm =
            makeCommodityBought(CommodityType.VCPU_LIMIT_QUOTA);

        final TopologyEntity.Builder wc1 = makeTopologyEntityBuilder(3L,
            EntityType.WORKLOAD_CONTROLLER_VALUE,
            Collections.singletonList(vcpuLimitQuotaComm),
            // Buying VCPUQuota & RequestQuota bought from namespace.
            Collections.singletonList(vcpuLimitQuotaBoughtComm));
        final TopologyEntity.Builder wc2 = makeTopologyEntityBuilder(4L,
            EntityType.WORKLOAD_CONTROLLER_VALUE,
            Collections.singletonList(vcpuLimitQuotaComm),
            // Buying VCPUQuota & RequestQuota bought from namespace.
            Collections.singletonList(vcpuLimitQuotaBoughtComm));
        final TopologyEntity.Builder wc3 = makeTopologyEntityBuilder(5L,
            EntityType.WORKLOAD_CONTROLLER_VALUE,
            Collections.singletonList(vcpuLimitQuotaComm),
            // Buying VCPUQuota & RequestQuota bought from namespace.
            Collections.singletonList(vcpuLimitQuotaBoughtComm));

        final TopologyEntity.Builder ns = makeTopologyEntityBuilder(4L,
            EntityType.NAMESPACE_VALUE,
            Collections.singletonList(vcpuLimitQuotaComm),
            Collections.emptyList());
        Stream.of(wc1, wc2, wc3).forEach(wc -> {
            ns.addConsumer(wc);
            wc.addProvider(ns);
        });

        // Test with consistentScalingOnHeterogenousSuppliers disabled. We should still early
        // exit after the first update.
        final LongOpenHashSet visited = new LongOpenHashSet();
        final CpuScalingFactorPostStitchingOperation disabledOperation =
            new CpuScalingFactorPostStitchingOperation(cpuCapacityStore, false);
        visited.clear();
        disabledOperation.updateScalingFactorForEntity(wc1.build(), 2.0, visited);
        disabledOperation.updateScalingFactorForEntity(wc2.build(), 3.0, visited);
        disabledOperation.updateScalingFactorForEntity(wc3.build(), 1.0, visited);
        assertSelling(ns, vcpuLimitQuotaComm.toBuilder().setScalingFactor(2.0).build());
    }

    /**
     * Test updateScalingFactorForEntity for supported provider entity.
     */
    @Test
    public void testUpdateScalingFactorForProviderEntity() {
        final CommoditySoldDTO vcpuComm = makeCommoditySold(CommodityType.VCPU, CPU_CAPACITY);
        final CommoditySoldDTO vcpuLimitQuotaComm = makeCommoditySold(CommodityType.VCPU_LIMIT_QUOTA,
            CPU_CAPACITY);
        final CommodityBoughtDTO vcpuLimitQuotaBoughtComm = makeCommodityBought(CommodityType.VCPU_LIMIT_QUOTA);

        final TopologyEntity.Builder pod = makeTopologyEntityBuilder(2L,
            EntityType.CONTAINER_POD_VALUE,
            Collections.singletonList(vcpuComm),
            Collections.singletonList(vcpuBoughtCommodity));
        final TopologyEntity.Builder wc = makeTopologyEntityBuilder(3L,
            EntityType.WORKLOAD_CONTROLLER_VALUE,
            Collections.singletonList(vcpuLimitQuotaComm),
            Collections.singletonList(vcpuLimitQuotaBoughtComm));
        final TopologyEntity.Builder vm = makeTopologyEntityBuilder(4L,
            EntityType.VIRTUAL_MACHINE_VALUE,
            Collections.singletonList(vcpuComm),
            Collections.singletonList(vcpuBoughtCommodity));
        pod.addProvider(wc);
        pod.addProvider(vm);

        operation.updateScalingFactorForEntity(pod.build(), CPU_SCALE_FACTOR, new LongOpenHashSet());
        // WorkloadController scalingFactor is updated because it's a supported provider of ContainerPod.
        assertSelling(wc, vcpuLimitQuotaComm.toBuilder().setScalingFactor(CPU_SCALE_FACTOR).build());
        // VM scalingFactor is not updated because it's not a supported provider to update from ContainerPod.
        assertSelling(vm, vcpuComm);
    }

    private static void assertSelling(@Nonnull final TopologyEntity.Builder entity,
                                      @Nonnull final CommoditySoldDTO... expectedSold) {
        final List<CommoditySoldDTO> sold = entity.getEntityBuilder().getCommoditySoldListList();
        assertThat(sold, containsInAnyOrder(expectedSold));
    }

    private static void assertBuying(@Nonnull final TopologyEntity.Builder entity,
                                     @Nonnull final CommodityBoughtDTO... expectedBought) {
        final List<CommodityBoughtDTO> bought = entity.getEntityBuilder()
            .getCommoditiesBoughtFromProvidersList().get(0)
            .getCommodityBoughtList();
        assertThat(bought, containsInAnyOrder(expectedBought));
    }
}