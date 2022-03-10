package com.vmturbo.stitching.poststitching;

import static com.vmturbo.stitching.poststitching.PostStitchingTestUtilities.makeCommodityBought;
import static com.vmturbo.stitching.poststitching.PostStitchingTestUtilities.makeCommoditySold;
import static com.vmturbo.stitching.poststitching.PostStitchingTestUtilities.makeTopologyEntityBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyDouble;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import it.unimi.dsi.fastutil.longs.LongOpenHashSet;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommodityBoughtView;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommoditySoldView;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl.CommoditiesBoughtFromProviderView;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TypeSpecificInfoImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TypeSpecificInfoImpl.PhysicalMachineInfoImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TypeSpecificInfoImpl.VirtualMachineInfoImpl;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.EntitySettingsCollection;
import com.vmturbo.stitching.TopologicalChangelog;
import com.vmturbo.stitching.TopologicalChangelog.EntityChangesBuilder;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.cpucapacity.CpuCapacityStore;
import com.vmturbo.stitching.journal.IStitchingJournal;
import com.vmturbo.stitching.poststitching.CpuScalingFactorPostStitchingOperation.CloudNativeVMCpuScalingFactorPostStitchingOperation;
import com.vmturbo.stitching.poststitching.CpuScalingFactorPostStitchingOperation.HostCpuScalingFactorPostStitchingOperation;
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
    private final HostCpuScalingFactorPostStitchingOperation hostOperation =
        spy(new HostCpuScalingFactorPostStitchingOperation(cpuCapacityStore, true));
    private final CloudNativeVMCpuScalingFactorPostStitchingOperation vmOperation =
        spy(new CloudNativeVMCpuScalingFactorPostStitchingOperation(true));
    private EntityChangesBuilder<TopologyEntity> resultBuilder;

    private final EntitySettingsCollection settingsMock = mock(EntitySettingsCollection.class);

    // Set up the CPU and CPU_PROVISIONED commodities for the test PM
    final CommoditySoldView cpuSoldCommodity = makeCommoditySold(CommodityType.CPU, CPU_CAPACITY);
    final CommoditySoldView cpuProvisionedSoldCommodity = makeCommoditySold(CommodityType.CPU_PROVISIONED,
            CPU_PROVISIONED_CAPACITY);
    final List<CommoditySoldView> pmCommoditiesSoldList = Lists.newArrayList(
            cpuSoldCommodity, cpuProvisionedSoldCommodity);
    private TopologyEntity.Builder pm1 = makeTopologyEntityBuilder(PM_OID,
            EntityType.PHYSICAL_MACHINE.getNumber(),
            pmCommoditiesSoldList,
            Collections.emptyList());

    // Set up the VCPU commodity for the test VM
    final CommodityBoughtView cpuBoughtCommodity = makeCommodityBought(CommodityType.CPU);
    final CommodityBoughtView cpuProvisionedBoughtCommodity = makeCommodityBought(
            CommodityType.CPU_PROVISIONED);
    final List<CommodityBoughtView> vmCommoditiesBoughtList = Lists.newArrayList(
            cpuBoughtCommodity, cpuProvisionedBoughtCommodity);
    final CommoditySoldView vcpuSoldCommodity = makeCommoditySold(CommodityType.VCPU, CPU_CAPACITY);
    final List<CommoditySoldView> vmCommoditiesSoldList = Lists.newArrayList(
            vcpuSoldCommodity);
    private TopologyEntity.Builder vm1 = makeTopologyEntityBuilder(VM_OID,
            EntityType.VIRTUAL_MACHINE.getNumber(),
            vmCommoditiesSoldList,
            vmCommoditiesBoughtList);

    // Set up the VCPU commodity for the test App
    final CommodityBoughtView vcpuBoughtCommodity = makeCommodityBought(CommodityType.VCPU);
    final List<CommodityBoughtView> appCommoditiesBoughtList = Lists.newArrayList(
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
        pm1.getTopologyEntityImpl().setTypeSpecificInfo(new TypeSpecificInfoImpl()
                .setPhysicalMachine(new PhysicalMachineInfoImpl()
                        .setCpuModel(TEST_CPU_MODEL)));

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
        final CommoditySoldView expectedCpuSoldCommodity = cpuSoldCommodity.copy()
                .setScalingFactor(CPU_SCALE_FACTOR);
        final CommoditySoldView expectedCpuProvisionedSoldCommodity = cpuProvisionedSoldCommodity
                .copy()
                .setScalingFactor(CPU_SCALE_FACTOR);
        final CommodityBoughtView expectedVmCpuCommodity = cpuBoughtCommodity.copy()
                .setScalingFactor(CPU_SCALE_FACTOR);
        final CommodityBoughtView expectedVmCpuProvisionedCommodity = cpuProvisionedBoughtCommodity
                .copy()
                .setScalingFactor(CPU_SCALE_FACTOR);
        final CommoditySoldView expectedvCpuSoldCommodity = vcpuSoldCommodity.copy()
                .setScalingFactor(CPU_SCALE_FACTOR);
        final CommodityBoughtView expectedvCpuBoughtCommodity = vcpuBoughtCommodity.copy()
                .setScalingFactor(CPU_SCALE_FACTOR);

        // Act
        final TopologicalChangelog<TopologyEntity> changeLog =
                hostOperation.performOperation(Stream.of(pm1.build(), vm1.build(), app1.build()),
                        settingsMock, resultBuilder);
        changeLog.getChanges().forEach(change -> change.applyChange(journal));
        // Assert
        final List<CommoditySoldView> soldPmCommodities = pm1.getTopologyEntityImpl()
                .getCommoditySoldListList();
        assertEquals(2, soldPmCommodities.size());
        assertThat(soldPmCommodities, containsInAnyOrder(expectedCpuSoldCommodity,
                expectedCpuProvisionedSoldCommodity));
        // check the related VM, as well
        final List<CommoditiesBoughtFromProviderView> boughtVmCommoditiesFrom = vm1.getTopologyEntityImpl()
                .getCommoditiesBoughtFromProvidersList();
        assertEquals(1, boughtVmCommoditiesFrom.size());
        final List<CommodityBoughtView> boughtVmCommodities = boughtVmCommoditiesFrom
                .iterator().next().getCommodityBoughtList();
        assertThat(boughtVmCommodities, containsInAnyOrder(expectedVmCpuCommodity,
                expectedVmCpuProvisionedCommodity));

        // App-VM relationship Assertion.
        final List<CommoditySoldView> soldVmCommodities = vm1.getTopologyEntityImpl()
                .getCommoditySoldListList();
        assertEquals(1, soldVmCommodities.size());
        assertThat(soldVmCommodities, containsInAnyOrder(expectedvCpuSoldCommodity));
        final List<CommoditiesBoughtFromProviderView> boughtAppCommoditiesFrom = app1.getTopologyEntityImpl()
                .getCommoditiesBoughtFromProvidersList();
        assertEquals(1, boughtAppCommoditiesFrom.size());
        final List<CommodityBoughtView> boughtAppCommodities = boughtAppCommoditiesFrom
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
        final CommoditySoldView vcpuComm = makeCommoditySold(CommodityType.VCPU, CPU_CAPACITY);

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

        final CommoditySoldView expectedContainerVcpuComm = vcpuComm.copy()
            .setScalingFactor(CPU_SCALE_FACTOR);
        final CommoditySoldView expectedPodVcpuComm = vcpuComm.copy();

        hostOperation.updateScalingFactorForEntity(container.build(), CPU_SCALE_FACTOR, new LongOpenHashSet());

        List<CommoditySoldView> containerCommSoldList = container.getTopologyEntityImpl().getCommoditySoldListList();
        assertEquals(1, containerCommSoldList.size());
        assertThat(containerCommSoldList, containsInAnyOrder(expectedContainerVcpuComm));

        // Here pod VCPU comm doesn't have CPU_SCALE_FACTOR propagate from container
        List<CommoditySoldView> podCommSoldList = pod.getTopologyEntityImpl().getCommoditySoldListList();
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
        final CommoditySoldView vcpuComm = makeCommoditySold(CommodityType.VCPU, CPU_CAPACITY);
        final CommoditySoldView vcpuLimitQuotaComm = makeCommoditySold(CommodityType.VCPU_LIMIT_QUOTA,
            CPU_CAPACITY);
        final CommoditySoldView vcpuRequestComm = makeCommoditySold(CommodityType.VCPU_REQUEST, CPU_CAPACITY);
        final CommoditySoldView vcpuRequestQuotaComm = makeCommoditySold(CommodityType.VCPU_REQUEST_QUOTA,
            CPU_CAPACITY);

        final CommodityBoughtView vcpuBoughtComm = vcpuBoughtCommodity;
        final CommodityBoughtView vcpuRequestBoughtComm = makeCommodityBought(CommodityType.VCPU_REQUEST);
        final CommodityBoughtView vcpuLimitQuotaBoughtComm = makeCommodityBought(CommodityType.VCPU_LIMIT_QUOTA);
        final CommodityBoughtView vcpuRequestQuotaBoughtComm = makeCommodityBought(CommodityType.VCPU_REQUEST_QUOTA);

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

        final CommoditySoldView expectedVcpuComm = vcpuComm.copy()
            .setScalingFactor(CPU_SCALE_FACTOR);
        final CommoditySoldView expectedVcpuLimitQuotaComm = vcpuLimitQuotaComm.copy()
            .setScalingFactor(CPU_SCALE_FACTOR);
        final CommoditySoldView expectedVcpuRequestComm = vcpuRequestComm.copy()
            .setScalingFactor(CPU_SCALE_FACTOR);
        final CommoditySoldView expectedVcpuRequestQuotaComm = vcpuRequestQuotaComm.copy()
            .setScalingFactor(CPU_SCALE_FACTOR);

        final CommodityBoughtView expectedVcpuBoughtComm = vcpuBoughtComm.copy()
                .setScalingFactor(CPU_SCALE_FACTOR);
        final CommodityBoughtView expectedLimitVcpuQuotaBoughtComm = vcpuLimitQuotaBoughtComm.copy()
                .setScalingFactor(CPU_SCALE_FACTOR);
        final CommodityBoughtView expectedVcpuRequestBoughtComm = vcpuRequestBoughtComm.copy()
                .setScalingFactor(CPU_SCALE_FACTOR);
        final CommodityBoughtView expectedVcpuRequestQuotaBoughtComm = vcpuRequestQuotaBoughtComm.copy()
                .setScalingFactor(CPU_SCALE_FACTOR);

        hostOperation.updateScalingFactorForEntity(pod.build(), CPU_SCALE_FACTOR, new LongOpenHashSet());

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
     * <p/>
     * Workload controllers should have the max of the hosts that they are connected to through their
     * pods, not necessarily the max of all the hosts connected to the namespace.
     */
    @Test
    public void testUpdateMaxScalingFactor() {
        final CommoditySoldView vcpuLimitQuotaComm = makeCommoditySold(CommodityType.VCPU_LIMIT_QUOTA,
            CPU_CAPACITY);
        final CommoditySoldView vcpuComm = makeCommoditySold(CommodityType.VCPU, CPU_CAPACITY);
        final CommodityBoughtView vcpuLimitQuotaBoughtComm =
            makeCommodityBought(CommodityType.VCPU_LIMIT_QUOTA);

        final TopologyEntity.Builder pod1 = makeTopologyEntityBuilder(10L,
            EntityType.CONTAINER_POD_VALUE,
            Collections.singletonList(vcpuComm),
            Collections.singletonList(vcpuBoughtCommodity));
        final TopologyEntity.Builder wc1 = makeTopologyEntityBuilder(3L,
            EntityType.WORKLOAD_CONTROLLER_VALUE,
            Collections.singletonList(vcpuLimitQuotaComm),
            // Buying VCPUQuota & RequestQuota bought from namespace.
            Collections.singletonList(vcpuLimitQuotaBoughtComm));

        final TopologyEntity.Builder pod2 = makeTopologyEntityBuilder(11L,
            EntityType.CONTAINER_POD_VALUE,
            Collections.singletonList(vcpuComm),
            Collections.singletonList(vcpuBoughtCommodity));
        final TopologyEntity.Builder wc2 = makeTopologyEntityBuilder(4L,
            EntityType.WORKLOAD_CONTROLLER_VALUE,
            Collections.singletonList(vcpuLimitQuotaComm),
            // Buying VCPUQuota & RequestQuota bought from namespace.
            Collections.singletonList(vcpuLimitQuotaBoughtComm));

        final TopologyEntity.Builder pod3 = makeTopologyEntityBuilder(12L,
            EntityType.CONTAINER_POD_VALUE,
            Collections.singletonList(vcpuComm),
            Collections.singletonList(vcpuBoughtCommodity));
        final TopologyEntity.Builder wc3 = makeTopologyEntityBuilder(5L,
            EntityType.WORKLOAD_CONTROLLER_VALUE,
            Collections.singletonList(vcpuLimitQuotaComm),
            // Buying VCPUQuota & RequestQuota bought from namespace.
            Collections.singletonList(vcpuLimitQuotaBoughtComm));

        final TopologyEntity.Builder ns = makeTopologyEntityBuilder(6L,
            EntityType.NAMESPACE_VALUE,
            Collections.singletonList(vcpuLimitQuotaComm),
            Collections.emptyList());
        Stream.of(wc1, wc2, wc3).forEach(wc -> {
            ns.addConsumer(wc);
            wc.addProvider(ns);
        });
        ImmutableMap.of(pod1, wc1, pod2, wc2, pod3, wc3).forEach((pod, wc) -> {
            wc.addConsumer(pod);
            pod.addProvider(wc);
        });

        final LongOpenHashSet visited = new LongOpenHashSet();
        hostOperation.updateScalingFactorForEntity(pod1.build(), 2.0, visited);
        hostOperation.updateScalingFactorForEntity(pod2.build(), 3.0, visited);
        hostOperation.updateScalingFactorForEntity(pod3.build(), 1.0, visited);
        assertSelling(ns, vcpuLimitQuotaComm.copy().setScalingFactor(3.0));

        // Verify that we do not propagate multiple updates back to the pods or workload
        // controllers. Doing so can cause a big performance hit in large topologies
        // with lots of container entities.
        Stream.of(pod1, wc1, pod2, wc2, pod3, wc3).forEach(entity -> {
            verify(hostOperation, times(1))
                .updateScalingFactorForEntity(eq(entity.build()), anyDouble(), any());
        });
    }

    /**
     * Some entities like namespaces may be connected to multiple hosts.
     * In these cases, we would like the namespace to get the largest scalingFactor of all hosts
     * that it is connected to. With heterogeneous scaling disabled we should just take
     * the first update to the namespace.
     */
    @Test
    public void testUpdateMaxScalingFactorHeterogeneousScalingDisabled() {
        final CommoditySoldView vcpuLimitQuotaComm = makeCommoditySold(CommodityType.VCPU_LIMIT_QUOTA,
            CPU_CAPACITY);
        final CommodityBoughtView vcpuLimitQuotaBoughtComm =
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
            new HostCpuScalingFactorPostStitchingOperation(cpuCapacityStore, false);
        visited.clear();
        disabledOperation.updateScalingFactorForEntity(wc1.build(), 2.0, visited);
        disabledOperation.updateScalingFactorForEntity(wc2.build(), 3.0, visited);
        disabledOperation.updateScalingFactorForEntity(wc3.build(), 1.0, visited);
        assertSelling(ns, vcpuLimitQuotaComm.copy().setScalingFactor(2.0));
    }

    /**
     * Test updateScalingFactorForEntity for supported provider entity.
     */
    @Test
    public void testUpdateScalingFactorForProviderEntity() {
        final CommoditySoldView vcpuComm = makeCommoditySold(CommodityType.VCPU, CPU_CAPACITY);
        final CommoditySoldView vcpuLimitQuotaComm = makeCommoditySold(CommodityType.VCPU_LIMIT_QUOTA,
            CPU_CAPACITY);
        final CommodityBoughtView vcpuLimitQuotaBoughtComm = makeCommodityBought(CommodityType.VCPU_LIMIT_QUOTA);

        final TopologyEntity.Builder pod = makeTopologyEntityBuilder(2L,
            EntityType.CONTAINER_POD_VALUE,
            Collections.singletonList(vcpuComm.copy()),
            Collections.singletonList(vcpuBoughtCommodity.copy()));
        final TopologyEntity.Builder wc = makeTopologyEntityBuilder(3L,
            EntityType.WORKLOAD_CONTROLLER_VALUE,
            Collections.singletonList(vcpuLimitQuotaComm),
            Collections.singletonList(vcpuLimitQuotaBoughtComm));
        final TopologyEntity.Builder vm = makeTopologyEntityBuilder(4L,
            EntityType.VIRTUAL_MACHINE_VALUE,
            Collections.singletonList(vcpuComm),
            Collections.singletonList(vcpuBoughtCommodity.copy()));
        pod.addProvider(wc);
        pod.addProvider(vm);

        hostOperation.updateScalingFactorForEntity(pod.build(), CPU_SCALE_FACTOR, new LongOpenHashSet());
        // WorkloadController scalingFactor is updated because it's a supported provider of ContainerPod.
        assertSelling(wc, vcpuLimitQuotaComm.copy().setScalingFactor(CPU_SCALE_FACTOR));
        // VM scalingFactor is not updated because it's not a supported provider to update from ContainerPod.
        assertSelling(vm, vcpuComm);
    }

    /**
     * Test {@link CloudNativeVMCpuScalingFactorPostStitchingOperation#performOperation} with only
     * on-prem VM.
     */
    @Test
    public void testCloudNativeVMPerformOperationWithOnPremOnly() {
        TopologyEntity vm = TopologyEntity.newBuilder(new TopologyEntityImpl()
            .setEnvironmentType(EnvironmentType.ON_PREM))
            .build();
        final TopologicalChangelog<TopologyEntity> changeLog =
            vmOperation.performOperation(Stream.of(vm),settingsMock, resultBuilder);
        // No changes needs to be applied in changeLog.
        assertEquals(0, changeLog.getChanges().size());
    }

    /**
     * Test {@link CloudNativeVMCpuScalingFactorPostStitchingOperation#performOperation} with cloud
     * and hybrid VMs.
     */
    @Test
    public void testCloudNativeVMPerformOperationWithCloudAndHybrid() {
        TopologyEntity vm1 = TopologyEntity.newBuilder(new TopologyEntityImpl()
            .setEnvironmentType(EnvironmentType.CLOUD))
            .build();
        TopologyEntity vm2 = TopologyEntity.newBuilder(new TopologyEntityImpl()
            .setEnvironmentType(EnvironmentType.HYBRID))
            .build();
        final TopologicalChangelog<TopologyEntity> changeLog =
            vmOperation.performOperation(Stream.of(vm1, vm2),settingsMock, resultBuilder);
        // The changes of 2 VMs are included in changeLog.
        assertEquals(2, changeLog.getChanges().size());
    }

    /**
     * Test {@link CpuScalingFactorPostStitchingOperation#updateScalingFactorForEntity} for cloud
     * native entities.
     */
    @Test
    public void testUpdateScalingFactorForCloudNativeEntities() {
        final CommoditySoldView vCPUSold = makeCommoditySold(CommodityType.VCPU, 4000);
        final CommoditySoldView vCPURequestSold = makeCommoditySold(CommodityType.VCPU_REQUEST, 4000);
        final CommodityBoughtView vCPUBought = makeCommodityBought(CommodityType.VCPU);

        TopologyEntity.Builder containerCluster = makeTopologyEntityBuilder(1L,
            EntityType.CONTAINER_PLATFORM_CLUSTER_VALUE,
            Lists.newArrayList(vCPUSold.copy(), vCPURequestSold.copy()),
            Collections.emptyList());

        TopologyEntity.Builder vm = makeTopologyEntityBuilder(2L,
            EntityType.VIRTUAL_MACHINE_VALUE,
            Lists.newArrayList(vCPUSold.copy(), vCPURequestSold.copy()),
            Lists.newArrayList(vCPUBought.copy()));
        vm.addAggregator(containerCluster);
        vm.getTopologyEntityImpl().setTypeSpecificInfo(new TypeSpecificInfoImpl()
            .setVirtualMachine(new VirtualMachineInfoImpl()
                .setNumCpus(2)));

        // VM has 2 consumers in different entity types.
        // ContainerPod will have millicoreScalingFactor updated,
        // while AppComponent won't have any scaling factor updated because it's not cloud native entity.
        //
        // VM --> ContainerPod
        //  |
        //  --->  ApplicationComponent
        final CommodityBoughtView vCPURequestBought = makeCommodityBought(CommodityType.VCPU);
        TopologyEntity.Builder containerPod = makeTopologyEntityBuilder(3L,
            EntityType.CONTAINER_POD_VALUE,
            Collections.emptyList(),
            Lists.newArrayList(vCPUBought.copy(), vCPURequestBought.copy()));
        vm.addConsumer(containerPod);

        TopologyEntity.Builder appComponent = makeTopologyEntityBuilder(3L,
            EntityType.APPLICATION_COMPONENT_VALUE,
            Collections.emptyList(),
            Lists.newArrayList(vCPUBought.copy()));
        vm.addConsumer(appComponent);

        vmOperation.updateScalingFactorForEntity(vm.build(), CPU_SCALE_FACTOR, new LongOpenHashSet());
        // VM VCPU commodity bought has CPU_SCALE_FACTOR updated
        assertBuying(vm, vCPUBought.copy().setScalingFactor(CPU_SCALE_FACTOR));
        // VM VCPU commodity sold has CPU_SCALE_FACTOR updated
        // VM VCPURequest commodity sold has millicore scalingFactor updated, which is
        // cpuSpeed (in MHz/millicore: 4000 / 2 / 1000 = 2) * CPU_SCALE_FACTOR
        assertSelling(vm, vCPUSold.copy().setScalingFactor(CPU_SCALE_FACTOR),
            vCPURequestSold.copy().setScalingFactor(2 * CPU_SCALE_FACTOR));
        // ContainerPod VCPU and VCPURequest commodities have millicore scalingFactor updated, which is
        // cpuSpeed (in MHz/millicore: 4000 / 2 / 1000 = 2) * CPU_SCALE_FACTOR
        assertBuying(containerPod, vCPUBought.copy().setScalingFactor(2 * CPU_SCALE_FACTOR),
            vCPURequestBought.copy().setScalingFactor(2 * CPU_SCALE_FACTOR));
        // AppComponent commodity won't have scalingFactor updated because it's not cloud native entity.
        assertBuying(appComponent, vCPUBought);
    }

    /**
     * Test {@link CpuScalingFactorPostStitchingOperation#updateScalingFactorForEntity} for cloud
     * entity which is not cloud native entity.
     */
    @Test
    public void testUpdateScalingFactorForNonCloudNativeCloudEntity() {
        final CommoditySoldView vCPUSold = makeCommoditySold(CommodityType.VCPU, CPU_CAPACITY);
        TopologyEntity.Builder vm = makeTopologyEntityBuilder(1L,
            EntityType.VIRTUAL_MACHINE_VALUE,
            Lists.newArrayList(vCPUSold),
            Collections.emptyList());
        vm.getTopologyEntityImpl().setEnvironmentType(EnvironmentType.CLOUD);

        final CommodityBoughtView vCPUBought = makeCommodityBought(CommodityType.VCPU);
        TopologyEntity.Builder app = makeTopologyEntityBuilder(2L,
            EntityType.APPLICATION_COMPONENT_VALUE,
            Collections.emptyList(),
            Lists.newArrayList(vCPUBought));
        app.getTopologyEntityImpl().setEnvironmentType(EnvironmentType.CLOUD);
        vm.addConsumer(app);
        vmOperation.updateScalingFactorForEntity(vm.build(), CPU_SCALE_FACTOR, new LongOpenHashSet());
        // Commodity of cloud AppComponent commodity doesn't have scalingFactor set up.
        assertBuying(app, vCPUBought.copy());
    }

    private static void assertSelling(@Nonnull final TopologyEntity.Builder entity,
                                      @Nonnull final CommoditySoldView... expectedSold) {
        final List<CommoditySoldView> sold = entity.getTopologyEntityImpl().getCommoditySoldListList();
        assertThat(sold, containsInAnyOrder(expectedSold));
    }

    private static void assertBuying(@Nonnull final TopologyEntity.Builder entity,
                                     @Nonnull final CommodityBoughtView... expectedBought) {
        final List<CommodityBoughtView> bought = entity.getTopologyEntityImpl()
            .getCommoditiesBoughtFromProvidersList().get(0)
            .getCommodityBoughtList();
        assertThat(bought, containsInAnyOrder(expectedBought));
    }
}