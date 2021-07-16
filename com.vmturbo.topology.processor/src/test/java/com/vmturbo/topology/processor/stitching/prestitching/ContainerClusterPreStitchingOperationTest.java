package com.vmturbo.topology.processor.stitching.prestitching;

import static com.vmturbo.topology.processor.topology.TopologyEntityUtils.loadEntityDTOsFromSDKDiscoveryTextFile;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.Builder;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.PreStitchingOperation;
import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.stitching.prestitching.ContainerClusterPreStitchingOperation;
import com.vmturbo.stitching.utilities.CommoditiesBought;
import com.vmturbo.topology.processor.identity.IdentityProviderImpl;
import com.vmturbo.topology.processor.stitching.StitchingContext;
import com.vmturbo.topology.processor.stitching.StitchingEntityData;
import com.vmturbo.topology.processor.stitching.StitchingResultBuilder;
import com.vmturbo.topology.processor.stitching.journal.StitchingJournal;
import com.vmturbo.topology.processor.targets.TargetStore;

/**
 * Unit test for {@link ContainerClusterPreStitchingOperationTest}.
 */
public class ContainerClusterPreStitchingOperationTest {

    private static final double FLOATING_POINT_DELTA = 1e-7;
    private static final long TARGET_ID = 1L;
    private static final String CLUSTER = "Kubernetes-OKD-311";
    private static final String NAMESPACE_WITHOUT_QUOTA = "default";
    private static final String NAMESPACE_WITH_QUOTA = "nsquota";
    private static final String WORKLOAD_CONTROLLER = "cpu-quota";
    private static final String CONTAINER_SPEC = "heterogeneous-test-single-cpu-spec";
    private static final String VM = "pt-okd-node-2";
    private static final String CONTAINER_POD = "nsquota/cpu-quota-5c8fd84d55-hrbfq";
    private static final String CONTAINER = "default/sc-limited-55bd6f6fd8-nzpng/sc-1-limited";
    private static final String APPLICATION = "App-turbo/kubeturbo-cheuk-173.110-674c97bb5b-5xrnj/kubeturbo";
    private final AtomicLong oid = new AtomicLong(10);
    private StitchingContext stitchingContext;
    private StitchingResultBuilder stitchingResultBuilder;
    private final StitchingJournal<StitchingEntity> stitchingJournal = new StitchingJournal<>();
    private final PreStitchingOperation preStitchingOperation =
            new ContainerClusterPreStitchingOperation();

    /**
     * Setup.
     *
     * @throws IOException if failed
     */
    @Before
    public void setup() throws IOException {
        final List<EntityDTO.Builder> entityDTOBuilders =
                loadEntityDTOsFromSDKDiscoveryTextFile(
                        "container_cluster_prestitching_operation/"
                                + "Kubernetes_Kubernetes_OKD_311-2021.06.15.15.24.20.496-FULL.txt");

        TargetStore targetStore = Mockito.mock(TargetStore.class);
        Mockito.when(targetStore.getAll()).thenReturn(Collections.emptyList());

        final List<StitchingEntityData> stitchingEntityDataList = entityDTOBuilders.stream()
                .map(this::createStitchingEntityData)
                .collect(Collectors.toList());
        final Map<String, StitchingEntityData> targetIdMap =
                stitchingEntityDataList.stream()
                        .collect(Collectors.toMap(StitchingEntityData::getLocalId, Function.identity()));
        final StitchingContext.Builder stitchingContextBuilder =
                StitchingContext.newBuilder(entityDTOBuilders.size(), targetStore)
                        .setIdentityProvider(Mockito.mock(IdentityProviderImpl.class));
        stitchingEntityDataList.forEach(entityData -> stitchingContextBuilder
                .addEntity(entityData, targetIdMap));
        stitchingContext = stitchingContextBuilder.build();
        stitchingResultBuilder = new StitchingResultBuilder(stitchingContext);
    }

    /**
     * A helper function to create stitching entity data.
     *
     * @param builder entity builder
     * @return the stitching entity data
     */
    @Nonnull
    private StitchingEntityData createStitchingEntityData(@Nonnull final EntityDTO.Builder builder) {
        return StitchingEntityData.newBuilder(builder)
                .oid(oid.getAndIncrement())
                .targetId(TARGET_ID)
                .supportsConnectedTo(true)
                .build();
    }

    /**
     * Test convert container cluster.
     */
    @Test
    public void testConvertContainerCluster() {
        // Verify stitching context has been properly set up
        Assert.assertEquals(810, stitchingContext.size());
        // Remember of the original values that are NOT supposed to change:
        final double vcpuBoughtByAppComponentUsed =
                getCommodityBought(getStitchingEntity(EntityType.APPLICATION_COMPONENT, APPLICATION),
                                   CommodityType.VCPU).getUsed();
        final Builder vcpuSoldByVM = getCommoditySold(getStitchingEntity(EntityType.VIRTUAL_MACHINE, VM),
                                                      CommodityType.VCPU);
        final double vcpuSoldByVMUsed = vcpuSoldByVM.getUsed();
        final double vcpuSoldByVMCapacity = vcpuSoldByVM.getCapacity();
        // Perform the operation
        preStitchingOperation.performOperation(
                stitchingContext.internalEntities(EntityType.CONTAINER_PLATFORM_CLUSTER, TARGET_ID)
                        .map(Function.identity()),
                stitchingResultBuilder)
                .getChanges()
                .forEach(change -> change.applyChange(stitchingJournal));
        // Verify VM
        verifyVM(vcpuSoldByVMUsed, vcpuSoldByVMCapacity);
        // Verify Pod
        verifyContainerPod();
        // Verify Container
        verifyContainer();
        // Verify Application
        verifyApplicationComponent(vcpuBoughtByAppComponentUsed);
        // Verify workload controller
        verifyWorkloadController();
        // Verify container spec
        verifyContainerSpec();
        // Verify namespace without quota
        verifyNamespaceWithoutQuota();
        // Verify namespace with quota
        verifyNamespaceWithQuota();
        // Verify cluster entity
        verifyCluster();
    }

    private void verifyCluster() {
        final StitchingEntity cluster = getStitchingEntity(EntityType.CONTAINER_PLATFORM_CLUSTER, CLUSTER);
        final Set<StitchingEntity> vms = getClusterVMs();
        // VCPU should be in millicore
        final Builder vcpuCluster = getCommoditySold(cluster, CommodityType.VCPU);
        Assert.assertEquals(getTotalCapacity(vms, CommodityType.VCPU, true),
                            vcpuCluster.getCapacity(), FLOATING_POINT_DELTA);
        Assert.assertEquals(getTotalUsed(vms, CommodityType.VCPU, true),
                            vcpuCluster.getUsed(), FLOATING_POINT_DELTA);
        // VCPU_REQUEST should be in millicore
        final Builder vcpuRequestCluster = getCommoditySold(cluster, CommodityType.VCPU_REQUEST);
        Assert.assertEquals(getTotalCapacity(vms, CommodityType.VCPU_REQUEST, false),
                            vcpuRequestCluster.getCapacity(), FLOATING_POINT_DELTA);
        Assert.assertEquals(getTotalUsed(vms, CommodityType.VCPU_REQUEST, false),
                            vcpuRequestCluster.getUsed(), FLOATING_POINT_DELTA);
    }

    private void verifyNamespaceWithoutQuota() {
        final List<Builder> commodities = getNamespaceCommodities(NAMESPACE_WITHOUT_QUOTA);
        // VCPU_REQUEST_QUOTA sold
        Assert.assertEquals(1e12, commodities.get(0).getCapacity(), FLOATING_POINT_DELTA);
        Assert.assertEquals(4567, commodities.get(0).getUsed(), FLOATING_POINT_DELTA);
        // VCPU_LIMIT_QUOTA sold
        Assert.assertEquals(1e12, commodities.get(1).getCapacity(), FLOATING_POINT_DELTA);
        Assert.assertEquals(20402, commodities.get(1).getUsed(), FLOATING_POINT_DELTA);
        // VCPU_REQUEST bought
        Assert.assertEquals(4567, commodities.get(2).getUsed(), FLOATING_POINT_DELTA);
        // VCPU bought
        Assert.assertEquals(1269.4858978, commodities.get(3).getUsed(), FLOATING_POINT_DELTA);
    }

    private void verifyNamespaceWithQuota() {
        final List<Builder> commodities = getNamespaceCommodities(NAMESPACE_WITH_QUOTA);
        // VCPU_REQUEST_QUOTA sold
        Assert.assertEquals(500, commodities.get(0).getCapacity(), FLOATING_POINT_DELTA);
        Assert.assertEquals(500, commodities.get(0).getUsed(), FLOATING_POINT_DELTA);
        // VCPU_LIMIT_QUOTA sold
        Assert.assertEquals(1000, commodities.get(1).getCapacity(), FLOATING_POINT_DELTA);
        Assert.assertEquals(1000, commodities.get(1).getUsed(), FLOATING_POINT_DELTA);
        // VCPU_REQUEST bought
        Assert.assertEquals(500, commodities.get(2).getUsed(), FLOATING_POINT_DELTA);
        // VCPU bought
        Assert.assertEquals(139.072438899, commodities.get(3).getUsed(), FLOATING_POINT_DELTA);
    }

    @Nonnull
    private List<Builder> getNamespaceCommodities(@Nonnull final String displayName) {
        final StitchingEntity namespace = getStitchingEntity(EntityType.NAMESPACE, displayName);
        return ImmutableList.of(
                getCommoditySold(namespace, CommodityType.VCPU_REQUEST_QUOTA),
                getCommoditySold(namespace, CommodityType.VCPU_LIMIT_QUOTA),
                getCommodityBought(namespace, CommodityType.VCPU_REQUEST),
                getCommodityBought(namespace, CommodityType.VCPU));
    }

    private void verifyWorkloadController() {
        final List<Builder> commodities = getWorkloadControllerCommodities();
        // VCPU_REQUEST_QUOTA sold
        Assert.assertEquals(500, commodities.get(0).getCapacity(), FLOATING_POINT_DELTA);
        Assert.assertEquals(250, commodities.get(0).getUsed(), FLOATING_POINT_DELTA);
        // VCPU_LIMIT_QUOTA sold
        Assert.assertEquals(1000, commodities.get(1).getCapacity(), FLOATING_POINT_DELTA);
        Assert.assertEquals(500, commodities.get(1).getUsed(), FLOATING_POINT_DELTA);
        // VCPU_REQUEST_QUOTA bought
        Assert.assertEquals(250, commodities.get(2).getUsed(), FLOATING_POINT_DELTA);
        // VCPU_LIMIT_QUOTA bought
        Assert.assertEquals(500, commodities.get(3).getUsed(), FLOATING_POINT_DELTA);
    }

    @Nonnull
    private List<Builder> getWorkloadControllerCommodities() {
        final StitchingEntity controller = getStitchingEntity(EntityType.WORKLOAD_CONTROLLER, WORKLOAD_CONTROLLER);
        return ImmutableList.of(
                getCommoditySold(controller, CommodityType.VCPU_REQUEST_QUOTA),
                getCommoditySold(controller, CommodityType.VCPU_LIMIT_QUOTA),
                getCommodityBought(controller, CommodityType.VCPU_REQUEST_QUOTA),
                getCommodityBought(controller, CommodityType.VCPU_LIMIT_QUOTA));
    }

    private void verifyContainerSpec() {
        final StitchingEntity containerSpec = getStitchingEntity(EntityType.CONTAINER_SPEC, CONTAINER_SPEC);
        final Builder vcpu = getCommoditySold(containerSpec, CommodityType.VCPU);
        final Builder vcpuRequest = getCommoditySold(containerSpec, CommodityType.VCPU_REQUEST);
        Assert.assertEquals(1, vcpuRequest.getCapacity(), FLOATING_POINT_DELTA);
        Assert.assertEquals(99.133241, vcpuRequest.getUsed(), FLOATING_POINT_DELTA);
        Assert.assertEquals(100, vcpu.getCapacity(), FLOATING_POINT_DELTA);
        Assert.assertEquals(99.133241, vcpu.getUsed(), FLOATING_POINT_DELTA);
    }

    private void verifyVM(final double expectedUsed, final double expectedCapacity) {
        final StitchingEntity vm = getStitchingEntity(EntityType.VIRTUAL_MACHINE, VM);
        final Builder vcpu = getCommoditySold(vm, CommodityType.VCPU);
        final Builder vcpuRequest = getCommoditySold(vm, CommodityType.VCPU_REQUEST);
        Assert.assertEquals(4000, vcpuRequest.getCapacity(), FLOATING_POINT_DELTA);
        Assert.assertEquals(2063, vcpuRequest.getUsed(), FLOATING_POINT_DELTA);
        Assert.assertEquals(expectedCapacity, vcpu.getCapacity(), FLOATING_POINT_DELTA);
        Assert.assertEquals(expectedUsed, vcpu.getUsed(), FLOATING_POINT_DELTA);
    }

    private void verifyContainerPod() {
        final StitchingEntity pod = getStitchingEntity(EntityType.CONTAINER_POD, CONTAINER_POD);
        final Builder vcpuSold = getCommoditySold(pod, CommodityType.VCPU);
        final Builder vcpuRequestSold = getCommoditySold(pod, CommodityType.VCPU_REQUEST);
        final Builder vcpuLimitQuotaSold = getCommoditySold(pod, CommodityType.VCPU_LIMIT_QUOTA);
        final Builder vcpuRequestQuotaSold = getCommoditySold(pod, CommodityType.VCPU_REQUEST_QUOTA);
        final Builder vcpuBought = getCommodityBought(pod, CommodityType.VCPU);
        final Builder vcpuRequestBought = getCommodityBought(pod, CommodityType.VCPU_REQUEST);
        final Builder vcpuLimitQuotaBought = getCommodityBought(pod, CommodityType.VCPU_LIMIT_QUOTA);
        final Builder vcpuRequestQuotaBought = getCommodityBought(pod, CommodityType.VCPU_REQUEST_QUOTA);
        // VCPU sold
        Assert.assertEquals(5000, vcpuSold.getCapacity(), FLOATING_POINT_DELTA);
        Assert.assertEquals(12.5329605, vcpuSold.getUsed(), FLOATING_POINT_DELTA);
        // VCPU bought from vm
        Assert.assertEquals(12.5329605, vcpuBought.getUsed(), FLOATING_POINT_DELTA);
        // VCPURequest sold
        Assert.assertEquals(5000, vcpuRequestSold.getCapacity(), FLOATING_POINT_DELTA);
        Assert.assertEquals(50, vcpuRequestSold.getUsed(), FLOATING_POINT_DELTA);
        // VCPURequest bought from vm
        Assert.assertEquals(50, vcpuRequestBought.getUsed(), FLOATING_POINT_DELTA);
        // VCPULimitQuota sold
        Assert.assertEquals(1000, vcpuLimitQuotaSold.getCapacity(), FLOATING_POINT_DELTA);
        Assert.assertEquals(100, vcpuLimitQuotaSold.getUsed(), FLOATING_POINT_DELTA);
        // VCPULimitQuota bought from controller
        Assert.assertEquals(100, vcpuLimitQuotaBought.getUsed(), FLOATING_POINT_DELTA);
        // VCPURequestQuota sold
        Assert.assertEquals(500, vcpuRequestQuotaSold.getCapacity(), FLOATING_POINT_DELTA);
        Assert.assertEquals(50, vcpuRequestQuotaSold.getUsed(), FLOATING_POINT_DELTA);
        // VCPURequestQuota bought from controller
        Assert.assertEquals(50, vcpuRequestQuotaBought.getUsed(), FLOATING_POINT_DELTA);
        Assert.assertTrue(pod.getEntityBuilder().hasContainerPodData());
        Assert.assertTrue(pod.getEntityBuilder().getContainerPodData().hasHostingNodeCpuFrequency());
        Assert.assertEquals(1997.833, pod.getEntityBuilder().getContainerPodData()
                .getHostingNodeCpuFrequency(), FLOATING_POINT_DELTA);
    }

    private void verifyContainer() {
        final StitchingEntity container = getStitchingEntity(EntityType.CONTAINER, CONTAINER);
        final Builder vcpuSold = getCommoditySold(container, CommodityType.VCPU);
        final Builder vcpuRequestSold = getCommoditySold(container, CommodityType.VCPU_REQUEST);
        final Builder vcpuBought = getCommodityBought(container, CommodityType.VCPU);
        final Builder vcpuRequestBought = getCommodityBought(container, CommodityType.VCPU_REQUEST);
        final Builder vcpuLimitQuotaBought = getCommodityBought(container, CommodityType.VCPU_LIMIT_QUOTA);
        final Builder vcpuRequestQuotaBought = getCommodityBought(container, CommodityType.VCPU_REQUEST_QUOTA);
        // VCPU sold
        Assert.assertEquals(1400, vcpuSold.getCapacity(), FLOATING_POINT_DELTA);
        Assert.assertEquals(943.1628985, vcpuSold.getUsed(), FLOATING_POINT_DELTA);
        // VCPU bought from pod
        Assert.assertEquals(943.1628985, vcpuBought.getUsed(), FLOATING_POINT_DELTA);
        // VCPURequest sold
        Assert.assertEquals(100, vcpuRequestSold.getCapacity(), FLOATING_POINT_DELTA);
        Assert.assertEquals(943.1628985, vcpuRequestSold.getUsed(), FLOATING_POINT_DELTA);
        // VCPURequest bought from pod
        Assert.assertEquals(100, vcpuRequestBought.getUsed(), FLOATING_POINT_DELTA);
        // VCPULimitQuota bought from container spec
        Assert.assertEquals(1400, vcpuLimitQuotaBought.getUsed(), FLOATING_POINT_DELTA);
        // VCPURequestQuota bought from container spec
        Assert.assertEquals(100, vcpuRequestQuotaBought.getUsed(), FLOATING_POINT_DELTA);
    }

    private void verifyApplicationComponent(final double expectedUsed) {
        final StitchingEntity app = getStitchingEntity(EntityType.APPLICATION_COMPONENT, APPLICATION);
        final Builder vcpuBought = getCommodityBought(app, CommodityType.VCPU);
        // VCPU bought from container is still in MHz
        Assert.assertEquals(expectedUsed, vcpuBought.getUsed(), FLOATING_POINT_DELTA);
        Assert.assertTrue(app.getEntityBuilder().hasApplicationData());
        Assert.assertTrue(app.getEntityBuilder().getApplicationData().hasHostingNodeCpuFrequency());
        Assert.assertEquals(1997.833, app.getEntityBuilder().getApplicationData()
                .getHostingNodeCpuFrequency(), FLOATING_POINT_DELTA);
    }

    private StitchingEntity getStitchingEntity(@Nonnull final EntityType entityType,
                                               @Nonnull final String displayName) {
        final Optional<StitchingEntity> stitchingEntity = stitchingContext
                .getEntitiesByEntityTypeAndTarget()
                .getOrDefault(entityType, Collections.emptyMap())
                .values().stream()
                .flatMap(List::stream)
                .filter(entity -> entity.getDisplayName().equals(displayName))
                .findFirst()
                .map(Function.identity());
        Assert.assertTrue(stitchingEntity.isPresent());
        return stitchingEntity.get();
    }

    private Set<StitchingEntity> getClusterVMs() {
        return stitchingContext
                .getEntitiesByEntityTypeAndTarget()
                .getOrDefault(EntityType.VIRTUAL_MACHINE, Collections.emptyMap())
                .values().stream()
                .flatMap(List::stream)
                .collect(Collectors.toSet());
    }

    private double getTotalUsed(@Nonnull final Set<StitchingEntity> entities,
                                @Nonnull final CommodityType commodityType,
                                boolean convert) {
        return entities.stream()
                .mapToDouble(vm -> {
                    if (convert) {
                        return getCommoditySold(vm, commodityType).getUsed()
                                / getCommoditySold(vm, commodityType).getCapacity()
                                * vm.getEntityBuilder().getVirtualMachineData().getNumCpus()
                                * 1000;
                    } else {
                        return getCommoditySold(vm, commodityType).getUsed();
                    }
                })
                .filter(Double::isFinite)
                .sum();
    }

    private double getTotalCapacity(@Nonnull final Set<StitchingEntity> entities,
                                    @Nonnull final CommodityType commodityType,
                                    boolean convert) {
        return entities.stream()
                .mapToDouble(vm -> {
                    if (convert) {
                        return vm.getEntityBuilder().getVirtualMachineData().getNumCpus() * 1000;
                    } else {
                        return getCommoditySold(vm, commodityType).getCapacity();
                    }
                })
                .sum();
    }

    private Builder getCommoditySold(@Nonnull final StitchingEntity entity,
                                                  @Nonnull final CommodityType commodityType) {
        final Optional<Builder> commoditySold = entity.getCommoditiesSold()
                .filter(builder -> builder.getCommodityType().equals(commodityType))
                .findAny();
        Assert.assertTrue(commoditySold.isPresent());
        return commoditySold.get();
    }

    private Builder getCommodityBought(@Nonnull final StitchingEntity entity,
                                                    @Nonnull final CommodityType commodityType) {
        final Optional<Builder> commoditybought =
                entity.getCommodityBoughtListByProvider().values().stream()
                        .flatMap(List::stream)
                        .map(CommoditiesBought::getBoughtList)
                        .flatMap(List::stream)
                        .filter(builder -> commodityType.equals(builder.getCommodityType()))
                .findAny();
        Assert.assertTrue(commoditybought.isPresent());
        return commoditybought.get();
    }
}
