package com.vmturbo.topology.processor.topology;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;

import org.apache.commons.collections4.ListUtils;
import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PerTargetEntityInformation;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.DiscoveryOrigin;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Origin;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.PlanScenarioOrigin;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ReservationOrigin;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.TopologyEntity.Builder;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetStore;
import com.vmturbo.topology.processor.topology.EnvironmentTypeInjector.InjectionSummary;

/**
 * Unit tests for {@link EnvironmentTypeInjector}.
 */
public class EnvironmentTypeInjectorTest {

    private static final long AWS_TARGET_ID = 1L;
    private static final long VC_TARGET_ID = 2L;
    private static final long K8S_TARGET_ID = 3L;
    private static final long WMI_TARGET_ID = 4L;
    private static final long APPDYNAMICS_TARGET_ID = 5L;
    private static final long APP_COMPONENT_OID = 6L;
    private static final long VM_OID = 7L;
    private static final long CONTAINER_OID = 8L;
    private static final long WORKLOAD_CONTROLLER_OID = 9L;
    private static final long NAMESPACE_OID = 10L;
    private static final long CONTAINER_SPEC_OID = 11L;
    private static final long CONTAINER_POD_OID = 12L;
    private static final long SERVICE_OID = 13L;
    private static final long CONTAINER_PLATFORM_CLUSTER_OID = 14L;
    private static final long VM2_OID = 15L;
    private static final long VV_OID = 16L;

    private TargetStore targetStore = mock(TargetStore.class);

    private EnvironmentTypeInjector environmentTypeInjector =
        new EnvironmentTypeInjector(targetStore);

    @Before
    public void setup() {
        addFakeTarget(AWS_TARGET_ID, SDKProbeType.AWS, ProbeCategory.CLOUD_MANAGEMENT);
        addFakeTarget(VC_TARGET_ID, SDKProbeType.VCENTER, ProbeCategory.HYPERVISOR);
        //The probe type of k8s is not static, it's a generated value with kubernetes as prefix
        //The only static part is the probe category, and we should use the category as criteria
        addFakeTarget(K8S_TARGET_ID, SDKProbeType.VCD, ProbeCategory.CLOUD_NATIVE);
        addFakeTarget(WMI_TARGET_ID, SDKProbeType.WMI, ProbeCategory.GUEST_OS_PROCESSES);
        addFakeTarget(APPDYNAMICS_TARGET_ID, SDKProbeType.APPDYNAMICS, ProbeCategory.APPLICATIONS_AND_DATABASES);
    }

    @Test
    public void testDiscoveredCloudEntity() {
        final TopologyGraph<TopologyEntity> graph = oneEntityGraph(builder -> builder.setOrigin(Origin.newBuilder()
            .setDiscoveryOrigin(DiscoveryOrigin.newBuilder()
                .putDiscoveredTargetData(AWS_TARGET_ID,
                    PerTargetEntityInformation.getDefaultInstance()))));

        final InjectionSummary injectionSummary = environmentTypeInjector.injectEnvironmentType(graph);


        assertTrue(graph.getEntity(VM_OID).isPresent());
        assertThat(graph.getEntity(VM_OID).get().getEnvironmentType(), is(EnvironmentType.CLOUD));

        assertThat(injectionSummary.getConflictingTypeCount(), is(0));
        assertThat(injectionSummary.getUnknownCount(), is(0));
        assertThat(injectionSummary.getEnvTypeCounts(), is(ImmutableMap.of(EnvironmentType.CLOUD, 1)));
    }

    @Test
    public void testDiscoveredOnPremEntity() {
        final TopologyGraph<TopologyEntity> graph = oneEntityGraph(builder -> builder.setOrigin(Origin.newBuilder()
            .setDiscoveryOrigin(DiscoveryOrigin.newBuilder()
                .putDiscoveredTargetData(VC_TARGET_ID, PerTargetEntityInformation.getDefaultInstance()))));

        final InjectionSummary injectionSummary = environmentTypeInjector.injectEnvironmentType(graph);

        assertTrue(graph.getEntity(VM_OID).isPresent());
        assertThat(graph.getEntity(VM_OID).get().getEnvironmentType(), is(EnvironmentType.ON_PREM));

        assertThat(injectionSummary.getConflictingTypeCount(), is(0));
        assertThat(injectionSummary.getUnknownCount(), is(0));
        assertThat(injectionSummary.getEnvTypeCounts(), is(ImmutableMap.of(EnvironmentType.ON_PREM, 1)));
    }

    @Test
    public void testDiscoveredStitchToCloudEntity() {
        Map<Long, Builder> topologyEntitiesMap = new HashMap<>();
        TopologyEntity.Builder vm1 = TopologyEntity
            .newBuilder(TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .setOid(VM_OID)
                .setOrigin(Origin.newBuilder().setDiscoveryOrigin(DiscoveryOrigin
                    .newBuilder().putDiscoveredTargetData(AWS_TARGET_ID,
                        PerTargetEntityInformation.getDefaultInstance()))));
        TopologyEntity.Builder container1 = TopologyEntity.newBuilder(
                newEntityBuilder(K8S_TARGET_ID, vm1.getOid())
                        .setEntityType(EntityType.CONTAINER_VALUE)
                        .setOid(CONTAINER_OID));
        topologyEntitiesMap.put(vm1.getOid(), vm1);
        topologyEntitiesMap.put(container1.getOid(), container1);
        TopologyGraph<TopologyEntity> graph = TopologyEntityTopologyGraphCreator.newGraph(topologyEntitiesMap);
        final InjectionSummary injectionSummary = environmentTypeInjector.injectEnvironmentType(graph);

        assertTrue(graph.getEntity(VM_OID).isPresent());
        assertTrue(graph.getEntity(CONTAINER_OID).isPresent());

        assertThat(graph.getEntity(VM_OID).get().getEnvironmentType(), is(EnvironmentType.CLOUD));
        assertThat(graph.getEntity(CONTAINER_OID).get().getEnvironmentType(), is(EnvironmentType.CLOUD));

        assertThat(injectionSummary.getConflictingTypeCount(), is(0));
        assertThat(injectionSummary.getUnknownCount(), is(0));
        assertThat(injectionSummary.getEnvTypeCounts(), is(ImmutableMap.of(EnvironmentType.CLOUD, 2)));
    }

    @Test
    public void testDiscoveredStitchToOnPremEntity() {
        Map<Long, TopologyEntity.Builder> topologyEntitiesMap = new HashMap<>();
        TopologyEntity.Builder vm1 = TopologyEntity
            .newBuilder(TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .setOid(VM_OID)
                .setOrigin(Origin.newBuilder().setDiscoveryOrigin(DiscoveryOrigin
                    .newBuilder().putDiscoveredTargetData(VC_TARGET_ID,
                        PerTargetEntityInformation.getDefaultInstance()))));
        TopologyEntity.Builder container1 = TopologyEntity.newBuilder(
                newEntityBuilder(K8S_TARGET_ID, vm1.getOid())
                        .setEntityType(EntityType.CONTAINER_VALUE)
                        .setOid(CONTAINER_OID));
        topologyEntitiesMap.put(vm1.getOid(), vm1);
        topologyEntitiesMap.put(container1.getOid(), container1);
        TopologyGraph<TopologyEntity> graph = TopologyEntityTopologyGraphCreator.newGraph(topologyEntitiesMap);
        final InjectionSummary injectionSummary = environmentTypeInjector.injectEnvironmentType(graph);

        assertTrue(graph.getEntity(VM_OID).isPresent());
        assertTrue(graph.getEntity(CONTAINER_OID).isPresent());

        assertThat(graph.getEntity(VM_OID).get().getEnvironmentType(), is(EnvironmentType.ON_PREM));
        assertThat(graph.getEntity(CONTAINER_OID).get().getEnvironmentType(), is(EnvironmentType.ON_PREM));

        assertThat(injectionSummary.getConflictingTypeCount(), is(0));
        assertThat(injectionSummary.getUnknownCount(), is(0));
        assertThat(injectionSummary.getEnvTypeCounts(), is(ImmutableMap.of(EnvironmentType.ON_PREM, 2)));
    }

    /**
     * Test traversing consumes relationships. The graph here looks like:
     * <p/>
     *        Provides          Consumes                   Consumes
     * AWS_VM <-- k8s_ContainerPod --> k8s_WorkloadController --> k8s_Namespace
     */
    @Test
    public void testWorkloadControllerTraversal() {
        Map<Long, TopologyEntity.Builder> topologyEntitiesMap = new HashMap<>();
        TopologyEntity.Builder vm1 = TopologyEntity
            .newBuilder(TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .setOid(VM_OID)
                .setOrigin(Origin.newBuilder().setDiscoveryOrigin(DiscoveryOrigin
                    .newBuilder().putDiscoveredTargetData(AWS_TARGET_ID,
                        PerTargetEntityInformation.getDefaultInstance()))));
        TopologyEntity.Builder workloadController = TopologyEntity.newBuilder(
                newEntityBuilder(K8S_TARGET_ID)
                        .setEntityType(EntityType.WORKLOAD_CONTROLLER_VALUE)
                        .setOid(WORKLOAD_CONTROLLER_OID));
        TopologyEntity.Builder pod1 = TopologyEntity.newBuilder(
                newEntityBuilder(K8S_TARGET_ID, vm1.getOid(), workloadController.getOid())
                        .setEntityType(EntityType.CONTAINER_POD_VALUE)
                        .setOid(CONTAINER_POD_OID));

        topologyEntitiesMap.put(vm1.getOid(), vm1);
        topologyEntitiesMap.put(pod1.getOid(), pod1);
        topologyEntitiesMap.put(workloadController.getOid(), workloadController);
        TopologyGraph<TopologyEntity> graph = TopologyEntityTopologyGraphCreator.newGraph(topologyEntitiesMap);
        final InjectionSummary injectionSummary = environmentTypeInjector.injectEnvironmentType(graph);

        assertTrue(graph.getEntity(VM_OID).isPresent());
        assertTrue(graph.getEntity(CONTAINER_POD_OID).isPresent());
        assertTrue(graph.getEntity(WORKLOAD_CONTROLLER_OID).isPresent());

        assertThat(graph.getEntity(VM_OID).get().getEnvironmentType(), is(EnvironmentType.CLOUD));
        assertThat(graph.getEntity(CONTAINER_POD_OID).get().getEnvironmentType(), is(EnvironmentType.CLOUD));
        assertThat(graph.getEntity(WORKLOAD_CONTROLLER_OID).get().getEnvironmentType(), is(EnvironmentType.CLOUD));

        assertThat(injectionSummary.getConflictingTypeCount(), is(0));
        assertThat(injectionSummary.getUnknownCount(), is(0));
        assertThat(injectionSummary.getEnvTypeCounts(), is(ImmutableMap.of(EnvironmentType.CLOUD, 3)));
    }


    /**
     * Test traversing aggregates relationships. The graph here looks like:
     * <p/>
     *        Provides          Provides        AggregatedBy
     * AWS_VM <-- k8s_ContainerPod <-- k8s_Container --> k8s_ContainerSpec
     */
    @Test
    public void testContainerSpecTraversal() {
        Map<Long, TopologyEntity.Builder> topologyEntitiesMap = new HashMap<>();
        TopologyEntity.Builder vm1 = TopologyEntity
            .newBuilder(TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .setOid(VM_OID)
                .setOrigin(Origin.newBuilder().setDiscoveryOrigin(DiscoveryOrigin.newBuilder()
                        .putDiscoveredTargetData(AWS_TARGET_ID,
                                PerTargetEntityInformation.getDefaultInstance())
                        .putDiscoveredTargetData(K8S_TARGET_ID,
                                PerTargetEntityInformation.getDefaultInstance()))));
        TopologyEntity.Builder containerSpec = TopologyEntity.newBuilder(newEntityBuilder(K8S_TARGET_ID)
            .setEntityType(EntityType.CONTAINER_SPEC_VALUE)
            .setOid(CONTAINER_SPEC_OID));
        TopologyEntity.Builder pod = TopologyEntity.newBuilder(
                newEntityBuilder(K8S_TARGET_ID, vm1.getOid())
                        .setEntityType(EntityType.CONTAINER_POD_VALUE)
                        .setOid(CONTAINER_POD_OID));
        TopologyEntity.Builder container = TopologyEntity.newBuilder(
                newEntityBuilder(K8S_TARGET_ID, pod.getOid())
                        .setEntityType(EntityType.CONTAINER_VALUE)
                        .setOid(CONTAINER_OID)
                        .addConnectedEntityList(TopologyEntityDTO.ConnectedEntity.newBuilder()
                                .setConnectionType(ConnectionType.AGGREGATED_BY_CONNECTION)
                                .setConnectedEntityId(containerSpec.getOid())
                                .setConnectedEntityType(EntityType.VIRTUAL_MACHINE_VALUE)));

        topologyEntitiesMap.put(vm1.getOid(), vm1);
        topologyEntitiesMap.put(container.getOid(), container);
        topologyEntitiesMap.put(containerSpec.getOid(), containerSpec);
        topologyEntitiesMap.put(pod.getOid(), pod);
        TopologyGraph<TopologyEntity> graph = TopologyEntityTopologyGraphCreator.newGraph(topologyEntitiesMap);
        final InjectionSummary injectionSummary = environmentTypeInjector.injectEnvironmentType(graph);

        assertTrue(graph.getEntity(VM_OID).isPresent());
        assertTrue(graph.getEntity(CONTAINER_OID).isPresent());
        assertTrue(graph.getEntity(CONTAINER_POD_OID).isPresent());
        assertTrue(graph.getEntity(CONTAINER_SPEC_OID).isPresent());

        assertThat(graph.getEntity(VM_OID).get().getEnvironmentType(), is(EnvironmentType.CLOUD));
        assertThat(graph.getEntity(CONTAINER_OID).get().getEnvironmentType(), is(EnvironmentType.CLOUD));
        assertThat(graph.getEntity(CONTAINER_POD_OID).get().getEnvironmentType(), is(EnvironmentType.CLOUD));
        assertThat(graph.getEntity(CONTAINER_SPEC_OID).get().getEnvironmentType(), is(EnvironmentType.CLOUD));

        assertThat(injectionSummary.getConflictingTypeCount(), is(0));
        assertThat(injectionSummary.getUnknownCount(), is(0));
        assertThat(injectionSummary.getEnvTypeCounts(), is(ImmutableMap.of(EnvironmentType.CLOUD, 4)));
    }

    /**
     * Test traversing consumes relationships. The graph here looks like:
     * <p/>
     *        Provides          Consumes
     * AWS_VM <-- k8s_ContainerPod --> k8s_Volume
     */
    @Test
    public void testVirtualVolumeTraversal() {
        Map<Long, TopologyEntity.Builder> topologyEntitiesMap = new HashMap<>();
        TopologyEntity.Builder vm1 = TopologyEntity
                .newBuilder(TopologyEntityDTO.newBuilder()
                        .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                        .setOid(VM_OID)
                        .setOrigin(Origin.newBuilder().setDiscoveryOrigin(DiscoveryOrigin.newBuilder()
                                .putDiscoveredTargetData(AWS_TARGET_ID,
                                        PerTargetEntityInformation.getDefaultInstance())
                                .putDiscoveredTargetData(K8S_TARGET_ID,
                                        PerTargetEntityInformation.getDefaultInstance()))));
        TopologyEntity.Builder vv = TopologyEntity.newBuilder(newEntityBuilder(K8S_TARGET_ID)
                .setEntityType(EntityType.VIRTUAL_VOLUME_VALUE)
                .setOid(VV_OID));
        TopologyEntity.Builder pod = TopologyEntity.newBuilder(
                newEntityBuilder(K8S_TARGET_ID, vm1.getOid(), vv.getOid())
                        .setEntityType(EntityType.CONTAINER_POD_VALUE)
                        .setOid(CONTAINER_POD_OID));


        topologyEntitiesMap.put(vm1.getOid(), vm1);
        topologyEntitiesMap.put(vv.getOid(), vv);
        topologyEntitiesMap.put(pod.getOid(), pod);
        TopologyGraph<TopologyEntity> graph = TopologyEntityTopologyGraphCreator.newGraph(topologyEntitiesMap);
        final InjectionSummary injectionSummary = environmentTypeInjector.injectEnvironmentType(graph);

        assertTrue(graph.getEntity(VM_OID).isPresent());
        assertTrue(graph.getEntity(CONTAINER_POD_OID).isPresent());

        assertThat(graph.getEntity(VM_OID).get().getEnvironmentType(), is(EnvironmentType.CLOUD));
        assertThat(graph.getEntity(CONTAINER_POD_OID).get().getEnvironmentType(), is(EnvironmentType.CLOUD));
        assertThat(graph.getEntity(VV_OID).get().getEnvironmentType(), is(EnvironmentType.CLOUD));

        assertThat(injectionSummary.getConflictingTypeCount(), is(0));
        assertThat(injectionSummary.getUnknownCount(), is(0));
        assertThat(injectionSummary.getEnvTypeCounts(), is(ImmutableMap.of(EnvironmentType.CLOUD, 3)));
    }

    /**
     * Test traversal for Namespace and ContainerPlatformCluster entities on on-prem infrastructure.
     * The graph here looks like:
     * <p/>
     *    AggregatedBy                AggregatedBy
     * VC_VM <-- ContainerPlatformCluster --> K8s_Namespace
     */
    @Test
    public void testOnPremNamespaceContainerClusterTraversal() {
        Map<Long, TopologyEntity.Builder> topologyEntitiesMap = new HashMap<>();
        TopologyEntity.Builder containerCluster = TopologyEntity
            .newBuilder(newEntityBuilder(K8S_TARGET_ID)
                .setEntityType(EntityType.CONTAINER_PLATFORM_CLUSTER_VALUE)
                .setOid(CONTAINER_PLATFORM_CLUSTER_OID));
        TopologyEntity.Builder vm = TopologyEntity
            .newBuilder(TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .setOid(VM_OID)
                .addConnectedEntityList(TopologyEntityDTO.ConnectedEntity.newBuilder()
                    .setConnectionType(ConnectionType.AGGREGATED_BY_CONNECTION)
                    .setConnectedEntityId(CONTAINER_PLATFORM_CLUSTER_OID)
                    .setConnectedEntityType(EntityType.CONTAINER_PLATFORM_CLUSTER_VALUE))
                .setOrigin(Origin.newBuilder().setDiscoveryOrigin(DiscoveryOrigin.newBuilder()
                    .putDiscoveredTargetData(VC_TARGET_ID,
                        PerTargetEntityInformation.getDefaultInstance())
                    .putDiscoveredTargetData(K8S_TARGET_ID,
                        PerTargetEntityInformation.getDefaultInstance()))));
        TopologyEntity.Builder namespace = TopologyEntity
            .newBuilder(newEntityBuilder(K8S_TARGET_ID)
                .setEntityType(EntityType.NAMESPACE_VALUE)
                .setOid(NAMESPACE_OID)
                .addConnectedEntityList(TopologyEntityDTO.ConnectedEntity.newBuilder()
                    .setConnectionType(ConnectionType.AGGREGATED_BY_CONNECTION)
                    .setConnectedEntityId(CONTAINER_PLATFORM_CLUSTER_OID)
                    .setConnectedEntityType(EntityType.CONTAINER_PLATFORM_CLUSTER_VALUE)));

        topologyEntitiesMap.put(vm.getOid(), vm);
        topologyEntitiesMap.put(containerCluster.getOid(), containerCluster);
        topologyEntitiesMap.put(namespace.getOid(), namespace);
        TopologyGraph<TopologyEntity> graph = TopologyEntityTopologyGraphCreator.newGraph(topologyEntitiesMap);
        final InjectionSummary injectionSummary = environmentTypeInjector.injectEnvironmentType(graph);

        assertTrue(graph.getEntity(VM_OID).isPresent());
        assertTrue(graph.getEntity(CONTAINER_PLATFORM_CLUSTER_OID).isPresent());
        assertTrue(graph.getEntity(NAMESPACE_OID).isPresent());

        assertThat(graph.getEntity(VM_OID).get().getEnvironmentType(), is(EnvironmentType.ON_PREM));
        assertThat(graph.getEntity(CONTAINER_PLATFORM_CLUSTER_OID).get().getEnvironmentType(), is(EnvironmentType.ON_PREM));
        assertThat(graph.getEntity(NAMESPACE_OID).get().getEnvironmentType(), is(EnvironmentType.ON_PREM));

        assertThat(injectionSummary.getConflictingTypeCount(), is(0));
        assertThat(injectionSummary.getUnknownCount(), is(0));
        assertThat(injectionSummary.getEnvTypeCounts(), is(ImmutableMap.of(EnvironmentType.ON_PREM, 3)));
    }

    /**
     * Test traversal for Namespace and ContainerPlatformCluster entities on hybrid infrastructure.
     * The graph here looks like:
     * <p/>
     *    AggregatedBy                AggregatedBy
     * VC_VM <-- ContainerPlatformCluster --> K8s_Namespace
     *                   |
     * AWS_VM <----------  AggregatedBy
     */
    @Test
    public void testHybridNamespaceContainerClusterTraversal() {
        Map<Long, TopologyEntity.Builder> topologyEntitiesMap = new HashMap<>();
        TopologyEntity.Builder containerCluster = TopologyEntity
            .newBuilder(newEntityBuilder(K8S_TARGET_ID)
                .setEntityType(EntityType.CONTAINER_PLATFORM_CLUSTER_VALUE)
                .setOid(CONTAINER_PLATFORM_CLUSTER_OID));
        TopologyEntity.Builder vm1 = TopologyEntity
            .newBuilder(TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .setOid(VM_OID)
                .addConnectedEntityList(TopologyEntityDTO.ConnectedEntity.newBuilder()
                    .setConnectionType(ConnectionType.AGGREGATED_BY_CONNECTION)
                    .setConnectedEntityId(CONTAINER_PLATFORM_CLUSTER_OID)
                    .setConnectedEntityType(EntityType.CONTAINER_PLATFORM_CLUSTER_VALUE))
                .setOrigin(Origin.newBuilder().setDiscoveryOrigin(DiscoveryOrigin.newBuilder()
                    .putDiscoveredTargetData(VC_TARGET_ID,
                        PerTargetEntityInformation.getDefaultInstance())
                    .putDiscoveredTargetData(K8S_TARGET_ID,
                        PerTargetEntityInformation.getDefaultInstance()))));
        TopologyEntity.Builder vm2 = TopologyEntity
            .newBuilder(TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .setOid(VM2_OID)
                .addConnectedEntityList(TopologyEntityDTO.ConnectedEntity.newBuilder()
                    .setConnectionType(ConnectionType.AGGREGATED_BY_CONNECTION)
                    .setConnectedEntityId(CONTAINER_PLATFORM_CLUSTER_OID)
                    .setConnectedEntityType(EntityType.CONTAINER_PLATFORM_CLUSTER_VALUE))
                .setOrigin(Origin.newBuilder().setDiscoveryOrigin(DiscoveryOrigin.newBuilder()
                    .putDiscoveredTargetData(AWS_TARGET_ID,
                        PerTargetEntityInformation.getDefaultInstance())
                    .putDiscoveredTargetData(K8S_TARGET_ID,
                        PerTargetEntityInformation.getDefaultInstance()))));
        TopologyEntity.Builder namespace = TopologyEntity
            .newBuilder(newEntityBuilder(K8S_TARGET_ID)
                .setEntityType(EntityType.NAMESPACE_VALUE)
                .setOid(NAMESPACE_OID)
                .addConnectedEntityList(TopologyEntityDTO.ConnectedEntity.newBuilder()
                    .setConnectionType(ConnectionType.AGGREGATED_BY_CONNECTION)
                    .setConnectedEntityId(CONTAINER_PLATFORM_CLUSTER_OID)
                    .setConnectedEntityType(EntityType.CONTAINER_PLATFORM_CLUSTER_VALUE)));

        topologyEntitiesMap.put(vm1.getOid(), vm1);
        topologyEntitiesMap.put(vm2.getOid(), vm2);
        topologyEntitiesMap.put(containerCluster.getOid(), containerCluster);
        topologyEntitiesMap.put(namespace.getOid(), namespace);
        TopologyGraph<TopologyEntity> graph = TopologyEntityTopologyGraphCreator.newGraph(topologyEntitiesMap);
        final InjectionSummary injectionSummary = environmentTypeInjector.injectEnvironmentType(graph);

        assertTrue(graph.getEntity(VM_OID).isPresent());
        assertTrue(graph.getEntity(VM2_OID).isPresent());
        assertTrue(graph.getEntity(CONTAINER_PLATFORM_CLUSTER_OID).isPresent());
        assertTrue(graph.getEntity(NAMESPACE_OID).isPresent());

        assertThat(graph.getEntity(VM_OID).get().getEnvironmentType(), is(EnvironmentType.ON_PREM));
        assertThat(graph.getEntity(VM2_OID).get().getEnvironmentType(), is(EnvironmentType.CLOUD));
        assertThat(graph.getEntity(CONTAINER_PLATFORM_CLUSTER_OID).get().getEnvironmentType(), is(EnvironmentType.HYBRID));
        assertThat(graph.getEntity(NAMESPACE_OID).get().getEnvironmentType(), is(EnvironmentType.HYBRID));

        assertThat(injectionSummary.getConflictingTypeCount(), is(0));
        assertThat(injectionSummary.getUnknownCount(), is(0));
        assertThat(injectionSummary.getEnvTypeCounts(),
            is(ImmutableMap.of(EnvironmentType.ON_PREM, 1, EnvironmentType.CLOUD, 1, EnvironmentType.HYBRID, 2)));
    }

    /**
     * Environment type for stitched APM entities with CLOUD should be CLOUD.
     */
    @Test
    public void testAPMStitched() {
        Map<Long, TopologyEntity.Builder> topologyEntitiesMap = new HashMap<>();
        TopologyEntity.Builder vm = TopologyEntity
                .newBuilder(TopologyEntityDTO.newBuilder()
                        .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                        .setOid(VM_OID)
                        .setOrigin(Origin.newBuilder().setDiscoveryOrigin(DiscoveryOrigin.newBuilder()
                                .putDiscoveredTargetData(AWS_TARGET_ID,
                                        PerTargetEntityInformation.getDefaultInstance()))));
        TopologyEntity.Builder appComponent = TopologyEntity.newBuilder(
                newEntityBuilder(APPDYNAMICS_TARGET_ID, vm.getOid())
                        .setEntityType(EntityType.APPLICATION_COMPONENT_VALUE)
                        .setOid(APP_COMPONENT_OID));

        topologyEntitiesMap.put(vm.getOid(), vm);
        topologyEntitiesMap.put(appComponent.getOid(), appComponent);

        TopologyGraph<TopologyEntity> graph = TopologyEntityTopologyGraphCreator.newGraph(topologyEntitiesMap);
        final InjectionSummary injectionSummary = environmentTypeInjector.injectEnvironmentType(graph);

        assertTrue(graph.getEntity(VM_OID).isPresent());
        assertTrue(graph.getEntity(APP_COMPONENT_OID).isPresent());

        assertThat(graph.getEntity(VM_OID).get().getEnvironmentType(), is(EnvironmentType.CLOUD));
        assertThat(graph.getEntity(APP_COMPONENT_OID).get().getEnvironmentType(), is(EnvironmentType.CLOUD));

        assertThat(injectionSummary.getConflictingTypeCount(), is(0));
        assertThat(injectionSummary.getUnknownCount(), is(0));
        assertThat(injectionSummary.getEnvTypeCounts(), is(ImmutableMap.of(EnvironmentType.CLOUD, 2)));
    }

    /**
     * Environment type for unstitched APM entities should be HYBRID.
     */
    @Test
    public void testAPMUnstitched() {
        Map<Long, TopologyEntity.Builder> topologyEntitiesMap = new HashMap<>();
        TopologyEntity.Builder appComponent = TopologyEntity.newBuilder(
                newEntityBuilder(APPDYNAMICS_TARGET_ID)
                        .setEntityType(EntityType.APPLICATION_COMPONENT_VALUE)
                        .setOid(APP_COMPONENT_OID));
        TopologyEntity.Builder service = TopologyEntity.newBuilder(
                newEntityBuilder(APPDYNAMICS_TARGET_ID, appComponent.getOid())
                        .setEntityType(EntityType.SERVICE_VALUE)
                        .setOid(SERVICE_OID));

        topologyEntitiesMap.put(appComponent.getOid(), appComponent);
        topologyEntitiesMap.put(service.getOid(), service);

        TopologyGraph<TopologyEntity> graph = TopologyEntityTopologyGraphCreator.newGraph(topologyEntitiesMap);
        final InjectionSummary injectionSummary = environmentTypeInjector.injectEnvironmentType(graph);

        assertTrue(graph.getEntity(APP_COMPONENT_OID).isPresent());
        assertTrue(graph.getEntity(SERVICE_OID).isPresent());

        assertThat(graph.getEntity(APP_COMPONENT_OID).get().getEnvironmentType(), is(EnvironmentType.HYBRID));
        assertThat(graph.getEntity(SERVICE_OID).get().getEnvironmentType(), is(EnvironmentType.HYBRID));

        assertThat(injectionSummary.getConflictingTypeCount(), is(0));
        assertThat(injectionSummary.getUnknownCount(), is(0));
        assertThat(injectionSummary.getEnvTypeCounts(), is(ImmutableMap.of(EnvironmentType.HYBRID, 2)));
    }

    /**
     * Setup two entities that consume from each other in a cycle.
     * Ensure that we do not end up in an infinite recursion to compute the environment type.
     * Instead, the cycle should be detected and we just return the default environment type.
     */
    @Test
    public void testCycleIsAborted() {
        Map<Long, TopologyEntity.Builder> topologyEntitiesMap = new HashMap<>();
        TopologyEntity.Builder pod = TopologyEntity.newBuilder(newEntityBuilder(K8S_TARGET_ID, CONTAINER_OID)
            .setEntityType(EntityType.CONTAINER_POD_VALUE)
            .setOid(CONTAINER_POD_OID));
        TopologyEntity.Builder container = TopologyEntity.newBuilder(
                newEntityBuilder(K8S_TARGET_ID, CONTAINER_POD_OID)
                        .setEntityType(EntityType.CONTAINER_VALUE)
                        .setOid(CONTAINER_OID));

        topologyEntitiesMap.put(container.getOid(), container);
        topologyEntitiesMap.put(pod.getOid(), pod);
        TopologyGraph<TopologyEntity> graph = TopologyEntityTopologyGraphCreator.newGraph(topologyEntitiesMap);
        final InjectionSummary injectionSummary = environmentTypeInjector.injectEnvironmentType(graph);

        assertTrue(graph.getEntity(CONTAINER_OID).isPresent());
        assertTrue(graph.getEntity(CONTAINER_POD_OID).isPresent());

        assertThat(graph.getEntity(CONTAINER_OID).get().getEnvironmentType(), is(EnvironmentType.HYBRID));
        assertThat(graph.getEntity(CONTAINER_POD_OID).get().getEnvironmentType(), is(EnvironmentType.HYBRID));

        assertThat(injectionSummary.getConflictingTypeCount(), is(0));
        assertThat(injectionSummary.getUnknownCount(), is(0));
        assertThat(injectionSummary.getEnvTypeCounts(), is(ImmutableMap.of(EnvironmentType.HYBRID, 2)));
    }

    @Test
    public void testPlanEntity() {
        final TopologyGraph<TopologyEntity> graph = oneEntityGraph(builder -> builder.setOrigin(Origin.newBuilder()
            .setPlanScenarioOrigin(PlanScenarioOrigin.newBuilder()
                .setPlanId(1111))));

        final InjectionSummary injectionSummary = environmentTypeInjector.injectEnvironmentType(graph);

        assertTrue(graph.getEntity(VM_OID).isPresent());
        assertThat(graph.getEntity(VM_OID).get().getEnvironmentType(), is(EnvironmentType.ON_PREM));

        assertThat(injectionSummary.getConflictingTypeCount(), is(0));
        assertThat(injectionSummary.getUnknownCount(), is(0));
        assertThat(injectionSummary.getEnvTypeCounts(), is(ImmutableMap.of(EnvironmentType.ON_PREM, 1)));
    }

    @Test
    public void testReservationEntity() {
        final TopologyGraph<TopologyEntity> graph = oneEntityGraph(builder -> builder.setOrigin(Origin.newBuilder()
            .setReservationOrigin(ReservationOrigin.newBuilder()
                .setReservationId(112))));

        final InjectionSummary injectionSummary = environmentTypeInjector.injectEnvironmentType(graph);

        assertTrue(graph.getEntity(VM_OID).isPresent());
        assertThat(graph.getEntity(VM_OID).get().getEnvironmentType(), is(EnvironmentType.ON_PREM));

        assertThat(injectionSummary.getConflictingTypeCount(), is(0));
        assertThat(injectionSummary.getUnknownCount(), is(0));
        assertThat(injectionSummary.getEnvTypeCounts(), is(ImmutableMap.of(EnvironmentType.ON_PREM, 1)));
    }

    @Test
    public void testUnsetOriginEntity() {
        final TopologyGraph<TopologyEntity> graph = oneEntityGraph(builder -> {});

        final InjectionSummary injectionSummary = environmentTypeInjector.injectEnvironmentType(graph);

        assertTrue(graph.getEntity(VM_OID).isPresent());
        assertThat(graph.getEntity(VM_OID).get().getEnvironmentType(), is(EnvironmentType.UNKNOWN_ENV));

        assertThat(injectionSummary.getConflictingTypeCount(), is(0));
        // One unknown count.
        assertThat(injectionSummary.getUnknownCount(), is(1));
        assertThat(injectionSummary.getEnvTypeCounts(), is(Collections.emptyMap()));
    }

    @Test
    public void testOverrideSetUnknownEnvType() {
        final TopologyGraph<TopologyEntity> graph = oneEntityGraph(builder -> {
            builder.setEnvironmentType(EnvironmentType.UNKNOWN_ENV);
            builder.setOrigin(Origin.newBuilder()
                .setDiscoveryOrigin(DiscoveryOrigin.newBuilder()
                    .putDiscoveredTargetData(AWS_TARGET_ID,
                        PerTargetEntityInformation.getDefaultInstance())));
        });

        final InjectionSummary injectionSummary = environmentTypeInjector.injectEnvironmentType(graph);

        assertTrue(graph.getEntity(VM_OID).isPresent());
        assertThat(graph.getEntity(VM_OID).get().getEnvironmentType(), is(EnvironmentType.CLOUD));

        assertThat(injectionSummary.getConflictingTypeCount(), is(0));
        // One unknown count.
        assertThat(injectionSummary.getUnknownCount(), is(0));
        assertThat(injectionSummary.getEnvTypeCounts(), is(ImmutableMap.of(EnvironmentType.CLOUD, 1)));
    }

    @Test
    public void testNoOverrideSetEnvType() {
        final long targetId = 1;
        final TopologyGraph<TopologyEntity> graph = oneEntityGraph(builder -> {
            builder.setEnvironmentType(EnvironmentType.ON_PREM);
            builder.setOrigin(Origin.newBuilder()
                .setDiscoveryOrigin(DiscoveryOrigin.newBuilder()
                    .putDiscoveredTargetData(targetId,
                        PerTargetEntityInformation.getDefaultInstance())));
        });

        final InjectionSummary injectionSummary = environmentTypeInjector.injectEnvironmentType(graph);

        assertTrue(graph.getEntity(VM_OID).isPresent());
        // The original environment type.
        assertThat(graph.getEntity(VM_OID).get().getEnvironmentType(), is(EnvironmentType.ON_PREM));

        assertThat(injectionSummary.getConflictingTypeCount(), is(1));
        // One unknown count.
        assertThat(injectionSummary.getUnknownCount(), is(0));
        assertThat(injectionSummary.getEnvTypeCounts(), is(Collections.emptyMap()));
    }

    @Nonnull
    private static TopologyEntityDTO.Builder newEntityBuilder(long targetId, long... providerIds) {
        return TopologyEntityDTO.newBuilder()
            .addAllCommoditiesBoughtFromProviders(Arrays.stream(providerIds)
                    .mapToObj(id -> TopologyEntityDTO.CommoditiesBoughtFromProvider
                        .newBuilder().setProviderId(id).build())
                    .collect(Collectors.toList()))
            .setOrigin(Origin.newBuilder().setDiscoveryOrigin(DiscoveryOrigin
                .newBuilder().putDiscoveredTargetData(targetId,
                    PerTargetEntityInformation.getDefaultInstance())));
    }

    @Nonnull
    private TopologyGraph<TopologyEntity> oneEntityGraph(final Consumer<TopologyEntityDTO.Builder> entityCustomizer) {
        final TopologyEntityDTO.Builder entityBuilder = TopologyEntityDTO.newBuilder()
            .setOid(VM_OID)
            .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE);
        entityCustomizer.accept(entityBuilder);
        final TopologyEntity.Builder entity = TopologyEntity.newBuilder(entityBuilder);
        return TopologyEntityTopologyGraphCreator.newGraph(ImmutableMap.of(VM_OID, entity));
    }

    private void addFakeTarget(final long targetId, final SDKProbeType probeType, final ProbeCategory probeCategory) {
        List<Target> curFakeTargets = ListUtils.emptyIfNull(targetStore.getAll());
        final Target newFakeTarget = mock(Target.class);
        when(newFakeTarget.getId()).thenReturn(targetId);
        when(targetStore.getProbeTypeForTarget(targetId)).thenReturn(Optional.of(probeType));
        when(targetStore.getProbeCategoryForTarget(targetId)).thenReturn(Optional.of(probeCategory));
        when(targetStore.getAll()).thenReturn(
            ListUtils.union(Collections.singletonList(newFakeTarget), curFakeTargets));
    }
}
