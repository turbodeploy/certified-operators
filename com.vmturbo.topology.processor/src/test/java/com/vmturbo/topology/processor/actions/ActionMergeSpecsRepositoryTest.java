package com.vmturbo.topology.processor.actions;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.junit.Assert;

import com.vmturbo.common.protobuf.action.ActionMergeSpecDTO.AtomicActionSpec;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PerTargetEntityInformation;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.AnalysisSettings;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.DiscoveryOrigin;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Origin;
import com.vmturbo.platform.common.dto.ActionExecution.ActionMergeExecutionTarget;
import com.vmturbo.platform.common.dto.ActionExecution.ActionMergePolicyDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionMergeTargetData;
import com.vmturbo.platform.common.dto.ActionExecution.ActionMergeTargetData.EntityRelationship;
import com.vmturbo.platform.common.dto.ActionExecution.ChainedActionMergeTargetData;
import com.vmturbo.platform.common.dto.ActionExecution.ChainedActionMergeTargetData.TargetDataLink;
import com.vmturbo.platform.common.dto.ActionExecution.ResizeMergeSpec;
import com.vmturbo.platform.common.dto.ActionExecution.ResizeMergeSpec.CommodityMergeData;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.Discovery.AccountDefEntry;
import com.vmturbo.platform.common.dto.Discovery.CustomAccountDefEntry;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.TopologyEntity.Builder;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.processor.actions.ActionMergeSpecsRepository.ActionMergeSpecsBuilder;
import com.vmturbo.topology.processor.actions.ActionMergeSpecsRepository.ActionMergeSpecsBuilder.ActionExecutionTarget;
import com.vmturbo.topology.processor.topology.TopologyEntityTopologyGraphCreator;
import com.vmturbo.topology.processor.topology.TopologyEntityUtils;

/**
 * Unit test to test creation of {@link AtomicActionSpec} for entities belonging to probes
 * that sent the {@link ActionMergePolicyDTO}.
 */
public class ActionMergeSpecsRepositoryTest {

    private static final long KUBERNETES_TARGET_ID = 1L;
    private static final long TERRAFORM_TARGET_ID = 2L;
    private static final long KUBERNETES_PROBE_ID = 1L;
    private static final long TERRAFORM_PROBE_ID = 2L;

    private static ActionMergePolicyDTO kubeTurboMergePolicy;
    private static ActionMergePolicyDTO terraformMergePolicy;

    private static TopologyGraph<TopologyEntity> graph;
    private static TopologyGraph<TopologyEntity> terraformGraph;

    /**
     * Create a test {@link ActionMergePolicyDTO} for the kubernetes probe environment.
     *
     * @return ActionMergePolicyDTO
     */
    private static ActionMergePolicyDTO createKubernetesPolicyDTO() {
        // Target which is determined using set of connections
        ChainedActionMergeTargetData mergeTargetData
                = ChainedActionMergeTargetData.newBuilder()
                .addTargetLinks(TargetDataLink.newBuilder()
                        .setMergeTarget(ActionMergeTargetData.newBuilder()
                                .setRelatedTo(EntityType.CONTAINER_SPEC)
                                .setRelatedBy(EntityRelationship.newBuilder()
                                        .setConnectionType(CommonDTO.ConnectedEntity.ConnectionType.AGGREGATED_BY_CONNECTION))
                        )
                        .setDeDuplicate(true)
                )
                .addTargetLinks(TargetDataLink.newBuilder()
                        .setMergeTarget(ActionMergeTargetData.newBuilder()
                                .setRelatedTo(EntityType.WORKLOAD_CONTROLLER)
                                .setRelatedBy(EntityRelationship.newBuilder()
                                        .setConnectionType(CommonDTO.ConnectedEntity.ConnectionType.CONTROLLED_BY_CONNECTION))
                        )
                )
                .build();

        kubeTurboMergePolicy = ActionMergePolicyDTO.newBuilder()
                .setEntityType(EntityType.CONTAINER)
                .addExecutionTargets(ActionMergeExecutionTarget.newBuilder()
                        .setChainedMergeTarget(mergeTargetData).build())
                .setResizeSpec(ResizeMergeSpec.newBuilder()
                        .addCommodityData(CommodityMergeData.newBuilder()
                                .setCommodityType(CommodityDTO.CommodityType.VMEM))
                        .addCommodityData(CommodityMergeData.newBuilder()
                                .setCommodityType(CommodityDTO.CommodityType.VCPU)))
                .build();

        return kubeTurboMergePolicy;
    }

    /**
     * Create a test {@link ActionMergePolicyDTO} for the Terraform probe environment.
     *
     * @return ActionMergePolicyDTO
     */
    private static ActionMergePolicyDTO createTerraformPolicyDTO() {
        ActionMergeTargetData vmToVmSpecData = ActionMergeTargetData.newBuilder()
                .setRelatedTo(EntityType.VM_SPEC)
                .setRelatedBy(EntityRelationship.newBuilder()
                        .setConnectionType(CommonDTO.ConnectedEntity.ConnectionType.AGGREGATED_BY_CONNECTION))
                .build();

        ActionMergeTargetData vmToControllerData = ActionMergeTargetData.newBuilder()
                .setRelatedTo(EntityType.WORKLOAD_CONTROLLER)
                .setRelatedBy(EntityRelationship.newBuilder()
                        .setConnectionType(CommonDTO.ConnectedEntity.ConnectionType.CONTROLLED_BY_CONNECTION))
                .build();

        ChainedActionMergeTargetData vmToVmSpecToControllerData
                = ChainedActionMergeTargetData.newBuilder()
                .addTargetLinks(TargetDataLink.newBuilder()
                        .setMergeTarget(ActionMergeTargetData.newBuilder()
                                .setRelatedTo(EntityType.VM_SPEC)
                                .setRelatedBy(EntityRelationship.newBuilder()
                                        .setConnectionType(CommonDTO.ConnectedEntity.ConnectionType.AGGREGATED_BY_CONNECTION))
                        )
                        .setDeDuplicate(true)
                )
                .addTargetLinks(TargetDataLink.newBuilder()
                        .setMergeTarget(ActionMergeTargetData.newBuilder()
                                .setRelatedTo(EntityType.WORKLOAD_CONTROLLER)
                                .setRelatedBy(EntityRelationship.newBuilder()
                                        .setConnectionType(CommonDTO.ConnectedEntity.ConnectionType.CONTROLLED_BY_CONNECTION))
                        )
                )
                .build();

        terraformMergePolicy = ActionMergePolicyDTO.newBuilder()
                .setEntityType(EntityType.VIRTUAL_MACHINE)
                .addExecutionTargets(ActionMergeExecutionTarget.newBuilder()
                                        .setChainedMergeTarget(vmToVmSpecToControllerData))
                .addExecutionTargets(ActionMergeExecutionTarget.newBuilder()
                        .setMergeTarget(vmToVmSpecData))
                .addExecutionTargets(ActionMergeExecutionTarget.newBuilder()
                                        .setMergeTarget(vmToControllerData))
                .setResizeSpec(ResizeMergeSpec.newBuilder()
                        .addCommodityData(CommodityMergeData.newBuilder()
                                        .setCommodityType(CommodityDTO.CommodityType.VMEM))
                        .addCommodityData(CommodityMergeData.newBuilder()
                                        .setCommodityType(CommodityDTO.CommodityType.VCPU)))
                .build();

        return terraformMergePolicy;
    }

    /**
     * Construct a test topology graph of a typical Kubernetes probe
     * containing containers, container specs and workload controllers.
     *
     * @return TopologyGraph
     */
    private static TopologyGraph<TopologyEntity> constructKubernetesTopology() {
        final Map<Long, Builder> topology = new HashMap<>();

        topology.put(1L, buildTopologyEntity(1L,
                CommodityDTO.CommodityType.VCPU.getNumber(),
                EntityType.CONTAINER_POD_VALUE,
                Collections.singleton(KUBERNETES_TARGET_ID)));
        topology.put(2L, buildTopologyEntity(2L,
                CommodityDTO.CommodityType.VCPU.getNumber(),
                EntityType.CONTAINER_POD_VALUE,
                Collections.singleton(KUBERNETES_TARGET_ID)));

        final TopologyEntity.Builder container1 = buildTopologyEntity(11L,
                        CommodityDTO.CommodityType.VCPU.getNumber(),
                        CommodityDTO.CommodityType.VCPU.getNumber(),
                        EntityType.CONTAINER_VALUE,
                        1L, Collections.singleton(KUBERNETES_TARGET_ID));

        final TopologyEntity.Builder container2 = buildTopologyEntity(12L,
                CommodityDTO.CommodityType.VCPU.getNumber(),
                CommodityDTO.CommodityType.VCPU.getNumber(),
                EntityType.CONTAINER_VALUE,
                1L, Collections.singleton(KUBERNETES_TARGET_ID));

        final TopologyEntity.Builder container3 = buildTopologyEntity(21L,
                CommodityDTO.CommodityType.VCPU.getNumber(),
                CommodityDTO.CommodityType.VCPU.getNumber(),
                EntityType.CONTAINER_VALUE,
                2L, Collections.singleton(KUBERNETES_TARGET_ID));

        final TopologyEntity.Builder container4 = buildTopologyEntity(22L,
                CommodityDTO.CommodityType.VCPU.getNumber(),
                CommodityDTO.CommodityType.VCPU.getNumber(),
                EntityType.CONTAINER_VALUE,
                2L, Collections.singleton(KUBERNETES_TARGET_ID));

        topology.put(11L, container1);
        topology.put(12L, container2);

        topology.put(21L, container3);
        topology.put(22L, container4);

        final TopologyEntity.Builder spec1 = buildTopologyEntity(31L,
                CommodityDTO.CommodityType.VCPU.getNumber(),
                EntityType.CONTAINER_SPEC_VALUE, Collections.singleton(KUBERNETES_TARGET_ID));
        container1.getEntityBuilder()
                .addConnectedEntityList(ConnectedEntity.newBuilder()
                        .setConnectedEntityId(31L)
                        .setConnectionType(ConnectionType.AGGREGATED_BY_CONNECTION)
                        .build());
        container2.getEntityBuilder()
                .addConnectedEntityList(ConnectedEntity.newBuilder()
                        .setConnectedEntityId(31L)
                        .setConnectionType(ConnectionType.AGGREGATED_BY_CONNECTION)
                        .build());


        final TopologyEntity.Builder spec2 = buildTopologyEntity(32L,
                CommodityDTO.CommodityType.VCPU.getNumber(),
                EntityType.CONTAINER_SPEC_VALUE,    //66, 64 is namespace
                Collections.singleton(KUBERNETES_TARGET_ID));
        container3.getEntityBuilder()
                .addConnectedEntityList(ConnectedEntity.newBuilder()
                        .setConnectedEntityId(32L)
                        .setConnectionType(ConnectionType.AGGREGATED_BY_CONNECTION)
                        .build());
        container4.getEntityBuilder()
                .addConnectedEntityList(ConnectedEntity.newBuilder()
                        .setConnectedEntityId(32L)
                        .setConnectionType(ConnectionType.AGGREGATED_BY_CONNECTION)
                        .build());

        final TopologyEntity.Builder controller = buildTopologyEntity(41L,
                CommodityDTO.CommodityType.VCPU.getNumber(),
                EntityType.WORKLOAD_CONTROLLER_VALUE,   //65 //connected to 66, provider is 64 (namespace
                Collections.singleton(KUBERNETES_TARGET_ID));
        spec1.getEntityBuilder()
                .addConnectedEntityList(ConnectedEntity.newBuilder()
                        .setConnectedEntityId(41L)
                        .setConnectionType(ConnectionType.CONTROLLED_BY_CONNECTION)
                        .build());
        spec2.getEntityBuilder()
                .addConnectedEntityList(ConnectedEntity.newBuilder()
                        .setConnectedEntityId(41L)
                        .setConnectionType(ConnectionType.CONTROLLED_BY_CONNECTION)
                        .build());

        topology.put(31L, spec1);
        topology.put(32L, spec2);
        topology.put(41L, controller);

        graph = TopologyEntityTopologyGraphCreator.newGraph(topology);

        return graph;
    }

    /**
     * Construct a test topology graph of a typical Terraform probe
     * containing VMs, VM specs and workload controllers.
     *
     * @return TopologyGraph
     */
    private static TopologyGraph<TopologyEntity> constructTerraformTopology() {
        final Map<Long, Builder> topology = new HashMap<>();

        final TopologyEntity.Builder vm1 = buildTopologyEntity(11L,
                CommodityDTO.CommodityType.VCPU.getNumber(),
                EntityType.VIRTUAL_MACHINE_VALUE,
                Collections.singleton(TERRAFORM_TARGET_ID));
        final TopologyEntity.Builder vm2 = buildTopologyEntity(12L,
                CommodityDTO.CommodityType.VCPU.getNumber(),
                EntityType.VIRTUAL_MACHINE_VALUE,
                Collections.singleton(TERRAFORM_TARGET_ID));

        final TopologyEntity.Builder vm3 = buildTopologyEntity(21L,
                CommodityDTO.CommodityType.VCPU.getNumber(),
                EntityType.VIRTUAL_MACHINE_VALUE,
                Collections.singleton(TERRAFORM_TARGET_ID));
        final TopologyEntity.Builder vm4 = buildTopologyEntity(22L,
                CommodityDTO.CommodityType.VCPU.getNumber(),
                EntityType.VIRTUAL_MACHINE_VALUE,
                Collections.singleton(TERRAFORM_TARGET_ID));

        topology.put(11L, vm1);
        topology.put(12L, vm2);

        topology.put(21L, vm3);
        topology.put(22L, vm4);

        final TopologyEntity.Builder spec1 = buildTopologyEntity(31L,
                CommodityDTO.CommodityType.VCPU.getNumber(),
                EntityType.VM_SPEC_VALUE, Collections.singleton(TERRAFORM_TARGET_ID));

        vm1.getEntityBuilder()
                .addConnectedEntityList(ConnectedEntity.newBuilder()
                        .setConnectedEntityId(31L)
                        .setConnectionType(ConnectionType.AGGREGATED_BY_CONNECTION)
                        .build());
        vm2.getEntityBuilder()
                .addConnectedEntityList(ConnectedEntity.newBuilder()
                        .setConnectedEntityId(31L)
                        .setConnectionType(ConnectionType.AGGREGATED_BY_CONNECTION)
                        .build());

        final TopologyEntity.Builder controller = buildTopologyEntity(41L,
                CommodityDTO.CommodityType.VCPU.getNumber(),
                EntityType.WORKLOAD_CONTROLLER_VALUE,   //65 //connected to 66, provider is 64 (namespace
                Collections.singleton(TERRAFORM_TARGET_ID));

        spec1.getEntityBuilder()
                .addConnectedEntityList(ConnectedEntity.newBuilder()
                        .setConnectedEntityId(41L)
                        .setConnectionType(ConnectionType.CONTROLLED_BY_CONNECTION)
                        .build());

        vm3.getEntityBuilder()
                .addConnectedEntityList(ConnectedEntity.newBuilder()
                        .setConnectedEntityId(41L)
                        .setConnectionType(ConnectionType.CONTROLLED_BY_CONNECTION)
                        .build());

        vm4.getEntityBuilder()
                .addConnectedEntityList(ConnectedEntity.newBuilder()
                        .setConnectedEntityId(41L)
                        .setConnectionType(ConnectionType.CONTROLLED_BY_CONNECTION)
                        .build());

        topology.put(31L, spec1);
        topology.put(41L, controller);

        terraformGraph = TopologyEntityTopologyGraphCreator.newGraph(topology);

        return terraformGraph;
    }

    private static TopologyEntity.Builder buildTopologyEntity(long oid, int soldType, int boughtType, int entityType,
                                                       long providerId,
                                                       final Collection<Long> targetIds) {
        DiscoveryOrigin.Builder origin = DiscoveryOrigin.newBuilder();
        targetIds.forEach(id -> origin.putDiscoveredTargetData(id,
                PerTargetEntityInformation.getDefaultInstance()));
        return TopologyEntityUtils.topologyEntityBuilder(
                TopologyEntityDTO.newBuilder()
                        .setAnalysisSettings(AnalysisSettings.newBuilder().build())
                        .setEntityType(entityType)
                        .setOrigin(Origin.newBuilder()
                                .setDiscoveryOrigin(origin)
                                .build())
                        .setOid(oid)
                        .addCommoditySoldList(CommoditySoldDTO.newBuilder().setCommodityType(
                                CommodityType.newBuilder().setType(soldType).setKey("").build()))
                        .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                                .setProviderId(providerId)
                                .addCommodityBought(
                                        CommodityBoughtDTO.newBuilder()
                                                .setCommodityType(
                                                        CommodityType.newBuilder().setType(boughtType).setKey("").build()
                                                ).setActive(true)
                                )
                ));
    }

    private static TopologyEntity.Builder buildTopologyEntity(long oid, int soldType, int entityType,
                                                              final Collection<Long> targetIds) {
        DiscoveryOrigin.Builder origin = DiscoveryOrigin.newBuilder();
        targetIds.forEach(id -> origin.putDiscoveredTargetData(id,
                PerTargetEntityInformation.getDefaultInstance()));
        return TopologyEntityUtils.topologyEntityBuilder(
                TopologyEntityDTO.newBuilder()
                        .setAnalysisSettings(AnalysisSettings.newBuilder().build())
                        .setEntityType(entityType)
                        .setOrigin(Origin.newBuilder()
                                .setDiscoveryOrigin(origin)
                                .build())
                        .setOid(oid)
                        .addCommoditySoldList(CommoditySoldDTO.newBuilder().setCommodityType(
                                CommodityType.newBuilder().setType(soldType).setKey("").build())));
    }

    /**
     * Set up before the tests.
     */
    @org.junit.Before
    public void setUp() {
        constructKubernetesTopology();
        constructTerraformTopology();
        createKubernetesPolicyDTO();
        createTerraformPolicyDTO();
    }

    /**
     * Test creation of a execution target of an entity
     * where the actions are merged on the entity itself.
     */
    @org.junit.Test
    public void createSelfMergeActionTarget() {
        ActionMergeTargetData mergeTargetData = ActionMergeTargetData.newBuilder()
                .setRelatedTo(EntityType.CONTAINER)
                .build();

        ActionMergePolicyDTO mergePolicy = ActionMergePolicyDTO.newBuilder()
                .setEntityType(EntityType.CONTAINER)
                .addExecutionTargets(ActionMergeExecutionTarget.newBuilder()
                        .setMergeTarget(mergeTargetData))
                .setResizeSpec(ResizeMergeSpec.newBuilder()
                        .addCommodityData(CommodityMergeData.newBuilder().setCommodityType(CommodityDTO.CommodityType.VMEM).build())
                        .addCommodityData(CommodityMergeData.newBuilder().setCommodityType(CommodityDTO.CommodityType.VCPU).build())
                        .build())
                .build();

        Optional<TopologyEntity> containerOpt = graph.getEntity(21L);
        final TopologyEntity container1 = containerOpt.get();

        containerOpt = graph.getEntity(22L);
        final TopologyEntity container2 = containerOpt.get();

        ActionMergeSpecsBuilder specsBuilder = new ActionMergeSpecsBuilder(KUBERNETES_TARGET_ID, graph);

        ActionExecutionTarget executionTarget1 = specsBuilder.getActionExecutionTarget(container1, mergePolicy);
        Assert.assertEquals(container1, executionTarget1.actionAggregationEntity());

        ActionExecutionTarget executionTarget2 = specsBuilder.getActionExecutionTarget(container2, mergePolicy);
        Assert.assertEquals(container2, executionTarget2.actionAggregationEntity());
    }

    /**
     * Test creation of a execution target for an atomic action spec when the target metadata is specified
     * as a list of connections.
     */
    @org.junit.Test
    public void createActionMergeTargetWithChainedTargetSpec() {

        System.out.println(kubeTurboMergePolicy);
        System.out.println(terraformMergePolicy);
        Optional<TopologyEntity> containerOpt = graph.getEntity(21L);
        final TopologyEntity container1 = containerOpt.get();

        containerOpt = graph.getEntity(22L);
        final TopologyEntity container2 = containerOpt.get();

        containerOpt = graph.getEntity(11L);
        final TopologyEntity container3 = containerOpt.get();

        containerOpt = graph.getEntity(12L);
        final TopologyEntity container4 = containerOpt.get();

        Optional<TopologyEntity> entityOpt = graph.getEntity(41L);
        final TopologyEntity controller = entityOpt.get();

        entityOpt = graph.getEntity(31L);
        final TopologyEntity containerSpec1 = entityOpt.get();

        entityOpt = graph.getEntity(32L);
        final TopologyEntity containerSpec2 = entityOpt.get();

        ActionMergeSpecsBuilder specsBuilder = new ActionMergeSpecsBuilder(KUBERNETES_TARGET_ID, graph);

        ActionExecutionTarget executionTarget1 = specsBuilder.getActionExecutionTarget(container1, kubeTurboMergePolicy);
        Assert.assertNotNull(executionTarget1);
        Assert.assertEquals(controller, executionTarget1.actionAggregationEntity());
        Assert.assertEquals(containerSpec2, executionTarget1.actionDeDuplicationEntity().get());

        ActionExecutionTarget executionTarget2 = specsBuilder.getActionExecutionTarget(container2, kubeTurboMergePolicy);
        Assert.assertNotNull(executionTarget2);
        Assert.assertEquals(controller, executionTarget2.actionAggregationEntity());
        Assert.assertEquals(containerSpec2, executionTarget2.actionDeDuplicationEntity().get());

        ActionExecutionTarget executionTarget3 = specsBuilder.getActionExecutionTarget(container3, kubeTurboMergePolicy);
        Assert.assertNotNull(executionTarget3);
        Assert.assertEquals(controller, executionTarget3.actionAggregationEntity());
        Assert.assertEquals(containerSpec1, executionTarget3.actionDeDuplicationEntity().get());

        ActionExecutionTarget executionTarget4 = specsBuilder.getActionExecutionTarget(container4, kubeTurboMergePolicy);
        Assert.assertNotNull(executionTarget4);
        Assert.assertEquals(controller, executionTarget4.actionAggregationEntity());
        Assert.assertEquals(containerSpec1, executionTarget4.actionDeDuplicationEntity().get());
    }

    /**
     * Test creation of a execution target for an atomic action spec when the action merge policy DTO
     * contains a list of target metadata.
     */
    @org.junit.Test
    public void createActionMergeTargetWithMultipleTargetSpecs() {
        Optional<TopologyEntity> vmOpt = terraformGraph.getEntity(11L);
        final TopologyEntity vm1 = vmOpt.get();

        vmOpt = terraformGraph.getEntity(12L);
        final TopologyEntity vm2 = vmOpt.get();

        vmOpt = terraformGraph.getEntity(21L);
        final TopologyEntity vm3 = vmOpt.get();

        vmOpt = terraformGraph.getEntity(22L);
        final TopologyEntity vm4 = vmOpt.get();

        Optional<TopologyEntity> entityOpt = terraformGraph.getEntity(41L);
        final TopologyEntity controller = entityOpt.get();

        entityOpt = terraformGraph.getEntity(31L);
        final TopologyEntity vmSpec1 = entityOpt.get();

        ActionMergeSpecsBuilder specsBuilder = new ActionMergeSpecsBuilder(TERRAFORM_TARGET_ID, terraformGraph);

        ActionExecutionTarget executionTarget1 = specsBuilder.getActionExecutionTarget(vm1, terraformMergePolicy);
        Assert.assertEquals(controller, executionTarget1.actionAggregationEntity());
        Assert.assertEquals(vmSpec1, executionTarget1.actionDeDuplicationEntity().get());

        ActionExecutionTarget executionTarget2 = specsBuilder.getActionExecutionTarget(vm2, terraformMergePolicy);
        Assert.assertEquals(controller, executionTarget2.actionAggregationEntity());
        Assert.assertEquals(vmSpec1, executionTarget1.actionDeDuplicationEntity().get());

        ActionExecutionTarget executionTarget3 = specsBuilder.getActionExecutionTarget(vm3, terraformMergePolicy);
        Assert.assertEquals(controller, executionTarget3.actionAggregationEntity());
        Assert.assertFalse(executionTarget3.actionDeDuplicationEntity().isPresent());

        ActionExecutionTarget executionTarget4 = specsBuilder.getActionExecutionTarget(vm4, terraformMergePolicy);
        Assert.assertEquals(controller, executionTarget4.actionAggregationEntity());
        Assert.assertFalse(executionTarget4.actionDeDuplicationEntity().isPresent());
    }

    /**
     * Test creation of {@link AtomicActionSpec} using the {@link ActionMergePolicyDTO} that
     * is similar to the one sent by the Kubernetes probe.
     */
    @org.junit.Test
    public void createActionMergeSpecsWithChainedTargetSpec() {

        ProbeInfo probeInfo = ProbeInfo.newBuilder()
                .setProbeCategory("cat")
                .setProbeType("Kubernetes")
                .addTargetIdentifierField("field")
                .addAccountDefinition(AccountDefEntry.newBuilder()
                        .setMandatory(true)
                        .setCustomDefinition(CustomAccountDefEntry.newBuilder()
                                .setName("name")
                                .setDisplayName("displayName")
                                .setDescription("description")
                                .setIsSecret(true)))
                .addActionMergePolicy(kubeTurboMergePolicy)
                .build();

        ActionMergeSpecsRepository repo = new ActionMergeSpecsRepository();
        repo.setPoliciesForProbe(1L, probeInfo);

        List<AtomicActionSpec> specs = repo.createAtomicActionSpecs(KUBERNETES_PROBE_ID,
                                                                    KUBERNETES_TARGET_ID,
                                                                    graph);
        Assert.assertEquals(2, specs.size());

        List<Long> deDuplicationTargetIds = Arrays.asList(31L, 32L);
        for (AtomicActionSpec mergeSpec : specs) {
            Assert.assertEquals(41L, mergeSpec.getAggregateEntity().getEntity().getId());
            Assert.assertTrue(mergeSpec.hasResizeSpec());
            Assert.assertTrue(mergeSpec.getResizeSpec().hasDeDuplicationTarget());
            Assert.assertTrue(deDuplicationTargetIds.contains(mergeSpec.getResizeSpec().getDeDuplicationTarget().getEntity().getId()));
        }
    }

    /**
     * Test creation of {@link AtomicActionSpec} using the {@link ActionMergePolicyDTO} that
     * is similar to the one sent by the Terraform probe.
     */
    @org.junit.Test
    public void createActionMergeSpecsWithMultipleTargetSpec() {
        ProbeInfo probeInfo = ProbeInfo.newBuilder()
                .setProbeCategory("cat")
                .setProbeType("Terraform")
                .addTargetIdentifierField("field")
                .addAccountDefinition(AccountDefEntry.newBuilder()
                        .setMandatory(true)
                        .setCustomDefinition(CustomAccountDefEntry.newBuilder()
                                .setName("name")
                                .setDisplayName("displayName")
                                .setDescription("description")
                                .setIsSecret(true)))
                .addActionMergePolicy(terraformMergePolicy)
                .build();

        ActionMergeSpecsRepository repo = new ActionMergeSpecsRepository();
        repo.setPoliciesForProbe(2L, probeInfo);

        List<AtomicActionSpec> specs = repo.createAtomicActionSpecs(TERRAFORM_PROBE_ID,
                TERRAFORM_TARGET_ID,
                terraformGraph);
        Assert.assertEquals(2, specs.size());
        for (AtomicActionSpec mergeSpec : specs) {
            Assert.assertEquals(41L, mergeSpec.getAggregateEntity().getEntity().getId());
            Assert.assertTrue(mergeSpec.hasResizeSpec());
        }

        List<AtomicActionSpec> specsWithoutDeduplicationTarget =
        specs.stream().filter(spec -> !spec.getResizeSpec().hasDeDuplicationTarget())
                .collect(Collectors.toList());

        Assert.assertEquals(1, specsWithoutDeduplicationTarget.size());

        List<AtomicActionSpec> specsWithDeduplicationTarget =
                specs.stream().filter(spec -> spec.getResizeSpec().hasDeDuplicationTarget())
                        .collect(Collectors.toList());

        Assert.assertEquals(1, specsWithDeduplicationTarget.size());

        specsWithDeduplicationTarget.stream().forEach(
                spec -> Assert.assertEquals(31L,
                                    spec.getResizeSpec().getDeDuplicationTarget().getEntity().getId())
        );
    }
}
