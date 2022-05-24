package com.vmturbo.topology.processor.topology;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.gson.Gson;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.cpucapacity.CpuCapacityServiceGrpc;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProjectType;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.PlanScope;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.PlanScopeEntry;
import com.vmturbo.common.protobuf.plan.TemplateDTO.GetTemplatesRequest;
import com.vmturbo.common.protobuf.plan.TemplateDTO.GetTemplatesResponse;
import com.vmturbo.common.protobuf.plan.TemplateDTO.ResourcesCategory;
import com.vmturbo.common.protobuf.plan.TemplateDTO.ResourcesCategory.ResourcesCategoryName;
import com.vmturbo.common.protobuf.plan.TemplateDTO.SingleTemplateResponse;
import com.vmturbo.common.protobuf.plan.TemplateDTO.Template;
import com.vmturbo.common.protobuf.plan.TemplateDTO.Template.Type;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateField;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateInfo;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateResource;
import com.vmturbo.common.protobuf.plan.TemplateDTOMoles.TemplateServiceMole;
import com.vmturbo.common.protobuf.plan.TemplateServiceGrpc;
import com.vmturbo.common.protobuf.plan.TemplateServiceGrpc.TemplateServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingProto.ListSettingPoliciesRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.NumericSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicyInfo;
import com.vmturbo.common.protobuf.setting.SettingProtoMoles.SettingPolicyServiceMole;
import com.vmturbo.common.protobuf.tag.TagPOJO.TagValuesImpl;
import com.vmturbo.common.protobuf.tag.TagPOJO.TagsImpl;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentMatcher;

import com.vmturbo.common.protobuf.group.GroupDTO;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupDTO.MemberType;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers.StaticMembersByType;
import com.vmturbo.common.protobuf.group.GroupDTOMoles.GroupServiceMole;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.PlanChanges;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.PlanChanges.UtilizationLevel;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.TopologyAddition;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.TopologyMigration;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.TopologyMigration.MigrationReference;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.TopologyRemoval;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.TopologyReplace;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PlanTopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommodityBoughtImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommodityBoughtView;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommoditySoldImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommoditySoldView;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommodityTypeImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommodityTypeView;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.PerTargetEntityInformationImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl.AnalysisSettingsImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl.CommoditiesBoughtFromProviderImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl.CommoditiesBoughtFromProviderView;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl.ConnectedEntityImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl.ConnectedEntityView;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl.DiscoveryOriginImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl.EditView;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl.OriginImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl.PlanScenarioOriginImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityView;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TypeSpecificInfoImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TypeSpecificInfoImpl.VirtualMachineInfoImpl;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.commons.analysis.AnalysisUtil;
import com.vmturbo.commons.analysis.InvertedIndex;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.common.featureflags.FeatureFlags;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityOrigin;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;
import com.vmturbo.platform.common.dto.CommonDTOREST;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.TopologyEntity.Builder;
import com.vmturbo.test.utils.FeatureFlagTestRule;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.processor.group.GroupResolutionException;
import com.vmturbo.topology.processor.group.GroupResolver;
import com.vmturbo.topology.processor.group.ResolvedGroup;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.template.TemplateConverterFactory;
import com.vmturbo.topology.processor.topology.clone.DefaultEntityCloneEditor;
import com.vmturbo.topology.processor.topology.pipeline.TopologyPipelineContext;
import com.vmturbo.topology.processor.util.TopologyEditorUtil;

/**
 * Unit tests for {@link ScenarioChange}.
 */
public class TopologyEditorTest {

    /**
     * Rule to manage feature flag enablement to make sure FeatureFlagManager store is set up.
     */
    @Rule
    public FeatureFlagTestRule featureFlagTestRule = new FeatureFlagTestRule();

    private static final long vmId = 10;
    private static final long pmId = 20;
    private static final long pm2Id = 21;
    private static final long stId = 30;
    private static final long podId = 40;
    private static final long podId_1 = 41;
    private static final long containerId = 50;
    private static final long workloadControllerId = 60;
    private static final long volumeId = 70;
    private static final long containerSpecId = 80;
    private static final long namespaceId = 90;
    private static final long clusterId = 134;
    private static final long destClusterId = 74327977713718L;
    private static final long destVMId = 101;
    private static final double USED = 100;
    private static final double VCPU_CAPACITY = 24257.928;
    private static final double VMEM_CAPACITY = 3.2460604E7;

    private static final CommodityTypeView MEM = new CommodityTypeImpl().setType(21);
    private static final CommodityTypeView CPU = new CommodityTypeImpl().setType(40);
    private static final CommodityTypeView LATENCY = new CommodityTypeImpl().setType(3);
    private static final CommodityTypeView IOPS = new CommodityTypeImpl().setType(4);
    private static final CommodityTypeView DATASTORE = new CommodityTypeImpl()
                    .setType(CommodityDTO.CommodityType.DATASTORE_VALUE);
    private static final CommodityTypeView DSPM = new CommodityTypeImpl()
                    .setType(CommodityDTO.CommodityType.DSPM_ACCESS_VALUE);
    private static final CommodityTypeView VCPU = new CommodityTypeImpl()
        .setType(CommodityDTO.CommodityType.VCPU_VALUE);
    private static final CommodityTypeView VMEM = new CommodityTypeImpl()
        .setType(CommodityDTO.CommodityType.VMEM_VALUE);
    private static final CommodityTypeView VCPU_REQUEST_QUOTA = new CommodityTypeImpl()
        .setType(CommodityDTO.CommodityType.VCPU_REQUEST_QUOTA_VALUE);
    private static final CommodityTypeView VCPU_LIMIT_QUOTA = new CommodityTypeImpl()
            .setType(CommodityDTO.CommodityType.VCPU_LIMIT_QUOTA_VALUE);
    private static final CommodityTypeView VMEM_REQUEST_QUOTA = new CommodityTypeImpl()
            .setType(CommodityDTO.CommodityType.VMEM_REQUEST_QUOTA_VALUE);
    private static final CommodityTypeView VMEM_LIMIT_QUOTA = new CommodityTypeImpl()
            .setType(CommodityDTO.CommodityType.VMEM_LIMIT_QUOTA_VALUE);
    private static final CommodityTypeView STORAGE_AMOUNT = new CommodityTypeImpl()
            .setType(CommodityDTO.CommodityType.STORAGE_AMOUNT_VALUE);
    private static final CommodityTypeView VMPM = new CommodityTypeImpl()
        .setType(CommodityDTO.CommodityType.VMPM_ACCESS_VALUE);
    private static final CommodityTypeView VMPM_NODE_TO_POD = new CommodityTypeImpl()
            .setType(CommodityDTO.CommodityType.VMPM_ACCESS_VALUE)
            .setKey("role=worker");
    private static final CommodityTypeView VMPM_POD_TO_CONTAINER = new CommodityTypeImpl()
            .setType(CommodityDTO.CommodityType.VMPM_ACCESS_VALUE)
            .setKey("MyKey");
    private static final CommodityTypeView TAINT_NODE_TO_POD_1 = new CommodityTypeImpl()
            .setType(CommodityDTO.CommodityType.TAINT_VALUE)
            .setKey("key2=value2:NoSchedule");
    private static final CommodityTypeView TAINT_NODE_TO_POD_2 = new CommodityTypeImpl()
            .setType(CommodityDTO.CommodityType.TAINT_VALUE)
            .setKey("node-role.kubernetes.io/master=:NoSchedule");
    private static final CommodityTypeView CLUSTER_KEYED = new CommodityTypeImpl()
            .setType(CommodityDTO.CommodityType.CLUSTER_VALUE)
            .setKey("foo");
    private static final CommodityTypeView DEST_CLUSTER_KEYED = new CommodityTypeImpl()
            .setType(CommodityDTO.CommodityType.CLUSTER_VALUE)
            .setKey("bar");
    private PlanScope scope;
    private TopologyPipelineContext context;
    private TopologyPipelineContext containerClusterPlanContext;
    private TopologyPipelineContext migrateContainerWorkloadPlanContext;
    private static final TopologyEntity.Builder destVM = TopologyEntityUtils.topologyEntityBuilder(
            new TopologyEntityImpl()
                    .setOid(destVMId)
                    .setDisplayName("destVM")
                    .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                    .setTags(new TagsImpl().putTags("[k8s taint] NoSchedule", new TagValuesImpl()
                                         // Taint with both key and value
                                         .addValues("key2=value2")
                                         // Taint with key only
                                         .addValues("node-role.kubernetes.io/master")))
                    .addConnectedEntityList(new ConnectedEntityImpl()
                                        .setConnectedEntityId(destClusterId)
                                        .setConnectionType(ConnectionType.AGGREGATED_BY_CONNECTION))
                    .addCommoditySoldList(new CommoditySoldImpl()
                                                  .setCommodityType(VCPU)
                                                  .setUsed(USED)
                                                  .setCapacity(VCPU_CAPACITY))
                    .addCommoditySoldList(new CommoditySoldImpl()
                                                  .setCommodityType(VMEM)
                                                  .setUsed(USED)
                                                  .setCapacity(VMEM_CAPACITY))
                    .addCommoditySoldList(new CommoditySoldImpl()
                                                  .setCommodityType(DEST_CLUSTER_KEYED))
                    .addCommoditySoldList(new CommoditySoldImpl()
                                                  .setCommodityType(VMPM_NODE_TO_POD))
                    .addCommoditySoldList(new CommoditySoldImpl()
                                                  .setCommodityType(TAINT_NODE_TO_POD_1))
                    .addCommoditySoldList(new CommoditySoldImpl()
                                                  .setCommodityType(TAINT_NODE_TO_POD_2))
    );
    private static final TopologyEntity.Builder vm = TopologyEntityUtils.topologyEntityBuilder(
        new TopologyEntityImpl()
            .setOid(vmId)
            .setDisplayName("VM")
            .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
            .setEnvironmentType(EnvironmentType.CLOUD)
            .setEntityState(EntityState.POWERED_ON)
            .setTypeSpecificInfo(new TypeSpecificInfoImpl().setVirtualMachine(
                    new VirtualMachineInfoImpl().setNumCpus(8)))
            .addConnectedEntityList(new ConnectedEntityImpl()
                .setConnectedEntityId(clusterId)
                .setConnectionType(ConnectionType.AGGREGATED_BY_CONNECTION))
            .addCommoditiesBoughtFromProviders(new CommoditiesBoughtFromProviderImpl()
                .setProviderId(pmId)
                .addCommodityBought(new CommodityBoughtImpl().setCommodityType(MEM)
                    .setUsed(USED))
                .addCommodityBought(new CommodityBoughtImpl().setCommodityType(CPU)
                    .setUsed(USED)))
            .addCommoditiesBoughtFromProviders(new CommoditiesBoughtFromProviderImpl()
                .setProviderId(stId)
                .addCommodityBought(new CommodityBoughtImpl().setCommodityType(LATENCY).setUsed(USED))
                .addCommodityBought(new CommodityBoughtImpl().setCommodityType(IOPS)
                    .setUsed(USED)))
            .addCommoditySoldList(new CommoditySoldImpl()
                .setCommodityType(VCPU)
                .setUsed(USED)
                .setCapacity(VCPU_CAPACITY))
            .addCommoditySoldList(new CommoditySoldImpl()
                .setCommodityType(VMEM)
                .setUsed(USED)
                .setCapacity(VMEM_CAPACITY))
            .addCommoditySoldList(new CommoditySoldImpl()
                .setCommodityType(CLUSTER_KEYED))
            .addCommoditySoldList(new CommoditySoldImpl()
                .setCommodityType(VMPM_NODE_TO_POD))
            .addCommoditySoldList(new CommoditySoldImpl()
                .setCommodityType(TAINT_NODE_TO_POD_1))
            .addCommoditySoldList(new CommoditySoldImpl()
                .setCommodityType(TAINT_NODE_TO_POD_2))
    );
    private static String clusterName = "ContainerPlatformCluster";
    private static final TopologyEntity.Builder cluster = TopologyEntityUtils.topologyEntityBuilder(
            new TopologyEntityImpl()
                    .setOid(clusterId)
                    .setDisplayName(clusterName)
                    .setEntityType(EntityType.CONTAINER_PLATFORM_CLUSTER_VALUE)
    );
    private static String destContainerClusterName = "DestinationContainerPlatformCluster";
    private static final TopologyEntity.Builder destCluster = TopologyEntityUtils.topologyEntityBuilder(
            new TopologyEntityImpl()
                    .setOid(destClusterId)
                    .setDisplayName(destContainerClusterName)
                    .setEntityType(EntityType.CONTAINER_PLATFORM_CLUSTER_VALUE)
                    .setOrigin(new OriginImpl()
                        .setDiscoveryOrigin(new DiscoveryOriginImpl()
                            .putDiscoveredTargetData(12345L, new PerTargetEntityInformationImpl()
                                    .setOrigin(EntityOrigin.DISCOVERED)
                                    .setVendorId("bar"))))
    );
    private static final TopologyEntity.Builder unplacedVm = TopologyEntityUtils.topologyEntityBuilder(
            new TopologyEntityImpl()
                    .setOid(vmId)
                    .setDisplayName("UNPLACED-VM")
                    .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                    .addCommoditiesBoughtFromProviders(new CommoditiesBoughtFromProviderImpl()
                            .addCommodityBought(new CommodityBoughtImpl().setCommodityType(MEM)
                                    .setUsed(USED))
                            .addCommodityBought(new CommodityBoughtImpl().setCommodityType(CPU)
                                    .setUsed(USED)))
                    .addCommoditiesBoughtFromProviders(new CommoditiesBoughtFromProviderImpl()
                            .setProviderId(stId)
                            .addCommodityBought(new CommodityBoughtImpl().setCommodityType(LATENCY)
                                    .setUsed(USED))
                            .addCommodityBought(new CommodityBoughtImpl().setCommodityType(IOPS)
                                    .setUsed(USED)))
    );

    private static final Builder createPM(long oid, @Nonnull String name) {
        return TopologyEntityUtils.topologyEntityBuilder(
                new TopologyEntityImpl()
                        .setOid(oid)
                        .setDisplayName(name)
                        .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
                        .addCommoditySoldList(new CommoditySoldImpl().setCommodityType(MEM).setUsed(USED))
                        .addCommoditySoldList(new CommoditySoldImpl().setCommodityType(CPU).setUsed(USED))
                        .addCommoditySoldList(new CommoditySoldImpl().setCommodityType(DATASTORE)
                                .setAccesses(stId))
        );
    }

    private static final TopologyEntity.Builder pm = createPM(pmId, "PM");
    private static final TopologyEntity.Builder pm2 = createPM(pm2Id, "PM-2");

    private final TopologyEntity.Builder st = TopologyEntityUtils.topologyEntityBuilder(
        new TopologyEntityImpl()
            .setOid(stId)
            .setDisplayName("ST")
            .setEntityType(EntityType.STORAGE_VALUE)
            .addCommoditySoldList(new CommoditySoldImpl().setCommodityType(LATENCY)
                .setAccesses(vmId).setUsed(USED))
            .addCommoditySoldList(new CommoditySoldImpl().setCommodityType(IOPS)
                .setAccesses(vmId).setUsed(USED))
            .addCommoditySoldList(new CommoditySoldImpl().setCommodityType(DSPM)
                            .setAccesses(pmId))
    );

    private static final String workloadControllerKey = "1a7237fb-c464-46d5-b6fd-b493cf417509";
    private static final String namespaceKey = "7115df29-394a-44d0-bbb4-91d3f210bb1d";
    private static final TopologyEntity.Builder container =
        TopologyEntityUtils.topologyEntityBuilder(new TopologyEntityImpl()
            .setOid(containerId)
            .setDisplayName("catalogue")
            .setEntityType(EntityType.CONTAINER_VALUE)
            .setAnalysisSettings(
                new AnalysisSettingsImpl()
                    .setSuspendable(true)
                    .setControllable(true)
            )
            .addCommoditiesBoughtFromProviders(
                new CommoditiesBoughtFromProviderImpl()
                    .setProviderId(podId)
                    .setProviderEntityType(EntityType.CONTAINER_POD_VALUE)
                    .addCommodityBought(new CommodityBoughtImpl()
                        .setCommodityType(VMEM)
                        .setUsed(USED))
                    .addCommodityBought(new CommodityBoughtImpl()
                        .setCommodityType(VCPU)
                        .setUsed(USED))
                    .addCommodityBought(new CommodityBoughtImpl()
                        .setCommodityType(VMPM_POD_TO_CONTAINER)
                        .setUsed(USED)))
            .addCommoditySoldList(new CommoditySoldImpl()
                .setCommodityType(VCPU)
                .setUsed(USED)
                .setCapacity(VCPU_CAPACITY)
                .setCapacityIncrement(100f))
            .addCommoditySoldList(new CommoditySoldImpl()
                .setCommodityType(VMEM)
                .setUsed(USED)
                .setCapacity(VMEM_CAPACITY))
            .addCommoditySoldList(new CommoditySoldImpl()
                .setCommodityType(VMPM)
                .setUsed(1.0)
                .setCapacity(1.0))
            .addConnectedEntityList(new ConnectedEntityImpl()
                .setConnectedEntityId(containerSpecId)
                .setConnectedEntityType(EntityType.CONTAINER_SPEC_VALUE)
                .setConnectionType(ConnectionType.CONTROLLED_BY_CONNECTION))
            .putEntityPropertyMap("KubernetesNamespace", "robotshop"));

    // Create a test pod that is suspendable
    private static final TopologyEntity.Builder pod = TopologyEntityUtils.topologyEntityBuilder(
        new TopologyEntityImpl()
            .setOid(podId)
            .setDisplayName("robotshop/catalogue-74768b7f66-mgssg")
            .setEntityType(EntityType.CONTAINER_POD_VALUE)
            .addCommoditiesBoughtFromProviders(new CommoditiesBoughtFromProviderImpl()
                .setProviderEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .setProviderId(vmId)
                .addCommodityBought(new CommodityBoughtImpl().setCommodityType(VMEM)
                    .setUsed(USED))
                .addCommodityBought(new CommodityBoughtImpl().setCommodityType(VCPU)
                    .setUsed(USED))
                .addCommodityBought(new CommodityBoughtImpl().setCommodityType(VMPM_NODE_TO_POD))
                .addCommodityBought(new CommodityBoughtImpl().setCommodityType(TAINT_NODE_TO_POD_1))
                .addCommodityBought(new CommodityBoughtImpl().setCommodityType(TAINT_NODE_TO_POD_2))
                .addCommodityBought(new CommodityBoughtImpl().setCommodityType(CLUSTER_KEYED)))
            .addCommoditiesBoughtFromProviders(new CommoditiesBoughtFromProviderImpl()
                .setProviderEntityType(EntityType.WORKLOAD_CONTROLLER_VALUE)
                .setProviderId(workloadControllerId)
                .addCommodityBought(new CommodityBoughtImpl()
                    .setCommodityType(VCPU_LIMIT_QUOTA.copy().setKey(workloadControllerKey))
                    .setUsed(USED))
                .addCommodityBought(new CommodityBoughtImpl()
                    .setCommodityType(VCPU_REQUEST_QUOTA.copy().setKey(workloadControllerKey))
                    .setUsed(USED))
                .addCommodityBought(new CommodityBoughtImpl()
                    .setCommodityType(VMEM_LIMIT_QUOTA.copy().setKey(workloadControllerKey))
                    .setUsed(USED))
                .addCommodityBought(new CommodityBoughtImpl()
                    .setCommodityType(VMEM_REQUEST_QUOTA.copy().setKey(workloadControllerKey))
                    .setUsed(USED))
                .setMovable(false))
            .addCommoditiesBoughtFromProviders(new CommoditiesBoughtFromProviderImpl()
                .setProviderEntityType(EntityType.VIRTUAL_VOLUME_VALUE)
                .setProviderId(volumeId)
                .addCommodityBought(new CommodityBoughtImpl().setCommodityType(STORAGE_AMOUNT).setUsed(USED)))
            .addCommoditySoldList(new CommoditySoldImpl()
                .setCommodityType(VCPU)
                .setUsed(USED)
                .setCapacity(VCPU_CAPACITY))
            .addCommoditySoldList(new CommoditySoldImpl()
                .setCommodityType(VMEM)
                .setUsed(USED)
                .setCapacity(VMEM_CAPACITY))
            .addCommoditySoldList(new CommoditySoldImpl()
                .setCommodityType(VMPM_POD_TO_CONTAINER)
                .setUsed(1.0)
                .setCapacity(1.0))
            .addConnectedEntityList(new ConnectedEntityImpl()
                .setConnectedEntityId(workloadControllerId)
                .setConnectedEntityType(EntityType.WORKLOAD_CONTROLLER_VALUE)
                .setConnectionType(ConnectionType.AGGREGATED_BY_CONNECTION))
            .putEntityPropertyMap("KubernetesNamespace", "robotshop")
    );

    private static final TopologyEntity.Builder pod1 = TopologyEntityUtils.topologyEntityBuilder(
            pod.getTopologyEntityImpl().copy()
                    .setOid(podId_1)
                    .setDisplayName("robotshop/catalogue-74768b7f66-m6nqq"));

    private static final CommoditySoldView VCPU_LIMIT_QUOTA_COMM = new CommoditySoldImpl()
            .setCommodityType(VCPU_LIMIT_QUOTA)
            .setUsed(USED)
            .setCapacity(VCPU_CAPACITY);
    private static final CommoditySoldView VMEM_LIMIT_QUOTA_COMM = new CommoditySoldImpl()
            .setCommodityType(VMEM_LIMIT_QUOTA)
            .setUsed(USED)
            .setCapacity(VMEM_CAPACITY);
    private static final CommoditySoldView VCPU_REQUEST_QUOTA_COMM = new CommoditySoldImpl()
            .setCommodityType(VCPU_REQUEST_QUOTA)
            .setUsed(USED)
            .setCapacity(VCPU_CAPACITY);
    private static final CommoditySoldView VMEM_REQUEST_QUOTA_COMM = new CommoditySoldImpl()
            .setCommodityType(VMEM_REQUEST_QUOTA)
            .setUsed(USED)
            .setCapacity(VMEM_CAPACITY);
    private static final TopologyEntity.Builder namespace = TopologyEntityUtils.topologyEntityBuilder(
            new TopologyEntityImpl()
                    .setOid(namespaceId)
                    .setDisplayName("robotshop")
                    .setEntityType(EntityType.NAMESPACE_VALUE)
                    .addCommoditySoldList(VCPU_LIMIT_QUOTA_COMM.copy().setCommodityType(
                            VCPU_LIMIT_QUOTA.copy().setKey(namespaceKey)))
                    .addCommoditySoldList(VMEM_LIMIT_QUOTA_COMM.copy().setCommodityType(
                            VMEM_LIMIT_QUOTA.copy().setKey(namespaceKey)))
                    .addCommoditySoldList(VCPU_REQUEST_QUOTA_COMM.copy().setCommodityType(
                            VCPU_REQUEST_QUOTA.copy().setKey(namespaceKey)))
                    .addCommoditySoldList(VMEM_REQUEST_QUOTA_COMM.copy().setCommodityType(
                            VMEM_REQUEST_QUOTA.copy().setKey(namespaceKey)))
                    .addCommoditiesBoughtFromProviders(
                            new CommoditiesBoughtFromProviderImpl()
                                    .setProviderId(clusterId)
                                    .setProviderEntityType(EntityType.CONTAINER_PLATFORM_CLUSTER_VALUE)
                                    .addCommodityBought(new CommodityBoughtImpl()
                                        .setCommodityType(VCPU_LIMIT_QUOTA)
                                        .setUsed(USED))
                                    .addCommodityBought(new CommodityBoughtImpl()
                                        .setCommodityType(VCPU_REQUEST_QUOTA)
                                        .setUsed(USED))
                                    .addCommodityBought(new CommodityBoughtImpl()
                                        .setCommodityType(CLUSTER_KEYED.copy()
                                            .setKey("cluster-22ecf12e-402c-438b-a3ae-d483b2a06b49"))
                                        .setUsed(1.0)))
                    .addConnectedEntityList(
                            new ConnectedEntityImpl()
                                    .setConnectedEntityId(clusterId)
                                    .setConnectionType(ConnectionType.AGGREGATED_BY_CONNECTION)
                                    .setConnectedEntityType(EntityType.CONTAINER_PLATFORM_CLUSTER_VALUE)));

    private static String sourceWorkloadControllerName = "catalogue";
    private static final TopologyEntity.Builder workloadController = TopologyEntityUtils.topologyEntityBuilder(
            new TopologyEntityImpl()
                    .setOid(workloadControllerId)
                    .setDisplayName(sourceWorkloadControllerName)
                    .setEntityType(EntityType.WORKLOAD_CONTROLLER_VALUE)
                    .addCommoditySoldList(VCPU_LIMIT_QUOTA_COMM.copy().setCommodityType(
                            VCPU_LIMIT_QUOTA.copy().setKey(workloadControllerKey)))
                    .addCommoditySoldList(VMEM_LIMIT_QUOTA_COMM.copy().setCommodityType(
                            VMEM_LIMIT_QUOTA.copy().setKey(workloadControllerKey)))
                    .addCommoditySoldList(VCPU_REQUEST_QUOTA_COMM.copy().setCommodityType(
                            VCPU_REQUEST_QUOTA.copy().setKey(workloadControllerKey)))
                    .addCommoditySoldList(VMEM_REQUEST_QUOTA_COMM.copy().setCommodityType(
                            VMEM_REQUEST_QUOTA.copy().setKey(workloadControllerKey)))
                    .addCommoditiesBoughtFromProviders(
                            new CommoditiesBoughtFromProviderImpl()
                                    .setProviderId(namespaceId)
                                    .setProviderEntityType(EntityType.NAMESPACE_VALUE)
                                    .addCommodityBought(new CommodityBoughtImpl()
                                        .setCommodityType(VCPU_LIMIT_QUOTA.copy().setKey(namespaceKey))
                                        .setUsed(USED))
                                    .addCommodityBought(new CommodityBoughtImpl()
                                        .setCommodityType(VMEM_LIMIT_QUOTA.copy().setKey(namespaceKey))
                                        .setUsed(USED))
                                    .addCommodityBought(new CommodityBoughtImpl()
                                        .setCommodityType(VCPU_REQUEST_QUOTA.copy().setKey(namespaceKey))
                                        .setUsed(USED))
                                    .addCommodityBought(new CommodityBoughtImpl()
                                        .setCommodityType(VMEM_REQUEST_QUOTA.copy().setKey(namespaceKey))
                                        .setUsed(USED)))
                    .addConnectedEntityList(
                            new ConnectedEntityImpl()
                                    .setConnectedEntityId(containerSpecId)
                                    .setConnectedEntityType(EntityType.CONTAINER_SPEC_VALUE)
                                    .setConnectionType(ConnectionType.OWNS_CONNECTION))
                    .addConnectedEntityList(
                            new ConnectedEntityImpl()
                                    .setConnectedEntityId(namespaceId)
                                    .setConnectedEntityType(EntityType.NAMESPACE_VALUE)
                                    .setConnectionType(ConnectionType.AGGREGATED_BY_CONNECTION))
                    .putEntityPropertyMap("KubernetesNamespace", "robotshop"));
    private static final TopologyEntity.Builder containerSpec = TopologyEntityUtils.topologyEntityBuilder(
            new TopologyEntityImpl()
                    .setOid(containerSpecId)
                    .setDisplayName("catalogue")
                    .setEntityType(EntityType.CONTAINER_SPEC_VALUE)
                    .addCommoditySoldList(new CommoditySoldImpl()
                                                  .setCommodityType(VCPU)
                                                  .setUsed(USED)
                                                  .setCapacity(VCPU_CAPACITY))
                    .addCommoditySoldList(new CommoditySoldImpl()
                                                  .setCommodityType(VMEM)
                                                  .setUsed(USED)
                                                  .setCapacity(VMEM_CAPACITY)));


    private static final int NUM_CLONES = 5;

    private static final long TEMPLATE_ID = 123;
    private static final long HCI_TEMPLATE_ID = 2116;

    private static final ScenarioChange ADD_VM = ScenarioChange.newBuilder()
                    .setTopologyAddition(TopologyAddition.newBuilder()
                        .setAdditionCount(NUM_CLONES)
                        .setEntityId(vmId)
                        .setTargetEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                        .build())
                    .build();

    private static final ScenarioChange ADD_HOST = ScenarioChange.newBuilder()
                    .setTopologyAddition(TopologyAddition.newBuilder()
                        .setAdditionCount(NUM_CLONES)
                        .setEntityId(pmId)
                        .setTargetEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
                        .build())
                    .build();

    private static final ScenarioChange ADD_STORAGE = ScenarioChange.newBuilder()
                    .setTopologyAddition(TopologyAddition.newBuilder()
                        .setAdditionCount(NUM_CLONES)
                        .setEntityId(stId)
                        .setTargetEntityType(EntityType.STORAGE_VALUE)
                        .build())
                    .build();

    private static final ScenarioChange REPLACE = ScenarioChange.newBuilder()
                    .setTopologyReplace(TopologyReplace.newBuilder()
                            .setAddTemplateId(TEMPLATE_ID)
                            .setRemoveEntityId(pmId))
                    .build();

    private static final ScenarioChange HCI_REPLACE = ScenarioChange.newBuilder()
            .setTopologyReplace(TopologyReplace.newBuilder()
                    .setAddTemplateId(HCI_TEMPLATE_ID)
                    .setRemoveEntityId(pm2Id))
            .build();

    private static final TemplateResource INFRASTRUCTURE_RESOURCE = TemplateResource.newBuilder()
            .setCategory(ResourcesCategory.newBuilder().setName(ResourcesCategoryName.Infrastructure))
            .addFields(TemplateField.newBuilder().setName("powerSize").setValue("1.0"))
            .addFields(TemplateField.newBuilder().setName("spaceSize").setValue("1.0"))
            .addFields(TemplateField.newBuilder().setName("coolingSize").setValue("1.0"))
            .build();

    private static final TemplateResource COMPUTE_RESOURCE = TemplateResource.newBuilder()
            .setCategory(ResourcesCategory.newBuilder().setName(ResourcesCategoryName.Compute))
            .addFields(TemplateField.newBuilder().setName("numOfCores").setValue("100.0"))
            .addFields(TemplateField.newBuilder().setName("cpuSpeed").setValue("2500.0"))
            .addFields(TemplateField.newBuilder().setName("ioThroughputSize").setValue("1.024E7"))
            .addFields(TemplateField.newBuilder().setName("memorySize").setValue("1.07374182E9"))
            .addFields(TemplateField.newBuilder().setName("networkThroughputSize").setValue("1.024E7"))
            .build();

    private static final TemplateResource STORAGE_RESOURCE = TemplateResource.newBuilder()
            .setCategory(ResourcesCategory.newBuilder()
                    .setName(ResourcesCategoryName.Storage)
                    .setType("disk"))
            .addFields(TemplateField.newBuilder().setName("diskIops").setValue("5000.0"))
            .addFields(TemplateField.newBuilder().setName("diskSize").setValue("3.072E7"))
            .addFields(TemplateField.newBuilder().setName("failuresToTolerate").setValue("1.0"))
            .addFields(TemplateField.newBuilder().setName("redundancyMethod").setValue("1.0"))
            .build();

    private static final String HCI_TEMPLATE_NAME = "btc 100 cores 1TB RAM 30TB Storage";
    private static final SingleTemplateResponse SINGLE_HCI_TEMPLATE_RESPONSE =
            SingleTemplateResponse.newBuilder()
                    .setTemplate(Template.newBuilder()
                            .setId(HCI_TEMPLATE_ID)
                            .setType(Type.USER)
                            .setTemplateInfo(TemplateInfo.newBuilder()
                                    .setEntityType(EntityType.HCI_PHYSICAL_MACHINE_VALUE)
                                    .setName(HCI_TEMPLATE_NAME)
                                    .setTemplateSpecId(4)
                                    .addResources(INFRASTRUCTURE_RESOURCE)
                                    .addResources(COMPUTE_RESOURCE)
                                    .addResources(STORAGE_RESOURCE)))
                    .build();
    private static final List<GetTemplatesResponse> HCI_TEMPLATE_RESPONSE =
            ImmutableList.of(GetTemplatesResponse.newBuilder()
                    .addTemplates(SINGLE_HCI_TEMPLATE_RESPONSE)
                    .build());

    private static final List<SettingPolicy> SETTING_POLICY_RESPONSE = ImmutableList.of(
            SettingPolicy.newBuilder()
                    .setSettingPolicyType(SettingPolicy.Type.DEFAULT)
                    .setInfo(SettingPolicyInfo.newBuilder()
                            .setEntityType(EntityType.STORAGE_VALUE)
                            .setEnabled(true)
                            .setName("Storage Defaults")
                            .setDisplayName("Storage Defaults")
                            .addSettings(Setting.newBuilder()
                                    .setSettingSpecName("hciHostCapacityReservation")
                                    .setNumericSettingValue(NumericSettingValue.newBuilder()
                                            .setValue(1f))))
                    .build()
    );

    private static final ScenarioChange ADD_POD = ScenarioChange.newBuilder()
        .setTopologyAddition(TopologyAddition.newBuilder()
            .setAdditionCount(NUM_CLONES)
            .setEntityId(podId)
            .setTargetEntityType(EntityType.CONTAINER_POD_VALUE)
            .build())
        .build();

    private static final ScenarioChange ADD_WORKLOAD = ScenarioChange.newBuilder()
            .setTopologyAddition(TopologyAddition.newBuilder()
                .setAdditionCount(1)
                .setEntityId(workloadControllerId)
                .setTargetEntityType(EntityType.WORKLOAD_CONTROLLER_VALUE))
            .build();

    private IdentityProvider identityProvider = mock(IdentityProvider.class);
    private long cloneId = 1000L;

    private TemplateConverterFactory templateConverterFactory = mock(TemplateConverterFactory.class);
    private TemplateConverterFactory liveTemplateConverterFactory = null;

    private GroupServiceMole groupServiceSpy = spy(new GroupServiceMole());
    private TemplateServiceMole templateServiceRpc = spy(new TemplateServiceMole());
    private SettingPolicyServiceMole settingPolicyServiceRpc = spy(new SettingPolicyServiceMole());

    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(groupServiceSpy, templateServiceRpc,
            settingPolicyServiceRpc);

    private TopologyEditor topologyEditor;

    private GroupResolver groupResolver = mock(GroupResolver.class);
    private TemplateServiceBlockingStub templateService = null;

    private final TopologyInfo topologyInfo = TopologyInfo.newBuilder()
            .setTopologyContextId(1)
            .setTopologyId(1)
            .setCreationTime(System.currentTimeMillis())
            .setTopologyType(TopologyType.PLAN)
            .build();

    private final TopologyInfo containerClusterPlanTopologyInfo = TopologyInfo.newBuilder()
            .setTopologyContextId(2)
            .setTopologyId(2)
            .setCreationTime(System.currentTimeMillis())
            .setTopologyType(TopologyType.PLAN)
            .setPlanInfo(PlanTopologyInfo.newBuilder()
                    .setPlanType(StringConstants.OPTIMIZE_CONTAINER_CLUSTER_PLAN).build())
            .build();

    private final TopologyInfo migrateContainerWorkloadPlanTopologyInfo = TopologyInfo.newBuilder()
            .setTopologyContextId(3)
            .setTopologyId(3)
            .setCreationTime(System.currentTimeMillis())
            .setTopologyType(TopologyType.PLAN)
            .setPlanInfo(PlanTopologyInfo.newBuilder()
                .setPlanType(StringConstants.MIGRATE_CONTAINER_WORKLOADS_PLAN))
            .build();

    private final Set<Long> sourceEntities = new HashSet<>();
    private final Set<Long> destinationEntities = new HashSet<>();

    /**
     * How many times is the dummy clone id to be compared to the entity id.
     */
    private static final long CLONE_ID_MULTIPLIER = 100;

    private PlanTopologyScopeEditor planTopologyScopeEditor;

    @Before
    public void setup() throws IOException {
        templateService = TemplateServiceGrpc.newBlockingStub(grpcServer.getChannel());
        when(identityProvider.getCloneId(any(TopologyEntityView.class)))
            .thenAnswer(invocation -> cloneId++);
        context = new TopologyPipelineContext(topologyInfo);
        containerClusterPlanContext = new TopologyPipelineContext(containerClusterPlanTopologyInfo);
        migrateContainerWorkloadPlanContext =
                new TopologyPipelineContext(migrateContainerWorkloadPlanTopologyInfo);
        topologyEditor = new TopologyEditor(identityProvider,
                templateConverterFactory,
                GroupServiceGrpc.newBlockingStub(grpcServer.getChannel()));
        scope = PlanScope.newBuilder().build();
        planTopologyScopeEditor = new PlanTopologyScopeEditor(
                GroupServiceGrpc.newBlockingStub(grpcServer.getChannel()));
        this.liveTemplateConverterFactory = new TemplateConverterFactory(
                templateService, identityProvider,
                SettingPolicyServiceGrpc.newBlockingStub(grpcServer.getChannel()),
                CpuCapacityServiceGrpc.newBlockingStub(grpcServer.getChannel()));
    }

    /**
     * Tests expandAndFlattenReferences in the context of an empty cluster. Verifies that an empty
     * cluster resolves to an empty OID set, as opposed to all OIDs in the {@link TopologyGraph}.
     *
     * @throws Exception If {@link GroupResolver} fails to resolve a group
     */
    @Test
    public void testResolveEmptyCluster() throws Exception {
        final long clusterOid = 1L;
        final List<MigrationReference> clusters = Lists.newArrayList(
                MigrationReference.newBuilder()
                        .setGroupType(GroupType.COMPUTE_VIRTUAL_MACHINE_CLUSTER_VALUE)
                        .setOid(clusterOid)
                        .build());

        final Grouping emptyCluster = Grouping.newBuilder()
                .setDefinition(GroupDefinition.newBuilder().setStaticGroupMembers(
                        GroupDTO.StaticMembers.getDefaultInstance()).build())
                .setId(clusterOid)
                .build();
        when(groupResolver.resolve(eq(emptyCluster), isA(TopologyGraph.class)))
                .thenReturn(emptyResolvedGroup(emptyCluster, ApiEntityType.PHYSICAL_MACHINE));

        final Map<Long, Grouping> groupIdToGroupMap = new HashMap<Long, Grouping>() {{
            put(clusterOid, emptyCluster);
        }};
        final TopologyEntity.Builder pmEntity = TopologyEntityUtils
                .topologyEntity(pmId, 0, 0, "PM", EntityType.PHYSICAL_MACHINE);
        final TopologyGraph<TopologyEntity> graph = TopologyEntityUtils.pojoGraphOf(pmEntity);
        final Set<Long> migratingEntities = topologyEditor.expandAndFlattenReferences(
                clusters,
                EntityType.VIRTUAL_MACHINE_VALUE,
                groupIdToGroupMap,
                groupResolver,
                graph);

        assertTrue(migratingEntities.isEmpty());
    }

    /**
     * Test migrating entities in a plan.
     *
     * @throws Exception To satisfy compiler.
     */
    //@Test
    // TODO: Fix this later.
    public void testTopologyMigrationVMClone() throws Exception {
        final long migrationSourceGroupId = 1000;
        final long migrationDestinationGroupId = 2000;
        final MigrationReference migrationSourceGroup = MigrationReference.newBuilder()
                .setOid(migrationSourceGroupId)
                .setGroupType(CommonDTOREST.GroupDTO.GroupType.REGULAR.getValue()).build();
        final MigrationReference migrationDestinationGroup = MigrationReference.newBuilder()
                .setOid(migrationDestinationGroupId)
                .setGroupType(CommonDTOREST.GroupDTO.GroupType.REGULAR.getValue()).build();

        final long pmId = 2;
        final long pmCloneId = pmId * CLONE_ID_MULTIPLIER;
        final long vmId = 3;
        final long vmCloneId = vmId * CLONE_ID_MULTIPLIER;
        final ScenarioChange migrateVm = ScenarioChange.newBuilder()
                .setTopologyMigration(TopologyMigration.newBuilder()
                        .addSource(migrationSourceGroup)
                        .addDestination(migrationDestinationGroup)
                        .setDestinationEntityType(TopologyMigration.DestinationEntityType.VIRTUAL_MACHINE)
                        .build())
                .build();

        final Grouping sourceGroup = Grouping.newBuilder()
                .setDefinition(GroupDefinition.newBuilder().setStaticGroupMembers(
                        GroupDTO.StaticMembers.newBuilder().addMembersByType(
                                GroupDTO.StaticMembers.StaticMembersByType.newBuilder().addMembers(vmId).build()
                        ).build()
                ))
                .setId(migrationSourceGroupId)
                .build();
        final long regionId = 11;
        final Grouping destinationGroup = Grouping.newBuilder()
                .setDefinition(GroupDefinition.newBuilder().setStaticGroupMembers(
                        GroupDTO.StaticMembers.newBuilder().addMembersByType(
                                GroupDTO.StaticMembers.StaticMembersByType.newBuilder().addMembers(regionId).build()
                        ).build()
                ))
                .setId(migrationDestinationGroupId)
                .build();
        final TopologyEntity.Builder pmEntity = TopologyEntityUtils
                .topologyEntity(pmId, 0, 0, "PM", EntityType.PHYSICAL_MACHINE);
        // Volumes/Storages
        final long vv1Id = 4;
        final long vv1CloneId = vv1Id * CLONE_ID_MULTIPLIER;
        final long s1Id = 5;
        final long s1CloneId = s1Id * CLONE_ID_MULTIPLIER;
        final long vv2Id = 6;
        final long vv2CloneId = vv2Id * CLONE_ID_MULTIPLIER;
        final long s2Id = 7;
        final long s2CloneId = s2Id * CLONE_ID_MULTIPLIER;
        final TopologyEntity.Builder storageEntity1 = TopologyEntityUtils
                .topologyEntity(s1Id, 0, 0, "S1", EntityType.STORAGE);
        final TopologyEntity.Builder volumeEntity1 = TopologyEntityUtils
                .topologyEntity(vv1Id, 0, 0, "VV1", EntityType.VIRTUAL_VOLUME, s1Id);
        final TopologyEntity.Builder storageEntity2 = TopologyEntityUtils
                .topologyEntity(s2Id, 0, 0, "S2", EntityType.STORAGE);
        final TopologyEntity.Builder volumeEntity2 = TopologyEntityUtils
                .topologyEntity(vv2Id, 0, 0, "VV2", EntityType.VIRTUAL_VOLUME, s2Id);
        final TopologyEntity.Builder vmEntity = TopologyEntityUtils
                .topologyEntity(vmId, 0, 0, "VM", EntityType.VIRTUAL_MACHINE, pmId, vv1Id, vv2Id);
        vmEntity.addProvider(pmEntity);

        // region connected VM
        final long vmToRemoveFromTargetRegionId = 8;
        final TopologyEntity.Builder regionConnectedEntityToRemove = TopologyEntityUtils
                .topologyEntity(vmToRemoveFromTargetRegionId, 0, 0, "regionVM", EntityType.VIRTUAL_MACHINE);
        ConnectedEntityView regionConnectedEntity = new ConnectedEntityImpl()
                .setConnectedEntityId(regionId)
                .setConnectedEntityType(EntityType.REGION_VALUE)
                .setConnectionType(ConnectionType.AGGREGATED_BY_CONNECTION);
        regionConnectedEntityToRemove.getTopologyEntityImpl().addConnectedEntityList(regionConnectedEntity);

        final long zoneId = 9;
        final TopologyEntity.Builder zone = TopologyEntityUtils
                .topologyEntity(zoneId, 0, 0, "zone", EntityType.AVAILABILITY_ZONE);

        final long vmToRemoveFromZoneId = 10;
        final TopologyEntity.Builder zoneConnectedEntityToRemove = TopologyEntityUtils
                .topologyEntity(vmToRemoveFromZoneId, 0, 0, "zoneVM", EntityType.VIRTUAL_MACHINE);
        ConnectedEntityView zoneConnectedEntityAggregatedBy = new ConnectedEntityImpl()
                .setConnectedEntityId(zoneId)
                .setConnectedEntityType(EntityType.AVAILABILITY_ZONE_VALUE)
                .setConnectionType(ConnectedEntity.ConnectionType.AGGREGATED_BY_CONNECTION);
        zoneConnectedEntityToRemove.getTopologyEntityImpl().addConnectedEntityList(zoneConnectedEntityAggregatedBy);

        ConnectedEntityView zoneConnectedEntityOwns = new ConnectedEntityImpl()
                .setConnectedEntityId(zoneId)
                .setConnectedEntityType(EntityType.AVAILABILITY_ZONE_VALUE)
                .setConnectionType(ConnectedEntity.ConnectionType.OWNS_CONNECTION);

        final TopologyEntity.Builder region = TopologyEntityUtils
                .topologyEntity(regionId, 0, 0, "region", EntityType.REGION);
        region.getTopologyEntityImpl().addConnectedEntityList(zoneConnectedEntityOwns);

        final Map<Long, TopologyEntity.Builder> topology = new HashMap<>();
        topology.put(pmId, pmEntity);
        topology.put(vmId, vmEntity);
        topology.put(vv1Id, volumeEntity1);
        topology.put(s1Id, storageEntity1);
        topology.put(vv2Id, volumeEntity2);
        topology.put(s2Id, storageEntity2);

        topology.put(regionId, region);
        topology.put(zoneId, zone);
        topology.put(vmToRemoveFromTargetRegionId, regionConnectedEntityToRemove);
        topology.put(vmToRemoveFromZoneId, zoneConnectedEntityToRemove);

        when(groupServiceSpy.getGroups(any())).thenReturn(Lists.newArrayList(sourceGroup, destinationGroup));
        when(groupResolver.resolve(eq(sourceGroup), isA(TopologyGraph.class)))
                .thenReturn(resolvedGroup(sourceGroup, ApiEntityType.VIRTUAL_MACHINE, vmId));
        when(groupResolver.resolve(eq(destinationGroup), isA(TopologyGraph.class)))
                .thenReturn(resolvedGroup(destinationGroup, ApiEntityType.REGION, regionId));
        // Make sure the right clone id is returned when a specific entity build is being cloned.
        ImmutableSet.of(vmCloneId, pmCloneId, vv1CloneId, s1CloneId, vv2CloneId, s2CloneId)
                .forEach(cloneId -> {
                    when(identityProvider.getCloneId(argThat(new EntityDtoMatcher(cloneId))))
                            .thenReturn(cloneId);
                });

        assertEquals(10, topology.size());
        topologyEditor.editTopology(topology, scope, Lists.newArrayList(migrateVm),
                                    context, groupResolver, sourceEntities, destinationEntities);

        // TODO: rework needed here, no longer using clones. Scoping for MCP not the same as OCP.
        assertEquals(10, topology.size());

        assertTrue(topology.containsKey(vmId));
        assertTrue(topology.containsKey(vmCloneId));
        assertTrue(topology.containsKey(pmId));
        assertTrue(topology.containsKey(pmCloneId));

        assertTrue(topology.containsKey(s1CloneId));
        assertTrue(topology.containsKey(s2CloneId));

        assertTrue(regionConnectedEntityToRemove.getTopologyEntityImpl().hasEdit()
                && regionConnectedEntityToRemove.getTopologyEntityImpl().getEdit().hasRemoved());
        assertTrue(zoneConnectedEntityToRemove.getTopologyEntityImpl().hasEdit()
                && zoneConnectedEntityToRemove.getTopologyEntityImpl().getEdit().hasRemoved());

        // Verify that the cloned source VM id has been added to the addedEntityOids.
        Collection<Long> clonedSourceVms = sourceEntities;
        assertFalse(clonedSourceVms.isEmpty());
        assertTrue(clonedSourceVms.contains(vmCloneId));
    }

    /**
     * Helper class to enable matching of entity DTO builders with the associated clone ids.
     */
    static class EntityDtoMatcher extends ArgumentMatcher<TopologyEntityView> {
        /**
         * Id of the cloned entity.
         */
        private final long cloneId;

        /**
         * Makes up a matcher.
         *
         * @param cloneId Id of cloned entity.
         */
        EntityDtoMatcher(final long cloneId) {
            this.cloneId = cloneId;
        }

        /**
         * Used to match an entity DTO builder with the cloneId for this matcher.
         *
         * @param input TopologyEntityImpl instance.
         * @return Whether builder's id matches the clone id of this matcher, goes by the
         * assumption that entityId x multiple = cloneId.
         */
        @Override
        public boolean matches(Object input) {
            if (!(input instanceof TopologyEntityImpl)) {
                return false;
            }
            TopologyEntityImpl dtoBuilder = (TopologyEntityImpl)input;
            return dtoBuilder.getOid() * CLONE_ID_MULTIPLIER == cloneId;
        }
    }

    /**
     * Test adding entities in a plan.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testTopologyAdditionVMClone() throws Exception {
        Map<Long, TopologyEntity.Builder> topology = Stream.of(vm, pm, st)
                .collect(Collectors.toMap(TopologyEntity.Builder::getOid, Function.identity()));
        List<ScenarioChange> changes = Lists.newArrayList(ADD_VM);

        topologyEditor.editTopology(topology, scope, changes, context,
                                    groupResolver, sourceEntities, destinationEntities);

        List<TopologyEntity.Builder> clones = topology.values().stream()
                .filter(entity -> entity.getOid() != vm.getOid())
                .filter(entity -> entity.getEntityType() == vm.getEntityType())
                .collect(Collectors.toList());
        clones.remove(vm);
        assertEquals(NUM_CLONES, clones.size());

        // Verify display names are (e.g.) "VM - Clone #123"
        List<String> names = clones.stream().map(TopologyEntity.Builder::getDisplayName)
                .collect(Collectors.toList());
        names.sort(String::compareTo);
        IntStream.range(0, NUM_CLONES).forEach(i -> {
            assertEquals(names.get(i), vm.getDisplayName() + " - Clone #" + i);
        });

        TopologyEntityImpl oneClone = clones.get(0).getTopologyEntityImpl();
        // clones are unplaced - all provider IDs are negative
        boolean allNegative = oneClone.getCommoditiesBoughtFromProvidersList()
                .stream()
                .map(CommoditiesBoughtFromProviderView::getProviderId)
                .allMatch(key -> key < 0);
        assertTrue(allNegative);

        // "oldProviders" entry in EntityPropertyMap captures the right placement
        @SuppressWarnings("unchecked")
        Map<Long, Long> oldProvidersMap = TopologyDTOUtil.parseOldProvidersMap(oneClone, new Gson());
        for (CommoditiesBoughtFromProviderView cloneCommBought :
                oneClone.getCommoditiesBoughtFromProvidersList()) {
            long oldProvider = oldProvidersMap.get(cloneCommBought.getProviderId());
            CommoditiesBoughtFromProviderView vmCommBought =
                    vm.getTopologyEntityImpl().getCommoditiesBoughtFromProvidersList().stream()
                            .filter(bought -> bought.getProviderId() == oldProvider)
                            .findAny().get();
            assertEquals(cloneCommBought.getCommodityBoughtList(),
                    vmCommBought.getCommodityBoughtList());
        }
        // Assert that the commodity sold usages are the same.
        assertEquals(vm.getTopologyEntityImpl().getCommoditySoldListList(), oneClone.getCommoditySoldListList());
        assertTrue(oneClone.getAnalysisSettings().getShopTogether());
    }

    /**
     * Test adding host and storages in a plan.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testTopologyAdditionHostAndStorageClones() throws Exception {
        Map<Long, TopologyEntity.Builder> topology = Stream.of(vm, pm, st)
                        .collect(Collectors.toMap(TopologyEntity.Builder::getOid, Function.identity()));

        // Add hosts and storages
        List<ScenarioChange> changes = Lists.newArrayList(ADD_HOST);
        changes.addAll(Lists.newArrayList(ADD_STORAGE));

        topologyEditor.editTopology(topology, scope, changes, context,
                                    groupResolver, sourceEntities, destinationEntities);
        List<TopologyEntity.Builder> pmClones = topology.values().stream()
                        .filter(entity -> entity.getOid() != pm.getOid())
                        .filter(entity -> entity.getEntityType() == pm.getEntityType())
                        .collect(Collectors.toList());

        List<TopologyEntity.Builder> storageClones = topology.values().stream()
                        .filter(entity -> entity.getOid() != st.getOid())
                        .filter(entity -> entity.getEntityType() == st.getEntityType())
                        .collect(Collectors.toList());

        assertEquals(NUM_CLONES, pmClones.size());
        assertEquals(NUM_CLONES, storageClones.size());

        // Verify display names are (e.g.) "PM - Clone #123"
        List<String> pmNames = pmClones.stream().map(TopologyEntity.Builder::getDisplayName)
                        .collect(Collectors.toList());
        pmNames.sort(String::compareTo);
        IntStream.range(0, NUM_CLONES).forEach(i -> {
            assertEquals(pmNames.get(i), pm.getDisplayName() + " - Clone #" + i);
        });

        List<String> storageNames = storageClones.stream().map(TopologyEntity.Builder::getDisplayName)
                        .collect(Collectors.toList());
        pmNames.sort(String::compareTo);
        IntStream.range(0, NUM_CLONES).forEach(i -> {
            assertEquals(storageNames.get(i), st.getDisplayName() + " - Clone #" + i);
        });

        // clones are unplaced - all provider IDs are negative
        Stream.concat(pmClones.stream(), storageClones.stream()).forEach(clone -> {
            boolean allNegative = clone.getTopologyEntityImpl()
                            .getCommoditiesBoughtFromProvidersList().stream()
                            .map(CommoditiesBoughtFromProviderView::getProviderId)
                            .allMatch(key -> key < 0);
            assertTrue(allNegative);
        });

        // Check if pm clones' datastore commodity is added and its access set to
        // original entity's accessed storage.
        Set<Long> connectedStorages = storageClones.stream().map(clone -> clone.getOid()).collect(Collectors.toSet());
        connectedStorages.add(stId);
        assertEquals(6, connectedStorages.size());
        pmClones.stream()
            .forEach(pmClone -> {
               List<CommoditySoldView> bicliqueCommList = pmClone.getTopologyEntityImpl().getCommoditySoldListList().stream()
                    .filter(commSold -> AnalysisUtil.DSPM_OR_DATASTORE.contains(commSold.getCommodityType().getType()))
                    .collect(Collectors.toList());
               Set<Long> connectedStoragesFound = new HashSet<>();
               // For each PM : 1 initial storage and 5 new storages that were cloned
               assertEquals(6, bicliqueCommList.size());
               bicliqueCommList.stream().forEach(comm -> {
                   assertEquals(DATASTORE.getType(), comm.getCommodityType().getType());
                   // add storage id we find access to
                   connectedStoragesFound.add(comm.getAccesses());
               });
               // Each clone should be connected to all storages
               assertTrue(connectedStoragesFound.equals(connectedStorages));
            });

        // Check if storage clones' DSPM commodity is added and its access set to
        // original entity's accessed host.
        Set<Long> connectedHosts = pmClones.stream().map(clone -> clone.getOid()).collect(Collectors.toSet());
        connectedHosts.add(pmId);
        assertEquals(6, connectedHosts.size());
        storageClones.stream()
            .forEach(stClone -> {
               List<CommoditySoldView> bicliqueCommList = stClone.getTopologyEntityImpl().getCommoditySoldListList().stream()
                    .filter(commSold -> AnalysisUtil.DSPM_OR_DATASTORE.contains(commSold.getCommodityType().getType()))
                    .collect(Collectors.toList());
               Set<Long> connectedHostsFound = new HashSet<>();
               // For each Storage : 1 initial host and 5 new hosts that were cloned
               assertEquals(6, bicliqueCommList.size());
               bicliqueCommList.stream().forEach(comm -> {
                   assertEquals(DSPM.getType(), comm.getCommodityType().getType());
                   // add host id we find access to
                   connectedHostsFound.add(comm.getAccesses());
               });
               // Each clone should be connected to all hosts
               assertTrue(connectedHostsFound.equals(connectedHosts));
            });

    }

    // test add PM when user choose from a host cluster
    @Test
    public void testAddPMFromHostGroup() throws Exception {
        final long groupId = 1L;
        final long pmId = 2L;
        final long vmId = 3L;
        final long pmCloneId = 200L;
        final Grouping group = Grouping.newBuilder()
            .setDefinition(GroupDefinition.getDefaultInstance())
            .setId(groupId)
            .build();
        final TopologyEntity.Builder pmEntity = TopologyEntityUtils
            .topologyEntity(pmId, 0, 0, "PM", EntityType.PHYSICAL_MACHINE);
        final TopologyEntity.Builder vmEntity = TopologyEntityUtils
            .topologyEntity(vmId, 0, 0, "VM", EntityType.VIRTUAL_MACHINE, pmId);
        final Map<Long, TopologyEntity.Builder> topology = new HashMap<>();
        topology.put(pmId, pmEntity);
        topology.put(vmId, vmEntity);
        final TopologyGraph<TopologyEntity> graph = TopologyEntityUtils.pojoGraphOf(pmEntity, vmEntity);
        final List<ScenarioChange> changes = Collections.singletonList(ScenarioChange.newBuilder()
            .setTopologyAddition(TopologyAddition.newBuilder().setGroupId(groupId).setTargetEntityType(14))
            .build());
        when(groupServiceSpy.getGroups(any())).thenReturn(Collections.singletonList(group));
        when(groupResolver.resolve(eq(group), isA(TopologyGraph.class)))
            .thenReturn(resolvedGroup(group, ApiEntityType.PHYSICAL_MACHINE, pmId));
        when(identityProvider.getCloneId(any(TopologyEntityView.class))).thenReturn(pmCloneId);
        topologyEditor.editTopology(topology, scope, changes, context, groupResolver, sourceEntities, destinationEntities);

        assertTrue(topology.containsKey(pmCloneId));
        final TopologyEntity.Builder cloneBuilder = topology.get(pmCloneId);
        assertThat(cloneBuilder.getDisplayName(), Matchers.containsString(pmEntity.getDisplayName()));
        assertThat(cloneBuilder.getTopologyEntityImpl().getOrigin().getPlanScenarioOrigin(),
            is(new PlanScenarioOriginImpl()
                .setPlanId(topologyInfo.getTopologyContextId())
                    .setOriginalEntityId(pmId)
                    .addAllOriginalEntityDiscoveringTargetIds(Collections.singletonList(0L))));

    }

    private ResolvedGroup resolvedGroup(Grouping group, ApiEntityType type, long member) {
        return new ResolvedGroup(group, Collections.singletonMap(type, Collections.singleton(member)));
    }

    private ResolvedGroup emptyResolvedGroup(Grouping group, ApiEntityType type) {
        return new ResolvedGroup(group, Collections.singletonMap(type, Sets.newHashSet()));
    }

    // test remove PM when user choose from a host cluster
    @Test
    public void testRemovePMFromHostGroup() throws Exception {
        final long groupId = 1L;
        final long pmId = 2L;
        final long vmId = 3L;
        final Grouping group = Grouping.newBuilder()
            .setDefinition(GroupDefinition.getDefaultInstance())
            .setId(groupId)
            .build();
        final TopologyEntity.Builder pmEntity = TopologyEntityUtils
            .topologyEntity(pmId, 0, 0, "PM", EntityType.PHYSICAL_MACHINE);
        final TopologyEntity.Builder vmEntity = TopologyEntityUtils
            .topologyEntity(vmId, 0, 0, "VM", EntityType.VIRTUAL_MACHINE, pmId);
        final Map<Long, TopologyEntity.Builder> topology = new HashMap<>();
        topology.put(pmId, pmEntity);
        topology.put(vmId, vmEntity);
        final TopologyGraph<TopologyEntity> graph = TopologyEntityUtils.pojoGraphOf(pmEntity, vmEntity);
        final List<ScenarioChange> changes = Collections.singletonList(ScenarioChange.newBuilder()
            .setTopologyRemoval(TopologyRemoval.newBuilder().setGroupId(groupId).setTargetEntityType(14))
            .build());
        when(groupServiceSpy.getGroups(any())).thenReturn(Collections.singletonList(group));
        when(groupResolver.resolve(eq(group), isA(TopologyGraph.class)))
            .thenReturn(resolvedGroup(group, ApiEntityType.PHYSICAL_MACHINE, pmId));
        topologyEditor.editTopology(topology, scope, changes,
                                    context, groupResolver, sourceEntities, destinationEntities);

        TopologyEntity.Builder pmToRemove = topology.get(pmId);
        assertTrue(pmToRemove.getTopologyEntityImpl().hasEdit()
            && pmToRemove.getTopologyEntityImpl().getEdit().hasRemoved());

    }

    // test add storage when user choose from a storage cluster
    @Test
    public void testAddSTFromSTGroup() throws Exception {
        final long groupId = 1L;
        final long stId = 2L;
        final long vmId = 3L;
        final long stCloneId = 200L;
        final Grouping group = Grouping.newBuilder()
            .setDefinition(GroupDefinition.getDefaultInstance())
            .setId(groupId)
            .build();
        final TopologyEntity.Builder stEntity = TopologyEntityUtils
            .topologyEntity(stId, 0, 0, "ST", EntityType.STORAGE);
        final TopologyEntity.Builder vmEntity = TopologyEntityUtils
            .topologyEntity(vmId, 0, 0, "VM", EntityType.VIRTUAL_MACHINE, stId);
        final Map<Long, TopologyEntity.Builder> topology = new HashMap<>();
        topology.put(stId, stEntity);
        topology.put(vmId, vmEntity);
        final TopologyGraph<TopologyEntity> graph = TopologyEntityUtils.pojoGraphOf(stEntity, vmEntity);
        final List<ScenarioChange> changes = Collections.singletonList(ScenarioChange.newBuilder()
            .setTopologyAddition(TopologyAddition.newBuilder().setGroupId(groupId).setTargetEntityType(2))
            .build());
        when(groupServiceSpy.getGroups(any())).thenReturn(Collections.singletonList(group));
        when(groupResolver.resolve(eq(group), isA(TopologyGraph.class)))
            .thenReturn(resolvedGroup(group, ApiEntityType.STORAGE, stId));
        when(identityProvider.getCloneId(any(TopologyEntityView.class))).thenReturn(stCloneId);
        topologyEditor.editTopology(topology, scope, changes, context, groupResolver, sourceEntities, destinationEntities);

        assertTrue(topology.containsKey(stCloneId));
        final TopologyEntity.Builder cloneBuilder = topology.get(stCloneId);
        assertThat(cloneBuilder.getDisplayName(), Matchers.containsString(stEntity.getDisplayName()));
        assertThat(cloneBuilder.getTopologyEntityImpl().getOrigin().getPlanScenarioOrigin(),
                is(new PlanScenarioOriginImpl()
                        .setPlanId(topologyInfo.getTopologyContextId())
                        .setOriginalEntityId(stId)
                        .addAllOriginalEntityDiscoveringTargetIds(Collections.singletonList(0L))));

    }

    // test remove storage when user choose from a storage cluster
    @Test
    public void testRemoveSTFromSTGroup() throws Exception {
        final long groupId = 1L;
        final long stId = 2L;
        final long vmId = 3L;
        final Grouping group = Grouping.newBuilder()
            .setDefinition(GroupDefinition.getDefaultInstance())
            .setId(groupId)
            .build();
        final TopologyEntity.Builder stEntity = TopologyEntityUtils
            .topologyEntity(stId, 0, 0, "ST", EntityType.STORAGE);
        final TopologyEntity.Builder vmEntity = TopologyEntityUtils
            .topologyEntity(vmId, 0, 0, "VM", EntityType.VIRTUAL_MACHINE, stId);
        final Map<Long, TopologyEntity.Builder> topology = new HashMap<>();
        topology.put(stId, stEntity);
        topology.put(vmId, vmEntity);
        final TopologyGraph<TopologyEntity> graph = TopologyEntityUtils.pojoGraphOf(stEntity, vmEntity);
        final List<ScenarioChange> changes = Collections.singletonList(ScenarioChange.newBuilder()
            .setTopologyRemoval(TopologyRemoval.newBuilder().setGroupId(groupId).setTargetEntityType(2))
            .build());
        when(groupServiceSpy.getGroups(any())).thenReturn(Collections.singletonList(group));
        when(groupResolver.resolve(eq(group), isA(TopologyGraph.class)))
            .thenReturn(resolvedGroup(group, ApiEntityType.STORAGE, stId));
        topologyEditor.editTopology(topology, scope, changes,
                                    context, groupResolver, sourceEntities, destinationEntities);

        TopologyEntity.Builder stToRemove = topology.get(stId);
        assertTrue(stToRemove.getTopologyEntityImpl().hasEdit()
            && stToRemove.getTopologyEntityImpl().getEdit().hasRemoved());

    }

    // test add VM when user choose from a host cluster
    @Test
    public void testAddVMFromHostGroup() throws Exception {
        final long groupId = 1L;
        final long vmId = 2L;
        final long hostId = 3L;
        final long vmCloneId = 100L;
        final Grouping group = Grouping.newBuilder()
            .setDefinition(GroupDefinition.getDefaultInstance())
            .setId(groupId)
            .build();
        final TopologyEntity.Builder hostEntity = TopologyEntityUtils
                .topologyEntity(hostId, 0, 0, "PM", EntityType.PHYSICAL_MACHINE);
        final TopologyEntity.Builder vmEntity = TopologyEntityUtils
                .topologyEntity(vmId, 0, 0, "VM", EntityType.VIRTUAL_MACHINE, hostId);
        final Map<Long, TopologyEntity.Builder> topology = new HashMap<>();
        topology.put(vmId, vmEntity);
        topology.put(hostId, hostEntity);
        final TopologyGraph<TopologyEntity> graph = TopologyEntityUtils.pojoGraphOf(hostEntity, vmEntity);

        final List<ScenarioChange> changes = Collections.singletonList(ScenarioChange.newBuilder()
                .setTopologyAddition(TopologyAddition.newBuilder().setGroupId(groupId).setTargetEntityType(10))
                .build());
        when(groupServiceSpy.getGroups(any())).thenReturn(Collections.singletonList(group));
        when(groupResolver.resolve(eq(group), isA(TopologyGraph.class)))
            .thenReturn(resolvedGroup(group, ApiEntityType.PHYSICAL_MACHINE, hostId));
        when(identityProvider.getCloneId(any(TopologyEntityView.class))).thenReturn(vmCloneId);

        topologyEditor.editTopology(topology, scope, changes,
                                    context, groupResolver, sourceEntities, destinationEntities);

        assertTrue(topology.containsKey(vmCloneId));
        final TopologyEntity.Builder cloneBuilder = topology.get(vmCloneId);
        assertThat(cloneBuilder.getDisplayName(), Matchers.containsString(vmEntity.getDisplayName()));
        assertThat(cloneBuilder.getTopologyEntityImpl().getOrigin().getPlanScenarioOrigin(),
            is(new PlanScenarioOriginImpl()
                .setPlanId(topologyInfo.getTopologyContextId())
                    .setOriginalEntityId(vmId)
                    .addAllOriginalEntityDiscoveringTargetIds(Collections.singletonList(0L))));
    }

    // test remove VM when user choose from a host cluster
    @Test
    public void testRemoveVMFromHostGroup() throws Exception {
        final long groupId = 1L;
        final long vmId = 2L;
        final long hostId = 3L;
        final Grouping group = Grouping.newBuilder()
            .setDefinition(GroupDefinition.getDefaultInstance())
            .setId(groupId)
            .build();
        final TopologyEntity.Builder hostEntity = TopologyEntityUtils
            .topologyEntity(hostId, 0, 0, "PM", EntityType.PHYSICAL_MACHINE);
        final TopologyEntity.Builder vmEntity = TopologyEntityUtils
            .topologyEntity(vmId, 0, 0, "VM", EntityType.VIRTUAL_MACHINE, hostId);
        final Map<Long, TopologyEntity.Builder> topology = new HashMap<>();
        topology.put(vmId, vmEntity);
        topology.put(hostId, hostEntity);
        final TopologyGraph<TopologyEntity> graph = TopologyEntityUtils.pojoGraphOf(hostEntity, vmEntity);

        final List<ScenarioChange> changes = Collections.singletonList(ScenarioChange.newBuilder()
            .setTopologyRemoval(TopologyRemoval.newBuilder().setGroupId(groupId).setTargetEntityType(10))
            .build());
        when(groupServiceSpy.getGroups(any())).thenReturn(Collections.singletonList(group));
        when(groupResolver.resolve(eq(group), isA(TopologyGraph.class)))
            .thenReturn(resolvedGroup(group, ApiEntityType.PHYSICAL_MACHINE, hostId));
        topologyEditor.editTopology(topology, scope, changes,
                                    context, groupResolver, sourceEntities, destinationEntities);

        TopologyEntity.Builder vmToRemove = topology.get(vmId);
        assertTrue(vmToRemove.getTopologyEntityImpl().hasEdit()
            && vmToRemove.getTopologyEntityImpl().getEdit().hasRemoved());
    }

    /**
     * Test that any consumer daemon pod is removed from the topology when the hosting VM
     * is removed by the edit.
     * */
    @Test
    public void testDaemonPodsRemovalWhenProviderVMRemoved() throws Exception {
        // Create a test pod that is marked as daemon and is consumer of our test VM
        final TopologyEntity.Builder daemonPod = TopologyEntityUtils.topologyEntityBuilder(
                new TopologyEntityImpl()
                        .setOid(51L)
                        .setDisplayName("DaemonPod")
                        .setEntityType(EntityType.CONTAINER_POD_VALUE)
                        .addCommoditiesBoughtFromProviders(new CommoditiesBoughtFromProviderImpl()
                                .setProviderId(vmId)
                                .addCommodityBought(new CommodityBoughtImpl().setCommodityType(VMEM)
                                        .setUsed(USED))
                                .addCommodityBought(new CommodityBoughtImpl().setCommodityType(VCPU)
                                        .setUsed(USED)))
                        .setAnalysisSettings(new AnalysisSettingsImpl()
                                .setDaemon(true)));
        final TopologyEntity.Builder daemonPodContainer =
                TopologyEntityUtils.topologyEntityBuilder(new TopologyEntityImpl()
                        .setOid(52L)
                        .setDisplayName("daemonPodContainer")
                        .setEntityType(EntityType.CONTAINER_VALUE)
                        .setAnalysisSettings(
                                new AnalysisSettingsImpl()
                                        .setSuspendable(false)
                                        .setControllable(false)
                        )
                        .addCommoditiesBoughtFromProviders(
                                new CommoditiesBoughtFromProviderImpl()
                                        .setProviderId(daemonPod.getOid())
                                        .addCommodityBought(new CommodityBoughtImpl()
                                                .setCommodityType(VMEM)
                                                .setUsed(USED))
                                        .addCommodityBought(new CommodityBoughtImpl()
                                                .setCommodityType(VCPU)
                                                .setUsed(USED)))
                        .addCommoditySoldList(new CommoditySoldImpl()
                                .setCommodityType(VCPU)
                                .setUsed(USED)
                                .setCapacity(VCPU_CAPACITY))
                        .addCommoditySoldList(new CommoditySoldImpl()
                                .setCommodityType(VMEM)
                                .setUsed(USED)
                                .setCapacity(VMEM_CAPACITY)));

        final TopologyEntity.Builder daemonPodApp = TopologyEntity.newBuilder(
                new TopologyEntityImpl()
                .setOid(53L)
                .setDisplayName("daemonPodApp")
                .setEntityType(EntityType.APPLICATION_COMPONENT_VALUE)
                .addCommoditiesBoughtFromProviders(new CommoditiesBoughtFromProviderImpl()
                        .setProviderId(daemonPodContainer.getOid())
                        .addCommodityBought(new CommodityBoughtImpl()
                                .setCommodityType(new CommodityTypeImpl()
                                        .setType(CommodityDTO.CommodityType.VCPU_VALUE))
                                .setUsed(50))
                        .addCommodityBought(new CommodityBoughtImpl()
                                .setCommodityType(new CommodityTypeImpl()
                                        .setType(CommodityDTO.CommodityType.VMEM_VALUE))
                                .setUsed(50))
                ));
        final TopologyEntity.Builder app = TopologyEntity.newBuilder(
                new TopologyEntityImpl()
                        .setOid(54L)
                        .setDisplayName("app")
                        .setEntityType(EntityType.APPLICATION_COMPONENT_VALUE)
                        .addCommoditiesBoughtFromProviders(new CommoditiesBoughtFromProviderImpl()
                                .setProviderId(container.getOid())
                                .addCommodityBought(new CommodityBoughtImpl()
                                        .setCommodityType(new CommodityTypeImpl()
                                                .setType(CommodityDTO.CommodityType.VCPU_VALUE))
                                        .setUsed(50))
                                .addCommodityBought(new CommodityBoughtImpl()
                                        .setCommodityType(new CommodityTypeImpl()
                                                .setType(CommodityDTO.CommodityType.VMEM_VALUE))
                                        .setUsed(50))
                        ));
        final TopologyEntity.Builder service = TopologyEntity.newBuilder(
                new TopologyEntityImpl()
                        .setOid(55L)
                        .setDisplayName("Service")
                        .setEntityType(EntityType.SERVICE_VALUE)
                        .setEntityState(TopologyDTO.EntityState.POWERED_ON)
                        .addCommoditiesBoughtFromProviders(new CommoditiesBoughtFromProviderImpl()
                                .setProviderId(daemonPodApp.getOid())
                                .addCommodityBought(new CommodityBoughtImpl()
                                        .setCommodityType(new CommodityTypeImpl()
                                                .setType(CommodityDTO.CommodityType.APPLICATION_VALUE)))
                                .setProviderId(app.getOid())
                                .addCommodityBought(new CommodityBoughtImpl()
                                        .setCommodityType(new CommodityTypeImpl()
                                                .setType(CommodityDTO.CommodityType.APPLICATION_VALUE)))
                        ));

        Map<Long, TopologyEntity.Builder> topology = Stream.of(service, daemonPodApp, app, container, daemonPodContainer, pod, daemonPod, vm)
                .collect(Collectors.toMap(TopologyEntity.Builder::getOid, Function.identity()));
        List<ScenarioChange> changes = Collections.singletonList(ScenarioChange.newBuilder()
                .setTopologyRemoval(TopologyRemoval.newBuilder()
                        .setEntityId(vmId)
                        .setTargetEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                        .build())
                .build());

        topologyEditor.editTopology(topology, scope, changes,
                                    context, groupResolver, sourceEntities, destinationEntities);

        assertTrue(vm.getTopologyEntityImpl().hasEdit()
                && vm.getTopologyEntityImpl().getEdit().hasRemoved());
        assertTrue(daemonPod.getTopologyEntityImpl().hasEdit()
                && daemonPod.getTopologyEntityImpl().getEdit().hasRemoved());
        assertTrue(daemonPodContainer.getTopologyEntityImpl().hasEdit()
                && daemonPod.getTopologyEntityImpl().getEdit().hasRemoved());
        assertTrue(daemonPodApp.getTopologyEntityImpl().hasEdit()
                && daemonPod.getTopologyEntityImpl().getEdit().hasRemoved());
        assertFalse(pod.getTopologyEntityImpl().hasEdit());
        assertFalse(container.getTopologyEntityImpl().hasEdit());
        assertFalse(app.getTopologyEntityImpl().hasEdit());
        assertFalse(service.getTopologyEntityImpl().hasEdit());
    }

    @Test
    public void testTopologyAdditionGroup() throws Exception {
        final long groupId = 7L;
        final long vmId = 1L;
        final long vmCloneId = 182;
        final Grouping group = Grouping.newBuilder()
                .setDefinition(GroupDefinition.getDefaultInstance())
                .setId(groupId)
                .build();
        final TopologyEntity.Builder vmEntity = TopologyEntityUtils.topologyEntityBuilder(
                new TopologyEntityImpl()
                    .setOid(vmId)
                    .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                    .setDisplayName("VM"));

        final Map<Long, TopologyEntity.Builder> topology = new HashMap<>();
        topology.put(vmId, vmEntity);

        final List<ScenarioChange> changes = Collections.singletonList(ScenarioChange.newBuilder()
                .setTopologyAddition(TopologyAddition.newBuilder()
                        .setGroupId(groupId).setTargetEntityType(EntityType.VIRTUAL_MACHINE_VALUE))
                .build());
        when(groupServiceSpy.getGroups(any())).thenReturn(Collections.singletonList(group));
        when(groupResolver.resolve(eq(group), isA(TopologyGraph.class)))
            .thenReturn(resolvedGroup(group, ApiEntityType.VIRTUAL_MACHINE, vmId));
        when(identityProvider.getCloneId(any(TopologyEntityView.class))).thenReturn(vmCloneId);

        topologyEditor.editTopology(topology, scope, changes,
                                    context, groupResolver, sourceEntities, destinationEntities);

        assertTrue(topology.containsKey(vmCloneId));
        final TopologyEntity.Builder cloneBuilder = topology.get(vmCloneId);
        assertThat(cloneBuilder.getDisplayName(), Matchers.containsString(vmEntity.getDisplayName()));
        assertThat(cloneBuilder.getTopologyEntityImpl().getOrigin().getPlanScenarioOrigin(),
                is(new PlanScenarioOriginImpl()
                        .setPlanId(topologyInfo.getTopologyContextId())
                        .setOriginalEntityId(vmId)));
        assertTrue(cloneBuilder.getTopologyEntityImpl().getAnalysisSettings().getShopTogether());
    }

    @Test
    public void testEditTopologyChangeUtilizationLevel() throws Exception {
        final Map<Long, TopologyEntity.Builder> topology = ImmutableMap.of(
            vm.getOid(), vm,
            pm.getOid(), pm,
            st.getOid(), st
        );
        final List<ScenarioChange> changes = ImmutableList.of(ScenarioChange.newBuilder().setPlanChanges(
            PlanChanges.newBuilder().setUtilizationLevel(
                UtilizationLevel.newBuilder().setPercentage(50).build()
            ).build()
        ).build());
        topologyEditor.editTopology(topology, scope, changes,
                                    context, groupResolver, sourceEntities, destinationEntities);
        final List<CommodityBoughtView> vmCommodities = topology.get(vmId)
            .getTopologyEntityImpl()
            .getCommoditiesBoughtFromProvidersList().stream()
            .map(CommoditiesBoughtFromProviderView::getCommodityBoughtList)
            .flatMap(List::stream)
            .collect(Collectors.toList());
        Assert.assertEquals(150, vmCommodities.get(0).getUsed(), 0);
        Assert.assertEquals(150, vmCommodities.get(1).getUsed(), 0);
        Assert.assertEquals(100, vmCommodities.get(2).getUsed(), 0);
        Assert.assertEquals(100, vmCommodities.get(3).getUsed(), 0);

        final List<CommoditySoldView> pmSoldCommodities = topology.get(pmId)
            .getTopologyEntityImpl()
            .getCommoditySoldListList();
        Assert.assertEquals(150, pmSoldCommodities.get(0).getUsed(), 0);
        Assert.assertEquals(150, pmSoldCommodities.get(1).getUsed(), 0);

        final List<CommoditySoldView> storageSoldCommodities = topology.get(stId)
            .getTopologyEntityImpl()
            .getCommoditySoldListList();
        Assert.assertEquals(100, storageSoldCommodities.get(0).getUsed(), 0);
        Assert.assertEquals(100, storageSoldCommodities.get(1).getUsed(), 0);
    }

    @Test
    public void testEditTopologyChangeUtilizationWithUnplacedVM() throws Exception {
        final Map<Long, TopologyEntity.Builder> topology = ImmutableMap.of(
                unplacedVm.getOid(), unplacedVm,
                pm.getOid(), pm,
                st.getOid(), st
        );
        final List<ScenarioChange> changes = ImmutableList.of(ScenarioChange.newBuilder().setPlanChanges(
                PlanChanges.newBuilder().setUtilizationLevel(
                        UtilizationLevel.newBuilder().setPercentage(50).build()
                ).build()
        ).build());
        topologyEditor.editTopology(topology, scope, changes,
                                    context, groupResolver, sourceEntities, destinationEntities);
        final List<CommodityBoughtView> vmCommodities = topology.get(vmId)
                .getTopologyEntityImpl()
                .getCommoditiesBoughtFromProvidersList().stream()
                .map(CommoditiesBoughtFromProviderView::getCommodityBoughtList)
                .flatMap(List::stream)
                .collect(Collectors.toList());
        Assert.assertEquals(150, vmCommodities.get(0).getUsed(), 0);
        Assert.assertEquals(150, vmCommodities.get(1).getUsed(), 0);
        Assert.assertEquals(USED, vmCommodities.get(2).getUsed(), 0);
        Assert.assertEquals(USED, vmCommodities.get(3).getUsed(), 0);

        final List<CommoditySoldView> pmSoldCommodities = topology.get(pmId)
                .getTopologyEntityImpl()
                .getCommoditySoldListList();
        Assert.assertEquals(USED, pmSoldCommodities.get(0).getUsed(), 0);
        Assert.assertEquals(USED, pmSoldCommodities.get(1).getUsed(), 0);

        final List<CommoditySoldView> storageSoldCommodities = topology.get(stId)
                .getTopologyEntityImpl()
                .getCommoditySoldListList();
        Assert.assertEquals(USED, storageSoldCommodities.get(0).getUsed(), 0);
        Assert.assertEquals(USED, storageSoldCommodities.get(1).getUsed(), 0);
    }

    /**
     * Test remove VMs which exist in a group but not in the system.
     * */
    @Test
    public void testRemoveNonExistentVMFromVMGroup() throws Exception {
        final long groupId = 1L;
        final long existVmId = 2L;
        final Grouping group = Grouping.newBuilder()
                .setDefinition(GroupDefinition.getDefaultInstance())
                .setId(groupId)
                .build();
        final TopologyEntity.Builder existEntity = TopologyEntityUtils
                .topologyEntity(existVmId, 0, 0, "ExistVM", EntityType.VIRTUAL_MACHINE);
        final Map<Long, TopologyEntity.Builder> topology = new HashMap<>();
        topology.put(existVmId, existEntity);

        final List<ScenarioChange> changes = Collections.singletonList(ScenarioChange.newBuilder()
                .setTopologyRemoval(TopologyRemoval.newBuilder().setGroupId(groupId)
                        .setTargetEntityType(10))
                .build());
        final long nonExistVmId = 3L;
        when(groupServiceSpy.getGroups(any())).thenReturn(Collections.singletonList(group));
        ResolvedGroup resolvedGrp = new ResolvedGroup(group, Collections.singletonMap(
                ApiEntityType.VIRTUAL_MACHINE, new HashSet(Arrays.asList(existVmId, nonExistVmId))));
        when(groupResolver.resolve(eq(group), isA(TopologyGraph.class))).thenReturn(resolvedGrp);
        topologyEditor.editTopology(topology, scope, changes,
                                    context, groupResolver, sourceEntities, destinationEntities);

        TopologyEntity.Builder vmToRemove = topology.get(existVmId);
        assertTrue(vmToRemove.getTopologyEntityImpl().hasEdit()
                && vmToRemove.getTopologyEntityImpl().getEdit().hasRemoved());
        assertNull(topology.get(nonExistVmId));
    }

    @Test
    public void testTopologyReplace() throws Exception {
        Map<Long, TopologyEntity.Builder> topology = Stream.of(vm, pm, st)
                .collect(Collectors.toMap(TopologyEntity.Builder::getOid, Function.identity()));
        List<ScenarioChange> changes = Lists.newArrayList(REPLACE);
        final Multimap<Long, Long> templateToReplacedEntity = ArrayListMultimap.create();
        templateToReplacedEntity.put(TEMPLATE_ID, pm.getTopologyEntityImpl().getOid());
        final Map<Long, Long> topologyAdditionEmpty = Collections.emptyMap();
        when(templateConverterFactory
                .generateTopologyEntityFromTemplates(eq(topologyAdditionEmpty),
                        eq(templateToReplacedEntity), eq(topology), eq(context.getTopologyInfo().getTopologyId())))
                .thenReturn(Stream.of(pm.getTopologyEntityImpl().copy()
                    .setOid(1234L)
                    .setDisplayName("Test PM1")));

        topologyEditor.editTopology(topology, scope, changes,
                                    context, groupResolver, sourceEntities, destinationEntities);
        final List<TopologyEntityView> topologyEntityDTOS = topology.entrySet().stream()
                .map(Entry::getValue)
                .map(Builder::getTopologyEntityImpl)
                .collect(Collectors.toList());
        assertEquals(4, topologyEntityDTOS.size());
        // verify that one of the entities is marked for removal
        assertEquals( 1, topologyEntityDTOS.stream()
                    .filter(TopologyEntityView::hasEdit)
                    .map(TopologyEntityView::getEdit)
                    .filter(EditView::hasReplaced)
                    .count());
    }

    /**
     * Test adding a container pod to a plan with constraints ignored.
     */
    @Test
    public void testTopologyAdditionPodCloneIgnoringConstraints() throws Exception {
        featureFlagTestRule.disable(FeatureFlags.APPLY_CONSTRAINTS_IN_CONTAINER_CLUSTER_PLAN);
        testTopologyAdditionPodClone(false);
    }

    /**
     * Test adding a container pod to a plan with constraints ignored.
     */
    @Test
    public void testTopologyAdditionPodCloneApplyingConstraints() throws Exception {
        featureFlagTestRule.enable(FeatureFlags.APPLY_CONSTRAINTS_IN_CONTAINER_CLUSTER_PLAN);
        testTopologyAdditionPodClone(true);
    }

    /**
     * Test adding a container pod to a plan.
     */
    private void testTopologyAdditionPodClone(boolean applyConstraints) throws Exception {
        Map<Long, TopologyEntity.Builder> topology = Stream.of(container, pod, workloadController, vm, pm, destVM, destCluster)
            .collect(Collectors.toMap(TopologyEntity.Builder::getOid, Function.identity()));
        List<ScenarioChange> changes = Lists.newArrayList(ADD_POD);
        PlanScope cntClusterPlanScope = PlanScope.newBuilder()
                .addScopeEntries(PlanScopeEntry.newBuilder()
                    .setScopeObjectOid(destClusterId)
                    .setClassName(ApiEntityType.CONTAINER_PLATFORM_CLUSTER.apiStr()))
                .build();
        topologyEditor.editTopology(topology, cntClusterPlanScope, changes, containerClusterPlanContext,
                                    groupResolver, sourceEntities, destinationEntities);

        Map<Integer, List<TopologyEntity.Builder>> clonedEntitiesMap = topology.values().stream()
            .filter(entity -> entity.getOid() != pod.getOid()
                && entity.getOid() != container.getOid())
            .filter(entity -> entity.getEntityType() == pod.getEntityType()
                || entity.getEntityType() == container.getEntityType())
            .collect(Collectors.groupingBy(TopologyEntity.Builder::getEntityType));
        final List<TopologyEntity.Builder> clonedContainerPods = clonedEntitiesMap.get(EntityType.CONTAINER_POD_VALUE);
        final List<TopologyEntity.Builder> clonedContainers = clonedEntitiesMap.get(EntityType.CONTAINER_VALUE);
        final List<TopologyEntity.Builder> clonedWorkloadControllers =
                clonedEntitiesMap.get(EntityType.WORKLOAD_CONTROLLER_VALUE);
        // Cloned pods and containers exist.
        assertNotNull("There must be cloned container pods.", clonedContainerPods);
        assertNotNull("There must be cloned containers.", clonedContainers);
        assertNull("There must not be workload controllers.", clonedWorkloadControllers);
        // There are NUM_CLONES cloned pods and containers.
        assertEquals(NUM_CLONES, clonedContainerPods.size());
        assertEquals(NUM_CLONES, clonedContainers.size());

        // Verify that the cloned containers are not controllable and not suspendable.
        final boolean noneControllable = clonedContainers.stream()
            .noneMatch(clone -> clone.getTopologyEntityImpl().getAnalysisSettings().getControllable());
        final boolean noneSuspendable = clonedContainers.stream()
            .noneMatch(clone -> clone.getTopologyEntityImpl().getAnalysisSettings().getSuspendable());
        assertTrue("Cloned containers must not be controllable", noneControllable);
        assertTrue("Cloned containers must not be suspendable", noneSuspendable);

        // Verify that cloned containers have correct bought keyed commodities
        int cloneCounter = 0;
        for (final TopologyEntity.Builder entity : clonedContainers) {
            for (final CommoditiesBoughtFromProviderView bought : entity.getTopologyEntityImpl().getCommoditiesBoughtFromProvidersList()) {
                assertEquals("Cloned containers only buy from pods", EntityType.CONTAINER_POD_VALUE,
                        bought.getProviderEntityType());

                final List<CommodityTypeView> keyed = bought.getCommodityBoughtList().stream().map(
                        CommodityBoughtView::getCommodityType).filter(CommodityTypeView::hasKey).collect(
                        Collectors.toList());
                if (applyConstraints) {
                    // Verify that the cloned containers each buy a VMPM keyed commodity
                    final CommodityTypeView expected = VMPM_POD_TO_CONTAINER.copy()
                            .setKey(VMPM_POD_TO_CONTAINER.getKey() + DefaultEntityCloneEditor.cloneSuffix(cloneCounter));
                    assertEquals("Cloned containers buy a keyed VMPM access commodity",
                                 Collections.singletonList(expected), keyed);
                } else {
                    // Verify that the cloned containers do not buy any keyed commodities
                    assertTrue("Cloned containers do not buy any keyed commodities", keyed.isEmpty());
                }
            }
            cloneCounter++;
        }

        // Verify that the cloned pods each only buy the VMPM keyed commodity but not the cluster one
        cloneCounter = 0;
        for (final TopologyEntity.Builder entity : clonedContainerPods) {
            for (final CommoditiesBoughtFromProviderView bought : entity.getTopologyEntityImpl().getCommoditiesBoughtFromProvidersList()) {
                assertNotEquals("Cloned pods should skip bought commodities from workload controller",
                        EntityType.WORKLOAD_CONTROLLER_VALUE, bought.getProviderEntityType());
                assertNotEquals("Cloned pods should skip bought commodities from virtual volume",
                        EntityType.VIRTUAL_VOLUME_VALUE, bought.getProviderEntityType());

                final Set<CommodityTypeView> keyed = bought.getCommodityBoughtList().stream()
                                .map(CommodityBoughtView::getCommodityType)
                                .filter(CommodityTypeView::hasKey)
                                .collect(Collectors.toSet());
                if (applyConstraints) {
                    // Verify that the cloned pods each buy the right commodities
                    assertEquals(4, keyed.size());
                    assertTrue("Cloned pods do buy VMPM_ACCESS commodities",
                               keyed.contains(VMPM_NODE_TO_POD));
                    assertTrue("Cloned pods do buy TAINT commodities",
                               keyed.contains(TAINT_NODE_TO_POD_1));
                    assertTrue("Cloned pods do buy TAINT commodities",
                               keyed.contains(TAINT_NODE_TO_POD_2));
                    assertTrue("Cloned pods do buy cluster commodity bound to the plan cluster",
                                keyed.contains(DEST_CLUSTER_KEYED));
                } else {
                    // Verify that the cloned pods do not buy any keyed commodities
                    assertTrue("Cloned pods do not buy any keyed commodities", keyed.isEmpty());
                }
            }
            for (final CommoditySoldView sold : entity.getTopologyEntityImpl().getCommoditySoldListList()) {
                if (sold.getCommodityType().hasKey()) {
                    final CommodityTypeView expected = VMPM_POD_TO_CONTAINER.copy()
                            .setKey(VMPM_POD_TO_CONTAINER.getKey() + DefaultEntityCloneEditor.cloneSuffix(cloneCounter));
                    assertEquals("Cloned pods sell keyed commodities but with a distinct key",
                            expected, sold.getCommodityType());
                }
            }
            cloneCounter++;
        }
    }

    /**
     * Test container workload migration plan.
     */
    @Test
    public void testTopologyMigrateContainerWorkload() throws Exception {
        final Set<TopologyEntity.Builder> originalEntities = ImmutableSet.of(
                container, pod, pod1, workloadController, containerSpec,
                namespace, vm, destVM,cluster, destCluster);
        featureFlagTestRule.enable(FeatureFlags.MIGRATE_CONTAINER_WORKLOAD_PLAN);
        Map<Long, TopologyEntity.Builder> topology = originalEntities.stream()
                .collect(Collectors.toMap(TopologyEntity.Builder::getOid, Function.identity()));
        List<ScenarioChange> changes = Lists.newArrayList(ADD_WORKLOAD);
        PlanScope cntClusterPlanScope = PlanScope.newBuilder()
                .addScopeEntries(PlanScopeEntry.newBuilder()
                    .setScopeObjectOid(destClusterId)
                    .setClassName(ApiEntityType.CONTAINER_PLATFORM_CLUSTER.apiStr()))
                .build();
        topologyEditor.editTopology(topology, cntClusterPlanScope, changes,
                                    migrateContainerWorkloadPlanContext,
                                    groupResolver, sourceEntities, destinationEntities);
        // Create topology graph
        final TopologyGraph<TopologyEntity> topologyGraph =
                TopologyEntityTopologyGraphCreator.newGraph(topology);
        final Set<Long> originalOidSet = originalEntities.stream()
                .map(TopologyEntity.Builder::getOid)
                .collect(Collectors.toSet());
        // Get cloned entities
        final List<TopologyEntity> clonedContainers =
                topologyGraph.entitiesOfType(EntityType.CONTAINER_VALUE)
                        .filter(entity -> !originalOidSet.contains(entity.getOid()))
                        .collect(Collectors.toList());
        final List<TopologyEntity> clonedPods =
                topologyGraph.entitiesOfType(EntityType.CONTAINER_POD_VALUE)
                        .filter(entity -> !originalOidSet.contains(entity.getOid()))
                        .collect(Collectors.toList());
        final List<TopologyEntity> clonedContainerSpecs =
                topologyGraph.entitiesOfType(EntityType.CONTAINER_SPEC_VALUE)
                        .filter(entity -> !originalOidSet.contains(entity.getOid()))
                        .collect(Collectors.toList());
        final List<TopologyEntity> clonedWorkloadControllers =
                topologyGraph.entitiesOfType(EntityType.WORKLOAD_CONTROLLER_VALUE)
                        .filter(entity -> !originalOidSet.contains(entity.getOid()))
                        .collect(Collectors.toList());
        final List<TopologyEntity> clonedNamespaces =
                topologyGraph.entitiesOfType(EntityType.NAMESPACE_VALUE)
                        .filter(entity -> !originalOidSet.contains(entity.getOid()))
                        .collect(Collectors.toList());
        final List<TopologyEntity> clusters =
                topologyGraph.entitiesOfType(EntityType.CONTAINER_PLATFORM_CLUSTER_VALUE)
                        .collect(Collectors.toList());
        assertEquals(1, clonedContainers.size());
        assertEquals(2, clonedPods.size());
        assertEquals(1, clonedContainerSpecs.size());
        assertEquals(1, clonedWorkloadControllers.size());
        assertEquals(1, clonedNamespaces.size());
        assertEquals(2, clusters.size());
        // Verify that the cloned containers are controllable but not suspendable.
        final boolean allControllable = clonedContainers.stream()
                .allMatch(clone -> clone.getTopologyEntityImpl().getAnalysisSettings().getControllable());
        final boolean noneSuspendable = clonedContainers.stream()
                .noneMatch(clone -> clone.getTopologyEntityImpl().getAnalysisSettings().getSuspendable());
        assertTrue("Cloned containers must be controllable", allControllable);
        assertTrue("Cloned containers must not be suspendable", noneSuspendable);
        // Verify that cloned containers have correct bought keyed commodities
        int cloneCounter = 0;
        for (final TopologyEntity entity : clonedContainers) {
            for (final CommoditiesBoughtFromProviderView bought : entity.getTopologyEntityImpl().getCommoditiesBoughtFromProvidersList()) {
                assertEquals("Cloned containers only buy from pods", EntityType.CONTAINER_POD_VALUE,
                             bought.getProviderEntityType());
                final List<CommodityTypeView> keyed = bought.getCommodityBoughtList().stream().map(
                        CommodityBoughtView::getCommodityType).filter(CommodityTypeView::hasKey).collect(
                        Collectors.toList());
                // Verify that the cloned containers each buy a VMPM keyed commodity
                final CommodityTypeView expected = VMPM_POD_TO_CONTAINER.copy()
                        .setKey(VMPM_POD_TO_CONTAINER.getKey() + DefaultEntityCloneEditor.cloneSuffix(cloneCounter));
                assertEquals("Cloned containers buy a keyed VMPM access commodity",
                             Collections.singletonList(expected), keyed);
            }
            cloneCounter++;
        }
        // Verify that the cloned pods each only buy the VMPM keyed commodity but not the cluster one
        cloneCounter = 0;
        for (final TopologyEntity entity : clonedPods) {
            for (final CommoditiesBoughtFromProviderView bought : entity.getTopologyEntityImpl()
                    .getCommoditiesBoughtFromProvidersList()) {
                assertNotEquals("Cloned pods should skip bought commodities from virtual volume",
                                EntityType.VIRTUAL_VOLUME_VALUE, bought.getProviderEntityType());
                final Set<CommodityTypeView> keyedComm = bought.getCommodityBoughtList().stream()
                        .map(CommodityBoughtView::getCommodityType)
                        .filter(CommodityTypeView::hasKey)
                        .collect(Collectors.toSet());
                final Set<String> keys = keyedComm.stream()
                        .map(CommodityTypeView::getKey)
                        .collect(Collectors.toSet());
                if (bought.getProviderEntityType() == EntityType.VIRTUAL_MACHINE_VALUE) {
                    // Verify that the cloned pods each buy the right commodities
                    assertEquals(4, keyedComm.size());
                    assertTrue("Cloned pods do buy VMPM_ACCESS commodities",
                               keyedComm.contains(VMPM_NODE_TO_POD));
                    assertTrue("Cloned pods do buy TAINT commodities",
                               keyedComm.contains(TAINT_NODE_TO_POD_1));
                    assertTrue("Cloned pods do buy TAINT commodities",
                               keyedComm.contains(TAINT_NODE_TO_POD_2));
                    assertTrue("Cloned pods do buy cluster commodity bound to the plan cluster",
                               keyedComm.contains(DEST_CLUSTER_KEYED));
                } else if (bought.getProviderEntityType() == EntityType.WORKLOAD_CONTROLLER_VALUE) {
                    // Verify that the cloned pods do not buy any keyed commodities
                    assertTrue(keys.contains(workloadControllerKey + DefaultEntityCloneEditor.cloneSuffix(cloneCounter)));
                }
            }
            for (final CommoditySoldView sold : entity.getTopologyEntityImpl().getCommoditySoldListList()) {
                if (sold.getCommodityType().hasKey()) {
                    final CommodityTypeView expected = VMPM_POD_TO_CONTAINER.copy()
                            .setKey(VMPM_POD_TO_CONTAINER.getKey() + DefaultEntityCloneEditor.cloneSuffix(cloneCounter));
                    assertEquals("Cloned pods sell keyed commodities but with a distinct key",
                                 expected, sold.getCommodityType());
                }
            }
        }
        // Verify that the two cloned pods have the same provider, which is the cloned workload controller
        assertTrue(clonedPods.stream()
                           .allMatch(pod -> pod.getProviders().stream()
                                   .allMatch(clonedWorkloadControllers.get(0)::equals)));
        // Verify that the two cloned pods have the same aggregator, which is the cloned workload controller
        assertTrue(clonedPods.stream()
                           .allMatch(pod -> pod.getAggregators().stream()
                                   .allMatch(clonedWorkloadControllers.get(0)::equals)));
        // Verify that the cloned workload controller is aggregated by the cloned namespace
        assertTrue(clonedWorkloadControllers.stream()
                           .allMatch(wc -> wc.getAggregators().stream()
                                   .allMatch(clonedNamespaces.get(0)::equals)));
        // Verify that the cloned workload controller has the correct display name
        assertEquals(sourceWorkloadControllerName + " - Clone from " + clusterName,
                     clonedWorkloadControllers.get(0).getDisplayName());
        // Verify that the cloned workload controller owns the cloned container spec
        assertTrue(clonedWorkloadControllers.stream()
                           .allMatch(wc -> wc.getOwnedEntities().stream()
                                   .allMatch(clonedContainerSpecs.get(0)::equals)));
        // Verify that the cloned namespace consumes from the destination cluster
        assertTrue(clonedNamespaces.stream()
                           .allMatch(ns -> ns.getProviders().stream()
                                   .map(TopologyEntity::getDisplayName)
                                   .allMatch(destCluster.getDisplayName()::equals)));
        // Verify that the cloned container is controlled by the cloned container spec
        assertTrue(clonedContainers.stream()
                           .allMatch(cnt -> cnt.getControllers().stream()
                                   .allMatch(clonedContainerSpecs.get(0)::equals)));

        // populate InvertedIndex
        InvertedIndex<TopologyEntity, CommoditiesBoughtFromProviderView>
                index = planTopologyScopeEditor.createInvertedIndex();
        topologyGraph.entities().forEach(index::add);
        // scope using inverted index
        TopologyGraph<TopologyEntity> result = planTopologyScopeEditor
                .indexBasedScoping(index, migrateContainerWorkloadPlanTopologyInfo, topologyGraph,
                                   groupResolver, cntClusterPlanScope, PlanProjectType.USER);
        // Verify that the original VM is not scoped
        assertFalse(result.getEntity(vmId).isPresent());
        assertTrue(result.getEntity(destVMId).isPresent());
        // Verify that quota capacity is removed (i.e., set to maximum)]
        assertTrue(clonedWorkloadControllers.stream()
                           .map(TopologyEntity::getTopologyEntityImpl)
                           .map(TopologyEntityImpl::getCommoditySoldListList)
                           .flatMap(List::stream)
                           .map(CommoditySoldView::getCapacity)
                           .allMatch(capacity -> capacity.equals(TopologyEditorUtil.MAX_QUOTA_CAPACITY)));
        assertTrue(clonedNamespaces.stream()
                           .map(TopologyEntity::getTopologyEntityImpl)
                           .map(TopologyEntityImpl::getCommoditySoldListList)
                           .flatMap(List::stream)
                           .map(CommoditySoldView::getCapacity)
                           .allMatch(capacity -> capacity.equals(TopologyEditorUtil.MAX_QUOTA_CAPACITY)));
    }

    /**
     * Just to verify if we are resolving DC groups correctly or not.
     */
    @Test
    public void resolveDataCenterGroups() throws Exception {
        long groupId = 10010;
        long dcId1 = 1000;
        long dcId2 = 2000;
        // PMs under the DCs.
        long pmId11 = 10001;
        long pmId12 = 10002;
        long pmId21 = 20001;
        // VMs under those PMs.
        long vmId111 = 100011;
        long vmId112 = 100012;
        long vmId121 = 100021;
        long vmId211 = 200011;

        final MemberType dcGroupType = MemberType.newBuilder()
                .setEntity(EntityType.DATACENTER_VALUE)
                .build();
        final List<MigrationReference> migrationReferences = ImmutableList.of(
                MigrationReference.newBuilder()
                        .setOid(groupId)
                        .setGroupType(GroupType.REGULAR_VALUE)
                        .build());
        final Map<Long, Grouping> groupIdToGroupMap = new HashMap<>();
        groupIdToGroupMap.put(groupId, Grouping.newBuilder()
                .setId(groupId)
                .addExpectedTypes(dcGroupType)
                .setDefinition(GroupDefinition.newBuilder()
                        .setType(GroupType.REGULAR)
                        .setStaticGroupMembers(StaticMembers.newBuilder()
                                .addMembersByType(StaticMembersByType.newBuilder()
                                        .setType(dcGroupType)
                                        .addMembers(dcId1)
                                        .addMembers(dcId2)
                                        .build())
                                .build())
                        .build())
                .build());
        final Map<ApiEntityType, Set<Long>> entityTypeToMembers = new HashMap<>();
        entityTypeToMembers.put(ApiEntityType.DATACENTER, ImmutableSet.of(dcId1, dcId2));
        final ResolvedGroup resolvedGroup = mock(ResolvedGroup.class);
        when(resolvedGroup.getEntitiesByType())
                .thenReturn(entityTypeToMembers);
        final GroupResolver groupResolver = mock(GroupResolver.class);
        when(groupResolver.resolve(any(), any()))
                .thenReturn(resolvedGroup);

        final TopologyGraph<TopologyEntity> topologyGraph = TopologyEntityUtils.pojoGraphOf(
                TopologyEntityUtils.topologyEntity(dcId1, 0, 0, "dc-1", EntityType.DATACENTER),
                TopologyEntityUtils.topologyEntity(dcId2, 0, 0, "dc-2", EntityType.DATACENTER),
                TopologyEntityUtils.topologyEntity(pmId11, 0, 0, "pm-11", EntityType.PHYSICAL_MACHINE, dcId1),
                TopologyEntityUtils.topologyEntity(pmId12, 0, 0, "pm-12", EntityType.PHYSICAL_MACHINE, dcId1),
                TopologyEntityUtils.topologyEntity(pmId21, 0, 0, "pm-21", EntityType.PHYSICAL_MACHINE, dcId2),
                TopologyEntityUtils.topologyEntity(vmId111, 0, 0, "vm-111", EntityType.VIRTUAL_MACHINE, pmId11),
                TopologyEntityUtils.topologyEntity(vmId112, 0, 0, "vm-112", EntityType.VIRTUAL_MACHINE, pmId11),
                TopologyEntityUtils.topologyEntity(vmId121, 0, 0, "vm-121", EntityType.VIRTUAL_MACHINE, pmId12),
                TopologyEntityUtils.topologyEntity(vmId211, 0, 0, "vm-211", EntityType.VIRTUAL_MACHINE, pmId21)
                );

        final Set<Long> workloadsInDc = TopologyEditor.expandAndFlattenReferences(
                migrationReferences,
                EntityType.VIRTUAL_MACHINE_VALUE,
                groupIdToGroupMap,
                groupResolver,
                topologyGraph);
        assertNotNull(workloadsInDc);
        assertEquals(4, workloadsInDc.size());
        assertTrue(workloadsInDc.contains(vmId111) && workloadsInDc.contains(vmId112)
                && workloadsInDc.contains(vmId121) && workloadsInDc.contains(vmId211));
    }

    /**
     * Ensure that HCI resources are successfully added to the plan scope.
     *
     * @throws GroupResolutionException on error
     */
    @Test
    public void testTopologyReplaceWithHCI() throws GroupResolutionException {
        Map<Long, TopologyEntity.Builder> topology = Stream.of(vm, pm2, st)
                .collect(Collectors.toMap(TopologyEntity.Builder::getOid, Function.identity()));
        List<ScenarioChange> changes = Lists.newArrayList(HCI_REPLACE);
        final Multimap<Long, Long> templateToReplacedEntity = ArrayListMultimap.create();
        templateToReplacedEntity.put(HCI_TEMPLATE_ID, pm2Id);

        TopologyEditor liveTopologyEditor = new TopologyEditor(identityProvider,
                liveTemplateConverterFactory,
                GroupServiceGrpc.newBlockingStub(grpcServer.getChannel()));

        when(settingPolicyServiceRpc.listSettingPolicies(any(ListSettingPoliciesRequest.class)))
                .thenReturn(SETTING_POLICY_RESPONSE);
        when(templateServiceRpc.getTemplates(any(GetTemplatesRequest.class)))
                .thenReturn(HCI_TEMPLATE_RESPONSE);

        liveTopologyEditor.editTopology(topology, scope, changes,
                context, groupResolver, sourceEntities, destinationEntities);
        assertEquals(4, topology.size());

        // Verify that the PM is marked for removal and there is a new HCI host that is flagged
        // for inclusion in the plan.
        AtomicInteger numSourcePMsFound = new AtomicInteger(0);
        AtomicInteger numHCIsFound = new AtomicInteger(0);

        topology.entrySet().stream()
                .map(Entry::getValue)
                .map(Builder::getTopologyEntityImpl)
                .forEach(entity -> {
                    if (entity.hasEdit() && entity.getEdit().hasReplaced()) {
                        if (entity.getOid() == pm2Id) {
                            numSourcePMsFound.incrementAndGet();
                        } else {
                            Assert.fail("Wrong entity was replaced");
                        }
                    }
                    if (entity.getDisplayName().startsWith(HCI_TEMPLATE_NAME)) {
                        numHCIsFound.incrementAndGet();
                    }
                });

        // verify that the PM is marked for removal
        assertEquals(1, numSourcePMsFound.get());
        // Verify that the HCI template was added and flagged for inclusion into the plan.
        assertEquals(1, numHCIsFound.get());
    }
}
