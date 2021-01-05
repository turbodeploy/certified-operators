package com.vmturbo.topology.processor.topology;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.gson.Gson;

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
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.AnalysisSettings;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Edit;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.PlanScenarioOrigin;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;
import com.vmturbo.commons.analysis.AnalysisUtil;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;
import com.vmturbo.platform.common.dto.CommonDTOREST;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.processor.consistentscaling.ConsistentScalingManager;
import com.vmturbo.topology.processor.group.GroupResolver;
import com.vmturbo.topology.processor.group.ResolvedGroup;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.template.TemplateConverterFactory;
import com.vmturbo.topology.processor.topology.pipeline.TopologyPipelineContext;

/**
 * Unit tests for {@link ScenarioChange}.
 */
public class TopologyEditorTest {

    private static final long vmId = 10;
    private static final long pmId = 20;
    private static final long stId = 30;
    private static final long podId = 40;
    private static final double USED = 100;
    private static final double VCPU_CAPACITY = 1000;
    private static final double VMEM_CAPACITY = 1000;

    private static final CommodityType MEM = CommodityType.newBuilder().setType(21).build();
    private static final CommodityType CPU = CommodityType.newBuilder().setType(40).build();
    private static final CommodityType LATENCY = CommodityType.newBuilder().setType(3).build();
    private static final CommodityType IOPS = CommodityType.newBuilder().setType(4).build();
    private static final CommodityType DATASTORE = CommodityType.newBuilder()
                    .setType(CommodityDTO.CommodityType.DATASTORE_VALUE).build();
    private static final CommodityType DSPM = CommodityType.newBuilder()
                    .setType(CommodityDTO.CommodityType.DSPM_ACCESS_VALUE).build();
    private static final CommodityType VCPU = CommodityType.newBuilder()
        .setType(CommodityDTO.CommodityType.VCPU_VALUE).build();
    private static final CommodityType VMEM = CommodityType.newBuilder()
        .setType(CommodityDTO.CommodityType.VMEM_VALUE).build();
    private static final CommodityType VMPM = CommodityType.newBuilder()
        .setType(CommodityDTO.CommodityType.VMPM_ACCESS_VALUE).build();
    private TopologyPipelineContext context;
    private static final TopologyEntity.Builder vm = TopologyEntityUtils.topologyEntityBuilder(
        TopologyEntityDTO.newBuilder()
            .setOid(vmId)
            .setDisplayName("VM")
            .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
            .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                .setProviderId(pmId)
                .addCommodityBought(CommodityBoughtDTO.newBuilder().setCommodityType(MEM)
                    .setUsed(USED).build())
                .addCommodityBought(CommodityBoughtDTO.newBuilder().setCommodityType(CPU)
                    .setUsed(USED).build())
                .build())
            .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                .setProviderId(stId)
                .addCommodityBought(CommodityBoughtDTO.newBuilder().setCommodityType(LATENCY)
                    .setUsed(USED).build())
                .addCommodityBought(CommodityBoughtDTO.newBuilder().setCommodityType(IOPS)
                    .setUsed(USED).build())
                .build())
            .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                .setCommodityType(VCPU)
                .setUsed(USED)
                .setCapacity(VCPU_CAPACITY))
            .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                .setCommodityType(VMEM)
                .setUsed(USED)
                .setCapacity(VMEM_CAPACITY))
    );

    private static final TopologyEntity.Builder unplacedVm = TopologyEntityUtils.topologyEntityBuilder(
            TopologyEntityDTO.newBuilder()
                    .setOid(vmId)
                    .setDisplayName("UNPLACED-VM")
                    .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                    .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                            .addCommodityBought(CommodityBoughtDTO.newBuilder().setCommodityType(MEM)
                                    .setUsed(USED).build())
                            .addCommodityBought(CommodityBoughtDTO.newBuilder().setCommodityType(CPU)
                                    .setUsed(USED).build())
                            .build())
                    .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                            .setProviderId(stId)
                            .addCommodityBought(CommodityBoughtDTO.newBuilder().setCommodityType(LATENCY)
                                    .setUsed(USED).build())
                            .addCommodityBought(CommodityBoughtDTO.newBuilder().setCommodityType(IOPS)
                                    .setUsed(USED).build())
                            .build())
    );

    private static final TopologyEntity.Builder pm = TopologyEntityUtils.topologyEntityBuilder(
        TopologyEntityDTO.newBuilder()
            .setOid(pmId)
            .setDisplayName("PM")
            .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
            .addCommoditySoldList(CommoditySoldDTO.newBuilder().setCommodityType(MEM).setUsed(USED)
                    .build())
            .addCommoditySoldList(CommoditySoldDTO.newBuilder().setCommodityType(CPU).setUsed(USED)
                    .build())
            .addCommoditySoldList(CommoditySoldDTO.newBuilder().setCommodityType(DATASTORE)
                            .setAccesses(stId).build())
    );

    private final TopologyEntity.Builder st = TopologyEntityUtils.topologyEntityBuilder(
        TopologyEntityDTO.newBuilder()
            .setOid(stId)
            .setDisplayName("ST")
            .setEntityType(EntityType.STORAGE_VALUE)
            .addCommoditySoldList(CommoditySoldDTO.newBuilder().setCommodityType(LATENCY)
                .setAccesses(vmId).setUsed(USED).build())
            .addCommoditySoldList(CommoditySoldDTO.newBuilder().setCommodityType(IOPS)
                .setAccesses(vmId).setUsed(USED).build())
            .addCommoditySoldList(CommoditySoldDTO.newBuilder().setCommodityType(DSPM)
                            .setAccesses(pmId).build())
    );

    // Create a test pod that is suspendable
    private static final TopologyEntity.Builder pod = TopologyEntityUtils.topologyEntityBuilder(
        TopologyEntityDTO.newBuilder()
            .setOid(podId)
            .setDisplayName("ContainerPod")
            .setEntityType(EntityType.CONTAINER_POD_VALUE)
            .setAnalysisSettings(AnalysisSettings.newBuilder().setSuspendable(true).build())
            .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                .setProviderId(vmId)
                .addCommodityBought(CommodityBoughtDTO.newBuilder().setCommodityType(VMEM)
                    .setUsed(USED).build())
                .addCommodityBought(CommodityBoughtDTO.newBuilder().setCommodityType(VCPU)
                    .setUsed(USED).build())
                .build())
            .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                .setCommodityType(VCPU)
                .setUsed(USED)
                .setCapacity(VCPU_CAPACITY))
            .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                .setCommodityType(VMEM)
                .setUsed(USED)
                .setCapacity(VMEM_CAPACITY))
            .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                .setCommodityType(VMPM)
                .setUsed(1.0)
                .setCapacity(1.0))
    );

    private static final int NUM_CLONES = 5;

    private static final long TEMPLATE_ID = 123;

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

    private static final ScenarioChange ADD_POD = ScenarioChange.newBuilder()
        .setTopologyAddition(TopologyAddition.newBuilder()
            .setAdditionCount(NUM_CLONES)
            .setEntityId(podId)
            .setTargetEntityType(EntityType.CONTAINER_POD_VALUE)
            .build())
        .build();

    private IdentityProvider identityProvider = mock(IdentityProvider.class);
    private long cloneId = 1000L;

    private TemplateConverterFactory templateConverterFactory = mock(TemplateConverterFactory.class);

    private GroupServiceMole groupServiceSpy = spy(new GroupServiceMole());

    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(groupServiceSpy);

    private TopologyEditor topologyEditor;

    private GroupResolver groupResolver = mock(GroupResolver.class);

    private final TopologyInfo topologyInfo = TopologyInfo.newBuilder()
            .setTopologyContextId(1)
            .setTopologyId(1)
            .setCreationTime(System.currentTimeMillis())
            .setTopologyType(TopologyType.PLAN)
            .build();

    /**
     * How many times is the dummy clone id to be compared to the entity id.
     */
    private static final long CLONE_ID_MULTIPLIER = 100;

    @Before
    public void setup() {
        when(identityProvider.getCloneId(any(TopologyEntityDTO.class)))
            .thenAnswer(invocation -> cloneId++);
        context = new TopologyPipelineContext(groupResolver, topologyInfo,
                mock(ConsistentScalingManager.class));
        topologyEditor = new TopologyEditor(identityProvider,
                templateConverterFactory,
                GroupServiceGrpc.newBlockingStub(grpcServer.getChannel()));
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
        final TopologyGraph<TopologyEntity> graph = TopologyEntityUtils.topologyGraphOf(pmEntity);
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
        ConnectedEntity regionConnectedEntity = ConnectedEntity.newBuilder()
                .setConnectedEntityId(regionId)
                .setConnectedEntityType(EntityType.REGION_VALUE)
                .setConnectionType(ConnectedEntity.ConnectionType.AGGREGATED_BY_CONNECTION)
                .build();
        regionConnectedEntityToRemove.getEntityBuilder().addConnectedEntityList(regionConnectedEntity);

        final long zoneId = 9;
        final TopologyEntity.Builder zone = TopologyEntityUtils
                .topologyEntity(zoneId, 0, 0, "zone", EntityType.AVAILABILITY_ZONE);

        final long vmToRemoveFromZoneId = 10;
        final TopologyEntity.Builder zoneConnectedEntityToRemove = TopologyEntityUtils
                .topologyEntity(vmToRemoveFromZoneId, 0, 0, "zoneVM", EntityType.VIRTUAL_MACHINE);
        ConnectedEntity zoneConnectedEntityAggregatedBy = ConnectedEntity.newBuilder()
                .setConnectedEntityId(zoneId)
                .setConnectedEntityType(EntityType.AVAILABILITY_ZONE_VALUE)
                .setConnectionType(ConnectedEntity.ConnectionType.AGGREGATED_BY_CONNECTION)
                .build();
        zoneConnectedEntityToRemove.getEntityBuilder().addConnectedEntityList(zoneConnectedEntityAggregatedBy);

        ConnectedEntity zoneConnectedEntityOwns = ConnectedEntity.newBuilder()
                .setConnectedEntityId(zoneId)
                .setConnectedEntityType(EntityType.AVAILABILITY_ZONE_VALUE)
                .setConnectionType(ConnectedEntity.ConnectionType.OWNS_CONNECTION)
                .build();


        final TopologyEntity.Builder region = TopologyEntityUtils
                .topologyEntity(regionId, 0, 0, "region", EntityType.REGION);
        region.getEntityBuilder().addConnectedEntityList(zoneConnectedEntityOwns);

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
        topologyEditor.editTopology(topology,
                Lists.newArrayList(migrateVm), context, groupResolver);

        // TODO: rework needed here, no longer using clones. Scoping for MCP not the same as OCP.
        assertEquals(10, topology.size());

        assertTrue(topology.containsKey(vmId));
        assertTrue(topology.containsKey(vmCloneId));
        assertTrue(topology.containsKey(pmId));
        assertTrue(topology.containsKey(pmCloneId));

        assertTrue(topology.containsKey(s1CloneId));
        assertTrue(topology.containsKey(s2CloneId));

        assertTrue(regionConnectedEntityToRemove.getEntityBuilder().hasEdit()
                && regionConnectedEntityToRemove.getEntityBuilder().getEdit().hasRemoved());
        assertTrue(zoneConnectedEntityToRemove.getEntityBuilder().hasEdit()
                && zoneConnectedEntityToRemove.getEntityBuilder().getEdit().hasRemoved());

        // Verify that the cloned source VM id has been added to the addedEntityOids.
        Collection<Long> clonedSourceVms = context.getSourceEntities();
        assertFalse(clonedSourceVms.isEmpty());
        assertTrue(clonedSourceVms.contains(vmCloneId));
    }

    /**
     * Helper class to enable matching of entity DTO builders with the associated clone ids.
     */
    static class EntityDtoMatcher extends ArgumentMatcher<TopologyEntityDTO> {
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
         * @param input TopologyEntityDTO.Builder instance.
         * @return Whether builder's id matches the clone id of this matcher, goes by the
         * assumption that entityId x multiple = cloneId.
         */
        @Override
        public boolean matches(Object input) {
            if (!(input instanceof TopologyEntityDTO.Builder)) {
                return false;
            }
            TopologyEntityDTO.Builder dtoBuilder = (TopologyEntityDTO.Builder)input;
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

        topologyEditor.editTopology(topology, changes, context, groupResolver);

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

        TopologyEntityDTO.Builder oneClone = clones.get(0).getEntityBuilder();
        // clones are unplaced - all provider IDs are negative
        boolean allNegative = oneClone.getCommoditiesBoughtFromProvidersList()
                .stream()
                .map(CommoditiesBoughtFromProvider::getProviderId)
                .allMatch(key -> key < 0);
        assertTrue(allNegative);

        // "oldProviders" entry in EntityPropertyMap captures the right placement
        @SuppressWarnings("unchecked")
        Map<Long, Long> oldProvidersMap = TopologyDTOUtil.parseOldProvidersMap(oneClone, new Gson());
        for (CommoditiesBoughtFromProvider cloneCommBought :
                oneClone.getCommoditiesBoughtFromProvidersList()) {
            long oldProvider = oldProvidersMap.get(cloneCommBought.getProviderId());
            CommoditiesBoughtFromProvider vmCommBought =
                    vm.getEntityBuilder().getCommoditiesBoughtFromProvidersList().stream()
                            .filter(bought -> bought.getProviderId() == oldProvider)
                            .findAny().get();
            assertEquals(cloneCommBought.getCommodityBoughtList(),
                    vmCommBought.getCommodityBoughtList());
        }
        // Assert that the commodity sold usages are the same.
        assertEquals(vm.getEntityBuilder().getCommoditySoldListList(), oneClone.getCommoditySoldListList());
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

        topologyEditor.editTopology(topology, changes, context, groupResolver);
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
            boolean allNegative = clone.getEntityBuilder()
                            .getCommoditiesBoughtFromProvidersList().stream()
                            .map(CommoditiesBoughtFromProvider::getProviderId)
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
               List<CommoditySoldDTO> bicliqueCommList = pmClone.getEntityBuilder().getCommoditySoldListList().stream()
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
               List<CommoditySoldDTO> bicliqueCommList = stClone.getEntityBuilder().getCommoditySoldListList().stream()
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
        final TopologyGraph<TopologyEntity> graph = TopologyEntityUtils.topologyGraphOf(pmEntity, vmEntity);
        final List<ScenarioChange> changes = Collections.singletonList(ScenarioChange.newBuilder()
            .setTopologyAddition(TopologyAddition.newBuilder().setGroupId(groupId).setTargetEntityType(14))
            .build());
        when(groupServiceSpy.getGroups(any())).thenReturn(Collections.singletonList(group));
        when(groupResolver.resolve(eq(group), isA(TopologyGraph.class)))
            .thenReturn(resolvedGroup(group, ApiEntityType.PHYSICAL_MACHINE, pmId));
        when(identityProvider.getCloneId(any(TopologyEntityDTO.class))).thenReturn(pmCloneId);
        topologyEditor.editTopology(topology, changes, context, groupResolver);

        assertTrue(topology.containsKey(pmCloneId));
        final TopologyEntity.Builder cloneBuilder = topology.get(pmCloneId);
        assertThat(cloneBuilder.getDisplayName(), Matchers.containsString(pmEntity.getDisplayName()));
        assertThat(cloneBuilder.getEntityBuilder().getOrigin().getPlanScenarioOrigin(),
            is(PlanScenarioOrigin.newBuilder()
                .setPlanId(topologyInfo.getTopologyContextId())
                .build()));

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
        final TopologyGraph<TopologyEntity> graph = TopologyEntityUtils.topologyGraphOf(pmEntity, vmEntity);
        final List<ScenarioChange> changes = Collections.singletonList(ScenarioChange.newBuilder()
            .setTopologyRemoval(TopologyRemoval.newBuilder().setGroupId(groupId).setTargetEntityType(14))
            .build());
        when(groupServiceSpy.getGroups(any())).thenReturn(Collections.singletonList(group));
        when(groupResolver.resolve(eq(group), isA(TopologyGraph.class)))
            .thenReturn(resolvedGroup(group, ApiEntityType.PHYSICAL_MACHINE, pmId));
        topologyEditor.editTopology(topology, changes, context, groupResolver);

        TopologyEntity.Builder pmToRemove = topology.get(pmId);
        assertTrue(pmToRemove.getEntityBuilder().hasEdit()
            && pmToRemove.getEntityBuilder().getEdit().hasRemoved());

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
        final TopologyGraph<TopologyEntity> graph = TopologyEntityUtils.topologyGraphOf(stEntity, vmEntity);
        final List<ScenarioChange> changes = Collections.singletonList(ScenarioChange.newBuilder()
            .setTopologyAddition(TopologyAddition.newBuilder().setGroupId(groupId).setTargetEntityType(2))
            .build());
        when(groupServiceSpy.getGroups(any())).thenReturn(Collections.singletonList(group));
        when(groupResolver.resolve(eq(group), isA(TopologyGraph.class)))
            .thenReturn(resolvedGroup(group, ApiEntityType.STORAGE, stId));
        when(identityProvider.getCloneId(any(TopologyEntityDTO.class))).thenReturn(stCloneId);
        topologyEditor.editTopology(topology, changes, context, groupResolver);

        assertTrue(topology.containsKey(stCloneId));
        final TopologyEntity.Builder cloneBuilder = topology.get(stCloneId);
        assertThat(cloneBuilder.getDisplayName(), Matchers.containsString(stEntity.getDisplayName()));
        assertThat(cloneBuilder.getEntityBuilder().getOrigin().getPlanScenarioOrigin(),
            is(PlanScenarioOrigin.newBuilder()
                .setPlanId(topologyInfo.getTopologyContextId())
                .build()));

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
        final TopologyGraph<TopologyEntity> graph = TopologyEntityUtils.topologyGraphOf(stEntity, vmEntity);
        final List<ScenarioChange> changes = Collections.singletonList(ScenarioChange.newBuilder()
            .setTopologyRemoval(TopologyRemoval.newBuilder().setGroupId(groupId).setTargetEntityType(2))
            .build());
        when(groupServiceSpy.getGroups(any())).thenReturn(Collections.singletonList(group));
        when(groupResolver.resolve(eq(group), isA(TopologyGraph.class)))
            .thenReturn(resolvedGroup(group, ApiEntityType.STORAGE, stId));
        topologyEditor.editTopology(topology, changes, context, groupResolver);

        TopologyEntity.Builder stToRemove = topology.get(stId);
        assertTrue(stToRemove.getEntityBuilder().hasEdit()
            && stToRemove.getEntityBuilder().getEdit().hasRemoved());

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
        final TopologyGraph<TopologyEntity> graph = TopologyEntityUtils.topologyGraphOf(hostEntity, vmEntity);

        final List<ScenarioChange> changes = Collections.singletonList(ScenarioChange.newBuilder()
                .setTopologyAddition(TopologyAddition.newBuilder().setGroupId(groupId).setTargetEntityType(10))
                .build());
        when(groupServiceSpy.getGroups(any())).thenReturn(Collections.singletonList(group));
        when(groupResolver.resolve(eq(group), isA(TopologyGraph.class)))
            .thenReturn(resolvedGroup(group, ApiEntityType.PHYSICAL_MACHINE, hostId));
        when(identityProvider.getCloneId(any(TopologyEntityDTO.class))).thenReturn(vmCloneId);

        topologyEditor.editTopology(topology, changes, context, groupResolver);

        assertTrue(topology.containsKey(vmCloneId));
        final TopologyEntity.Builder cloneBuilder = topology.get(vmCloneId);
        assertThat(cloneBuilder.getDisplayName(), Matchers.containsString(vmEntity.getDisplayName()));
        assertThat(cloneBuilder.getEntityBuilder().getOrigin().getPlanScenarioOrigin(),
            is(PlanScenarioOrigin.newBuilder()
                .setPlanId(topologyInfo.getTopologyContextId())
                .build()));
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
        final TopologyGraph<TopologyEntity> graph = TopologyEntityUtils.topologyGraphOf(hostEntity, vmEntity);

        final List<ScenarioChange> changes = Collections.singletonList(ScenarioChange.newBuilder()
            .setTopologyRemoval(TopologyRemoval.newBuilder().setGroupId(groupId).setTargetEntityType(10))
            .build());
        when(groupServiceSpy.getGroups(any())).thenReturn(Collections.singletonList(group));
        when(groupResolver.resolve(eq(group), isA(TopologyGraph.class)))
            .thenReturn(resolvedGroup(group, ApiEntityType.PHYSICAL_MACHINE, hostId));
        topologyEditor.editTopology(topology, changes, context, groupResolver);

        TopologyEntity.Builder vmToRemove = topology.get(vmId);
        assertTrue(vmToRemove.getEntityBuilder().hasEdit()
            && vmToRemove.getEntityBuilder().getEdit().hasRemoved());
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
                TopologyEntityDTO.newBuilder()
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
        when(identityProvider.getCloneId(any(TopologyEntityDTO.class))).thenReturn(vmCloneId);

        topologyEditor.editTopology(topology, changes, context, groupResolver);

        assertTrue(topology.containsKey(vmCloneId));
        final TopologyEntity.Builder cloneBuilder = topology.get(vmCloneId);
        assertThat(cloneBuilder.getDisplayName(), Matchers.containsString(vmEntity.getDisplayName()));
        assertThat(cloneBuilder.getEntityBuilder().getOrigin().getPlanScenarioOrigin(),
            is(PlanScenarioOrigin.newBuilder()
                .setPlanId(topologyInfo.getTopologyContextId())
                .build()));
        assertTrue(cloneBuilder.getEntityBuilder().getAnalysisSettings().getShopTogether());
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
        topologyEditor.editTopology(topology, changes, context, groupResolver);
        final List<CommodityBoughtDTO> vmCommodities = topology.get(vmId)
            .getEntityBuilder()
            .getCommoditiesBoughtFromProvidersList().stream()
            .map(CommoditiesBoughtFromProvider::getCommodityBoughtList)
            .flatMap(List::stream)
            .collect(Collectors.toList());
        Assert.assertEquals(150, vmCommodities.get(0).getUsed(), 0);
        Assert.assertEquals(150, vmCommodities.get(1).getUsed(), 0);
        Assert.assertEquals(100, vmCommodities.get(2).getUsed(), 0);
        Assert.assertEquals(100, vmCommodities.get(3).getUsed(), 0);

        final List<CommoditySoldDTO> pmSoldCommodities = topology.get(pmId)
            .getEntityBuilder()
            .getCommoditySoldListList();
        Assert.assertEquals(150, pmSoldCommodities.get(0).getUsed(), 0);
        Assert.assertEquals(150, pmSoldCommodities.get(1).getUsed(), 0);

        final List<CommoditySoldDTO> storageSoldCommodities = topology.get(stId)
            .getEntityBuilder()
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
        topologyEditor.editTopology(topology, changes, context, groupResolver);
        final List<CommodityBoughtDTO> vmCommodities = topology.get(vmId)
                .getEntityBuilder()
                .getCommoditiesBoughtFromProvidersList().stream()
                .map(CommoditiesBoughtFromProvider::getCommodityBoughtList)
                .flatMap(List::stream)
                .collect(Collectors.toList());
        Assert.assertEquals(150, vmCommodities.get(0).getUsed(), 0);
        Assert.assertEquals(150, vmCommodities.get(1).getUsed(), 0);
        Assert.assertEquals(USED, vmCommodities.get(2).getUsed(), 0);
        Assert.assertEquals(USED, vmCommodities.get(3).getUsed(), 0);

        final List<CommoditySoldDTO> pmSoldCommodities = topology.get(pmId)
                .getEntityBuilder()
                .getCommoditySoldListList();
        Assert.assertEquals(USED, pmSoldCommodities.get(0).getUsed(), 0);
        Assert.assertEquals(USED, pmSoldCommodities.get(1).getUsed(), 0);

        final List<CommoditySoldDTO> storageSoldCommodities = topology.get(stId)
                .getEntityBuilder()
                .getCommoditySoldListList();
        Assert.assertEquals(USED, storageSoldCommodities.get(0).getUsed(), 0);
        Assert.assertEquals(USED, storageSoldCommodities.get(1).getUsed(), 0);
    }


    @Test
    public void testTopologyReplace() throws Exception {
        Map<Long, TopologyEntity.Builder> topology = Stream.of(vm, pm, st)
                .collect(Collectors.toMap(TopologyEntity.Builder::getOid, Function.identity()));
        List<ScenarioChange> changes = Lists.newArrayList(REPLACE);
        final Multimap<Long, Long> templateToReplacedEntity = ArrayListMultimap.create();
        templateToReplacedEntity.put(TEMPLATE_ID, pm.getEntityBuilder().getOid());
        final Map<Long, Long> topologyAdditionEmpty = Collections.emptyMap();
        when(templateConverterFactory
                .generateTopologyEntityFromTemplates(eq(topologyAdditionEmpty),
                        eq(templateToReplacedEntity), eq(topology)))
                .thenReturn(Stream.of(pm.getEntityBuilder().clone()
                    .setOid(1234L)
                    .setDisplayName("Test PM1")));

        topologyEditor.editTopology(topology, changes, context, groupResolver);
        final List<TopologyEntityDTO> topologyEntityDTOS = topology.entrySet().stream()
                .map(Entry::getValue)
                .map(entity -> entity.getEntityBuilder().build())
                .collect(Collectors.toList());
        assertEquals(4, topologyEntityDTOS.size());
        // verify that one of the entities is marked for removal
        assertEquals( 1, topologyEntityDTOS.stream()
                    .filter(TopologyEntityDTO::hasEdit)
                    .map(TopologyEntityDTO::getEdit)
                    .filter(Edit::hasReplaced)
                    .count());
    }
    /**
     * Test adding a container pod to a plan.
     */
    @Test
    public void testTopologyAdditionPodClone() throws Exception {
        Map<Long, TopologyEntity.Builder> topology = Stream.of(pod, vm, pm)
            .collect(Collectors.toMap(TopologyEntity.Builder::getOid, Function.identity()));
        List<ScenarioChange> changes = Lists.newArrayList(ADD_POD);

        topologyEditor.editTopology(topology, changes, context, groupResolver);

        List<TopologyEntity.Builder> clones = topology.values().stream()
            .filter(entity -> entity.getOid() != pod.getOid())
            .filter(entity -> entity.getEntityType() == pod.getEntityType())
            .collect(Collectors.toList());
        assertEquals(NUM_CLONES, clones.size());

        // Verify that these pods are not suspendable
        boolean foundSuspendable = clones.stream()
            .anyMatch(clone -> clone.getEntityBuilder().getAnalysisSettings().getSuspendable());
        assertFalse("Cloned ContainerPods must not be suspendable", foundSuspendable);
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

        final TopologyGraph<TopologyEntity> topologyGraph = TopologyEntityUtils.topologyGraphOf(
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
}
