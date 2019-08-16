package com.vmturbo.topology.processor.topology;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

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

import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.gson.Gson;

import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupInfo;
import com.vmturbo.common.protobuf.group.GroupDTOMoles.GroupServiceMole;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.PlanChanges;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.PlanChanges.UtilizationLevel;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.TopologyAddition;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.TopologyReplace;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Edit;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.PlanScenarioOrigin;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.commons.analysis.AnalysisUtil;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.processor.group.GroupResolver;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.template.TemplateConverterFactory;

/**
 * Unit tests for {@link ScenarioChange}.
 */
public class TopologyEditorTest {

    private static final long vmId = 10;
    private static final long pmId = 20;
    private static final long stId = 30;
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
                .setAccesses(vmId).build())
            .addCommoditySoldList(CommoditySoldDTO.newBuilder().setCommodityType(CPU).setUsed(USED)
                .setAccesses(vmId).build())
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

    private static final int NUM_CLONES = 5;

    private static final long TEMPLATE_ID = 123;

    private static final ScenarioChange ADD_VM = ScenarioChange.newBuilder()
                    .setTopologyAddition(TopologyAddition.newBuilder()
                        .setAdditionCount(NUM_CLONES)
                        .setEntityId(vmId)
                        .build())
                    .build();

    private static final ScenarioChange ADD_HOST = ScenarioChange.newBuilder()
                    .setTopologyAddition(TopologyAddition.newBuilder()
                        .setAdditionCount(NUM_CLONES)
                        .setEntityId(pmId)
                        .build())
                    .build();

    private static final ScenarioChange ADD_STORAGE = ScenarioChange.newBuilder()
                    .setTopologyAddition(TopologyAddition.newBuilder()
                        .setAdditionCount(NUM_CLONES)
                        .setEntityId(stId)
                        .build())
                    .build();

    private static final ScenarioChange REPLACE = ScenarioChange.newBuilder()
                    .setTopologyReplace(TopologyReplace.newBuilder()
                            .setAddTemplateId(TEMPLATE_ID)
                            .setRemoveEntityId(pmId))
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

    @Before
    public void setup() {
        when(identityProvider.getCloneId(any(TopologyEntityDTO.class)))
            .thenAnswer(invocation -> cloneId++);
        topologyEditor = new TopologyEditor(identityProvider, templateConverterFactory,
                GroupServiceGrpc.newBlockingStub(grpcServer.getChannel()));
    }

    /**
     * Test adding entities in a plan.
     */
    @Test
    public void testTopologyAdditionVMClone() {
        Map<Long, TopologyEntity.Builder> topology = Stream.of(vm, pm, st)
                .collect(Collectors.toMap(TopologyEntity.Builder::getOid, Function.identity()));
        List<ScenarioChange> changes = Lists.newArrayList(ADD_VM);

        topologyEditor.editTopology(topology, changes, topologyInfo, groupResolver);

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
        Map<String, Double> oldProviders = new Gson().fromJson(
            oneClone.getEntityPropertyMapMap().get("oldProviders"), Map.class);
        Map<Long, Long> oldProvidersMap = oldProviders.entrySet().stream().collect(
            Collectors.toMap(e -> Long.decode(e.getKey()), e -> e.getValue().longValue()));
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
    }

    /**
     * Test adding host and storages in a plan.
     */
    @Test
    public void testTopologyAdditionHostAndStorageClones() {
        Map<Long, TopologyEntity.Builder> topology = Stream.of(vm, pm, st)
                        .collect(Collectors.toMap(TopologyEntity.Builder::getOid, Function.identity()));

                // Add hosts and storages
                List<ScenarioChange> changes = Lists.newArrayList(ADD_HOST);
                changes.addAll(Lists.newArrayList(ADD_STORAGE));

                topologyEditor.editTopology(topology, changes, topologyInfo, groupResolver);

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

    @Test
    public void testTopologyAdditionGroup() {
        final long groupId = 7L;
        final long vmId = 1L;
        final long vmCloneId = 182;
        final Group group = Group.newBuilder()
                .setGroup(GroupInfo.getDefaultInstance())
                .setId(groupId)
                .build();
        final TopologyEntity.Builder vmEntity = TopologyEntityUtils.topologyEntityBuilder(
                TopologyEntityDTO.newBuilder()
                    .setOid(vmId)
                    .setEntityType(10)
                    .setDisplayName("VM"));

        final Map<Long, TopologyEntity.Builder> topology = new HashMap<>();
        topology.put(vmId, vmEntity);

        final List<ScenarioChange> changes = Collections.singletonList(ScenarioChange.newBuilder()
                .setTopologyAddition(TopologyAddition.newBuilder()
                        .setGroupId(groupId))
                .build());
        when(groupServiceSpy.getGroups(any())).thenReturn(Collections.singletonList(group));
        when(groupResolver.resolve(eq(group), isA(TopologyGraph.class)))
                .thenReturn(Collections.singleton(vmId));
        when(identityProvider.getCloneId(any(TopologyEntityDTO.class))).thenReturn(vmCloneId);

        topologyEditor.editTopology(topology, changes, topologyInfo, groupResolver);

        assertTrue(topology.containsKey(vmCloneId));
        final TopologyEntity.Builder cloneBuilder = topology.get(vmCloneId);
        assertThat(cloneBuilder.getDisplayName(), Matchers.containsString(vmEntity.getDisplayName()));
        assertThat(cloneBuilder.getEntityBuilder().getOrigin().getPlanScenarioOrigin(),
            is(PlanScenarioOrigin.newBuilder()
                .setPlanId(topologyInfo.getTopologyContextId())
                .build()));
    }

    @Test
    public void testEditTopologyChangeUtilizationLevel() {
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
        topologyEditor.editTopology(topology, changes, topologyInfo, groupResolver);
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
    public void testEditTopologyChangeUtilizationWithUnplacedVM() {
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
        topologyEditor.editTopology(topology, changes, topologyInfo, groupResolver);
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
    public void testTopologyReplace() {
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

        topologyEditor.editTopology(topology, changes, topologyInfo, groupResolver);
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
}
