package com.vmturbo.topology.processor.topology;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.gson.Gson;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

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
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
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

    private static final CommodityType MEM = CommodityType.newBuilder().setType(21).build();
    private static final CommodityType CPU = CommodityType.newBuilder().setType(40).build();
    private static final CommodityType LATENCY = CommodityType.newBuilder().setType(3).build();
    private static final CommodityType IOPS = CommodityType.newBuilder().setType(4).build();

    private final static TopologyEntity.Builder vm = TopologyEntityUtils.topologyEntityBuilder(
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
    );

    private final static TopologyEntity.Builder unplacedVm = TopologyEntityUtils.topologyEntityBuilder(
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

    private final static TopologyEntity.Builder pm = TopologyEntityUtils.topologyEntityBuilder(
        TopologyEntityDTO.newBuilder()
            .setOid(pmId)
            .setDisplayName("PM")
            .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
            .addCommoditySoldList(CommoditySoldDTO.newBuilder().setCommodityType(MEM).setUsed(USED)
                .setAccesses(vmId).build())
            .addCommoditySoldList(CommoditySoldDTO.newBuilder().setCommodityType(CPU).setUsed(USED)
                .setAccesses(vmId).build())
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
    );

    private static int NUM_CLONES = 5;

    private static long TEMPLATE_ID = 123;

    private final ScenarioChange ADD = ScenarioChange.newBuilder()
                    .setTopologyAddition(TopologyAddition.newBuilder()
                        .setAdditionCount(NUM_CLONES)
                        .setEntityId(vmId)
                        .build())
                    .build();

    private final ScenarioChange REPLACE = ScenarioChange.newBuilder()
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
        when(identityProvider.getCloneId(Mockito.any(TopologyEntityDTO.class)))
            .thenAnswer(invocation -> cloneId++);
        topologyEditor = new TopologyEditor(identityProvider, templateConverterFactory,
                GroupServiceGrpc.newBlockingStub(grpcServer.getChannel()));
    }

    /**
     * Test adding entities in a plan.
     */
    @Test
    public void testTopologyAddition() {
        Map<Long, TopologyEntity.Builder> topology = Stream.of(vm, pm, st)
                .collect(Collectors.toMap(TopologyEntity.Builder::getOid, Function.identity()));
        List<ScenarioChange> changes = Lists.newArrayList(ADD);

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
        final Multimap<Long, TopologyEntityDTO> templateToReplacedEntity = ArrayListMultimap.create();
        templateToReplacedEntity.put(TEMPLATE_ID, pm.getEntityBuilder().build());
        final Map<Long, Long> topologyAdditionEmpty = Collections.emptyMap();
        when(templateConverterFactory.
                generateTopologyEntityFromTemplates(Mockito.eq(topologyAdditionEmpty),
                        Mockito.eq(templateToReplacedEntity)))
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
        // make sure consumers of replaced PM are unplaced
        assertTrue(topologyEntityDTOS.stream()
                .filter(topologyEntity ->
                        topologyEntity.getEntityType() == EntityType.VIRTUAL_MACHINE_VALUE)
                .map(TopologyEntityDTO::getCommoditiesBoughtFromProvidersList)
                .flatMap(List::stream)
                .anyMatch(commoditiesBoughtFromProvider ->
                        !commoditiesBoughtFromProvider.hasProviderId() ||
                                commoditiesBoughtFromProvider.getProviderId() <= 0));
    }
}
