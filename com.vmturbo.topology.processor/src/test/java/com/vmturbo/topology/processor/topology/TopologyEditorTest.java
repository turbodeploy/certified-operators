package com.vmturbo.topology.processor.topology;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.gson.Gson;

import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.TopologyAddition;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.TopologyReplace;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Builder;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTOOrBuilder;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.template.TemplateConverterFactory;

/**
 * Unit tests for {@link ScenarioChange}.
 */
public class TopologyEditorTest {

    private static final long vmId = 10;
    private static final long pmId = 20;
    private static final long stId = 30;

    private static final CommodityType MEM = CommodityType.newBuilder().setType(1).build();
    private static final CommodityType CPU = CommodityType.newBuilder().setType(2).build();
    private static final CommodityType LATENCY = CommodityType.newBuilder().setType(3).build();
    private static final CommodityType IOPS = CommodityType.newBuilder().setType(4).build();

    private final static TopologyEntityDTO vm = TopologyEntityDTO.newBuilder()
            .setOid(vmId)
            .setDisplayName("VM")
            .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
            .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                .setProviderId(pmId)
                .addCommodityBought(CommodityBoughtDTO.newBuilder().setCommodityType(MEM).build())
                .addCommodityBought(CommodityBoughtDTO.newBuilder().setCommodityType(CPU).build())
                .build())
            .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                .setProviderId(stId)
                .addCommodityBought(CommodityBoughtDTO.newBuilder().setCommodityType(LATENCY).build())
                .addCommodityBought(CommodityBoughtDTO.newBuilder().setCommodityType(IOPS).build())
                .build())
            .build();

    private final static TopologyEntityDTO pm = TopologyEntityDTO.newBuilder()
            .setOid(pmId)
            .setDisplayName("PM")
            .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
            .addCommoditySoldList(CommoditySoldDTO.newBuilder().setCommodityType(MEM).build())
            .addCommoditySoldList(CommoditySoldDTO.newBuilder().setCommodityType(CPU).build())
            .build();

    private final TopologyEntityDTO st = TopologyEntityDTO.newBuilder()
            .setOid(stId)
            .setDisplayName("ST")
            .setEntityType(EntityType.STORAGE_VALUE)
            .addCommoditySoldList(CommoditySoldDTO.newBuilder().setCommodityType(LATENCY).build())
            .addCommoditySoldList(CommoditySoldDTO.newBuilder().setCommodityType(IOPS).build())
            .build();

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

    private TopologyEditor topologyEditor =
            new TopologyEditor(identityProvider, templateConverterFactory);

    @Before
    public void setup() {
        when(identityProvider.getCloneId(Mockito.any(TopologyEntityDTO.class))).thenAnswer(new Answer<Long>() {
            @Override
            public Long answer(InvocationOnMock invocation) throws Throwable {
                return cloneId++;
            }
        });
    }

    /**
     * Test adding entities in a plan.
     */
    @Test
    public void testTopologyAddition() {
        Map<Long, TopologyEntityDTO.Builder> topology = Stream.of(vm, pm, st)
                .map(TopologyEntityDTO::toBuilder)
                .collect(Collectors.toMap(TopologyEntityDTOOrBuilder::getOid, Function.identity()));
        List<ScenarioChange> changes = Lists.newArrayList(ADD);

        topologyEditor.editTopology(topology, changes);

        List<TopologyEntityDTOOrBuilder> clones = topology.values().stream()
                        .filter(entity -> entity.getOid() != vm.getOid())
                        .filter(entity -> entity.getEntityType() == vm.getEntityType())
                        .collect(Collectors.toList());
        clones.remove(vm);
        assertEquals(NUM_CLONES, clones.size());

        // Verify display names are (e.g.) "VM - Clone #123"
        List<String> names = clones.stream().map(TopologyEntityDTOOrBuilder::getDisplayName)
                        .collect(Collectors.toList());
        names.sort(String::compareTo);
        IntStream.range(0, NUM_CLONES).forEach(i -> {
            assertEquals(names.get(i), vm.getDisplayName() + " - Clone #" + i);
        });

        TopologyEntityDTOOrBuilder oneClone = clones.get(0);
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
                            vm.getCommoditiesBoughtFromProvidersList().stream()
                .filter(bought -> bought.getProviderId() == oldProvider)
                .findAny().get();
            assertEquals(cloneCommBought.getCommodityBoughtList(),
                vmCommBought.getCommodityBoughtList());
        }
    }

    @Test
    public void testTopologyReplace() {
        Map<Long, TopologyEntityDTO.Builder> topology = Stream.of(vm, pm, st)
                .map(TopologyEntityDTO::toBuilder)
                .collect(Collectors.toMap(TopologyEntityDTOOrBuilder::getOid, Function.identity()));
        List<ScenarioChange> changes = Lists.newArrayList(REPLACE);
        final Multimap<Long, TopologyEntityDTO> templateToReplacedEntity = ArrayListMultimap.create();
        templateToReplacedEntity.put(TEMPLATE_ID, pm);
        when(templateConverterFactory.
                generateTopologyEntityFromTemplates(Collections.emptyMap(),
                        templateToReplacedEntity))
                .thenReturn(Stream.of(TopologyEntityDTO.newBuilder(pm)
                        .setOid(1234L)
                        .setDisplayName("Test PM1")));

        topologyEditor.editTopology(topology, changes);
        final List<TopologyEntityDTO> topologyEntityDTOS = topology.entrySet().stream()
                .map(Entry::getValue)
                .map(Builder::build)
                .collect(Collectors.toList());
        assertEquals(3, topologyEntityDTOS.size());
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
