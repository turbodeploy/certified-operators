package com.vmturbo.topology.processor.analysis;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.collect.Lists;
import com.google.gson.Gson;

import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.TopologyAddition;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.template.TemplateConverterFactory;
import com.vmturbo.topology.processor.template.TopologyEntityConstructor;

/**
 * Unit tests for {@link ScenarioChange}.
 */
public class ScenarioChangeTest {

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

    private final ScenarioChange ADD = ScenarioChange.newBuilder()
                    .setTopologyAddition(TopologyAddition.newBuilder()
                        .setAdditionCount(NUM_CLONES)
                        .setEntityId(vmId)
                        .build())
                    .build();

    private static IdentityProvider identityProvider = Mockito.mock(IdentityProvider.class);
    private static long cloneId = 1000L;

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
        List<TopologyEntityDTO> topology = Lists.newArrayList(vm, pm, st);
        List<ScenarioChange> changes = Lists.newArrayList(ADD);
        TemplateConverterFactory templateConverterFactory = Mockito.mock(TemplateConverterFactory.class);
        Collection<TopologyEntityDTO> result =
            AnalysisService.editTopology(topology, changes, identityProvider, templateConverterFactory);
        List<TopologyEntityDTO> clones = result.stream()
                        .filter(entity -> entity.getEntityType() == vm.getEntityType())
                        .collect(Collectors.toList());
        clones.remove(vm);
        assertEquals(NUM_CLONES, clones.size());

        // Verify display names are (e.g.) "VM - Clone #123"
        List<String> names = clones.stream().map(TopologyEntityDTO::getDisplayName)
                        .collect(Collectors.toList());
        names.sort(String::compareTo);
        IntStream.range(0, NUM_CLONES).forEach(i -> {
            assertEquals(names.get(i), vm.getDisplayName() + " - Clone #" + i);
        });

        TopologyEntityDTO oneClone = clones.get(0);
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
}
