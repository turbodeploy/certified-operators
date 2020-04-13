package com.vmturbo.topology.processor.template;

import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.plan.TemplateDTO.GetTemplatesRequest;
import com.vmturbo.common.protobuf.plan.TemplateDTO.GetTemplatesResponse;
import com.vmturbo.common.protobuf.plan.TemplateDTO.SingleTemplateResponse;
import com.vmturbo.common.protobuf.plan.TemplateDTO.Template;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplatesFilter;
import com.vmturbo.common.protobuf.plan.TemplateDTOMoles.TemplateServiceMole;
import com.vmturbo.common.protobuf.plan.TemplateServiceGrpc;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Builder;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.processor.identity.IdentityProvider;

/**
 * Unit tests for {@link TemplateConverterFactory}.
 */
public class TemplateConverterFactoryTest {

    private final TemplateServiceMole templateServiceMole = Mockito.spy(new TemplateServiceMole());

    private final IdentityProvider identityProvider = Mockito.mock(IdentityProvider.class);

    private TemplateConverterFactory templateConverterFactory;

    private static final Long TEMPLATE_ID = 123L;

    private final Set<Integer> provisionCommodityType =
            ImmutableSet.of(CommodityType.CPU_PROVISIONED_VALUE, CommodityType.MEM_PROVISIONED_VALUE,
                    CommodityType.STORAGE_PROVISIONED_VALUE);

    private Map<Long, TopologyEntity.Builder> topology = Maps.newHashMap();

    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(templateServiceMole);

    @Before
    public void setup() {
        this.templateConverterFactory = new TemplateConverterFactory(
                TemplateServiceGrpc.newBlockingStub(grpcServer.getChannel()), identityProvider);
    }

    @Test
    public void testTemplateAddition() {
        final Map<Long, Long> templateAdditions = ImmutableMap.of(TEMPLATE_ID, 2L);
        when(templateServiceMole
               .getTemplates(GetTemplatesRequest.newBuilder()
                   .setFilter(TemplatesFilter.newBuilder()
                        .addTemplateIds(TEMPLATE_ID))
                   .build()))
                .thenReturn(Lists.newArrayList(GetTemplatesResponse.newBuilder()
                    .addTemplates(SingleTemplateResponse.newBuilder()
                        .setTemplate(Template.newBuilder()
                            .setId(TEMPLATE_ID)
                            .setTemplateInfo(TemplateConverterTestUtil.VM_TEMPLATE_INFO)))
                    .build()));
        final Stream<TopologyEntityDTO.Builder> topologyEntityForTemplates =
                templateConverterFactory.generateTopologyEntityFromTemplates(templateAdditions,
                        ArrayListMultimap.create(), topology);
        final List<TopologyEntityDTO> topologyEntityDTOList = topologyEntityForTemplates
                .map(Builder::build)
                .collect(Collectors.toList());
        Assert.assertEquals(2, topologyEntityDTOList.size());
        Assert.assertEquals(1,
                topologyEntityDTOList.get(0).getCommoditiesBoughtFromProvidersList().stream()
                        .filter(commoditiesBoughtFromProvider ->
                                commoditiesBoughtFromProvider.getProviderEntityType() ==
                                        EntityType.PHYSICAL_MACHINE_VALUE)
                        .count());
        Assert.assertEquals(1,
                topologyEntityDTOList.get(0).getCommoditiesBoughtFromProvidersList().stream()
                        .filter(commoditiesBoughtFromProvider ->
                                commoditiesBoughtFromProvider.getProviderEntityType() ==
                                        EntityType.STORAGE_VALUE)
                        .count());
        Assert.assertTrue(topologyEntityDTOList.stream()
                .anyMatch(entity -> entity.getDisplayName().contains("Cloning")));
        Assert.assertTrue(topologyEntityDTOList.stream()
                .allMatch(entity -> entity.getAnalysisSettings().getShopTogether()));
    }

    @Test
    public void testTemplateReplace() {
        final TopologyEntityDTO originalTopologyEntityOne = TopologyEntityDTO.newBuilder()
                .setOid(1)
                .setEntityType(10)
                .addAllCommoditySoldList(TemplateConverterTestUtil.VM_COMMODITY_SOLD)
                .addAllCommoditiesBoughtFromProviders(
                        TemplateConverterTestUtil.VM_COMMODITY_BOUGHT_FROM_PROVIDER)
                .build();
        final TopologyEntityDTO originalTopologyEntityTwo = TopologyEntityDTO.newBuilder()
                .setOid(2)
                .setEntityType(10)
                .addAllCommoditySoldList(TemplateConverterTestUtil.VM_COMMODITY_SOLD)
                .addAllCommoditiesBoughtFromProviders(
                        TemplateConverterTestUtil.VM_COMMODITY_BOUGHT_FROM_PROVIDER)
                .build();
        topology.put(originalTopologyEntityOne.getOid(),
            TopologyEntity.newBuilder(originalTopologyEntityOne.toBuilder()));
        topology.put(originalTopologyEntityTwo.getOid(),
            TopologyEntity.newBuilder(originalTopologyEntityTwo.toBuilder()));
        when(templateServiceMole.getTemplates(GetTemplatesRequest.newBuilder()
                .setFilter(TemplatesFilter.newBuilder()
                    .addTemplateIds(TEMPLATE_ID))
                .build()))
            .thenReturn(Lists.newArrayList(GetTemplatesResponse.newBuilder()
                .addTemplates(SingleTemplateResponse.newBuilder()
                    .setTemplate(Template.newBuilder()
                        .setId(TEMPLATE_ID)
                        .setTemplateInfo(TemplateConverterTestUtil.VM_TEMPLATE_INFO)))
                .build()));
        final Multimap<Long, Long> templateToReplacedEntity = ArrayListMultimap.create();
        templateToReplacedEntity.put(TEMPLATE_ID, originalTopologyEntityOne.getOid());
        templateToReplacedEntity.put(TEMPLATE_ID, originalTopologyEntityTwo.getOid());
        final Stream<TopologyEntityDTO.Builder> topologyEntityForTemplates =
                templateConverterFactory.generateTopologyEntityFromTemplates(Collections.emptyMap(),
                        templateToReplacedEntity, topology);
        final List<TopologyEntityDTO> topologyEntityDTOList = topologyEntityForTemplates
                .map(Builder::build)
                .collect(Collectors.toList());
        Assert.assertEquals(2, topologyEntityDTOList.size());
        Assert.assertTrue(topologyEntityDTOList.stream()
                .anyMatch(entity -> entity.getDisplayName().contains("Replacing")));
        Assert.assertTrue(topologyEntityDTOList.stream()
                .allMatch(entity -> entity.getAnalysisSettings().getShopTogether()));
    }

    @Test
    public void testTemplateAdditionForReservation() throws Exception {
        final Map<Long, Long> templateAdditions = ImmutableMap.of(TEMPLATE_ID, 3L);
        when(templateServiceMole.getTemplates(GetTemplatesRequest.newBuilder()
                .setFilter(TemplatesFilter.newBuilder()
                    .addTemplateIds(TEMPLATE_ID))
                .build()))
            .thenReturn(Lists.newArrayList(GetTemplatesResponse.newBuilder()
                .addTemplates(SingleTemplateResponse.newBuilder()
                    .setTemplate(Template.newBuilder()
                        .setId(TEMPLATE_ID)
                        .setTemplateInfo(TemplateConverterTestUtil.VM_TEMPLATE_INFO)))
                .build()));
        final Stream<TopologyEntityDTO.Builder> topologyEntityForTemplates =
                templateConverterFactory.generateReservationEntityFromTemplates(templateAdditions, topology);
        final List<TopologyEntityDTO> topologyEntityDTOList = topologyEntityForTemplates
                .map(Builder::build)
                .collect(Collectors.toList());
        Assert.assertEquals(3, topologyEntityDTOList.size());
        Assert.assertTrue(topologyEntityDTOList.get(0).getCommoditiesBoughtFromProvidersList().stream()
                .allMatch(commoditiesBoughtFromProvider ->
                        commoditiesBoughtFromProvider.getCommodityBoughtList().stream()
                                .filter(commodityBoughtDTO -> commodityBoughtDTO.getUsed() > 0)
                                .allMatch(commodityBoughtDTO ->
                                        provisionCommodityType.contains(commodityBoughtDTO
                                                .getCommodityType().getType()))));
        Assert.assertTrue(topologyEntityDTOList.stream()
                .allMatch(entity -> entity.getAnalysisSettings().getShopTogether()));
    }
}
