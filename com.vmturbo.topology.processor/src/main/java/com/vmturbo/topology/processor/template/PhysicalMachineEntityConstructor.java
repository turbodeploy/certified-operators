package com.vmturbo.topology.processor.template;

import static com.vmturbo.common.protobuf.plan.TemplateDTO.ResourcesCategory.ResourcesCategoryName.Compute;
import static com.vmturbo.common.protobuf.plan.TemplateDTO.ResourcesCategory.ResourcesCategoryName.Infrastructure;
import static com.vmturbo.topology.processor.template.TemplatesConverterUtils.addCommodityConstraints;
import static com.vmturbo.topology.processor.template.TemplatesConverterUtils.createCommodityBoughtDTO;
import static com.vmturbo.topology.processor.template.TemplatesConverterUtils.createCommoditySoldDTO;
import static com.vmturbo.topology.processor.template.TemplatesConverterUtils.getActiveCommoditiesWithKeysGroups;
import static com.vmturbo.topology.processor.template.TemplatesConverterUtils.getCommoditySoldConstraint;

import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.common.protobuf.plan.TemplateDTO.Template;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateResource;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Create a topologyEntityDTO from Physical Machine template. The new Topology Entity contains such as OID,
 * displayName, commodity sold, commodity bought, entity state, provider policy and consumer policy.
 * And also it will try to keep all commodity constrains from the original topology entity.
 */
public class PhysicalMachineEntityConstructor implements TopologyEntityConstructor {
    private static final String ZERO = "0";

    /**
     * Create a topologyEntityDTO from Physical Machine template. It mainly created Commodity bought
     * and Commodity sold from template fields.
     *
     * @param template physical machine template.
     * @param topologyEntityBuilder builder of TopologyEntityDTO which could contains some basic setting already.
     * @param originalTopologyEntity the original topology entity which this template want to keep its
     *                               commodity constraints. It could be null, if it is new adding template.
     * @return {@link TopologyEntityDTO}
     */
    @Override
    public TopologyEntityDTO.Builder createTopologyEntityFromTemplate(
            @Nonnull final Template template,
            @Nonnull final TopologyEntityDTO.Builder topologyEntityBuilder,
            @Nullable final TopologyEntityDTO originalTopologyEntity) {
        final List<CommoditiesBoughtFromProvider> commodityBoughtConstraints = getActiveCommoditiesWithKeysGroups(
            originalTopologyEntity);
        final Set<CommoditySoldDTO> commoditySoldConstraints = getCommoditySoldConstraint(
            originalTopologyEntity);
        final List<TemplateResource> computeTemplateResources =
            TemplatesConverterUtils.getTemplateResources(template, Compute);
        addComputeCommodities(topologyEntityBuilder, computeTemplateResources);

        final List<TemplateResource> InfraTemplateResources =
            TemplatesConverterUtils.getTemplateResources(template, Infrastructure);
        addInfraCommodities(topologyEntityBuilder, InfraTemplateResources);
        addCommodityConstraints(topologyEntityBuilder, commoditySoldConstraints, commodityBoughtConstraints);
        return topologyEntityBuilder;
    }

    /**
     * Generate commodities for compute template resources.
     *
     * @param topologyEntityBuilder builder of TopologyEntityDTO.
     * @param computeTemplateResources a list of compute template resources.
     */
    private static void addComputeCommodities(@Nonnull TopologyEntityDTO.Builder topologyEntityBuilder,
                                              @Nonnull List<TemplateResource> computeTemplateResources) {
        final Map<String, String> fieldNameValueMap =
            TemplatesConverterUtils.createFieldNameValueMap(computeTemplateResources);
        addComputeCommoditiesSold(topologyEntityBuilder, fieldNameValueMap);
    }

    /**
     * Generate a list of CPU, Memory, IO and Network commodity sold.
     *
     * @param topologyEntityBuilder builder of TopologyEntityDTO.
     * @param fieldNameValueMap a Map which key is template field name and value is field value.
     */
    private static void addComputeCommoditiesSold(@Nonnull final TopologyEntityDTO.Builder topologyEntityBuilder,
                                                  @Nonnull Map<String, String> fieldNameValueMap) {
        addComputeCommoditiesCpuMemSold(topologyEntityBuilder, fieldNameValueMap);
        addComputeCommoditiesIONetSold(topologyEntityBuilder, fieldNameValueMap);
    }

    /**
     * Generate CPU and Memory commodity sold and add to TopologyEntityDTO.
     *
     * @param topologyEntityBuilder builder of TopologyEntityDTO.
     * @param fieldNameValueMap a Map which key is template field name and value is field value.
     */
    private static void addComputeCommoditiesCpuMemSold(@Nonnull final TopologyEntityDTO.Builder topologyEntityBuilder,
                                                        @Nonnull Map<String, String> fieldNameValueMap) {
        final double numOfCpu = Double.valueOf(
            fieldNameValueMap.getOrDefault(TemplatesConverterUtils.NUM_OF_CORES, ZERO));
        final double cpuSpeed = Double.valueOf(
            fieldNameValueMap.getOrDefault(TemplatesConverterUtils.CPU_SPEED, ZERO));
        final double memSize = Double.valueOf(
            fieldNameValueMap.getOrDefault(TemplatesConverterUtils.MEMORY_SIZE, ZERO));

        CommoditySoldDTO cpuCommodity =
            createCommoditySoldDTO(CommodityDTO.CommodityType.CPU_VALUE, numOfCpu * cpuSpeed);
        CommoditySoldDTO  memCommodity =
            createCommoditySoldDTO(CommodityDTO.CommodityType.MEM_VALUE, memSize);
        topologyEntityBuilder
            .addCommoditySoldList(cpuCommodity)
            .addCommoditySoldList(memCommodity);
    }

    /**
     * Generate IO and Network commodity sold and add to TopologyEntityDTO.
     *
     * @param topologyEntityBuilder builder of TopologyEntityDTO.
     * @param fieldNameValueMap a Map which key is template field name and value is field value.
     */
    private static void addComputeCommoditiesIONetSold(@Nonnull final TopologyEntityDTO.Builder topologyEntityBuilder,
                                                       @Nonnull Map<String, String> fieldNameValueMap) {
        final double ioThroughputSize = Double.valueOf(
            fieldNameValueMap.getOrDefault(TemplatesConverterUtils.IO_THROUGHPUT_SIZE, ZERO));
        final double networkThroughputSize = Double.valueOf(
            fieldNameValueMap.getOrDefault(TemplatesConverterUtils.NETWORK_THROUGHPUT_SIZE, ZERO));

        CommoditySoldDTO ioThroughputCommodity =
            createCommoditySoldDTO(CommodityDTO.CommodityType.IO_THROUGHPUT_VALUE, ioThroughputSize);
        CommoditySoldDTO networkCommodity =
            createCommoditySoldDTO(CommodityDTO.CommodityType.NET_THROUGHPUT_VALUE, networkThroughputSize);
        topologyEntityBuilder
            .addCommoditySoldList(ioThroughputCommodity)
            .addCommoditySoldList(networkCommodity);
    }

    /**
     * Generate commodities for infrastructure template resources.
     *
     * @param topologyEntityBuilder builder of TopologyEntityDTO.
     * @param infraTemplateResources  a list of infrastructure template resources.
     */
    private static void addInfraCommodities(@Nonnull TopologyEntityDTO.Builder topologyEntityBuilder,
                                            @Nonnull List<TemplateResource> infraTemplateResources) {
        final Map<String, String> fieldNameValueMap =
            TemplatesConverterUtils.createFieldNameValueMap(infraTemplateResources);
        addInfraCommoditiesBought(topologyEntityBuilder, fieldNameValueMap);
    }

    /**
     * Generate a list of commodity bought and add to TopologyEntityDTO.
     *
     * @param topologyEntityBuilder builder of TopologyEntityDTO.
     * @param fieldNameValueMap a Map which key is template field name and value is field value.
     */
    private static void addInfraCommoditiesBought(
            @Nonnull final TopologyEntityDTO.Builder topologyEntityBuilder,
            @Nonnull Map<String, String> fieldNameValueMap) {
        final double powerSize = Double.valueOf(
            fieldNameValueMap.getOrDefault(TemplatesConverterUtils.POWER_SIZE, ZERO));
        final double spaceSize = Double.valueOf(
            fieldNameValueMap.getOrDefault(TemplatesConverterUtils.SPACE_SIZE, ZERO));
        final double coolingSize = Double.valueOf(
            fieldNameValueMap.getOrDefault(TemplatesConverterUtils.COOLING_SIZE, ZERO));

        CommodityBoughtDTO powerSizeCommodity =
            createCommodityBoughtDTO(CommodityDTO.CommodityType.POWER_VALUE, powerSize);
        CommodityBoughtDTO spaceSizeCommodity =
            createCommodityBoughtDTO(CommodityDTO.CommodityType.COOLING_VALUE, spaceSize);
        CommodityBoughtDTO coolingSizeCommodity =
            createCommodityBoughtDTO(CommodityDTO.CommodityType.SPACE_VALUE, coolingSize);

        final CommoditiesBoughtFromProvider newCommoditiesBoughtFromProvider =
            CommoditiesBoughtFromProvider.newBuilder()
                .addCommodityBought(powerSizeCommodity)
                .addCommodityBought(spaceSizeCommodity)
                .addCommodityBought(coolingSizeCommodity)
                .setMovable(true)
                .setProviderEntityType(EntityType.DATACENTER_VALUE)
                .build();
        topologyEntityBuilder.addCommoditiesBoughtFromProviders(newCommoditiesBoughtFromProvider);
    }
}
