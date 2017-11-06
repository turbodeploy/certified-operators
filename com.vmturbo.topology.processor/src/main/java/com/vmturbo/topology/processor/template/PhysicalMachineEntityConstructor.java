package com.vmturbo.topology.processor.template;

import static com.vmturbo.common.protobuf.plan.TemplateDTO.ResourcesCategory.ResourcesCategoryName.Compute;
import static com.vmturbo.common.protobuf.plan.TemplateDTO.ResourcesCategory.ResourcesCategoryName.Infrastructure;
import static com.vmturbo.topology.processor.template.TemplatesConverterUtils.createCommodityBoughtDTO;
import static com.vmturbo.topology.processor.template.TemplatesConverterUtils.createCommoditySoldDTO;

import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.plan.TemplateDTO.Template;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateInfo;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateResource;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateSpec;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;

/**
 * Create a topologyEntityDTO from Physical Machine template. The new Topology Entity contains such as OID,
 * displayName, commodity sold, commodity bought, entity state, provider policy and consumer policy.
 */
public class PhysicalMachineEntityConstructor implements TopologyEntityConstructor {

    private static final String ZERO = "0";

    /**
     * Create a topologyEntityDTO from Physical Machine template. It mainly created Commodity bought
     * and Commodity sold from template fields.
     *
     * @param template physical machine template.
     * @return {@link TopologyEntityDTO}
     */
    @Override
    public TopologyEntityDTO createTopologyEntityFromTemplate(
        @Nonnull final Template template,
        @Nonnull final TopologyEntityDTO.Builder topologyEntityBuilder) {
        final List<TemplateResource> computeTemplateResources =
            TemplatesConverterUtils.getTemplateResources(template, Compute);
        addComputeCommodities(topologyEntityBuilder, computeTemplateResources);

        final List<TemplateResource> storageTemplateResources =
            TemplatesConverterUtils.getTemplateResources(template, Infrastructure);
        addInfraCommodities(topologyEntityBuilder, storageTemplateResources);

        return topologyEntityBuilder.build();
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
    private static void addInfraCommoditiesBought(@Nonnull final TopologyEntityDTO.Builder topologyEntityBuilder,
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

        topologyEntityBuilder.addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
            .addCommodityBought(powerSizeCommodity)
            .addCommodityBought(spaceSizeCommodity)
            .addCommodityBought(coolingSizeCommodity));
    }
}
