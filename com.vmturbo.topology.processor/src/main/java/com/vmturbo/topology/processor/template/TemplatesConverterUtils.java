package com.vmturbo.topology.processor.template;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.plan.TemplateDTO.ResourcesCategory.ResourcesCategoryName;
import com.vmturbo.common.protobuf.plan.TemplateDTO.Template;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateField;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateInfo;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateResource;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConsumerPolicy;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ProviderPolicy;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.common.dto.CommonDTOREST.CommodityDTO;

/**
 * A helper class used for create TopologyEntity from Templates.
 */
public class TemplatesConverterUtils {
    // compute
    public static final String CPU_CONSUMED_FACTOR = "cpuConsumedFactor";
    public static final String CPU_SPEED = "cpuSpeed";
    public static final String IO_THROUGHPUT = "ioThroughput";
    public static final String IO_THROUGHPUT_SIZE = "ioThroughputSize";
    public static final String MEMORY_CONSUMED_FACTOR = "memoryConsumedFactor";
    public static final String MEMORY_SIZE = "memorySize";
    public static final String NUM_OF_CPU = "numOfCpu";
    public static final String NUM_OF_CORES = "numOfCores";
    public static final String NETWORK_THROUGHPUT = "networkThroughput";
    public static final String NETWORK_THROUGHPUT_SIZE = "networkThroughputSize";
    // storage
    public static final String DISK_IOPS = "diskIops";
    public static final String DISK_SIZE = "diskSize";
    public static final String DISK_CONSUMED_FACTOR = "diskConsumedFactor";

    //Infrastructure
    public static final String POWER_SIZE = "powerSize";
    public static final String SPACE_SIZE = "spaceSize";
    public static final String COOLING_SIZE = "coolingSize";

    /**
     * Generate a {@link TopologyEntityDTO} builder contains common fields between different templates.
     *
     * @param template {@link Template}
     * @return {@link TopologyEntityDTO} builder
     */
    public static TopologyEntityDTO.Builder generateTopologyEntityBuilder(@Nonnull final Template template) {
        final TemplateInfo templateInfo = template.getTemplateInfo();
        final TopologyEntityDTO.Builder topologyEntityBuilder = TopologyEntityDTO.newBuilder()
            .setEntityType(templateInfo.getEntityType())
            .setEntityState(EntityState.POWERED_ON)
            .setProviderPolicy(ProviderPolicy.newBuilder()
                .setIsAvailableAsProvider(true)
                .build())
            .setConsumerPolicy(ConsumerPolicy.newBuilder()
                .setShopsTogether(false)
                .build());
        return topologyEntityBuilder;
    }

    /**
     * Filter input template resources and only keep resources which category name matched with input name
     * parameter.
     *
     * @param template {@link Template} used to get all TemplateResource.
     * @param name {@link ResourcesCategoryName}.
     * @return a list of {@link TemplateResource} which category is equal to input name.
     */
    public static List<TemplateResource> getTemplateResources(@Nonnull Template template,
                                                              @Nonnull ResourcesCategoryName name) {

        return template.getTemplateInfo()
            .getResourcesList()
            .stream()
            .filter(resource -> resource.getCategory().getName().equals(name))
            .collect(Collectors.toList());
    }

    /**
     * Get all template fields from template resources and generate a mapping from template field name
     * to template field value.
     *
     * @param templateResources list of {@link TemplateResource}.
     * @return A Map which key is template field name and value is template field value.
     */
    public static Map<String, String> createFieldNameValueMap(@Nonnull final List<TemplateResource> templateResources) {
        final List<TemplateField> fields = getTemplateField(templateResources);
        // Since for one templateSpec, its field name is unique. There should be no conflicts.
        return fields.stream()
            .collect(Collectors.toMap(TemplateField::getName, TemplateField::getValue));
    }

    /**
     * Get all template fields from template resources.
     *
     * @param templateResources list of {@link TemplateResource}.
     * @return list of {@link TemplateField}.
     */
    private static List<TemplateField> getTemplateField(@Nonnull final List<TemplateResource> templateResources) {
        return templateResources.stream()
            .map(TemplateResource::getFieldsList)
            .flatMap(List::stream)
            .collect(Collectors.toList());
    }

    public static CommodityBoughtDTO createCommodityBoughtDTO(int commodityType, double used) {
        return CommodityBoughtDTO.newBuilder()
            .setUsed(used)
            .setActive(true)
            .setCommodityType(CommodityType.newBuilder()
                .setType(commodityType))
            .build();
    }

    public static CommoditySoldDTO createCommoditySoldDTO(int commodityType, double capacity) {
        return CommoditySoldDTO.newBuilder()
            .setActive(true)
            .setCapacity(capacity)
            .setCommodityType(CommodityType.newBuilder()
                .setType(commodityType))
            .build();
    }
}
