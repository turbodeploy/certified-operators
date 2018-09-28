package com.vmturbo.topology.processor.template;

import static com.vmturbo.common.protobuf.plan.TemplateDTO.ResourcesCategory.ResourcesCategoryName.Compute;
import static com.vmturbo.common.protobuf.plan.TemplateDTO.ResourcesCategory.ResourcesCategoryName.Infrastructure;
import static com.vmturbo.topology.processor.template.TemplatesConverterUtils.addCommodityConstraints;
import static com.vmturbo.topology.processor.template.TemplatesConverterUtils.createCommodityBoughtDTO;
import static com.vmturbo.topology.processor.template.TemplatesConverterUtils.createCommoditySoldDTO;
import static com.vmturbo.topology.processor.template.TemplatesConverterUtils.getActiveCommoditiesWithKeysGroups;
import static com.vmturbo.topology.processor.template.TemplatesConverterUtils.getCommoditySoldConstraint;
import static com.vmturbo.topology.processor.template.TemplatesConverterUtils.updateRelatedEntityAccesses;

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
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Builder;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTOOrBuilder;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.PhysicalMachineInfo;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;

/**
 * Create a topologyEntityDTO from Physical Machine template. The new Topology Entity contains such as OID,
 * displayName, commodity sold, commodity bought, entity state, provider policy and consumer policy.
 * And also it will try to keep all commodity constrains from the original topology entity.
 */
public class PhysicalMachineEntityConstructor implements TopologyEntityConstructor {

    private static final String ZERO = "0";

    // Max 256 LUNS based on https://www.vmware.com/pdf/vsphere6/r60/vsphere-60-configuration-maximums.pdf
    public static final int MAX_LUN_LIMIT = 256;

    // Random numbers ported from legacy.
    public static final double QX_VCPU_BASE_COEFFICIENT = 20000.0;
    public static final double BALLOONING_DEFAULT_CAPACITY = 1.0E9;
    public static final double SWAPPING_DEFAULT_CAPACITY = 5000.0;

    /**
     * Create a topologyEntityDTO from Physical Machine template. It mainly created Commodity bought
     * and Commodity sold from template fields.
     *
     * @param topologyEntityBuilder builder of TopologyEntityDTO which could contains some setting already.
     * @param topology The topology map from OID -> TopologyEntity.Builder. When performing a replace,
     *                 entities related to the entity being replaced may be updated to fix up relationships
     *                 to point to the new entity along with the old entity.
     * @param originalTopologyEntity the original topology entity which this template want to keep its
     *                               commodity constrains. It could be null, if it is new adding template.
     * @return {@link TopologyEntityDTO.Builder}
     */
    @Override
    public TopologyEntityDTO.Builder createTopologyEntityFromTemplate(
            @Nonnull final Template template,
            @Nonnull final TopologyEntityDTO.Builder topologyEntityBuilder,
            @Nonnull final Map<Long, TopologyEntity.Builder> topology,
            @Nullable final TopologyEntityDTOOrBuilder originalTopologyEntity) {
        final List<CommoditiesBoughtFromProvider> commodityBoughtConstraints = getActiveCommoditiesWithKeysGroups(
            originalTopologyEntity);
        final Set<CommoditySoldDTO> commoditySoldConstraints = getCommoditySoldConstraint(
            originalTopologyEntity);
        final List<TemplateResource> computeTemplateResources =
            TemplatesConverterUtils.getTemplateResources(template, Compute);
        addComputeCommodities(topologyEntityBuilder, computeTemplateResources);

        // shopRogether entities are not allowed to sell biclique commodities (why???), and hosts need
        // to sell biclique commodities, so set shopTogether to false.
        topologyEntityBuilder.getAnalysisSettingsBuilder().setShopTogether(false);

        final List<TemplateResource> infraTemplateResources =
            TemplatesConverterUtils.getTemplateResources(template, Infrastructure);
        addInfraCommodities(topologyEntityBuilder, infraTemplateResources);
        addCommodityConstraints(topologyEntityBuilder, commoditySoldConstraints, commodityBoughtConstraints);
        if (originalTopologyEntity != null) {
            updateRelatedEntityAccesses(originalTopologyEntity.getOid(), topologyEntityBuilder.getOid(),
                commoditySoldConstraints, topology);
        }
        // if the template has a 'cpu_model' then add it to the new TopologyEntityDTO
        if (template.hasTemplateInfo() && template.getTemplateInfo().hasCpuModel()) {
            topologyEntityBuilder.setTypeSpecificInfo(
                TypeSpecificInfo.newBuilder().setPhysicalMachine(PhysicalMachineInfo.newBuilder()
                    .setCpuModel(template.getTemplateInfo().getCpuModel())
                ));
        }
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
        addComputeCommoditiesExtentBallooningSwapping(topologyEntityBuilder);
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

        final CommoditySoldDTO cpuCommodity =
            createCommoditySoldDTO(CommodityDTO.CommodityType.CPU_VALUE, numOfCpu * cpuSpeed);
        final CommoditySoldDTO  memCommodity =
            createCommoditySoldDTO(CommodityDTO.CommodityType.MEM_VALUE, memSize);
        topologyEntityBuilder
            .addCommoditySoldList(cpuCommodity)
            .addCommoditySoldList(memCommodity);

        // Because we don't have access to settings at this time, we can't calculate capacities for these
        // provisioned commodities. By leaving capacities unset, they will be set later in the topology
        // pipeline when settings are avaialble by the OverprovisionCapacityPostStitchingOperation.
        final CommoditySoldDTO cpuProvisionedCommodity =
            createCommoditySoldDTO(CommodityType.CPU_PROVISIONED_VALUE);
        final CommoditySoldDTO memProvisionedCommodity =
            createCommoditySoldDTO(CommodityType.MEM_PROVISIONED_VALUE);
        topologyEntityBuilder
            .addCommoditySoldList(cpuProvisionedCommodity)
            .addCommoditySoldList(memProvisionedCommodity);

        // QxVCPU. Note that 1,2,4,8,16 are the number of VCPUs.
        final CommoditySoldDTO q1VcpuCommodity =
            createCommoditySoldDTO(CommodityType.Q1_VCPU_VALUE, QX_VCPU_BASE_COEFFICIENT * 1.0);
        final CommoditySoldDTO q2VcpuCommodity =
            createCommoditySoldDTO(CommodityType.Q2_VCPU_VALUE, QX_VCPU_BASE_COEFFICIENT * 2.0);
        final CommoditySoldDTO q4VcpuCommodity =
            createCommoditySoldDTO(CommodityType.Q4_VCPU_VALUE, QX_VCPU_BASE_COEFFICIENT * 4.0);
        final CommoditySoldDTO q8VcpuCommodity =
            createCommoditySoldDTO(CommodityType.Q8_VCPU_VALUE, QX_VCPU_BASE_COEFFICIENT * 8.0);
        final CommoditySoldDTO q16VcpuCommodity =
            createCommoditySoldDTO(CommodityType.Q16_VCPU_VALUE, QX_VCPU_BASE_COEFFICIENT * 16.0);
        topologyEntityBuilder
            .addCommoditySoldList(q1VcpuCommodity)
            .addCommoditySoldList(q2VcpuCommodity)
            .addCommoditySoldList(q4VcpuCommodity)
            .addCommoditySoldList(q8VcpuCommodity)
            .addCommoditySoldList(q16VcpuCommodity);
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
     * Generate Extent, Ballooning, and Swapping commodities sold and add to the DTO.
     *
     * @param topologyEntityBuilder The entity to receive the commodities.
     */
    private static void addComputeCommoditiesExtentBallooningSwapping(
        @Nonnull final Builder topologyEntityBuilder) {

        final CommoditySoldDTO extent =
            createCommoditySoldDTO(CommodityType.EXTENT_VALUE, MAX_LUN_LIMIT);
        final CommoditySoldDTO ballooning =
            // TODO: set price weight field to -1.0 when it is introduced in CommoditySoldDTO
            createCommoditySoldDTO(CommodityType.BALLOONING_VALUE, BALLOONING_DEFAULT_CAPACITY);
        final CommoditySoldDTO swapping =
            createCommoditySoldDTO(CommodityType.SWAPPING_VALUE, SWAPPING_DEFAULT_CAPACITY);

        // TODO: Flows?????

        topologyEntityBuilder
            .addCommoditySoldList(extent)
            .addCommoditySoldList(ballooning)
            .addCommoditySoldList(swapping);
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
