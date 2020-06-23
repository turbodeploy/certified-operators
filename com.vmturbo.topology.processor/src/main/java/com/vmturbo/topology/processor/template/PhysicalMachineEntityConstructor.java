package com.vmturbo.topology.processor.template;

import static com.vmturbo.common.protobuf.plan.TemplateDTO.ResourcesCategory.ResourcesCategoryName.Compute;
import static com.vmturbo.common.protobuf.plan.TemplateDTO.ResourcesCategory.ResourcesCategoryName.Infrastructure;

import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.TemplateProtoUtil;
import com.vmturbo.common.protobuf.plan.TemplateDTO.Template;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateResource;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Builder;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.PhysicalMachineInfo;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.processor.identity.IdentityProvider;

/**
 * Create a topologyEntityDTO from Physical Machine template. The new Topology Entity contains such as OID,
 * displayName, commodity sold, commodity bought, entity state, provider policy and consumer policy.
 * And also it will try to keep all commodity constrains from the original topology entity.
 */
public class PhysicalMachineEntityConstructor extends TopologyEntityConstructor
        implements ITopologyEntityConstructor {

    private static final String ZERO = "0";
    private static final Logger logger = LogManager.getLogger();

    // Max 256 LUNS based on https://www.vmware.com/pdf/vsphere6/r60/vsphere-60-configuration-maximums.pdf
    public static final int MAX_LUN_LIMIT = 256;

    // Semi-random capacity number based on VC standard 20-second real-time metric interval.
    public static final double QX_VCPU_BASE_COEFFICIENT = 20000.0;
    // Random numbers ported from legacy.
    public static final double BALLOONING_DEFAULT_CAPACITY = 1.0E9;
    public static final double SWAPPING_DEFAULT_CAPACITY = 5000.0;

    @Override
    public TopologyEntityDTO.Builder createTopologyEntityFromTemplate(
            @Nonnull final Template template, @Nonnull Map<Long, TopologyEntity.Builder> topology,
            @Nullable TopologyEntityDTO.Builder originalTopologyEntity,
            @Nonnull TemplateActionType actionType, @Nonnull IdentityProvider identityProvider,
            @Nullable String nameSuffix) throws TopologyEntityConstructorException {
        TopologyEntityDTO.Builder topologyEntityBuilder = super.generateTopologyEntityBuilder(
                template, originalTopologyEntity, actionType, identityProvider,
                EntityType.PHYSICAL_MACHINE_VALUE, nameSuffix);

        final Map<String, String> computeTemplateResources = createFieldNameValueMap(
                getTemplateResources(template, Compute));
        addComputeCommodities(topologyEntityBuilder, computeTemplateResources);

        // shopRogether entities are not allowed to sell biclique commodities (why???), and hosts need
        // to sell biclique commodities, so set shopTogether to false.
        topologyEntityBuilder.getAnalysisSettingsBuilder().setShopTogether(false);

        final List<TemplateResource> infraTemplateResources = getTemplateResources(template,
                Infrastructure);
        addInfraCommodities(topologyEntityBuilder, infraTemplateResources);
        setAccessCommodities(topologyEntityBuilder, originalTopologyEntity, topology);

        String templateName = template.hasTemplateInfo() && template.getTemplateInfo().hasName() ?
                        template.getTemplateInfo().getName() : "";
        // Set type specific info
        PhysicalMachineInfo.Builder pmInfoBuilder = PhysicalMachineInfo.newBuilder();
        int numCores = Double.valueOf(
                computeTemplateResources.getOrDefault(TemplateProtoUtil.PM_COMPUTE_NUM_OF_CORE, ZERO)).intValue();
        if (numCores > 0) {
            pmInfoBuilder.setNumCpus(numCores);
        } else {
           logger.error("Incorrect/empty value for number of cores {} for template {}.",
               computeTemplateResources.get(TemplateProtoUtil.PM_COMPUTE_NUM_OF_CORE),
               templateName);
        }

        int cpuSpeed = Double.valueOf(
                computeTemplateResources.getOrDefault(TemplateProtoUtil.PM_COMPUTE_CPU_SPEED, ZERO)).intValue();
        if (cpuSpeed > 0) {
            pmInfoBuilder.setCpuCoreMhz(cpuSpeed);
        } else {
           logger.error("Incorrect/empty value of cpu speed {} for template {}. ",
               computeTemplateResources.get(TemplateProtoUtil.PM_COMPUTE_CPU_SPEED),
               templateName);
        }

        // if the template has a 'cpu_model' then add it to the new TopologyEntityDTO
        if (template.hasTemplateInfo() && template.getTemplateInfo().hasCpuModel()) {
            pmInfoBuilder.setCpuModel(template.getTemplateInfo().getCpuModel());
        }
        topologyEntityBuilder.setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
            .setPhysicalMachine(pmInfoBuilder));

        return topologyEntityBuilder;
    }

    /**
     * Set the access commodities for the new host, based on the original host
     * access commodities.
     *
     * @param newHost new host
     * @param originalHost original host
     * @param topology topology
     * @throws TopologyEntityConstructorException error setting access
     *             commodities
     */
    public static void setAccessCommodities(@Nonnull TopologyEntityDTO.Builder newHost,
            @Nullable TopologyEntityDTO.Builder originalHost,
            @Nonnull Map<Long, TopologyEntity.Builder> topology)
            throws TopologyEntityConstructorException {
        Set<CommoditySoldDTO> commoditySoldConstraints = getCommoditySoldConstraint(originalHost);
        List<CommoditiesBoughtFromProvider> commodityBoughtConstraints = getActiveCommoditiesWithKeysGroups(
                originalHost);

        addCommodityConstraints(newHost, commoditySoldConstraints, commodityBoughtConstraints);

        if (originalHost != null) {
            updateRelatedEntityAccesses(originalHost, newHost, commoditySoldConstraints, topology);
        }
    }

    /**
     * Generate commodities for compute template resources.
     *
     * @param topologyEntityBuilder builder of TopologyEntityDTO.
     * @param computeTemplateResources a map (name -> value) of compute template resources.
     */
    private static void addComputeCommodities(@Nonnull TopologyEntityDTO.Builder topologyEntityBuilder,
                                              @Nonnull Map<String, String> computeTemplateResources) {
        addComputeCommoditiesSold(topologyEntityBuilder, computeTemplateResources);
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
        addMiscComputeCommodities(topologyEntityBuilder);
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
            fieldNameValueMap.getOrDefault(TemplateProtoUtil.PM_COMPUTE_NUM_OF_CORE, ZERO));
        final double cpuSpeed = Double.valueOf(
            fieldNameValueMap.getOrDefault(TemplateProtoUtil.PM_COMPUTE_CPU_SPEED, ZERO));
        final double memSize = Double.valueOf(
            fieldNameValueMap.getOrDefault(TemplateProtoUtil.PM_COMPUTE_MEM_SIZE, ZERO));

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
            fieldNameValueMap.getOrDefault(TemplateProtoUtil.PM_COMPUTE_IO_THROUGHPUT_SIZE, ZERO));
        final double networkThroughputSize = Double.valueOf(
            fieldNameValueMap.getOrDefault(TemplateProtoUtil.PM_COMPUTE_NETWORK_THROUGHPUT_SIZE, ZERO));

        CommoditySoldDTO ioThroughputCommodity =
            createCommoditySoldDTO(CommodityDTO.CommodityType.IO_THROUGHPUT_VALUE, ioThroughputSize);
        CommoditySoldDTO networkCommodity =
            createCommoditySoldDTO(CommodityDTO.CommodityType.NET_THROUGHPUT_VALUE, networkThroughputSize);
        topologyEntityBuilder
            .addCommoditySoldList(ioThroughputCommodity)
            .addCommoditySoldList(networkCommodity);
    }

    /**
     * Generate Extent, Ballooning, Swapping, HostLunAccess commodities sold and add to the DTO.
     *
     * @param topologyEntityBuilder The entity to receive the commodities.
     */
    private static void addMiscComputeCommodities(
        @Nonnull final Builder topologyEntityBuilder) {

        final CommoditySoldDTO extent =
            createCommoditySoldDTO(CommodityType.EXTENT_VALUE, Double.valueOf(MAX_LUN_LIMIT));
        final CommoditySoldDTO ballooning =
            // TODO: set price weight field to -1.0 when it is introduced in CommoditySoldDTO
            createCommoditySoldDTO(CommodityType.BALLOONING_VALUE, BALLOONING_DEFAULT_CAPACITY);
        final CommoditySoldDTO swapping =
            createCommoditySoldDTO(CommodityType.SWAPPING_VALUE, SWAPPING_DEFAULT_CAPACITY);
        final CommoditySoldDTO hostLunAccess =
            createCommoditySoldDTO(CommodityType.HOST_LUN_ACCESS_VALUE, Double.valueOf(MAX_LUN_LIMIT));

        // TODO: Flows?????

        topologyEntityBuilder
            .addCommoditySoldList(extent)
            .addCommoditySoldList(ballooning)
            .addCommoditySoldList(swapping)
            .addCommoditySoldList(hostLunAccess);
    }


    /**
     * Generate commodities for infrastructure template resources.
     *
     * @param topologyEntityBuilder builder of TopologyEntityDTO.
     * @param infraTemplateResources  a list of infrastructure template resources.
     */
    private static void addInfraCommodities(@Nonnull TopologyEntityDTO.Builder topologyEntityBuilder,
                                            @Nonnull List<TemplateResource> infraTemplateResources) {
        final Map<String, String> fieldNameValueMap = createFieldNameValueMap(
                infraTemplateResources);
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
            fieldNameValueMap.getOrDefault(TemplateProtoUtil.PM_INFRA_POWER_SIZE, ZERO));
        final double spaceSize = Double.valueOf(
            fieldNameValueMap.getOrDefault(TemplateProtoUtil.PM_INFRA_SPACE_SIZE, ZERO));
        final double coolingSize = Double.valueOf(
            fieldNameValueMap.getOrDefault(TemplateProtoUtil.PM_INFRA_COOLING_SIZE, ZERO));

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
