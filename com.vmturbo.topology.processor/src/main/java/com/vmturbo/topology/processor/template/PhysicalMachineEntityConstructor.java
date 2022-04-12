package com.vmturbo.topology.processor.template;

import static com.vmturbo.common.protobuf.plan.TemplateDTO.ResourcesCategory.ResourcesCategoryName.Compute;
import static com.vmturbo.common.protobuf.plan.TemplateDTO.ResourcesCategory.ResourcesCategoryName.Infrastructure;
import static com.vmturbo.common.protobuf.topology.TopologyDTOUtil.QX_VCPU_BASE_COEFFICIENT;

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
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommodityBoughtView;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommoditySoldView;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl.CommoditiesBoughtFromProviderImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl.CommoditiesBoughtFromProviderView;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TypeSpecificInfoImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TypeSpecificInfoImpl.PhysicalMachineInfoImpl;
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

    // Random numbers ported from legacy.
    public static final double BALLOONING_DEFAULT_CAPACITY = 1.0E9;
    public static final double SWAPPING_DEFAULT_CAPACITY = 5000.0;

    @Override
    public TopologyEntityImpl createTopologyEntityFromTemplate(
            @Nonnull final Template template, @Nonnull Map<Long, TopologyEntity.Builder> topology,
            @Nullable TopologyEntityImpl originalTopologyEntity,
            @Nonnull TemplateActionType actionType, @Nonnull IdentityProvider identityProvider,
            @Nullable String nameSuffix) throws TopologyEntityConstructorException {
        TopologyEntityImpl topologyEntityBuilder = super.generateTopologyEntityBuilder(
                template, originalTopologyEntity, actionType, identityProvider,
                EntityType.PHYSICAL_MACHINE_VALUE, nameSuffix);

        final Map<String, String> computeTemplateResources = createFieldNameValueMap(
                getTemplateResources(template, Compute));
        addComputeCommodities(topologyEntityBuilder, computeTemplateResources);

        // shopRogether entities are not allowed to sell biclique commodities (why???), and hosts need
        // to sell biclique commodities, so set shopTogether to false.
        topologyEntityBuilder.getOrCreateAnalysisSettings().setShopTogether(false);

        final List<TemplateResource> infraTemplateResources = getTemplateResources(template,
                Infrastructure);
        addInfraCommodities(topologyEntityBuilder, infraTemplateResources);
        setAccessCommodities(topologyEntityBuilder, originalTopologyEntity, topology);

        String templateName = template.hasTemplateInfo() && template.getTemplateInfo().hasName() ?
                        template.getTemplateInfo().getName() : "";
        // Set type specific info
        PhysicalMachineInfoImpl pmInfo = new PhysicalMachineInfoImpl();
        int numCores = Double.valueOf(
                computeTemplateResources.getOrDefault(TemplateProtoUtil.PM_COMPUTE_NUM_OF_CORE, ZERO)).intValue();
        if (numCores > 0) {
            pmInfo.setNumCpus(numCores);
        } else {
           logger.error("Incorrect/empty value for number of cores {} for template {}.",
               computeTemplateResources.get(TemplateProtoUtil.PM_COMPUTE_NUM_OF_CORE),
               templateName);
        }

        int cpuSpeed = Double.valueOf(
                computeTemplateResources.getOrDefault(TemplateProtoUtil.PM_COMPUTE_CPU_SPEED, ZERO)).intValue();
        if (cpuSpeed > 0) {
            pmInfo.setCpuCoreMhz(cpuSpeed);
        } else {
           logger.error("Incorrect/empty value of cpu speed {} for template {}. ",
               computeTemplateResources.get(TemplateProtoUtil.PM_COMPUTE_CPU_SPEED),
               templateName);
        }

        // if the template has a 'cpu_model' then add it to the new TopologyEntityDTO
        if (template.hasTemplateInfo() && template.getTemplateInfo().hasCpuModel()) {
            pmInfo.setCpuModel(template.getTemplateInfo().getCpuModel());
        }
        topologyEntityBuilder.setTypeSpecificInfo(new TypeSpecificInfoImpl()
            .setPhysicalMachine(pmInfo));

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
    public static void setAccessCommodities(@Nonnull TopologyEntityImpl newHost,
            @Nullable TopologyEntityImpl originalHost,
            @Nonnull Map<Long, TopologyEntity.Builder> topology)
            throws TopologyEntityConstructorException {
        Set<CommoditySoldView> commoditySoldConstraints = getCommoditySoldConstraint(originalHost);
        List<CommoditiesBoughtFromProviderView> commodityBoughtConstraints = getActiveCommoditiesWithKeysGroups(
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
    private static void addComputeCommodities(@Nonnull TopologyEntityImpl topologyEntityBuilder,
                                              @Nonnull Map<String, String> computeTemplateResources) {
        addComputeCommoditiesSold(topologyEntityBuilder, computeTemplateResources);
    }

    /**
     * Generate a list of CPU, Memory, IO and Network commodity sold.
     *
     * @param topologyEntityBuilder builder of TopologyEntityDTO.
     * @param fieldNameValueMap a Map which key is template field name and value is field value.
     */
    private static void addComputeCommoditiesSold(@Nonnull final TopologyEntityImpl topologyEntityBuilder,
                                                  @Nonnull Map<String, String> fieldNameValueMap) {
        addComputeCommoditiesCpuMemSold(topologyEntityBuilder, fieldNameValueMap);
        addComputeCommoditiesIONetSold(topologyEntityBuilder, fieldNameValueMap);
        addComputeCommoditiesNumVcoreReadyQueueSold(topologyEntityBuilder, fieldNameValueMap);
        addMiscComputeCommodities(topologyEntityBuilder);
    }


    /**
     * Generate NUM_VCORE and CPU_READY commodity sold and add to TopologyEntityDTO.
     *
     * @param topologyEntityBuilder builder of TopologyEntityDTO.
     * @param fieldNameValueMap a Map which key is template field name and value is field value.
     */
    private static void addComputeCommoditiesNumVcoreReadyQueueSold(@Nonnull final TopologyEntityImpl topologyEntityBuilder,
            @Nonnull Map<String, String> fieldNameValueMap) {
        final double numOfCpu = Double.valueOf(
                fieldNameValueMap.getOrDefault(TemplateProtoUtil.PM_COMPUTE_NUM_OF_CORE, ZERO));

        // add the num_vcore and the cpu ready commodity
        // We expect user to input logical cores for host template so we just
        // multiply that with 20 seconds per core cpu ready capacity
        // and come up with total cpu ready capacity.
        final double cpuReady = numOfCpu * QX_VCPU_BASE_COEFFICIENT;
        final CommoditySoldView numVcoreCommodity =
                createCommoditySoldDTO(CommodityType.NUM_VCORE_VALUE, numOfCpu);
        final CommoditySoldView cpuReadyCommodity =
                createCommoditySoldDTO(CommodityType.CPU_READY_VALUE, cpuReady);
        topologyEntityBuilder
                .addCommoditySoldList(numVcoreCommodity)
                .addCommoditySoldList(cpuReadyCommodity);

    }


    /**
     * Generate CPU and Memory commodity sold and add to TopologyEntityDTO.
     *
     * @param topologyEntityBuilder builder of TopologyEntityDTO.
     * @param fieldNameValueMap a Map which key is template field name and value is field value.
     */
    private static void addComputeCommoditiesCpuMemSold(@Nonnull final TopologyEntityImpl topologyEntityBuilder,
                                                        @Nonnull Map<String, String> fieldNameValueMap) {
        final double numOfCpu = Double.valueOf(
            fieldNameValueMap.getOrDefault(TemplateProtoUtil.PM_COMPUTE_NUM_OF_CORE, ZERO));
        final double cpuSpeed = Double.valueOf(
            fieldNameValueMap.getOrDefault(TemplateProtoUtil.PM_COMPUTE_CPU_SPEED, ZERO));
        final double memSize = Double.valueOf(
            fieldNameValueMap.getOrDefault(TemplateProtoUtil.PM_COMPUTE_MEM_SIZE, ZERO));

        final CommoditySoldView cpuCommodity =
            createCommoditySoldDTO(CommodityDTO.CommodityType.CPU_VALUE, numOfCpu * cpuSpeed);
        final CommoditySoldView  memCommodity =
            createCommoditySoldDTO(CommodityDTO.CommodityType.MEM_VALUE, memSize);
        topologyEntityBuilder
            .addCommoditySoldList(cpuCommodity)
            .addCommoditySoldList(memCommodity);

        // Because we don't have access to settings at this time, we can't calculate capacities for these
        // provisioned commodities. By leaving capacities unset, they will be set later in the topology
        // pipeline when settings are avaialble by the OverprovisionCapacityPostStitchingOperation.
        final CommoditySoldView cpuProvisionedCommodity =
            createCommoditySoldDTO(CommodityType.CPU_PROVISIONED_VALUE);
        final CommoditySoldView memProvisionedCommodity =
            createCommoditySoldDTO(CommodityType.MEM_PROVISIONED_VALUE);
        topologyEntityBuilder
            .addCommoditySoldList(cpuProvisionedCommodity)
            .addCommoditySoldList(memProvisionedCommodity);

        // QxVCPU. Note that 1,2,4,8,16,32,64 are the number of VCPUs.
        final CommoditySoldView q1VcpuCommodity =
            createCommoditySoldDTO(CommodityType.Q1_VCPU_VALUE, QX_VCPU_BASE_COEFFICIENT * 1.0);
        final CommoditySoldView q2VcpuCommodity =
            createCommoditySoldDTO(CommodityType.Q2_VCPU_VALUE, QX_VCPU_BASE_COEFFICIENT * 2.0);
        final CommoditySoldView q4VcpuCommodity =
            createCommoditySoldDTO(CommodityType.Q4_VCPU_VALUE, QX_VCPU_BASE_COEFFICIENT * 4.0);
        final CommoditySoldView q8VcpuCommodity =
            createCommoditySoldDTO(CommodityType.Q8_VCPU_VALUE, QX_VCPU_BASE_COEFFICIENT * 8.0);
        final CommoditySoldView q16VcpuCommodity =
            createCommoditySoldDTO(CommodityType.Q16_VCPU_VALUE, QX_VCPU_BASE_COEFFICIENT * 16.0);
        final CommoditySoldView q32VcpuCommodity =
                createCommoditySoldDTO(CommodityType.Q32_VCPU_VALUE, QX_VCPU_BASE_COEFFICIENT * 32.0);
        final CommoditySoldView q64VcpuCommodity =
                createCommoditySoldDTO(CommodityType.Q64_VCPU_VALUE, QX_VCPU_BASE_COEFFICIENT * 64.0);
        topologyEntityBuilder
            .addCommoditySoldList(q1VcpuCommodity)
            .addCommoditySoldList(q2VcpuCommodity)
            .addCommoditySoldList(q4VcpuCommodity)
            .addCommoditySoldList(q8VcpuCommodity)
            .addCommoditySoldList(q16VcpuCommodity)
            .addCommoditySoldList(q32VcpuCommodity)
            .addCommoditySoldList(q64VcpuCommodity);
    }

    /**
     * Generate IO and Network commodity sold and add to TopologyEntityDTO.
     *
     * @param topologyEntityBuilder builder of TopologyEntityDTO.
     * @param fieldNameValueMap a Map which key is template field name and value is field value.
     */
    private static void addComputeCommoditiesIONetSold(@Nonnull final TopologyEntityImpl topologyEntityBuilder,
                                                       @Nonnull Map<String, String> fieldNameValueMap) {
        final double ioThroughputSize = Double.valueOf(
            fieldNameValueMap.getOrDefault(TemplateProtoUtil.PM_COMPUTE_IO_THROUGHPUT_SIZE, ZERO));
        final double networkThroughputSize = Double.valueOf(
            fieldNameValueMap.getOrDefault(TemplateProtoUtil.PM_COMPUTE_NETWORK_THROUGHPUT_SIZE, ZERO));

        CommoditySoldView ioThroughputCommodity =
            createCommoditySoldDTO(CommodityDTO.CommodityType.IO_THROUGHPUT_VALUE, ioThroughputSize);
        CommoditySoldView networkCommodity =
            createCommoditySoldDTO(CommodityDTO.CommodityType.NET_THROUGHPUT_VALUE, networkThroughputSize);
        topologyEntityBuilder
            .addCommoditySoldList(ioThroughputCommodity)
            .addCommoditySoldList(networkCommodity);
    }

    /**
     * Generate Extent, Ballooning, Swapping, HostLunAccess commodities sold and add to the DTO.
     *
     * @param topologyEntity The entity to receive the commodities.
     */
    private static void addMiscComputeCommodities(
        @Nonnull final TopologyEntityImpl topologyEntity) {

        final CommoditySoldView extent =
            createCommoditySoldDTO(CommodityType.EXTENT_VALUE, Double.valueOf(MAX_LUN_LIMIT));
        final CommoditySoldView ballooning =
            // TODO: set price weight field to -1.0 when it is introduced in CommoditySoldView
            createCommoditySoldDTO(CommodityType.BALLOONING_VALUE, BALLOONING_DEFAULT_CAPACITY);
        final CommoditySoldView swapping =
            createCommoditySoldDTO(CommodityType.SWAPPING_VALUE, SWAPPING_DEFAULT_CAPACITY);
        final CommoditySoldView hostLunAccess =
            createCommoditySoldDTO(CommodityType.HOST_LUN_ACCESS_VALUE, Double.valueOf(MAX_LUN_LIMIT));

        // TODO: Flows?????

        topologyEntity
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
    private static void addInfraCommodities(@Nonnull TopologyEntityImpl topologyEntityBuilder,
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
            @Nonnull final TopologyEntityImpl topologyEntityBuilder,
            @Nonnull Map<String, String> fieldNameValueMap) {
        final double powerSize = Double.valueOf(
            fieldNameValueMap.getOrDefault(TemplateProtoUtil.PM_INFRA_POWER_SIZE, ZERO));
        final double spaceSize = Double.valueOf(
            fieldNameValueMap.getOrDefault(TemplateProtoUtil.PM_INFRA_SPACE_SIZE, ZERO));
        final double coolingSize = Double.valueOf(
            fieldNameValueMap.getOrDefault(TemplateProtoUtil.PM_INFRA_COOLING_SIZE, ZERO));

        CommodityBoughtView powerSizeCommodity =
            createCommodityBoughtView(CommodityDTO.CommodityType.POWER_VALUE, powerSize);
        CommodityBoughtView spaceSizeCommodity =
            createCommodityBoughtView(CommodityDTO.CommodityType.COOLING_VALUE, spaceSize);
        CommodityBoughtView coolingSizeCommodity =
            createCommodityBoughtView(CommodityDTO.CommodityType.SPACE_VALUE, coolingSize);

        final CommoditiesBoughtFromProviderView newCommoditiesBoughtFromProvider =
            new CommoditiesBoughtFromProviderImpl()
                .addCommodityBought(powerSizeCommodity)
                .addCommodityBought(spaceSizeCommodity)
                .addCommodityBought(coolingSizeCommodity)
                .setMovable(true)
                .setProviderEntityType(EntityType.DATACENTER_VALUE);
        topologyEntityBuilder.addCommoditiesBoughtFromProviders(newCommoditiesBoughtFromProvider);
    }
}
