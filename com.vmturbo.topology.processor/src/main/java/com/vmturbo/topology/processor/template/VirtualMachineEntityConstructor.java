package com.vmturbo.topology.processor.template;

import static com.vmturbo.common.protobuf.plan.TemplateDTO.ResourcesCategory.ResourcesCategoryName.Compute;
import static com.vmturbo.common.protobuf.plan.TemplateDTO.ResourcesCategory.ResourcesCategoryName.Storage;
import static com.vmturbo.topology.processor.template.TemplatesConverterUtils.createCommodityBoughtDTO;
import static com.vmturbo.topology.processor.template.TemplatesConverterUtils.createCommoditySoldDTO;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

import com.google.common.collect.Lists;

import com.vmturbo.common.protobuf.plan.TemplateDTO.Template;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateField;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateInfo;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateResource;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;

/**
 * Create a TopologyEntityDTO from Virtual Machine Template. The new Topology Entity contains such as OID,
 * displayName, commodity sold, commodity bought, entity state, provider policy and consumer policy.
 */
public class VirtualMachineEntityConstructor implements TopologyEntityConstructor {
    private static final String ZERO = "0";

    private static final String RDM = "RDM";

    /**
     * Create a TopologyEntityDTO from Virtual Machine Template. based on input template.
     *
     * @param template virtual machine template.
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
            TemplatesConverterUtils.getTemplateResources(template, Storage);
        addStorageCommodities(topologyEntityBuilder, storageTemplateResources);

        return topologyEntityBuilder.build();
    }

    /**
     * Generate compute commodity bought and commodity sold from a list of compute template resources.
     *
     * @param topologyEntityBuilder builder of TopologyEntityDTO.
     * @param computeTemplateResources a list of compute resources.
     */
    private static void addComputeCommodities(@Nonnull TopologyEntityDTO.Builder topologyEntityBuilder,
                                              @Nonnull List<TemplateResource> computeTemplateResources) {
        final Map<String, String> fieldNameValueMap =
            TemplatesConverterUtils.createFieldNameValueMap(computeTemplateResources);
        addComputeCommoditiesBought(topologyEntityBuilder, fieldNameValueMap);
        addComputeCommoditiesSold(topologyEntityBuilder, fieldNameValueMap);
    }

    /**
     * Generate a list of commodity bought and add to TopologyEntityDTO.
     *
     * @param topologyEntityBuilder builder of TopologyEntityDTO.
     * @param fieldNameValueMap a Map which key is template field name and value is field value.
     */
    private static void addComputeCommoditiesBought(@Nonnull final TopologyEntityDTO.Builder topologyEntityBuilder,
                                                    @Nonnull Map<String, String> fieldNameValueMap) {
        final List<CommodityBoughtDTO> cpuCommodity =
            addComputeCommoditiesBoughtCPU(fieldNameValueMap);
        final List<CommodityBoughtDTO> memCommodity =
            addComputeCommoditiesBoughtMem(fieldNameValueMap);
        final List<CommodityBoughtDTO> ioNetCommodity =
            addComputeCommoditiesBoughtIONet(fieldNameValueMap);

        final CommoditiesBoughtFromProvider commoditiesBoughtFromProvider =
            CommoditiesBoughtFromProvider.newBuilder()
                .addAllCommodityBought(cpuCommodity)
                .addAllCommodityBought(memCommodity)
                .addAllCommodityBought(ioNetCommodity)
                .build();
        topologyEntityBuilder.addCommoditiesBoughtFromProviders(commoditiesBoughtFromProvider);
    }

    /**
     * Generate a list of CPU related commodity bought.
     *
     * @param fieldNameValueMap a Map which key is template field name and value is field value.
     * @return a list of {@link CommodityBoughtDTO}.
     */
    private static List<CommodityBoughtDTO> addComputeCommoditiesBoughtCPU(@Nonnull Map<String, String> fieldNameValueMap) {
        final double numOfCpu = Double.valueOf(
            fieldNameValueMap.getOrDefault(TemplatesConverterUtils.NUM_OF_CPU, ZERO));
        final double cpuSpeed = Double.valueOf(
            fieldNameValueMap.getOrDefault(TemplatesConverterUtils.CPU_SPEED, ZERO));
        final double cpuConsumedFactor = Double.valueOf(
            fieldNameValueMap.getOrDefault(TemplatesConverterUtils.CPU_CONSUMED_FACTOR, ZERO));

        final double used = numOfCpu * cpuSpeed * cpuConsumedFactor;
        CommodityBoughtDTO cpuCommodity =
            createCommodityBoughtDTO(CommodityDTO.CommodityType.CPU_VALUE, used);
        return Lists.newArrayList(cpuCommodity);
    }

    /**
     * Generate a list of Memory related commodity bought.
     *
     * @param fieldNameValueMap a Map which key is template field name and value is field value.
     * @return a list of {@link CommodityBoughtDTO}.
     */
    private static List<CommodityBoughtDTO> addComputeCommoditiesBoughtMem(@Nonnull Map<String, String> fieldNameValueMap) {
        final double memorySize = Double.valueOf(
            fieldNameValueMap.getOrDefault(TemplatesConverterUtils.MEMORY_SIZE, ZERO));
        final double memoryConsumedFactor = Double.valueOf(
            fieldNameValueMap.getOrDefault(TemplatesConverterUtils.MEMORY_CONSUMED_FACTOR, ZERO));
        final double used = memorySize * memoryConsumedFactor;
        CommodityBoughtDTO memCommodity =
            createCommodityBoughtDTO(CommodityDTO.CommodityType.MEM_VALUE, used);
        CommodityBoughtDTO memProvCommodity =
            createCommodityBoughtDTO(CommodityDTO.CommodityType.MEM_PROVISIONED_VALUE, memorySize);
        return Lists.newArrayList(memCommodity, memProvCommodity);
    }

    /**
     * Generate a list of IO and Network related commodity bought.
     *
     * @param fieldNameValueMap a Map which key is template field name and value is field value.
     * @return a list of {@link CommodityBoughtDTO}.
     */
    private static List<CommodityBoughtDTO> addComputeCommoditiesBoughtIONet(@Nonnull Map<String, String> fieldNameValueMap) {
        final double ioThroughput = Double.valueOf(
            fieldNameValueMap.getOrDefault(TemplatesConverterUtils.IO_THROUGHPUT, ZERO));
        final double netThroughput = Double.valueOf(
            fieldNameValueMap.getOrDefault(TemplatesConverterUtils.NETWORK_THROUGHPUT, ZERO));

        CommodityBoughtDTO ioThroughputCommodity =
            createCommodityBoughtDTO(CommodityDTO.CommodityType.IO_THROUGHPUT_VALUE, ioThroughput);
        CommodityBoughtDTO netThroughputCommodity =
            createCommodityBoughtDTO(CommodityDTO.CommodityType.NET_THROUGHPUT_VALUE, netThroughput);
        return Lists.newArrayList(ioThroughputCommodity, netThroughputCommodity);
    }

    /**
     * Generate a list of Commodity sold and add to TopologyEntityDTO.
     *
     * @param topologyEntityBuilder builder of TopologyEntityDTO.
     * @param fieldNameValueMap a Map which key is template field name and value is field value.
     */
    private static void addComputeCommoditiesSold(@Nonnull final TopologyEntityDTO.Builder topologyEntityBuilder,
                                                  @Nonnull Map<String, String> fieldNameValueMap) {
        final double numOfCpu = Double.valueOf(
            fieldNameValueMap.getOrDefault(TemplatesConverterUtils.NUM_OF_CPU, ZERO));
        final double cpuSpeed = Double.valueOf(
            fieldNameValueMap.getOrDefault(TemplatesConverterUtils.CPU_SPEED, ZERO));
        final double memorySize = Double.valueOf(
            fieldNameValueMap.getOrDefault(TemplatesConverterUtils.MEMORY_SIZE, ZERO));

        final double totalCpuSold = numOfCpu * cpuSpeed;
        CommoditySoldDTO cpuSoldCommodity =
            createCommoditySoldDTO(CommodityDTO.CommodityType.VCPU_VALUE, totalCpuSold);
        CommoditySoldDTO memorySizeCommodity =
            createCommoditySoldDTO(CommodityDTO.CommodityType.VMEM_VALUE, memorySize);
        topologyEntityBuilder.addCommoditySoldList(cpuSoldCommodity);
        topologyEntityBuilder.addCommoditySoldList(memorySizeCommodity);
    }

    /**
     * Convert a list of storage template resources to storage commodity bought and commodity sold.
     *
     * @param topologyEntityBuilder builder of TopologyEntityDTO.
     * @param storageTemplateResources a list of storage template resources.
     */
    private static void addStorageCommodities(@Nonnull TopologyEntityDTO.Builder topologyEntityBuilder,
                                              @Nonnull List<TemplateResource> storageTemplateResources) {
        final double totalDiskSize = storageTemplateResources.stream()
            .map(TemplateResource::getFieldsList)
            .flatMap(List::stream)
            .filter(templateField -> templateField.getName().equals(TemplatesConverterUtils.DISK_SIZE))
            .map(TemplateField::getValue)
            .mapToDouble(Double::valueOf)
            .sum();
        addStorageCommoditiesSoldTotalDiskSize(topologyEntityBuilder, totalDiskSize);
        storageTemplateResources.stream()
            .forEach(stTemplateSource -> {
                final List<CommodityBoughtDTO> storageRelatedCommoditiesBought = new ArrayList<>();
                final List<CommodityBoughtDTO> storageCommoditiesBought =
                    addStorageCommoditiesBought(stTemplateSource, stTemplateSource.getCategory().getType());
                storageRelatedCommoditiesBought.addAll(storageCommoditiesBought);
                if (stTemplateSource.getCategory().getType().equals(RDM)) {
                    // create an extra LUN commodity between the VM and this rdm storage
                    storageRelatedCommoditiesBought.add(addLunCommoditiesBought());
                }
                topologyEntityBuilder.addCommoditiesBoughtFromProviders(
                    CommoditiesBoughtFromProvider.newBuilder()
                        .addAllCommodityBought(storageRelatedCommoditiesBought)
                        .build());
            });
    }

    /**
     * Generate a list of storage related Commodity bought.
     *
     * @param stTemplateSource storage template resources.
     * @param type category type of template resource.
     * @return a list of {@link CommodityBoughtDTO}.
     */
    private static List<CommodityBoughtDTO> addStorageCommoditiesBought(@Nonnull TemplateResource stTemplateSource,
                                                                        @Nonnull String type) {
        final Map<String, String> fieldNameValueMap =
            TemplatesConverterUtils.createFieldNameValueMap(Lists.newArrayList(stTemplateSource));

        List<CommodityBoughtDTO> storageCommodityBought =
            addStorageCommoditiesBoughtST(fieldNameValueMap, type);
        return storageCommodityBought;
    }

    /**
     * Generate a list of storage commodity bought, and also handle RDM disk type.
     *
     * @param fieldNameValueMap a Map which key is template field name and value is field value.
     * @param type category type of template resource.
     * @return a list of {@link CommodityBoughtDTO}.
     */
    private static List<CommodityBoughtDTO> addStorageCommoditiesBoughtST(
        @Nonnull Map<String, String> fieldNameValueMap,
        @Nonnull String type) {
        final double disSize = type.equals(RDM) ? Double.MIN_VALUE :
            Double.valueOf(fieldNameValueMap.getOrDefault(TemplatesConverterUtils.DISK_SIZE, ZERO));
        final double disConsumedFactor = Double.valueOf(
            fieldNameValueMap.getOrDefault(TemplatesConverterUtils.DISK_CONSUMED_FACTOR, ZERO));
        final double disIops = type.equals(RDM) ? Double.MIN_VALUE :
            Double.valueOf(fieldNameValueMap.getOrDefault(TemplatesConverterUtils.DISK_IOPS, ZERO));

        CommodityBoughtDTO stAmountCommodity =
            createCommodityBoughtDTO(CommodityDTO.CommodityType.STORAGE_AMOUNT_VALUE, disSize * disConsumedFactor);
        CommodityBoughtDTO stProvCommodity =
            createCommodityBoughtDTO(CommodityDTO.CommodityType.STORAGE_PROVISIONED_VALUE, disSize);
        CommodityBoughtDTO stAccessCommodity =
            createCommodityBoughtDTO(CommodityDTO.CommodityType.STORAGE_ACCESS_VALUE, disIops);
        return Lists.newArrayList(stAmountCommodity, stProvCommodity, stAccessCommodity);
    }

    /**
     * Generate storage commodity sold and add to TopologyEntityDTO.
     *
     * @param topologyEntityBuilder builder of TopologyEntityDTO.
     * @param totalDiskSize total disk size for sold.
     */
    private static void addStorageCommoditiesSoldTotalDiskSize(@Nonnull TopologyEntityDTO.Builder topologyEntityBuilder,
                                                               @Nonnull double totalDiskSize) {
        if (Double.isFinite(totalDiskSize)) {
            CommoditySoldDTO totalDiskSizeCommodity =
                createCommoditySoldDTO(CommodityDTO.CommodityType.VSTORAGE_VALUE, totalDiskSize);
            topologyEntityBuilder.addCommoditySoldList(totalDiskSizeCommodity);
        }
    }

    /**
     * Generate Lun commodity bought for RDM type storage.
     *
     * @return {@link CommodityBoughtDTO}
     */
    private static CommodityBoughtDTO addLunCommoditiesBought() {
        CommodityBoughtDTO lunCommodityBought =
            createCommodityBoughtDTO(CommodityDTO.CommodityType.EXTENT_VALUE, 1);
        return lunCommodityBought;
    }
}
