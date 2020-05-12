package com.vmturbo.topology.processor.template;

import static com.vmturbo.common.protobuf.plan.TemplateDTO.ResourcesCategory.ResourcesCategoryName.Compute;
import static com.vmturbo.common.protobuf.plan.TemplateDTO.ResourcesCategory.ResourcesCategoryName.Storage;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;

import com.vmturbo.common.protobuf.TemplateProtoUtil;
import com.vmturbo.common.protobuf.plan.TemplateDTO.Template;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateField;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateResource;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfo;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.processor.identity.IdentityProvider;

/**
 * Create a TopologyEntityDTO from Virtual Machine Template. The new Topology Entity contains such as OID,
 * displayName, commodity sold, commodity bought, entity state, provider policy and consumer policy.
 * And also it will try to keep all commodity constrains from the original topology entity.
 *
 * <p>This class will also handle reservation virtual machine entity, the only difference between normal
 * virtual machine with reservation virtual machine is that reservation virtual machine entity only
 * buy provision commodity, all other commodity bought value will be set to zero. It use isReservationEntity
 * field to represent it is a reservation virtual machine or not.
 */
public class VirtualMachineEntityConstructor extends TopologyEntityConstructor
        implements ITopologyEntityConstructor {

    private static final String ZERO = "0";
    private static final String RDM = "RDM";

    // represent that whether the new created entity is reservation entity or not. If it is reservation
    // entity, it will only buy provision commodity.
    private final boolean isReservationEntity;

    public VirtualMachineEntityConstructor() {
        this.isReservationEntity = false;
    }

    public VirtualMachineEntityConstructor(final boolean isReservationEntity) {
        this.isReservationEntity = isReservationEntity;
    }

    @Override
    public TopologyEntityDTO.Builder createTopologyEntityFromTemplate(
            @Nonnull final Template template, @Nullable Map<Long, TopologyEntity.Builder> topology,
            @Nullable TopologyEntity.Builder originalTopologyEntity, boolean isReplaced,
            @Nonnull IdentityProvider identityProvider) throws TopologyEntityConstructorException {
        TopologyEntityDTO.Builder topologyEntityBuilder = super.generateTopologyEntityBuilder(
                template, originalTopologyEntity, isReplaced, identityProvider,
                EntityType.VIRTUAL_MACHINE_VALUE);

        final List<CommoditiesBoughtFromProvider> commodityBoughtConstraints =
                sortAccessCommodityBought(getActiveCommoditiesWithKeysGroups(originalTopologyEntity));
        final Set<CommoditySoldDTO> commoditySoldConstraints = getCommoditySoldConstraint(
            originalTopologyEntity);
        final List<TemplateResource> computeTemplateResources = getTemplateResources(template,
                Compute);
        addComputeCommodities(topologyEntityBuilder, computeTemplateResources);

        final List<TemplateResource> storageTemplateResources = getTemplateResources(template,
                Storage);
        addStorageCommodities(topologyEntityBuilder, storageTemplateResources);
        addCommodityConstraints(topologyEntityBuilder, commoditySoldConstraints, commodityBoughtConstraints);
        handleProviderIdForCommodityBought(topologyEntityBuilder);
        return topologyEntityBuilder;
    }

    /**
     * If there are commodity bought have provider id which means it contains some biclique commodities,
     * we need to unplaced these commodity bought and put original provider id into entityProperty map.
     *
     * @param topologyEntityBuilder {@link TopologyEntityDTO.Builder}
     */
    private void handleProviderIdForCommodityBought(
            @Nonnull final TopologyEntityDTO.Builder topologyEntityBuilder) {
        final Map<Long, Long> oldProvidersMap = Maps.newHashMap();
        long fakeProvider = 0;
        final List<CommoditiesBoughtFromProvider> cloneCommodityBoughtGroups = new ArrayList<>();
        for (CommoditiesBoughtFromProvider bought :
            topologyEntityBuilder.getCommoditiesBoughtFromProvidersList()) {
            if (bought.hasProviderId()) {
                final long oldProvider = bought.getProviderId();
                CommoditiesBoughtFromProvider cloneCommodityBought = bought.toBuilder()
                                .setProviderId(--fakeProvider)
                                .build();
                cloneCommodityBoughtGroups.add(cloneCommodityBought);
                oldProvidersMap.put(fakeProvider, oldProvider);
            } else {
                cloneCommodityBoughtGroups.add(bought);
            }
        }
        topologyEntityBuilder.clearCommoditiesBoughtFromProviders()
            .addAllCommoditiesBoughtFromProviders(cloneCommodityBoughtGroups);
        Map<String, String> entityProperties = Maps.newHashMap();
        if (!oldProvidersMap.isEmpty()) {
            // TODO: OM-26631 - get rid of unstructured data and Gson
            entityProperties.put("oldProviders", new Gson().toJson(oldProvidersMap));
        }
        topologyEntityBuilder.putAllEntityPropertyMap(entityProperties);
    }

    /**
     * Generate compute commodity bought and commodity sold from a list of compute template resources.
     *
     * @param topologyEntityBuilder builder of TopologyEntityDTO.
     * @param computeTemplateResources a list of compute resources.
     */
    private void addComputeCommodities(
            @Nonnull TopologyEntityDTO.Builder topologyEntityBuilder,
            @Nonnull List<TemplateResource> computeTemplateResources) {
        final Map<String, String> fieldNameValueMap = createFieldNameValueMap(
                computeTemplateResources);
        addComputeCommoditiesBought(topologyEntityBuilder, fieldNameValueMap);
        addComputeCommoditiesSold(topologyEntityBuilder, fieldNameValueMap);
    }

    /**
     * Generate a list of commodity bought and add to TopologyEntityDTO.
     *
     * @param topologyEntityBuilder builder of TopologyEntityDTO.
     * @param fieldNameValueMap a Map which key is template field name and value is field value.
     */
    private void addComputeCommoditiesBought(
            @Nonnull final TopologyEntityDTO.Builder topologyEntityBuilder,
            @Nonnull Map<String, String> fieldNameValueMap) {
        final List<CommodityBoughtDTO> cpuCommodity =
            addComputeCommoditiesBoughtCPU(fieldNameValueMap);
        final List<CommodityBoughtDTO> memCommodity =
            addComputeCommoditiesBoughtMem(fieldNameValueMap);
        final List<CommodityBoughtDTO> ioNetCommodity =
            addComputeCommoditiesBoughtIONet(fieldNameValueMap);

        final CommoditiesBoughtFromProvider commoditiesBoughtFromProvider =
            CommoditiesBoughtFromProvider.newBuilder()
                .setProviderEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
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
    private List<CommodityBoughtDTO> addComputeCommoditiesBoughtCPU(
            @Nonnull Map<String, String> fieldNameValueMap) {
        final double numOfCpu = Double.valueOf(
            fieldNameValueMap.getOrDefault(TemplateProtoUtil.VM_COMPUTE_NUM_OF_VCPU, ZERO));
        final double cpuSpeed = Double.valueOf(
            fieldNameValueMap.getOrDefault(TemplateProtoUtil.VM_COMPUTE_VCPU_SPEED, ZERO));
        final double cpuConsumedFactor = Double.valueOf(
            fieldNameValueMap.getOrDefault(TemplateProtoUtil.VM_COMPUTE_CPU_CONSUMED_FACTOR, ZERO));

        // if created entity is reservation entity, cpu used value should be 0.
        final double used = this.isReservationEntity ? 0.0 : numOfCpu * cpuSpeed * cpuConsumedFactor;
        CommodityBoughtDTO cpuCommodity =
                createCommodityBoughtDTO(CommodityDTO.CommodityType.CPU_VALUE, used);
        CommodityBoughtDTO cpuProvisionCommodity =
                createCommodityBoughtDTO(CommodityType.CPU_PROVISIONED_VALUE, numOfCpu * cpuSpeed);
        return Lists.newArrayList(cpuCommodity, cpuProvisionCommodity);
    }

    /**
     * Generate a list of Memory related commodity bought.
     *
     * @param fieldNameValueMap a Map which key is template field name and value is field value.
     * @return a list of {@link CommodityBoughtDTO}.
     */
    private List<CommodityBoughtDTO> addComputeCommoditiesBoughtMem(
            @Nonnull Map<String, String> fieldNameValueMap) {
        final double memorySize = Double.valueOf(
            fieldNameValueMap.getOrDefault(TemplateProtoUtil.VM_COMPUTE_MEM_SIZE, ZERO));
        final double memoryConsumedFactor = Double.valueOf(
            fieldNameValueMap.getOrDefault(TemplateProtoUtil.VM_COMPUTE_MEM_CONSUMED_FACTOR, ZERO));
        // if created entity is reservation entity, memory used value should be 0.
        final double used = this.isReservationEntity ? 0.0 : memorySize * memoryConsumedFactor;
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
    private List<CommodityBoughtDTO> addComputeCommoditiesBoughtIONet(
            @Nonnull Map<String, String> fieldNameValueMap) {
        // if created entity is reservation entity, io throughput used value should be 0.
        final double ioThroughput = this.isReservationEntity ? 0.0 : Double.valueOf(
            fieldNameValueMap.getOrDefault(TemplateProtoUtil.VM_COMPUTE_IO_THROUGHPUT, ZERO));
        // if created entity is reservation entity, network throughput used value should be 0.
        final double netThroughput = this.isReservationEntity ? 0.0 : Double.valueOf(
            fieldNameValueMap.getOrDefault(TemplateProtoUtil.VM_COMPUTE_NETWORK_THROUGHPUT, ZERO));

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
    private void addComputeCommoditiesSold(@Nonnull final TopologyEntityDTO.Builder topologyEntityBuilder,
                                                  @Nonnull Map<String, String> fieldNameValueMap) {
        final double numOfCpu = Double.valueOf(
            fieldNameValueMap.getOrDefault(TemplateProtoUtil.VM_COMPUTE_NUM_OF_VCPU, ZERO));
        final double cpuSpeed = Double.valueOf(
            fieldNameValueMap.getOrDefault(TemplateProtoUtil.VM_COMPUTE_VCPU_SPEED, ZERO));
        final double memorySize = Double.valueOf(
            fieldNameValueMap.getOrDefault(TemplateProtoUtil.VM_COMPUTE_MEM_SIZE, ZERO));

        final double totalCpuSold = numOfCpu * cpuSpeed;
        CommoditySoldDTO cpuSoldCommodity =
                createCommoditySoldDTO(CommodityDTO.CommodityType.VCPU_VALUE, totalCpuSold);
        CommoditySoldDTO memorySizeCommodity =
                createCommoditySoldDTO(CommodityDTO.CommodityType.VMEM_VALUE, memorySize);
        topologyEntityBuilder.addCommoditySoldList(cpuSoldCommodity);
        topologyEntityBuilder.addCommoditySoldList(memorySizeCommodity);

        if (numOfCpu > 0) {
            topologyEntityBuilder.setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                .setVirtualMachine(VirtualMachineInfo.newBuilder().setNumCpus((int)numOfCpu)));
        }
    }

    /**
     * Convert a list of storage template resources to storage commodity bought and commodity sold.
     *
     * @param topologyEntityBuilder builder of TopologyEntityDTO.
     * @param storageTemplateResources a list of storage template resources.
     */
    private void addStorageCommodities(@Nonnull TopologyEntityDTO.Builder topologyEntityBuilder,
                                              @Nonnull List<TemplateResource> storageTemplateResources) {
        final double totalDiskSize = storageTemplateResources.stream()
            .map(TemplateResource::getFieldsList)
            .flatMap(List::stream)
            .filter(templateField -> templateField.getName().equals(TemplateProtoUtil.VM_STORAGE_DISK_SIZE))
            .map(TemplateField::getValue)
            .mapToDouble(Double::valueOf)
            .sum();
        addStorageCommoditiesSoldTotalDiskSize(topologyEntityBuilder, totalDiskSize);
        // put those storage templates fist which category type is RDM.
        final List<TemplateResource> storageTemplateSortedByRDM =
            sortTemplateResource(storageTemplateResources);
        for (TemplateResource stTemplateSource : storageTemplateSortedByRDM) {
            final List<CommodityBoughtDTO> storageRelatedCommoditiesBought = new ArrayList<>();
            final List<CommodityBoughtDTO> storageCommoditiesBought =
                addStorageCommoditiesBought(stTemplateSource, stTemplateSource.getCategory().getType());
            storageRelatedCommoditiesBought.addAll(storageCommoditiesBought);
            final CommoditiesBoughtFromProvider.Builder commoditiesBoughtFromProvider =
                CommoditiesBoughtFromProvider.newBuilder()
                    .setProviderEntityType(EntityType.STORAGE_VALUE);
            if (stTemplateSource.getCategory().getType().toUpperCase().equals(RDM)) {
                // create an extra LUN commodity between the VM and this rdm storage
                storageRelatedCommoditiesBought.add(addLunCommoditiesBought());
            }
            commoditiesBoughtFromProvider.addAllCommodityBought(storageRelatedCommoditiesBought);
            topologyEntityBuilder.addCommoditiesBoughtFromProviders(commoditiesBoughtFromProvider.build());
        }
    }

    /**
     * Sort template resources by category type, and it tries to put those template resources at first
     * place which category type is RDM, and also we need to sort commodity bought constraints by
     * EXTENT commodity type. Then we can keep the EXTENT constraints as much as possible for RDM storage.
     *
     * @param storageTemplateResources a list of {@link TemplateResource}
     * @return a list of {@link TemplateResource} after sorted.
     */
    private List<TemplateResource> sortTemplateResource(
            @Nonnull List<TemplateResource> storageTemplateResources) {
        return storageTemplateResources.stream()
            .sorted((templateFirst, templateSecond) -> {
                            final boolean firstIsRDM = templateFirst.getCategory().getType()
                                    .toUpperCase().equals(RDM);
                            final boolean secondIsRDM = templateSecond.getCategory().getType()
                                    .toUpperCase().equals(RDM);
                            return (firstIsRDM ^ secondIsRDM) ? (firstIsRDM ? -1 : 1) : 0;
                    })
            .collect(Collectors.toList());
    }

    /**
     * Sort {@link CommoditiesBoughtFromProvider} by EXTENT commodity type, it tries to pull those
     * {@link CommoditiesBoughtFromProvider} at first place which has EXTENT commodity. The reason
     * we need this sort is that we want to try the best to copy commodity constraints between same storage
     * types. For example: original VM#1 has two commodity bought groups: first one is normal DISK type,
     * second one is RDM type. And template VM also has two commodity bought groups, one is DISK and the
     * other one is RDM type. Because the provider entity type are all STORAGE, in order to copy
     * correct constraints between same disk types, at here, we sort commodity bought group by EXTENT
     * commodity(only RDM storage type will have this commodity), In this case, it will copy
     * constraints between same disk types.
     *
     * @param accessCommodityBought a list of {@link CommoditiesBoughtFromProvider}
     * @return a list of {@link CommoditiesBoughtFromProvider} after sorted.
     */
    private List<CommoditiesBoughtFromProvider> sortAccessCommodityBought(
        @Nonnull final List<CommoditiesBoughtFromProvider> accessCommodityBought) {
        return accessCommodityBought.stream()
            .sorted((accessCommoditiesFirst, accessCommoditiesSecond) -> {
                boolean firstAccessCommoditiesContainsRDM =
                    accessCommoditiesFirst.getCommodityBoughtList().stream()
                        .anyMatch(accessCommodity -> accessCommodity
                            .getCommodityType().getType() == CommodityDTO.CommodityType.EXTENT_VALUE);
                boolean secondAccessCommoditiesContainsRDM =
                        accessCommoditiesSecond.getCommodityBoughtList().stream()
                                .anyMatch(accessCommodity -> accessCommodity
                                        .getCommodityType().getType() == CommodityDTO.CommodityType.EXTENT_VALUE);
                return (firstAccessCommoditiesContainsRDM ^ secondAccessCommoditiesContainsRDM) ?
                        (firstAccessCommoditiesContainsRDM ? -1 : 1) : 0;
            }).collect(Collectors.toList());
    }

    /**
     * Generate a list of storage related Commodity bought.
     *
     * @param stTemplateSource storage template resources.
     * @param type category type of template resource.
     * @return a list of {@link CommodityBoughtDTO}.
     */
    private List<CommodityBoughtDTO> addStorageCommoditiesBought(
            @Nonnull TemplateResource stTemplateSource,
            @Nonnull String type) {
        final Map<String, String> fieldNameValueMap = createFieldNameValueMap(
                Lists.newArrayList(stTemplateSource));

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
    private List<CommodityBoughtDTO> addStorageCommoditiesBoughtST(
            @Nonnull Map<String, String> fieldNameValueMap,
            @Nonnull String type) {
        final double disSize = type.toUpperCase().equals(RDM) ? Double.MIN_VALUE :
            Double.valueOf(fieldNameValueMap.getOrDefault(TemplateProtoUtil.VM_STORAGE_DISK_SIZE, ZERO));
        // if created entity is reservation entity, storage amount used value should be 0.
        final double disConsumedFactor = this.isReservationEntity ? 0.0 : Double.valueOf(
            fieldNameValueMap.getOrDefault(TemplateProtoUtil.VM_STORAGE_DISK_CONSUMED_FACTOR, ZERO));
        final double disIops = type.toUpperCase().equals(RDM) ? Double.MIN_VALUE :
            Double.valueOf(fieldNameValueMap.getOrDefault(TemplateProtoUtil.VM_STORAGE_DISK_IOPS, ZERO));
        CommodityBoughtDTO stAmountCommodity =
            createCommodityBoughtDTO(CommodityDTO.CommodityType.STORAGE_AMOUNT_VALUE,
                disSize * disConsumedFactor);
        CommodityBoughtDTO stProvCommodity =
            createCommodityBoughtDTO(CommodityDTO.CommodityType.STORAGE_PROVISIONED_VALUE, disSize);
        // if created entity is reservation entity, storage access used value should be 0.
        final double storageAccessValue = this.isReservationEntity ? 0.0 : disIops;
        CommodityBoughtDTO stAccessCommodity =
            createCommodityBoughtDTO(CommodityDTO.CommodityType.STORAGE_ACCESS_VALUE, storageAccessValue);
        return Lists.newArrayList(stAmountCommodity, stProvCommodity, stAccessCommodity);
    }

    /**
     * Generate storage commodity sold and add to TopologyEntityDTO.
     *
     * @param topologyEntityBuilder builder of TopologyEntityDTO.
     * @param totalDiskSize total disk size for sold.
     */
    private void addStorageCommoditiesSoldTotalDiskSize(
            @Nonnull TopologyEntityDTO.Builder topologyEntityBuilder,
            @Nonnull double totalDiskSize) {
        if (Double.isFinite(totalDiskSize)) {
            CommoditySoldDTO totalDiskSizeCommodity =
                    createCommoditySoldDTO(CommodityDTO.CommodityType.VSTORAGE_VALUE,
                            totalDiskSize);
            topologyEntityBuilder.addCommoditySoldList(totalDiskSizeCommodity);
        }
    }

    /**
     * Generate Lun commodity bought for RDM type storage.
     *
     * @return {@link CommodityBoughtDTO}
     */
    private CommodityBoughtDTO addLunCommoditiesBought() {
        CommodityBoughtDTO lunCommodityBought =
            createCommodityBoughtDTO(CommodityDTO.CommodityType.EXTENT_VALUE, 1);
        return lunCommodityBought;
    }
}
