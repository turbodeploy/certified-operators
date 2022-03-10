package com.vmturbo.topology.processor.template;

import static com.vmturbo.common.protobuf.plan.TemplateDTO.ResourcesCategory.ResourcesCategoryName.Compute;
import static com.vmturbo.common.protobuf.plan.TemplateDTO.ResourcesCategory.ResourcesCategoryName.Storage;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.TemplateProtoUtil;
import com.vmturbo.common.protobuf.cpucapacity.CpuCapacity.CpuModelScaleFactorResponse;
import com.vmturbo.common.protobuf.cpucapacity.CpuCapacity.CpuScaleFactorRequest;
import com.vmturbo.common.protobuf.cpucapacity.CpuCapacityServiceGrpc.CpuCapacityServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.TemplateDTO.Template;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateField;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateResource;
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommodityBoughtView;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommoditySoldView;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl.CommoditiesBoughtFromProviderImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl.CommoditiesBoughtFromProviderView;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TypeSpecificInfoImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TypeSpecificInfoImpl.VirtualMachineInfoImpl;
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
 * buy provision commodity, all other commodity bought value will be set to zero.
 */
public class VirtualMachineEntityConstructor extends TopologyEntityConstructor
        implements ITopologyEntityConstructor {

    private static final Logger logger = LogManager.getLogger();

    private static final String ZERO = "0";
    private static final String RDM = "RDM";

    private final CpuCapacityServiceBlockingStub cpuCapacityService;

    private static final Gson GSON = new Gson();

    /**
     * Creates an object uses for converting templates into TopologyEntityImpl.
     *
     * @param cpuCapacityService the service used to estimate the scaling factor of a cpu model.
     */
    public VirtualMachineEntityConstructor(@Nonnull CpuCapacityServiceBlockingStub cpuCapacityService) {
        this.cpuCapacityService = Objects.requireNonNull(cpuCapacityService);
    }

    @Override
    public TopologyEntityImpl createTopologyEntityFromTemplate(
            @Nonnull final Template template, @Nonnull Map<Long, TopologyEntity.Builder> topology,
            @Nullable TopologyEntityImpl originalTopologyEntity,
            @Nonnull TemplateActionType actionType, @Nonnull IdentityProvider identityProvider,
            @Nullable String nameSuffix) throws TopologyEntityConstructorException {
        TopologyEntityImpl topologyEntityBuilder = super.generateTopologyEntityBuilder(
                template, originalTopologyEntity, actionType, identityProvider,
                EntityType.VIRTUAL_MACHINE_VALUE, nameSuffix);

        final List<CommoditiesBoughtFromProviderView> commodityBoughtConstraints =
                sortAccessCommodityBought(getActiveCommoditiesWithKeysGroups(originalTopologyEntity));
        final Set<CommoditySoldView> commoditySoldConstraints = getCommoditySoldConstraint(
            originalTopologyEntity);
        final List<TemplateResource> computeTemplateResources = getTemplateResources(template,
                Compute);
        addComputeCommodities(topologyEntityBuilder, computeTemplateResources, template);

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
     * @param topologyEntityBuilder {@link TopologyEntityImpl}
     */
    private void handleProviderIdForCommodityBought(
            @Nonnull final TopologyEntityImpl topologyEntityBuilder) {
        final Map<Long, Long> oldProvidersMap = Maps.newHashMap();
        long fakeProvider = 0;
        final List<CommoditiesBoughtFromProviderView> cloneCommodityBoughtGroups = new ArrayList<>();
        for (CommoditiesBoughtFromProviderView bought
                : topologyEntityBuilder.getCommoditiesBoughtFromProvidersList()) {
            if (bought.hasProviderId()) {
                final long oldProvider = bought.getProviderId();
                CommoditiesBoughtFromProviderView cloneCommodityBought = bought.copy()
                                .setProviderId(--fakeProvider);
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
            entityProperties.put(TopologyDTOUtil.OLD_PROVIDERS, GSON.toJson(oldProvidersMap));
        }
        topologyEntityBuilder.putAllEntityPropertyMap(entityProperties);
    }

    /**
     * Generate compute commodity bought and commodity sold from a list of compute template resources.
     *
     * @param topologyEntityBuilder builder of TopologyEntityDTO.
     * @param computeTemplateResources a list of compute resources.
     * @param template the template that could have a cpu model used for estimating the scaling
     *                 factor.
     */
    private void addComputeCommodities(
            @Nonnull TopologyEntityImpl topologyEntityBuilder,
            @Nonnull List<TemplateResource> computeTemplateResources,
            @Nonnull final Template template) {
        final Map<String, String> fieldNameValueMap = createFieldNameValueMap(
                computeTemplateResources);

        double scalingFactor = 1.0;
        if (template.hasTemplateInfo()
                && template.getTemplateInfo().hasCpuModel()
                && StringUtils.isNotBlank(template.getTemplateInfo().getCpuModel())) {
            String cpuModel = template.getTemplateInfo().getCpuModel();
            CpuModelScaleFactorResponse response = cpuCapacityService.getCpuScaleFactors(CpuScaleFactorRequest.newBuilder()
                .addCpuModelNames(cpuModel)
                .build());
            Double nullableScalingFactor = response.getScaleFactorByCpuModelMap().get(cpuModel);
            if (nullableScalingFactor != null) {
                scalingFactor = nullableScalingFactor;
            }
            logger.debug("plan vm template with oid {} has cpu model {} with scaling factor {}",
                template.getId(), cpuModel, scalingFactor);
        } else {
            logger.warn("plan vm template with oid {} did not have a cpu model. "
                    + "falling back to 1.0 scaling factor."
                    + " hasTemplateInfo={}, hasCpuModel={}, isNotBlank={}",
                template.getId(),
                template.hasTemplateInfo(),
                template.hasTemplateInfo()
                    && template.getTemplateInfo().hasCpuModel(),
                template.hasTemplateInfo()
                    && template.getTemplateInfo().hasCpuModel()
                    && StringUtils.isNotBlank(template.getTemplateInfo().getCpuModel()));
        }

        addComputeCommoditiesBought(topologyEntityBuilder, fieldNameValueMap, scalingFactor);
        addComputeCommoditiesSold(topologyEntityBuilder, fieldNameValueMap, scalingFactor);
    }

    /**
     * Generate a list of commodity bought and add to TopologyEntityDTO.
     *
     * @param topologyEntityBuilder builder of TopologyEntityDTO.
     * @param fieldNameValueMap a Map which key is template field name and value is field value.
     * @param scalingFactor multiplier of how much better or worse this VM's cpu usage or capacity
     *                      needs to be scaled because of the CPU model it expects.
     */
    private void addComputeCommoditiesBought(
            @Nonnull final TopologyEntityImpl topologyEntityBuilder,
            @Nonnull Map<String, String> fieldNameValueMap,
            @Nonnull final double scalingFactor) {
        final List<CommodityBoughtView> cpuCommodity =
            addComputeCommoditiesBoughtCPU(fieldNameValueMap, scalingFactor);
        final List<CommodityBoughtView> memCommodity =
            addComputeCommoditiesBoughtMem(fieldNameValueMap);
        final List<CommodityBoughtView> ioNetCommodity =
            addComputeCommoditiesBoughtIONet(fieldNameValueMap);

        final CommoditiesBoughtFromProviderView commoditiesBoughtFromProvider =
            new CommoditiesBoughtFromProviderImpl()
                .setProviderEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
                .addAllCommodityBought(cpuCommodity)
                .addAllCommodityBought(memCommodity)
                .addAllCommodityBought(ioNetCommodity);
        topologyEntityBuilder.addCommoditiesBoughtFromProviders(commoditiesBoughtFromProvider);
    }

    /**
     * Generate a list of CPU related commodity bought.
     *
     * @param fieldNameValueMap a Map which key is template field name and value is field value.
     * @param scalingFactor multiplier of how much better or worse this VM's cpu usage or capacity
     *                      needs to be scaled because of the CPU model it expects.
     * @return a list of {@link CommodityBoughtView}.
     */
    private List<CommodityBoughtView> addComputeCommoditiesBoughtCPU(
            @Nonnull Map<String, String> fieldNameValueMap,
            double scalingFactor) {
        final double numOfCpu = Double.valueOf(
            fieldNameValueMap.getOrDefault(TemplateProtoUtil.VM_COMPUTE_NUM_OF_VCPU, ZERO));
        final double cpuSpeed = Double.valueOf(
            fieldNameValueMap.getOrDefault(TemplateProtoUtil.VM_COMPUTE_VCPU_SPEED, ZERO));
        final double cpuConsumedFactor = Double.valueOf(
            fieldNameValueMap.getOrDefault(TemplateProtoUtil.VM_COMPUTE_CPU_CONSUMED_FACTOR, ZERO));

        // if created entity is reservation entity, cpu used value should be 0.
        final double used = numOfCpu * cpuSpeed * cpuConsumedFactor;
        final CommodityBoughtView partialCpuCommodity =
                createCommodityBoughtView(CommodityDTO.CommodityType.CPU_VALUE, used);
        final CommodityBoughtView cpuCommodity = partialCpuCommodity.copy()
            .setScalingFactor(scalingFactor);
        final CommodityBoughtView partialCpuProvisionCommodity =
                createCommodityBoughtView(CommodityType.CPU_PROVISIONED_VALUE, numOfCpu * cpuSpeed);
        final CommodityBoughtView cpuProvisionCommodity = partialCpuProvisionCommodity.copy()
            .setScalingFactor(scalingFactor);

        return Lists.newArrayList(cpuCommodity, cpuProvisionCommodity);
    }

    /**
     * Generate a list of Memory related commodity bought.
     *
     * @param fieldNameValueMap a Map which key is template field name and value is field value.
     * @return a list of {@link CommodityBoughtView}.
     */
    private List<CommodityBoughtView> addComputeCommoditiesBoughtMem(
            @Nonnull Map<String, String> fieldNameValueMap) {
        final double memorySize = Double.valueOf(
            fieldNameValueMap.getOrDefault(TemplateProtoUtil.VM_COMPUTE_MEM_SIZE, ZERO));
        final double memoryConsumedFactor = Double.valueOf(
            fieldNameValueMap.getOrDefault(TemplateProtoUtil.VM_COMPUTE_MEM_CONSUMED_FACTOR, ZERO));
        // if created entity is reservation entity, memory used value should be 0.
        final double used = memorySize * memoryConsumedFactor;
        CommodityBoughtView memCommodity =
            createCommodityBoughtView(CommodityDTO.CommodityType.MEM_VALUE, used);
        CommodityBoughtView memProvCommodity =
            createCommodityBoughtView(CommodityDTO.CommodityType.MEM_PROVISIONED_VALUE, memorySize);
        return Lists.newArrayList(memCommodity, memProvCommodity);
    }

    /**
     * Generate a list of IO and Network related commodity bought.
     *
     * @param fieldNameValueMap a Map which key is template field name and value is field value.
     * @return a list of {@link CommodityBoughtView}.
     */
    private List<CommodityBoughtView> addComputeCommoditiesBoughtIONet(
            @Nonnull Map<String, String> fieldNameValueMap) {
        // if created entity is reservation entity, io throughput used value should be 0.
        final double ioThroughput = Double.valueOf(
            fieldNameValueMap.getOrDefault(TemplateProtoUtil.VM_COMPUTE_IO_THROUGHPUT, ZERO));
        // if created entity is reservation entity, network throughput used value should be 0.
        final double netThroughput = Double.valueOf(
            fieldNameValueMap.getOrDefault(TemplateProtoUtil.VM_COMPUTE_NETWORK_THROUGHPUT, ZERO));

        CommodityBoughtView ioThroughputCommodity =
            createCommodityBoughtView(CommodityDTO.CommodityType.IO_THROUGHPUT_VALUE, ioThroughput);
        CommodityBoughtView netThroughputCommodity =
            createCommodityBoughtView(CommodityDTO.CommodityType.NET_THROUGHPUT_VALUE, netThroughput);
        return Lists.newArrayList(ioThroughputCommodity, netThroughputCommodity);
    }

    /**
     * Generate a list of Commodity sold and add to TopologyEntityDTO.
     *
     * @param topologyEntityBuilder builder of TopologyEntityDTO.
     * @param fieldNameValueMap a Map which key is template field name and value is field value.
     * @param scalingFactor multiplier of how much better or worse this VM's cpu usage or capacity
     *                      needs to be scaled because of the CPU model it expects.
     */
    private void addComputeCommoditiesSold(
            @Nonnull final TopologyEntityImpl topologyEntityBuilder,
            @Nonnull final Map<String, String> fieldNameValueMap,
            @Nonnull final double scalingFactor) {
        final double numOfCpu = Double.valueOf(
            fieldNameValueMap.getOrDefault(TemplateProtoUtil.VM_COMPUTE_NUM_OF_VCPU, ZERO));
        final double cpuSpeed = Double.valueOf(
            fieldNameValueMap.getOrDefault(TemplateProtoUtil.VM_COMPUTE_VCPU_SPEED, ZERO));
        final double memorySize = Double.valueOf(
            fieldNameValueMap.getOrDefault(TemplateProtoUtil.VM_COMPUTE_MEM_SIZE, ZERO));

        final double totalCpuSold = numOfCpu * cpuSpeed;
        final CommoditySoldView partialCpuSoldCommodity =
                createCommoditySoldDTO(CommodityDTO.CommodityType.VCPU_VALUE, totalCpuSold);
        final CommoditySoldView cpuSoldCommodity = partialCpuSoldCommodity.copy()
            .setScalingFactor(scalingFactor);
        final CommoditySoldView memorySizeCommodity =
                createCommoditySoldDTO(CommodityDTO.CommodityType.VMEM_VALUE, memorySize);
        topologyEntityBuilder.addCommoditySoldList(cpuSoldCommodity);
        topologyEntityBuilder.addCommoditySoldList(memorySizeCommodity);

        if (numOfCpu > 0) {
            topologyEntityBuilder.setTypeSpecificInfo(new TypeSpecificInfoImpl()
                .setVirtualMachine(new VirtualMachineInfoImpl().setNumCpus((int)numOfCpu)));
        }
    }

    /**
     * Convert a list of storage template resources to storage commodity bought and commodity sold.
     *
     * @param topologyEntityBuilder builder of TopologyEntityDTO.
     * @param storageTemplateResources a list of storage template resources.
     */
    private void addStorageCommodities(@Nonnull TopologyEntityImpl topologyEntityBuilder,
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
            final List<CommodityBoughtView> storageRelatedCommoditiesBought = new ArrayList<>();
            final List<CommodityBoughtView> storageCommoditiesBought =
                addStorageCommoditiesBought(stTemplateSource, stTemplateSource.getCategory().getType());
            storageRelatedCommoditiesBought.addAll(storageCommoditiesBought);
            final CommoditiesBoughtFromProviderImpl commoditiesBoughtFromProvider =
                new CommoditiesBoughtFromProviderImpl()
                    .setProviderEntityType(EntityType.STORAGE_VALUE);
            if (stTemplateSource.getCategory().getType().toUpperCase().equals(RDM)) {
                // create an extra LUN commodity between the VM and this rdm storage
                storageRelatedCommoditiesBought.add(addLunCommoditiesBought());
            }
            commoditiesBoughtFromProvider.addAllCommodityBought(storageRelatedCommoditiesBought);
            topologyEntityBuilder.addCommoditiesBoughtFromProviders(commoditiesBoughtFromProvider);
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
     * Sort {@link CommoditiesBoughtFromProviderView} by EXTENT commodity type, it tries to pull those
     * {@link CommoditiesBoughtFromProviderView} at first place which has EXTENT commodity. The reason
     * we need this sort is that we want to try the best to copy commodity constraints between same storage
     * types. For example: original VM#1 has two commodity bought groups: first one is normal DISK type,
     * second one is RDM type. And template VM also has two commodity bought groups, one is DISK and the
     * other one is RDM type. Because the provider entity type are all STORAGE, in order to copy
     * correct constraints between same disk types, at here, we sort commodity bought group by EXTENT
     * commodity(only RDM storage type will have this commodity), In this case, it will copy
     * constraints between same disk types.
     *
     * @param accessCommodityBought a list of {@link CommoditiesBoughtFromProviderView}
     * @return a list of {@link CommoditiesBoughtFromProviderView} after sorted.
     */
    private List<CommoditiesBoughtFromProviderView> sortAccessCommodityBought(
        @Nonnull final List<CommoditiesBoughtFromProviderView> accessCommodityBought) {
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
                return (firstAccessCommoditiesContainsRDM ^ secondAccessCommoditiesContainsRDM)
                        ? (firstAccessCommoditiesContainsRDM ? -1 : 1)
                        : 0;
            }).collect(Collectors.toList());
    }

    /**
     * Generate a list of storage related Commodity bought.
     *
     * @param stTemplateSource storage template resources.
     * @param type category type of template resource.
     * @return a list of {@link CommodityBoughtView}.
     */
    private List<CommodityBoughtView> addStorageCommoditiesBought(
            @Nonnull TemplateResource stTemplateSource,
            @Nonnull String type) {
        final Map<String, String> fieldNameValueMap = createFieldNameValueMap(
                Lists.newArrayList(stTemplateSource));

        List<CommodityBoughtView> storageCommodityBought =
            addStorageCommoditiesBoughtST(fieldNameValueMap, type);
        return storageCommodityBought;
    }

    /**
     * Generate a list of storage commodity bought, and also handle RDM disk type.
     *
     * @param fieldNameValueMap a Map which key is template field name and value is field value.
     * @param type category type of template resource.
     * @return a list of {@link CommodityBoughtView}.
     */
    private List<CommodityBoughtView> addStorageCommoditiesBoughtST(
            @Nonnull Map<String, String> fieldNameValueMap,
            @Nonnull String type) {
        final double disSize = type.toUpperCase().equals(RDM)
            ? Double.MIN_VALUE
            : Double.valueOf(fieldNameValueMap.getOrDefault(TemplateProtoUtil.VM_STORAGE_DISK_SIZE, ZERO));
        // if created entity is reservation entity, storage amount used value should be 0.
        final double disConsumedFactor = Double.valueOf(
            fieldNameValueMap.getOrDefault(TemplateProtoUtil.VM_STORAGE_DISK_CONSUMED_FACTOR, ZERO));
        final double disIops = type.toUpperCase().equals(RDM)
            ? Double.MIN_VALUE
            : Double.valueOf(fieldNameValueMap.getOrDefault(TemplateProtoUtil.VM_STORAGE_DISK_IOPS, ZERO));
        CommodityBoughtView stAmountCommodity =
            createCommodityBoughtView(CommodityDTO.CommodityType.STORAGE_AMOUNT_VALUE,
                disSize * disConsumedFactor);
        CommodityBoughtView stProvCommodity =
            createCommodityBoughtView(CommodityDTO.CommodityType.STORAGE_PROVISIONED_VALUE, disSize);
        // if created entity is reservation entity, storage access used value should be 0.
        final double storageAccessValue = disIops;
        CommodityBoughtView stAccessCommodity =
            createCommodityBoughtView(CommodityDTO.CommodityType.STORAGE_ACCESS_VALUE, storageAccessValue);
        return Lists.newArrayList(stAmountCommodity, stProvCommodity, stAccessCommodity);
    }

    /**
     * Generate storage commodity sold and add to TopologyEntityDTO.
     *
     * @param topologyEntityBuilder builder of TopologyEntityDTO.
     * @param totalDiskSize total disk size for sold.
     */
    private void addStorageCommoditiesSoldTotalDiskSize(
            @Nonnull TopologyEntityImpl topologyEntityBuilder,
            @Nonnull double totalDiskSize) {
        if (Double.isFinite(totalDiskSize)) {
            CommoditySoldView totalDiskSizeCommodity =
                    createCommoditySoldDTO(CommodityDTO.CommodityType.VSTORAGE_VALUE,
                            totalDiskSize);
            topologyEntityBuilder.addCommoditySoldList(totalDiskSizeCommodity);
        }
    }

    /**
     * Generate Lun commodity bought for RDM type storage.
     *
     * @return {@link CommodityBoughtView}
     */
    private CommodityBoughtView addLunCommoditiesBought() {
        CommodityBoughtView lunCommodityBought =
            createCommodityBoughtView(CommodityDTO.CommodityType.EXTENT_VALUE, 1);
        return lunCommodityBought;
    }
}
