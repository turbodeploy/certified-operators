package com.vmturbo.plan.orchestrator.templates;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.plan.TemplateDTO.ResourcesCategory;
import com.vmturbo.common.protobuf.plan.TemplateDTO.ResourcesCategory.ResourcesCategoryName;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateField;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateInfo;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateResource;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateSpec;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.ProfileDTO.CommodityProfileDTO;
import com.vmturbo.platform.common.dto.ProfileDTO.EntityProfileDTO;
import com.vmturbo.platform.common.dto.ProfileDTO.EntityProfileDTO.PMProfileDTO;
import com.vmturbo.platform.common.dto.ProfileDTO.EntityProfileDTO.VMProfileDTO;

/**
 * A convert class for converting {@link EntityProfileDTO} object
 * to {@link com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateInfo} object, and also it will try
 * to find template spec match with converted template.
 */
public class TemplatesMapper {

    private static final Logger logger = LogManager.getLogger();

    private static final float sizeDivisor = 1024f;

    private static final float percentageDivisor = 100f;


    /**
     * Convert Probe send EntityProfileDTO object o {@link com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateInfo}
     * object. And also find the matched template spec.
     *
     * @param profile EntityProfileDTO object contains discovered templates.
     * @param templateSpecMap A Map for entity type to {@link TemplateSpec} object.
     * @return Converted templateInstance object.
     * @throws NoMatchingTemplateSpecException
     */
    public static TemplateInfo createTemplateInfo(@Nonnull EntityProfileDTO profile,
                                                  Map<String, TemplateSpec> templateSpecMap)
                                                  throws NoMatchingTemplateSpecException {
        Objects.requireNonNull(profile);
        Optional<TemplateSpec> templateSpec = Optional.ofNullable(templateSpecMap.get(profile.getEntityType().name()));
        if (!templateSpec.isPresent()) {
            throw new NoMatchingTemplateSpecException("Not find template spec for entity type " + profile.getEntityType());
        }
        return TemplateInfo.newBuilder()
            .setName(profile.getDisplayName())
            .setEntityType(profile.getEntityType().getNumber())
            .setModel(profile.getModel())
            .setVendor(profile.getVendor())
            .setDescription(profile.getDescription())
            .setTemplateSpecId(templateSpec.get().getId())
            .setProbeTemplateId(profile.getId())
            .addAllResources(createTemplateResource(profile))
            .build();
    }

    /**
     * Convert probe discovered templates to {@link TemplateResource}
     * @param profile EntityProfileDTO object contains discovered templates.
     * @return list of TemplateResource
     */
    private static List<TemplateResource> createTemplateResource(@Nonnull EntityProfileDTO profile) {

        List<TemplateResource> templateResources = new ArrayList<>();
        // Handle storage template which is not belong to one of EntityTypeSpecificData
        if (profile.getEntityType().equals(EntityType.STORAGE)) {
            TemplateResource storageTemplateResource = TemplateResource.newBuilder()
                .setCategory(ResourcesCategory.newBuilder().setName(ResourcesCategoryName.Storage))
                .addAllFields(createStorageTemplateFields(profile))
                .build();
            templateResources.add(storageTemplateResource);
            return templateResources;
        }
        switch (profile.getEntityTypeSpecificDataCase()) {
            case VMPROFILEDTO:
                TemplateResource vmTemplateComputeResource = TemplateResource.newBuilder()
                    .setCategory(ResourcesCategory.newBuilder().setName(ResourcesCategoryName.Compute))
                    .addAllFields(createVMTemplateComputeFields(profile))
                    .build();
                TemplateResource vmTemplateStorageResource = TemplateResource.newBuilder()
                    .setCategory(ResourcesCategory.newBuilder().setName(ResourcesCategoryName.Storage))
                    .addAllFields(createVMTemplateStorageFields(profile))
                    .build();
                templateResources.add(vmTemplateComputeResource);
                templateResources.add(vmTemplateStorageResource);
                break;
            case PMPROFILEDTO:
                TemplateResource pmTemplateComputeResource = TemplateResource.newBuilder()
                    .setCategory(ResourcesCategory.newBuilder().setName(ResourcesCategoryName.Compute))
                    .addAllFields(createPMTemplateComputeFields(profile))
                    .build();
                TemplateResource pmTemplateInfraResource = TemplateResource.newBuilder()
                    .setCategory(ResourcesCategory.newBuilder().setName(ResourcesCategoryName.Infrastructure))
                    .addAllFields(createPMTemplateInfraFields(profile))
                    .build();
                templateResources.add(pmTemplateComputeResource);
                templateResources.add(pmTemplateInfraResource);
                break;
            case DBPROFILEDTO:
                logger.info("DB template not implemented yet.");
                break;
            default:
                logger.info(profile.getEntityTypeSpecificDataCase() + " not supported yet.");
        }
        return templateResources;
    }

    /**
     * Convert for VM template compute category fields.
     *
     * @param profile  EntityProfileDTO object contains discovered templates.
     * @return list of TemplateFields
     */
    private static List<TemplateField> createVMTemplateComputeFields(EntityProfileDTO profile) {
        final VMProfileDTO vmProfileDTO = profile.getVmProfileDTO();
        List<TemplateField> templateFields = new ArrayList<>();
        templateFields.add(createTemplateField(DiscoveredTemplatesConstantFields.VM_COMPUTE_NUM_OF_VCPU,
            String.valueOf(vmProfileDTO.getNumVCPUs())));
        templateFields.add(createTemplateField(DiscoveredTemplatesConstantFields.VM_COMPUTE_VCPU_SPEED,
            String.valueOf(vmProfileDTO.getVCPUSpeed())));
        templateFields.add(createTemplateField(DiscoveredTemplatesConstantFields.VM_COMPUTE_CPU_CONSUMED_FACTOR,
            String.valueOf(getFieldFromCommodityDTO(profile.getCommodityProfileList(),
                CommodityType.CPU).map(CommodityProfileDTO::getConsumedFactor)
                .map(value -> value / percentageDivisor)
                .orElse(0.0f))));
        templateFields.add(createTemplateField(DiscoveredTemplatesConstantFields.VM_COMPUTE_MEM_SIZE,
            String.valueOf(getFieldFromCommodityDTO(profile.getCommodityProfileList(),
                CommodityType.VMEM).map(CommodityProfileDTO::getCapacity)
                .map(value -> value / sizeDivisor)
                .orElse(0.0f))));
        templateFields.add(createTemplateField(DiscoveredTemplatesConstantFields.VM_COMPUTE_MEM_CONSUMED_FACTOR,
            String.valueOf(getFieldFromCommodityDTO(profile.getCommodityProfileList(),
                CommodityType.MEM).map(CommodityProfileDTO::getConsumedFactor)
                .map(value -> value / percentageDivisor)
                .orElse(0.0f))));
        templateFields.add(createTemplateField(DiscoveredTemplatesConstantFields.VM_COMPUTE_IO_THROUGHPUT_CONSUMED,
            String.valueOf(getFieldFromCommodityDTO(profile.getCommodityProfileList(),
                CommodityType.IO_THROUGHPUT).map(CommodityProfileDTO::getConsumed)
                .map(value -> value / sizeDivisor)
                .orElse(0.0f))));
        templateFields.add(createTemplateField(DiscoveredTemplatesConstantFields.VM_COMPUTE_NETWORK_THROUGHPUT_CONSUMED,
            String.valueOf(getFieldFromCommodityDTO(profile.getCommodityProfileList(),
                CommodityType.NET_THROUGHPUT).map(CommodityProfileDTO::getConsumed)
                .map(value -> value / sizeDivisor)
                .orElse(0.0f))));
        return templateFields;
    }

    /**
     * Convert for VM template storage category fields.
     *
     * @param profile EntityProfileDTO object contains discovered templates.
     * @return list of TemplateField
     */
    private static List<TemplateField> createVMTemplateStorageFields(EntityProfileDTO profile) {
        List<TemplateField> templateFields = new ArrayList<>();
        templateFields.add(createTemplateField(DiscoveredTemplatesConstantFields.VM_STORAGE_DISK_SIZE,
            String.valueOf(getFieldFromCommodityDTO(profile.getCommodityProfileList(),
                CommodityType.VSTORAGE).map(CommodityProfileDTO::getCapacity)
                .map(value -> value / sizeDivisor)
                .orElse(0.0f))));
        templateFields.add(createTemplateField(DiscoveredTemplatesConstantFields.VM_STORAGE_DISK_IOPS_CONSUMED,
            String.valueOf(getFieldFromCommodityDTO(profile.getCommodityProfileList(),
                CommodityType.STORAGE_ACCESS).map(CommodityProfileDTO::getConsumed)
                .orElse(0.0f))));
        templateFields.add(createTemplateField(DiscoveredTemplatesConstantFields.VM_STORAGE_DISK_CONSUMED_FACTOR,
            String.valueOf(getFieldFromCommodityDTO(profile.getCommodityProfileList(),
                CommodityType.STORAGE).map(CommodityProfileDTO::getConsumedFactor)
                .orElse(0.0f))));

        return templateFields;
    }

    /**
     * Convert for PM template compute category fields.
     *
     * @param profile EntityProfileDTO object contains discovered templates.
     * @return list of TemplateField
     */
    private static List<TemplateField> createPMTemplateComputeFields(EntityProfileDTO profile) {
        final PMProfileDTO pmProfileDTO = profile.getPmProfileDTO();
        List<TemplateField> templateFields = new ArrayList<>();
        templateFields.add(createTemplateField(DiscoveredTemplatesConstantFields.PM_COMPUTE_NUM_OF_CORE,
            String.valueOf(pmProfileDTO.getNumCores())));
        templateFields.add(createTemplateField(DiscoveredTemplatesConstantFields.PM_COMPUTE_CPU_SPEED,
            String.valueOf(pmProfileDTO.getCpuCoreSpeed())));
        templateFields.add(createTemplateField(DiscoveredTemplatesConstantFields.PM_COMPUTE_IO_THROUGHPUT_SIZE,
            String.valueOf(getFieldFromCommodityDTO(profile.getCommodityProfileList(),
                CommodityType.IO_THROUGHPUT).map(CommodityProfileDTO::getCapacity)
                .map(value -> value / sizeDivisor)
                .orElse(0.0f))));
        templateFields.add(createTemplateField(DiscoveredTemplatesConstantFields.PM_COMPUTE_MEM_SIZE,
            String.valueOf(getFieldFromCommodityDTO(profile.getCommodityProfileList(),
                CommodityType.MEM).map(CommodityProfileDTO::getCapacity)
                .map(value -> value / sizeDivisor)
                .orElse(0.0f))));
        templateFields.add(createTemplateField(DiscoveredTemplatesConstantFields.PM_COMPUTE_NETWORK_THROUGHTPUT_SIZE,
            String.valueOf(getFieldFromCommodityDTO(profile.getCommodityProfileList(),
                CommodityType.NET_THROUGHPUT).map(CommodityProfileDTO::getCapacity)
                .map(value -> value / sizeDivisor)
                .orElse(0.0f))));
        return templateFields;
    }

    /**
     * Convert for PM template infrastructure category fields.
     * @param profile EntityProfileDTO object contains discovered templates.
     *
     * @return list of TemplateField
     */
    private static List<TemplateField> createPMTemplateInfraFields(EntityProfileDTO profile) {
        List<TemplateField> templateFields = new ArrayList<>();
        templateFields.add(createTemplateField(DiscoveredTemplatesConstantFields.PM_INFRA_POWER_SIZE,
            String.valueOf(getFieldFromCommodityDTO(profile.getCommodityProfileList(),
                CommodityType.POWER).map(CommodityProfileDTO::getCapacity)
                .orElse(1.0f))));
        templateFields.add(createTemplateField(DiscoveredTemplatesConstantFields.PM_INFRA_SPACE_SIZE,
            String.valueOf(getFieldFromCommodityDTO(profile.getCommodityProfileList(),
                CommodityType.SPACE).map(CommodityProfileDTO::getCapacity)
                .orElse(1.0f))));
        templateFields.add(createTemplateField(DiscoveredTemplatesConstantFields.PM_INFRA_COOLING_SIZE,
            String.valueOf(getFieldFromCommodityDTO(profile.getCommodityProfileList(),
                CommodityType.COOLING).map(CommodityProfileDTO::getCapacity)
                .orElse(1.0f))));
        return templateFields;
    }

    /**
     * Convert for Storage template storage category fields.
     *
     * @param profile EntityProfileDTO object contains discovered templates.
     * @return list of TemplateField
     */
    private static List<TemplateField> createStorageTemplateFields(EntityProfileDTO profile) {
        List<TemplateField> templateFields = new ArrayList<>();
        templateFields.add(createTemplateField(DiscoveredTemplatesConstantFields.STORAGE_DISK_IOPS,
            String.valueOf(getFieldFromCommodityDTO(profile.getCommodityProfileList(),
                CommodityType.STORAGE_ACCESS).map(CommodityProfileDTO::getCapacity).orElse(0.0f))));
        templateFields.add(createTemplateField(DiscoveredTemplatesConstantFields.STORAGE_DISK_SIZE,
            String.valueOf(getFieldFromCommodityDTO(profile.getCommodityProfileList(),
                CommodityType.STORAGE).map(CommodityProfileDTO::getCapacity)
                .map(value -> value / sizeDivisor)
                .orElse(0.0f))));
        return templateFields;
    }

    private static TemplateField createTemplateField(String name, String value) {
        return TemplateField.newBuilder().setName(name).setValue(value).build();
    }

    private static Optional<CommodityProfileDTO> getFieldFromCommodityDTO(List<CommodityProfileDTO> commodityProfileDTOs,
                                                                          CommodityType type) {
        return commodityProfileDTOs.stream()
            .filter(commodityProfileDTO -> commodityProfileDTO.getCommodityType().equals(type))
            .findFirst();
    }
}
