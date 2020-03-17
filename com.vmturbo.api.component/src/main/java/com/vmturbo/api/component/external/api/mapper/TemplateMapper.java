package com.vmturbo.api.component.external.api.mapper;

import static com.vmturbo.common.protobuf.plan.TemplateDTO.ResourcesCategory.ResourcesCategoryName.Compute;
import static com.vmturbo.common.protobuf.plan.TemplateDTO.ResourcesCategory.ResourcesCategoryName.Infrastructure;
import static com.vmturbo.common.protobuf.plan.TemplateDTO.ResourcesCategory.ResourcesCategoryName.Storage;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.Lists;

import org.apache.commons.lang.NotImplementedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.component.external.api.util.TemplatesUtils;
import com.vmturbo.api.dto.deploymentprofile.DeploymentProfileApiDTO;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.template.ResourceApiDTO;
import com.vmturbo.api.dto.template.TemplateApiDTO;
import com.vmturbo.api.dto.template.TemplateApiInputDTO;
import com.vmturbo.common.protobuf.plan.DeploymentProfileDTO.DeploymentProfile;
import com.vmturbo.common.protobuf.plan.TemplateDTO.ResourcesCategory;
import com.vmturbo.common.protobuf.plan.TemplateDTO.ResourcesCategory.ResourcesCategoryName;
import com.vmturbo.common.protobuf.plan.TemplateDTO.Template;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateField;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateInfo;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateInfo.Builder;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateResource;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateSpec;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateSpecField;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateSpecResource;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.components.common.utils.StringConstants;

/**
 * A Mapper class for template, it provide two convert functions: mapToTemplateApiDTO and
 * mapToTemplateInfo to allow object transformation between external API object with internal
 * TemplateInfo object.
 */
public class TemplateMapper {
    private final Logger logger = LogManager.getLogger();

    private static final float ONE = 1.0f;

    /**
     * Convert {@link Template} object to {@link TemplateApiDTO}. It will use matched template spec
     * to find units for template fields.
     *
     * @param template {@link Template}
     * @param templateSpec matched with template parameter.
     * @param deploymentProfiles The {@link DeploymentProfile}s associated with this template.
     * @return API format - {@link TemplateApiDTO}.
     */
    public TemplateApiDTO mapToTemplateApiDTO(@Nonnull final Template template,
                                              @Nonnull final TemplateSpec templateSpec,
                                              @Nonnull final List<DeploymentProfile> deploymentProfiles) {
        final TemplateApiDTO dto = buildBasicTemplateApiDTO(template);

        dto.setComputeResources(Lists.newArrayList());
        dto.setStorageResources(Lists.newArrayList());
        dto.setInfrastructureResources(Lists.newArrayList());
        List<TemplateResource> templateResourceList = template.getTemplateInfo().getResourcesList();
        // For one templateSpec, its field names should be unique.
        Map<String, TemplateSpecField> templateSpecFieldMap = templateSpec.getResourcesList()
            .stream()
            .map(TemplateSpecResource::getFieldsList)
            .flatMap(List::stream)
            .collect(Collectors.toMap(TemplateSpecField::getName, Function.identity()));

        templateResourceList.stream().forEach(resource -> {
            switch (resource.getCategory().getName()) {
                case Compute:
                    addComputeResource(dto, resource.getFieldsList(), templateSpecFieldMap);
                    break;
                case Infrastructure:
                    addInfrastructureResource(dto, resource.getFieldsList(), templateSpecFieldMap);
                    break;
                case Storage:
                    addStorageResource(dto, resource, templateSpecFieldMap);
                    break;
                default:
                    throw new NotImplementedException("Not support category type: " +
                        resource.getCategory().getName());
            }
        });

        // In XL the relationship between templates and deployment profiles is many-many but the
        // API DTO only supports one. We just pick the first. Since users can't create templates
        // associated with multiple deployment profiles, and we don't allow editing discovered
        // deployment profiles, we don't need to worry about data loss from only keeping one here
        // and saving the resulting DTO.
        deploymentProfiles.stream().findFirst().ifPresent(profile -> {
            final DeploymentProfileApiDTO apiDto = new DeploymentProfileApiDTO();
            apiDto.setUuid(Long.toString(profile.getId()));
            apiDto.setDisplayName(profile.getDeployInfo().getName());
            apiDto.setClassName(StringConstants.SERVICE_CATALOG_ITEM);
            dto.setDeploymentProfile(apiDto);
        });

        return dto;
    }

    /**
     * * Given a templateInfo it checks whether the input fields are the correct ones for the
     * entity type. If there's at least one field that is not correct, it will throw a
     * IllegalArgumentException
     *
     * @param templateInfo {@link TemplateInfo} that contains the fields
     * @param templateSpec matched with template parameter.
     * @throws IllegalArgumentException If the template is invalid.
     */
    private void checkIfValidTemplate(TemplateInfo templateInfo, TemplateSpec templateSpec)
            throws IllegalArgumentException {
        Map<String, TemplateSpecField> templateSpecFieldMap = templateSpec.getResourcesList().stream()
            .map(TemplateSpecResource::getFieldsList)
            .flatMap(List::stream)
            .collect(Collectors.toMap(TemplateSpecField::getName, Function.identity()));

        List<TemplateResource> templateResourceList = templateInfo.getResourcesList();
        for (TemplateResource resource : templateResourceList) {
            for (TemplateField field : resource.getFieldsList()) {
                if (!templateSpecFieldMap.containsKey(field.getName())) {
                    throw new IllegalArgumentException("Wrong field " + field.getName() +
                        " for template of entity type " + templateSpec.getEntityType());
                }
            }
        }
    }

    /**
     * Convert {@link TemplateApiInputDTO} to {@link TemplateInfo}, it will use matched templateSpec
     * to find all fields which have default value. If they are not provided value from inputDTO, they
     * will be assigned with default value.
     *
     * @param inputDTO {@link TemplateApiInputDTO} contains input template information.
     * @param templateSpec matched template spec contains template field constant information.
     * @param entityType value of template entity type.
     * @return converted {@link TemplateInfo}
     * @throws IllegalArgumentException If the template is invalid.
     */
    public TemplateInfo mapToTemplateInfo(TemplateApiInputDTO inputDTO,
                                          TemplateSpec templateSpec,
                                          int entityType) throws IllegalArgumentException {
        Builder templateInfoBuilder = TemplateInfo.newBuilder()
            .setName(inputDTO.getDisplayName())
            .setTemplateSpecId(templateSpec.getId());
        setBasicApiInputDTOProperty(templateInfoBuilder, inputDTO, entityType);
        setResourceProperty(templateInfoBuilder, inputDTO, templateSpec);
        TemplateInfo templateInfo = templateInfoBuilder.build();
        checkIfValidTemplate(templateInfo, templateSpec);
        return templateInfo;
    }

    /**
     * Construct a new External API {@link TemplateApiDTO} with values from the internal
     * {@link Template} protobuf.
     *
     * @param template the internal Template protobuf from which the values are taken
     * @return an External API TemplateApiDTO populated from the given Template protobuf
     */
    private TemplateApiDTO buildBasicTemplateApiDTO(Template template) {
        TemplateApiDTO dto = new TemplateApiDTO();
        final TemplateInfo templateInfo = template.getTemplateInfo();
        dto.setUuid(String.valueOf(template.getId()));
        dto.setDisplayName(templateInfo.getName());
        dto.setModel(templateInfo.getModel());
        dto.setCpuModel(templateInfo.getCpuModel());
        dto.setVendor(templateInfo.getVendor());
        dto.setClassName(UIEntityType.fromType(templateInfo.getEntityType()).apiStr() +
                TemplatesUtils.PROFILE);
        dto.setDescription(templateInfo.getDescription());
        if (templateInfo.hasPrice()) {
            dto.setPrice(templateInfo.getPrice());
        }
        // If template has a targetId, then it is discovered template
        dto.setDiscovered(template.hasTargetId());

        return dto;
    }


    private void addComputeResource(@Nonnull TemplateApiDTO dto,
                                    @Nonnull List<TemplateField> fields,
                                    @Nonnull Map<String, TemplateSpecField> templateSpecFieldMap) {
        final List<ResourceApiDTO> computeResources = new ArrayList<>();
        final List<StatApiDTO> statApiDTOs = convertFieldToStat(fields, templateSpecFieldMap);
        ResourceApiDTO resourceApiDTO = new ResourceApiDTO();
        resourceApiDTO.setStats(statApiDTOs);
        computeResources.add(resourceApiDTO);
        // allow at most 1 element for compute resource
        dto.setComputeResources(computeResources);
    }

    private void addInfrastructureResource(@Nonnull TemplateApiDTO dto,
                                           @Nonnull List<TemplateField> fields,
                                           @Nonnull Map<String, TemplateSpecField> templateSpecFieldMap) {
        final List<ResourceApiDTO> infrastructureResources = new ArrayList<>();
        final List<StatApiDTO> statApiDTOs = convertFieldToStat(fields, templateSpecFieldMap);
        ResourceApiDTO resourceApiDTO = new ResourceApiDTO();
        resourceApiDTO.setStats(statApiDTOs);
        infrastructureResources.add(resourceApiDTO);
        // allow at most 1 element for infrastructure resource
        dto.setInfrastructureResources(infrastructureResources);
    }

    private void addStorageResource(@Nonnull TemplateApiDTO dto,
                                    @Nonnull TemplateResource resource,
                                    @Nonnull Map<String, TemplateSpecField> templateSpecFieldMap) {
        final List<StatApiDTO> statApiDTOs = convertFieldToStat(resource.getFieldsList(), templateSpecFieldMap);
        ResourceApiDTO resourceApiDTO = new ResourceApiDTO();
        resourceApiDTO.setStats(statApiDTOs);
        if (resource.getCategory().hasType()) {
            resourceApiDTO.setType(resource.getCategory().getType());
        }
        dto.getStorageResources().add(resourceApiDTO);
    }

    /**
     * Convert list of {@link TemplateField} to list of {@link StatApiDTO}. It use templateSpecFieldMap
     * map to find matched templateSpecField to get units value.
     *
     * @param fields list of {@link TemplateField}
     * @param templateSpecFieldMap key is field name, value is {@link TemplateSpecField}
     * @return list of {@link StatApiDTO}
     */
    private List<StatApiDTO> convertFieldToStat(@Nonnull List<TemplateField> fields,
                                                @Nonnull Map<String, TemplateSpecField> templateSpecFieldMap) {
        final Set<String> missingFields = new HashSet<>();
        final List<StatApiDTO> statApiDTOs = fields.stream()
            .filter(field -> {
                if (!templateSpecFieldMap.containsKey(field.getName())) {
                    missingFields.add(field.getName());
                    return false;
                }
                return true;
            })
            .map(field -> {
                StatApiDTO statApiDTO = new StatApiDTO();
                statApiDTO.setName(field.getName());
                TemplateSpecField templateSpecField = templateSpecFieldMap.get(field.getName());
                final float divisor = templateSpecField.hasDivisor() ?
                    templateSpecField.getDivisor() : ONE;
                statApiDTO.setValue(Float.parseFloat(field.getValue()) / divisor);
                Optional.ofNullable(templateSpecField.getUnits())
                    .ifPresent(statApiDTO::setUnits);
                return statApiDTO;
            }).collect(Collectors.toList());
        if (!missingFields.isEmpty()) {
            logger.warn("Can not find match template spec field: " + missingFields);
        }
        return statApiDTOs;
    }

    private void setBasicApiInputDTOProperty(@Nonnull TemplateInfo.Builder templateInfo,
                                             @Nonnull TemplateApiInputDTO inputDTO,
                                             int entityType) {
        if (inputDTO.getModel() != null) {
            templateInfo.setModel(inputDTO.getModel());
        }
        if (inputDTO.getCpuModel() != null) {
            templateInfo.setCpuModel(inputDTO.getCpuModel());
        }
        if (inputDTO.getVendor() != null) {
            templateInfo.setVendor(inputDTO.getVendor());
        }
        if (inputDTO.getClassName() != null) {
            templateInfo.setEntityType(entityType);
        }
        if (inputDTO.getDescription() != null) {
            templateInfo.setDescription(inputDTO.getDescription());
        }
        if (inputDTO.getPrice() != null) {
            templateInfo.setPrice(inputDTO.getPrice());
        }
    }

    private void setResourceProperty(@Nonnull TemplateInfo.Builder templateInfo,
                                     @Nonnull TemplateApiInputDTO inputDTO,
                                     @Nonnull TemplateSpec templateSpec) {
        processResourceProperty(templateInfo, templateSpec, inputDTO.getComputeResources(), Compute);
        processResourceProperty(templateInfo, templateSpec, inputDTO.getInfrastructureResources(), Infrastructure);
        processResourceProperty(templateInfo, templateSpec, inputDTO.getStorageResources(), Storage);
    }

    /**
     * Convert list of {@link ResourceApiDTO} to list of {@link TemplateResource}. And it handle
     * the default value template fields, if inputDTO doesn't provide value, it will be assigned default value.
     *
     * @param templateInfo {@link TemplateInfo.Builder} need to created.
     * @param templateSpec matched template spec contains constant value for template field.
     * @param resourceApiDTOS list of {@link ResourceApiDTO}.
     * @param categoryName represent which category need to create.
     */
    private void processResourceProperty(@Nonnull TemplateInfo.Builder templateInfo,
                                         @Nonnull TemplateSpec templateSpec,
                                         List<ResourceApiDTO> resourceApiDTOS,
                                         @Nonnull ResourcesCategoryName categoryName) {
        // UI's template field units are different with internal commodity value units.
        // We need a map to perform unit conversion. The Map key is Template field name, and value
        // is divisor for this template field. And if there is no divisor in template spec, we set
        // to default 1.0f
        final Map<String, Float> multiplierMap = templateSpec.getResourcesList().stream()
            .map(TemplateSpecResource::getFieldsList)
            .flatMap(List::stream)
            .collect(Collectors.toMap(TemplateSpecField::getName,
                templateSpecField ->
                    templateSpecField.hasDivisor() ? Float.valueOf(templateSpecField.getDivisor()) : ONE
            ));
        final List<TemplateField> defaultTemplateFields =
            getDefaultTemplateFields(templateSpec, resourceApiDTOS, categoryName, multiplierMap);

        if (resourceApiDTOS != null) {
            switch (categoryName) {
                case Compute:
                    processOnlyFirstResource(templateInfo, resourceApiDTOS, categoryName,
                        defaultTemplateFields, multiplierMap);
                    break;
                case Infrastructure:
                    processOnlyFirstResource(templateInfo, resourceApiDTOS, categoryName,
                        defaultTemplateFields, multiplierMap);
                    break;
                case Storage:
                    processMultipleResource(templateInfo, resourceApiDTOS, categoryName,
                        defaultTemplateFields, multiplierMap);
                    break;
                default:
                    logger.error("Category type {} is not supported yet.", categoryName);
                    throw new NotImplementedException(categoryName + " type is not supported yet.");
            }
        } else if (!defaultTemplateFields.isEmpty()) {
            addDefaultTemplateField(templateInfo, categoryName, defaultTemplateFields);
        }
    }

    /**
     * For Compute and Infrastructure category resource, it only allow at most one element in the
     * ResourceApiDTO list, so we only need to pick the first element to process.
     *
     * @param templateInfo {@link TemplateInfo.Builder} need to created.
     * @param resourceApiDTOS list of {@link ResourceApiDTO}.
     * @param categoryName represent which category need to create.
     * @param defaultTemplateFields all the template fields which contains default value.
     * @param multiplierMap a Map contains all template field multiplier, key is template field name and
     *                      value is multiplier.
     */
    private void processOnlyFirstResource(@Nonnull TemplateInfo.Builder templateInfo,
                                          List<ResourceApiDTO> resourceApiDTOS,
                                          @Nonnull ResourcesCategoryName categoryName,
                                          @Nonnull List<TemplateField> defaultTemplateFields,
                                          @Nonnull Map<String, Float> multiplierMap) {
        resourceApiDTOS.stream()
            .findFirst()
            .map(resourceApiDTO ->
                convertToTemplateResource(resourceApiDTO, categoryName, Optional.empty(),
                    defaultTemplateFields, multiplierMap))
            .ifPresent(resource -> templateInfo.addResources(resource));
    }

    /**
     * For Storage category resource, it allow multiple elements in ResourceApiDTO list, and each
     * element could have different type, we need to process all the elements.
     *
     * @param templateInfo {@link TemplateInfo.Builder} need to created.
     * @param resourceApiDTOS list of {@link ResourceApiDTO}.
     * @param categoryName represent which category need to create.
     * @param defaultTemplateFields all the template fields which contains default value.
     * @param multiplierMap a Map contains all template field multiplier, key is template field name and
     *                      value is multiplier.
     */
    private void processMultipleResource(@Nonnull Builder templateInfo,
                                         List<ResourceApiDTO> resourceApiDTOS,
                                         @Nonnull ResourcesCategoryName categoryName,
                                         @Nonnull List<TemplateField> defaultTemplateFields,
                                         @Nonnull Map<String, Float> multiplierMap) {
        resourceApiDTOS.stream()
            .map(resourceApiDTO ->
                convertToTemplateResource(resourceApiDTO, categoryName,
                    Optional.ofNullable(resourceApiDTO.getType()), defaultTemplateFields, multiplierMap))
            .forEach(resource -> templateInfo.addResources(resource));
    }

    /**
     * Convert {@link ResourceApiDTO} to list of {@link TemplateField}. And also we need to add
     * defaultTemplateFields to final template fields

     *
     * @param resourceApiDTO {@link ResourceApiDTO}.
     * @param categoryName represent which category need to create.
     * @param type represent which category type.
     * @param defaultTemplateFields all the template fields which contains default value.
     * @param multiplierMap Map of (stat name) -> multiplier to use when converting the stat to
     *                      a template resource.
     * @return {@link TemplateResource}
     */
    private TemplateResource convertToTemplateResource(@Nonnull ResourceApiDTO resourceApiDTO,
                                                       @Nonnull ResourcesCategoryName categoryName,
                                                       Optional<String> type,
                                                       @Nonnull List<TemplateField> defaultTemplateFields,
                                                       @Nonnull Map<String, Float> multiplierMap) {
        TemplateResource.Builder templateResourceBuilder = TemplateResource.newBuilder();
        if (resourceApiDTO.getStats() != null) {
            final List<TemplateField> templateFields = resourceApiDTO.getStats().stream()
                .map(stat -> {
                    TemplateField templateField = TemplateField.newBuilder()
                        .setName(stat.getName())
                        .setValue(String.valueOf(stat.getValue() *
                            multiplierMap.getOrDefault(stat.getName(), ONE)))
                        .build();
                    return templateField;
                })
                .collect(Collectors.toList());
            templateResourceBuilder.addAllFields(templateFields);
        }
        defaultTemplateFields.stream().forEach(templateResourceBuilder::addFields);
        ResourcesCategory.Builder resourcesCategory = ResourcesCategory.newBuilder()
            .setName(categoryName);
        type.ifPresent(resourcesCategory::setType);
        templateResourceBuilder.setCategory(resourcesCategory.build());
        return templateResourceBuilder.build();
    }

    /**
     * Get all template fields which related template spec field defined default value.
     *
     * @param templateSpec {@link TemplateSpec} object contains all template spec fields.
     * @param resourceApiDTO {@link ResourceApiDTO}.
     * @param name of resources category.
     * @param multiplierMap Map of (stat name) -> multiplier to use when converting the stat to
     *                      a template resource.
     * @return all template fields which belong to input category and also contains default value.
     */
    private List<TemplateField> getDefaultTemplateFields(@Nonnull TemplateSpec templateSpec,
                                                         List<ResourceApiDTO> resourceApiDTO,
                                                         @Nonnull ResourcesCategoryName name,
                                                         @Nonnull Map<String, Float> multiplierMap) {
        return templateSpec.getResourcesList().stream()
            .filter(templateSpecResource -> templateSpecResource.getCategory().getName().equals(name))
            .map(TemplateSpecResource::getFieldsList)
            .flatMap(List::stream)
            .filter(templateSpecField -> {
                if (resourceApiDTO == null) {
                    return true;
                } else {
                    Set<String> statName = resourceApiDTO.stream()
                        .map(ResourceApiDTO::getStats)
                        .flatMap(List::stream)
                        .map(StatApiDTO::getName)
                        .collect(Collectors.toSet());
                    return !statName.contains(templateSpecField.getName());
                }
            })
            .map(templateSpecField -> TemplateField.newBuilder()
                    .setName(templateSpecField.getName())
                    .setValue(String.valueOf(templateSpecField.getDefaultValue() *
                        multiplierMap.getOrDefault(templateSpecField.getName(), ONE)))
                    .build())
            .collect(Collectors.toList());
    }

    /**
     * If inputDTO doesn't have resourceApiDTOS, but it have default template fields need to add,
     * it will call this function to add default template fields.
     *
     * @param templateInfo {@link TemplateInfo.Builder} need to created.
     * @param name of resources category.
     * @param defaultTemplateFields all the template fields which contains default value.
     */
    private void addDefaultTemplateField(@Nonnull TemplateInfo.Builder templateInfo,
                                         @Nonnull ResourcesCategoryName name,
                                         @Nonnull List<TemplateField> defaultTemplateFields) {
        TemplateResource templateResource = TemplateResource.newBuilder()
            .setCategory(ResourcesCategory.newBuilder()
                .setName(name)
                .build())
            .addAllFields(defaultTemplateFields)
            .build();
        templateInfo.addResources(templateResource);
    }
}
