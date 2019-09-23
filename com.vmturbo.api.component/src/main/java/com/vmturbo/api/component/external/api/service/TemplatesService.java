package com.vmturbo.api.component.external.api.service;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;

import org.apache.commons.lang.NotImplementedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.validation.Errors;

import com.vmturbo.api.component.external.api.mapper.CpuInfoMapper;
import com.vmturbo.api.component.external.api.mapper.TemplateMapper;
import com.vmturbo.api.component.external.api.util.ApiUtils;
import com.vmturbo.api.component.external.api.util.TemplatesUtils;
import com.vmturbo.api.dto.businessunit.EntityPriceDTO;
import com.vmturbo.api.dto.deploymentprofile.DeploymentProfileApiDTO;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.template.CpuModelApiDTO;
import com.vmturbo.api.dto.template.ResourceApiDTO;
import com.vmturbo.api.dto.template.TemplateApiDTO;
import com.vmturbo.api.dto.template.TemplateApiInputDTO;
import com.vmturbo.api.exceptions.UnauthorizedObjectException;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.api.serviceinterfaces.ITemplatesService;
import com.vmturbo.api.utils.ParamStrings;
import com.vmturbo.common.protobuf.cpucapacity.CpuCapacity.CpuModelListRequest;
import com.vmturbo.common.protobuf.cpucapacity.CpuCapacity.CpuModelListResponse;
import com.vmturbo.common.protobuf.cpucapacity.CpuCapacityServiceGrpc.CpuCapacityServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.TemplateDTO.CreateTemplateRequest;
import com.vmturbo.common.protobuf.plan.TemplateDTO.DeleteTemplateRequest;
import com.vmturbo.common.protobuf.plan.TemplateDTO.EditTemplateRequest;
import com.vmturbo.common.protobuf.plan.TemplateDTO.GetTemplateRequest;
import com.vmturbo.common.protobuf.plan.TemplateDTO.GetTemplateSpecByEntityTypeRequest;
import com.vmturbo.common.protobuf.plan.TemplateDTO.GetTemplateSpecRequest;
import com.vmturbo.common.protobuf.plan.TemplateDTO.GetTemplatesRequest;
import com.vmturbo.common.protobuf.plan.TemplateDTO.Template;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateInfo;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateSpec;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplatesFilter;
import com.vmturbo.common.protobuf.plan.TemplateServiceGrpc.TemplateServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.TemplateSpecServiceGrpc.TemplateSpecServiceBlockingStub;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Service implementation for /templates endpoint.
 **/
public class TemplatesService implements ITemplatesService {

    private final Logger logger = LogManager.getLogger();

    private final TemplateServiceBlockingStub templateService;

    private final TemplateSpecServiceBlockingStub templateSpecService;

    /**
     * The service to fetch the list of known CPU Models and their performance scaling values.
     */
    private final CpuCapacityServiceBlockingStub cpuCapacityService;

    /**
     * Maps between internal protobuf format of Templates an the external API
     * TemplateApiDTO and TemplateApiInputDTO structures.
     */
    private final TemplateMapper templateMapper;

    /**
     * Mapper from internal CPUInfo protobuf to External API  CpuModelApiDTO.
     */
    private final CpuInfoMapper cpuInfoMapper;

    private final TemplatesUtils templatesUtils;


    TemplatesService(@Nonnull final TemplateServiceBlockingStub templateService,
                    @Nonnull final TemplateMapper templateMapper,
                    @Nonnull final TemplateSpecServiceBlockingStub templateSpecService,
                    @Nonnull final CpuCapacityServiceBlockingStub cpuCapacityService,
                    @Nonnull final CpuInfoMapper cpuInfoMapper,
                    @Nonnull final TemplatesUtils templatesUtils,
                    final int cpuCatalogLifeHours) {
        this.templateService = Objects.requireNonNull(templateService);
        this.templateMapper = Objects.requireNonNull(templateMapper);
        this.templateSpecService = Objects.requireNonNull(templateSpecService);
        this.cpuCapacityService = Objects.requireNonNull(cpuCapacityService);
        this.cpuInfoMapper = Objects.requireNonNull(cpuInfoMapper);
        this.templatesUtils = Objects.requireNonNull(templatesUtils);
    }

    /**
     * Get all templates including user defined and probe discovered templates.
     *
     * @return list of {@link TemplateApiDTO}.
     */
    @Override
    public List<TemplateApiDTO> getTemplates() {
        return templatesUtils.getTemplates(GetTemplatesRequest.getDefaultInstance())
            .collect(Collectors.toList());
    }

    /**
     * Get templates by template id, entity type, or template name.
     *
     * @param uuidOrType id of template or entity type.
     * @return list of {@link TemplateApiDTO}.
     * @throws Exception If something goes wrong.
     */
    @Override
    public List<TemplateApiDTO> getTemplate(String uuidOrType) throws Exception {
        Optional<Integer> entityType = getEntityType(uuidOrType.toLowerCase());
        final List<TemplateApiDTO> retTemplates;
        if (entityType.isPresent()) {
            // call template rpc service to get templates and then convert to TemplateApiDTO
            final GetTemplatesRequest request = GetTemplatesRequest.newBuilder()
                .setFilter(TemplatesFilter.newBuilder()
                    .setEntityType(entityType.get()))
                .build();
            retTemplates = templatesUtils.getTemplates(request)
                .collect(Collectors.toList());
        } else if (isLongType(uuidOrType)) {
            // The search key is not a valid entity type.  Try to search by UUID if it is a number.
            final GetTemplatesRequest request = GetTemplatesRequest.newBuilder()
                .setFilter(TemplatesFilter.newBuilder()
                    .addTemplateIds(Long.valueOf(uuidOrType)))
                .build();
            retTemplates = Collections.singletonList(templatesUtils.getTemplates(request)
                // We can assume there will only be one with the ID.
                .findFirst()
                .orElseThrow(() -> new UnknownObjectException("The search criterion is not a valid template " +
                    "UUID, template name or entity type: " + uuidOrType)));
        } else {
            // Try to search by template name.
            final GetTemplatesRequest request = GetTemplatesRequest.newBuilder()
                .setFilter(TemplatesFilter.newBuilder()
                    .addTemplateName(uuidOrType))
                .build();
            retTemplates = templatesUtils.getTemplates(request)
                .collect(Collectors.toList());
        }
        return retTemplates;
    }

    /**
     * u
     * Create a new user defined template.
     *
     * @param inputDto input template information {@link TemplateApiInputDTO}.
     * @return a {@link TemplateApiDTO}.
     * @throws UnauthorizedObjectException If the input template format is wrong.
     */
    @Override
    public TemplateApiDTO addTemplate(TemplateApiInputDTO inputDto) throws UnauthorizedObjectException {
        int entityType = convertClassNameToEntityType(inputDto.getClassName());
        GetTemplateSpecByEntityTypeRequest templateSpecRequest = GetTemplateSpecByEntityTypeRequest.newBuilder()
            .setEntityType(entityType)
            .build();
        TemplateSpec templateSpec = templateSpecService.getTemplateSpecByEntityType(templateSpecRequest);
        final TemplateInfo templateInfo = templateMapper.mapToTemplateInfo(inputDto, templateSpec, entityType);
        final CreateTemplateRequest request = CreateTemplateRequest.newBuilder()
            .setTemplateInfo(templateInfo)
            .build();
        final Template template = templateService.createTemplate(request);
        return templateMapper.mapToTemplateApiDTO(template, templateSpec, Collections.emptyList());
    }

    /**
     * Update a existing template, if no such template, throw unknown object exception.
     *
     * @param uuid     id of the template need to update.
     * @param inputDto input template information {@link TemplateApiInputDTO}.
     * @return a updated template {@link TemplateApiDTO}
     * @throws Exception If something goes wrong when executing the call.
     */
    @Override
    public TemplateApiDTO editTemplate(String uuid, TemplateApiInputDTO inputDto) throws Exception {
        try {
            final Template retrievedTemplate = getTemplateById(Long.valueOf(uuid));
            final TemplateSpec templateSpec = getTemplateSpecById(retrievedTemplate.getTemplateInfo().getTemplateSpecId());
            int entityType = convertClassNameToEntityType(inputDto.getClassName());
            final TemplateInfo templateInfo = templateMapper.mapToTemplateInfo(inputDto, templateSpec, entityType);
            final EditTemplateRequest request = EditTemplateRequest.newBuilder()
                .setTemplateInfo(templateInfo)
                .setTemplateId(Long.parseLong(uuid))
                .build();
            final Template template = templateService.editTemplate(request);
            return templateMapper.mapToTemplateApiDTO(template, templateSpec, Collections.emptyList());
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode().equals(Code.NOT_FOUND)) {
                throw new UnknownObjectException(e.getStatus().getDescription());
            } else {
                throw e;
            }
        }
    }

    /**
     * Delete a existing template, if no such template, throw unknown object exception.
     *
     * @param uuid id of the template need to delete.
     * @return boolean represent if delete succeeded.
     * @throws Exception If something goes wrong executing the call.
     */
    @Override
    public Boolean deleteTemplate(String uuid) throws Exception {
        try {
            final DeleteTemplateRequest request = DeleteTemplateRequest.newBuilder()
                .setTemplateId(Long.parseLong(uuid))
                .build();
            templateService.deleteTemplate(request);
            return true;
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode().equals(Code.NOT_FOUND)) {
                throw new UnknownObjectException(e.getStatus().getDescription());
            } else {
                throw e;
            }
        }
    }

    /**
     * Validate input template information to make sure all the fields are allowed.
     *
     * @param dto    contains all input template information.
     * @param errors Validation errors. If there are errors with this template, they will be added to
     *               this object.
     */
    @Override
    public void validateInput(final TemplateApiInputDTO dto, Errors errors) {
        final StringBuilder errorMsgBuilder = new StringBuilder();
        if (dto.getClassName() == null) {
            errorMsgBuilder.append(". className is a required field");
        }
        if (dto.getDisplayName() == null) {
            errorMsgBuilder.append(". displayName is a required field");
        }
        if (dto.getComputeResources() != null) {
            for (ResourceApiDTO resource : dto.getComputeResources()) {
                String msg = validateStatNames(resource, TemplatesUtils.allowedComputeStats,
                    "computeResources");
                errorMsgBuilder.append(msg.equals("") ? "" : ". " + msg);
            }
        }
        if (dto.getStorageResources() != null) {
            for (ResourceApiDTO resource : dto.getStorageResources()) {
                String msg = validateStatNames(resource, TemplatesUtils.allowedStorageStats,
                    "storageResources");
                errorMsgBuilder.append(msg.equals("") ? "" : ". " + msg);
                msg = validateTypesNames(resource.getType(), TemplatesUtils.allowedStorageTypes,
                    "storageResources");
                errorMsgBuilder.append(msg.equals("") ? "" : ". " + msg);
            }
        }
        final String errorMsg = errorMsgBuilder.toString();
        if (!errorMsg.isEmpty()) {
            throw new IllegalArgumentException(errorMsg.substring(2));
        }
    }

    @Override
    public List<CpuModelApiDTO> getCpuList() {
        final CpuModelListResponse cpuModelListResponse = cpuCapacityService.getCpuModelList(
            CpuModelListRequest.getDefaultInstance());
        return cpuModelListResponse.getCpuInfoList().stream()
            .map(cpuInfoMapper::convertCpuDTO)
            .collect(Collectors.toList());
    }

    @Override
    public List<DeploymentProfileApiDTO> getDeploymentProfiles(String uuid) {
        throw ApiUtils.notImplementedInXL();
    }

    private String validateStatNames(ResourceApiDTO resource, Set<String> allowedStats,
                                     String resourceName) {
        if (resource.getStats() != null) {
            return resource.getStats().stream()
                .map(StatApiDTO::getName)
                .filter(name -> !allowedStats.contains(name))
                .collect(Collectors.joining(",", "The following stat names in " + resourceName
                    + " are not allowed: ", "."));
        } else {
            return "";
        }
    }

    private String validateTypesNames(String typeName, Set<String> allowedTypes,
                                      String resourceName) {
        if (typeName != null && !allowedTypes.contains(typeName)) {
            return "The following type names in " + resourceName + " is not allowed: " + typeName;
        }
        return "";
    }

    /**
     * Try to parse uuidOrType parameter. If input uuidOrType is virtual machine, physical machine
     * or storage then return its entity type value, or if it is a uuid, then return empty.
     *
     * @param uuidOrType could be a id or entity type string.
     * @return Optional object represent if uuidOrType is entity type or not.
     */
    private Optional<Integer> getEntityType(String uuidOrType) {
        if (uuidOrType.equals(ParamStrings.VIRTUAL_MACHINE)) {
            return Optional.of(EntityType.VIRTUAL_MACHINE.getNumber());
        } else if (uuidOrType.equals(ParamStrings.PHYSICAL_MACHINE)) {
            return Optional.of(EntityType.PHYSICAL_MACHINE.getNumber());
        } else if (uuidOrType.equals(ParamStrings.STORAGE)) {
            return Optional.of(EntityType.STORAGE.getNumber());
        }
        return Optional.empty();
    }

    private boolean isLongType(String type) {
        try {
            Long.parseLong(type);
        } catch (NumberFormatException e) {
            return false;
        }
        return true;
    }

    /**
     * Try to convert UI className to entity type. Note that className for template contains profile
     * suffix which need to removed in order to match with entity type.
     *
     * @param className UI passed value to represent type.
     * @return value of entity type.
     */
    private int convertClassNameToEntityType(String className) {
        String entityName = className.endsWith(TemplatesUtils.PROFILE) ?
            className.replace(TemplatesUtils.PROFILE, "") : className;
        switch (entityName.toLowerCase()) {
            case ParamStrings.VIRTUAL_MACHINE:
                return EntityType.VIRTUAL_MACHINE.getNumber();
            case ParamStrings.PHYSICAL_MACHINE:
                return EntityType.PHYSICAL_MACHINE.getNumber();
            case ParamStrings.STORAGE:
                return EntityType.STORAGE.getNumber();
            default:
                logger.error("Entity type {} is not supported yet.", entityName);
                throw new NotImplementedException(entityName + " type is not supported yet.");
        }
    }

    private Template getTemplateById(long id) {
        final GetTemplateRequest templateRequest = GetTemplateRequest.newBuilder()
            .setTemplateId(id)
            .build();
        return templateService.getTemplate(templateRequest).getTemplate();
    }

    private TemplateSpec getTemplateSpecById(long id) {
        final GetTemplateSpecRequest templateSpecRequest = GetTemplateSpecRequest.newBuilder()
            .setId(id)
            .build();
        return templateSpecService.getTemplateSpec(templateSpecRequest);
    }

    @Nonnull
    @Override
    public Set<String> getCloudTemplatesOses(@Nonnull String scopeUuid)
        throws UnknownObjectException {
        // TODO implement as soon as cloud templates are published in XL
        throw ApiUtils.notImplementedInXL();
    }

    @Nonnull
    @Override
    public List<EntityPriceDTO> getTemplatePrices(@Nonnull String entityUuid, @Nonnull String uuidTemplate, @Nonnull String dcUuid)
        throws UnknownObjectException {
        throw ApiUtils.notImplementedInXL();
    }

    @Nonnull
    @Override
    public List<ServiceEntityApiDTO> getTemplateEntities(@Nonnull String templateUuid) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }
}

