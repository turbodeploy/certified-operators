package com.vmturbo.api.component.external.api.service;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.annotation.Nonnull;

import org.apache.commons.lang.NotImplementedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.validation.Errors;

import com.google.common.collect.Lists;

import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;

import com.vmturbo.api.component.external.api.mapper.TemplateMapper;
import com.vmturbo.api.component.external.api.util.ApiUtils;
import com.vmturbo.api.component.external.api.util.TemplatesUtils;
import com.vmturbo.api.dto.template.CpuModelApiDTO;
import com.vmturbo.api.dto.template.ResourceApiDTO;
import com.vmturbo.api.dto.template.TemplateApiDTO;
import com.vmturbo.api.dto.template.TemplateApiInputDTO;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.api.serviceinterfaces.ITemplatesService;
import com.vmturbo.api.utils.ParamStrings;
import com.vmturbo.common.protobuf.plan.TemplateDTO.CreateTemplateRequest;
import com.vmturbo.common.protobuf.plan.TemplateDTO.DeleteTemplateRequest;
import com.vmturbo.common.protobuf.plan.TemplateDTO.EditTemplateRequest;
import com.vmturbo.common.protobuf.plan.TemplateDTO.GetTemplateRequest;
import com.vmturbo.common.protobuf.plan.TemplateDTO.GetTemplateSpecByEntityTypeRequest;
import com.vmturbo.common.protobuf.plan.TemplateDTO.GetTemplateSpecRequest;
import com.vmturbo.common.protobuf.plan.TemplateDTO.GetTemplateSpecsRequest;
import com.vmturbo.common.protobuf.plan.TemplateDTO.GetTemplatesByTypeRequest;
import com.vmturbo.common.protobuf.plan.TemplateDTO.GetTemplatesRequest;
import com.vmturbo.common.protobuf.plan.TemplateDTO.Template;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateInfo;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateSpec;
import com.vmturbo.common.protobuf.plan.TemplateServiceGrpc.TemplateServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.TemplateSpecServiceGrpc.TemplateSpecServiceBlockingStub;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Service implementation for /templates endpoint
 **/
public class TemplatesService implements ITemplatesService {

    private final Logger logger = LogManager.getLogger();

    private final TemplateServiceBlockingStub templateService;

    private final TemplateSpecServiceBlockingStub templateSpecService;

    private final TemplateMapper templateMapper;

    public TemplatesService(@Nonnull final TemplateServiceBlockingStub templateService,
                            @Nonnull final TemplateMapper templateMapper,
                            @Nonnull final TemplateSpecServiceBlockingStub templateSpecService) {
        this.templateService = templateService;
        this.templateMapper = templateMapper;
        this.templateSpecService = templateSpecService;
    }

    /**
     * Get all templates including user defined and probe discovered templates.
     *
     * @return list of {@link TemplateApiDTO}.
     * @throws Exception
     */
    @Override
    public List<TemplateApiDTO> getTemplates() throws Exception {
        GetTemplatesRequest templateRequest = GetTemplatesRequest.getDefaultInstance();
        Iterable<Template> templates = () -> templateService.getTemplates(templateRequest);
        GetTemplateSpecsRequest templateSpecRequest = GetTemplateSpecsRequest.getDefaultInstance();
        Iterable<TemplateSpec> templateSpecs = () -> templateSpecService.getTemplateSpecs(templateSpecRequest);
        Map<Long, TemplateSpec> templateSpecMap = StreamSupport.stream(templateSpecs.spliterator(), false)
            .collect(Collectors.toMap(entry -> entry.getId(), Function.identity()));
        return StreamSupport.stream(templates.spliterator(), false)
            .map(template -> templateMapper.mapToTemplateApiDTO(template,
                templateSpecMap.get(template.getTemplateInfo().getTemplateSpecId())))
            .collect(Collectors.toList());
    }

    /**
     * Get templates by template id or entity type.
     *
     * @param uuidOrType id of template or entity type.
     * @return list of {@link TemplateApiDTO}.
     * @throws Exception
     */
    @Override
    public List<TemplateApiDTO> getTemplate(String uuidOrType) throws Exception {
        try {
            Optional<Integer> entityType = getEntityType(uuidOrType.toLowerCase());
            if (entityType.isPresent()) {
                // call template rpc service to get templates and then convert to TemplateApiDTO
                GetTemplatesByTypeRequest request = GetTemplatesByTypeRequest.newBuilder()
                    .setEntityType(entityType.get())
                    .build();
                Iterable<Template> templates = () -> templateService.getTemplatesByType(request);
                GetTemplateSpecByEntityTypeRequest templateSpecRequest = GetTemplateSpecByEntityTypeRequest.newBuilder()
                    .setEntityType(entityType.get())
                    .build();
                TemplateSpec templateSpec = templateSpecService.getTemplateSpecByEntityType(templateSpecRequest);
                return StreamSupport.stream(templates.spliterator(), false)
                    .map(template -> templateMapper.mapToTemplateApiDTO(template, templateSpec))
                    .collect(Collectors.toList());
            }
            else {
                // if it is not entity type, then it will be uuid.
                final Template template = getTemplateById(Long.valueOf(uuidOrType));
                TemplateSpec templateSpec = getTemplateSpecById(template.getTemplateInfo().getTemplateSpecId());
                return Lists.newArrayList(templateMapper.mapToTemplateApiDTO(template, templateSpec));
            }
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode().equals(Code.NOT_FOUND)) {
                throw new UnknownObjectException(e.getStatus().getDescription());
            }
            else {
                throw e;
            }
        }
    }

    /**
     * Create a new user defined template.
     *
     * @param inputDto input template information {@link TemplateApiInputDTO}.
     * @return a {@link TemplateApiDTO}.
     * @throws Exception
     */
    @Override
    public TemplateApiDTO addTemplate(TemplateApiInputDTO inputDto) throws Exception {
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
        return templateMapper.mapToTemplateApiDTO(template, templateSpec);
    }

    /**
     * Update a existing template, if no such template, throw unknown object exception.
     *
     * @param uuid id of the template need to update.
     * @param inputDto input template information {@link TemplateApiInputDTO}.
     * @return a updated template {@link TemplateApiDTO}
     * @throws Exception
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
            return templateMapper.mapToTemplateApiDTO(template, templateSpec);
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
     * @throws Exception
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
     * @param o contains all input template information.
     * @param errors
     */
    @Override
    public void validateInput(Object o, Errors errors) {
        final StringBuilder errorMsgBuilder = new StringBuilder();
        final TemplateApiInputDTO dto = (TemplateApiInputDTO)o;
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
    public List<CpuModelApiDTO> getCpuList() throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    private String validateStatNames(ResourceApiDTO resource, Set<String> allowedStats,
                                     String resourceName) {
        final StringBuilder msgBuilder = new StringBuilder();
        if (resource.getStats() != null) {
            resource.getStats().stream()
                .map(StatApiDTO::getName)
                .filter(name -> !allowedStats.contains(name))
                .collect(Collectors.joining(",", "The following stat names in " + resourceName
                    + " are not allowed: ", "."));
        }
        return msgBuilder.toString();
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
        if(uuidOrType.equals(ParamStrings.VIRTUAL_MACHINE)) {
            return Optional.of(EntityType.VIRTUAL_MACHINE.getNumber());
        }
        else if (uuidOrType.equals(ParamStrings.PHYSICAL_MACHINE)) {
            return Optional.of(EntityType.PHYSICAL_MACHINE.getNumber());
        }
        else if (uuidOrType.equals(ParamStrings.STORAGE)) {
            return Optional.of(EntityType.STORAGE.getNumber());
        }
        else if (isLongType(uuidOrType)) {
            return Optional.empty();
        }
        else {
            logger.error("Entity type {} is not supported yet.", uuidOrType);
            throw new NotImplementedException(uuidOrType + " type is not supported yet.");
        }
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
        return templateService.getTemplate(templateRequest);
    }

    private TemplateSpec getTemplateSpecById(long id) {
        final GetTemplateSpecRequest templateSpecRequest = GetTemplateSpecRequest.newBuilder()
            .setId(id)
            .build();
        return templateSpecService.getTemplateSpec(templateSpecRequest);
    }
}

