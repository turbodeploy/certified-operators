package com.vmturbo.plan.orchestrator.templates;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.exception.DataAccessException;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.plan.TemplateDTO.CreateTemplateRequest;
import com.vmturbo.common.protobuf.plan.TemplateDTO.DeleteTemplateRequest;
import com.vmturbo.common.protobuf.plan.TemplateDTO.DeleteTemplatesByTargetRequest;
import com.vmturbo.common.protobuf.plan.TemplateDTO.EditTemplateRequest;
import com.vmturbo.common.protobuf.plan.TemplateDTO.GetTemplateRequest;
import com.vmturbo.common.protobuf.plan.TemplateDTO.GetTemplatesByIdsRequest;
import com.vmturbo.common.protobuf.plan.TemplateDTO.GetTemplatesByNameRequest;
import com.vmturbo.common.protobuf.plan.TemplateDTO.GetTemplatesByTypeRequest;
import com.vmturbo.common.protobuf.plan.TemplateDTO.GetTemplatesRequest;
import com.vmturbo.common.protobuf.plan.TemplateDTO.Template;
import com.vmturbo.common.protobuf.plan.TemplateServiceGrpc.TemplateServiceImplBase;
import com.vmturbo.plan.orchestrator.plan.NoSuchObjectException;
import com.vmturbo.plan.orchestrator.templates.exceptions.DuplicateTemplateException;
import com.vmturbo.plan.orchestrator.templates.exceptions.IllegalTemplateOperationException;

/**
 * gRPC implementation of Templates RPC service. Most of the logic is simply delegated to
 * {@link TemplatesDao}.
 */
public class TemplatesRpcService extends TemplateServiceImplBase {

    private final Logger logger = LogManager.getLogger();

    private final TemplatesDao templatesDao;

    public TemplatesRpcService(@Nonnull TemplatesDao templatesDao) {
        this.templatesDao = Objects.requireNonNull(templatesDao);
    }

    @Override
    public void deleteTemplatesByTarget(DeleteTemplatesByTargetRequest request,
                                          StreamObserver<Template> responseObserver) {
        if (!request.hasTargetId()) {
            logger.error("Missing target ID for delete templates.");
            responseObserver.onError(Status.INVALID_ARGUMENT
                .withDescription("Delete templates by target ID must have an target ID").asException());
            return;
        }
        try {
            final List<Template> deletedTemplates = templatesDao.deleteTemplateByTargetId(request.getTargetId());
            deletedTemplates.stream().forEach(responseObserver::onNext);
            responseObserver.onCompleted();
        } catch (DataAccessException e) {
            responseObserver.onError(Status.INTERNAL
                .withDescription("Failed to delete discovered templates by target " + request.getTargetId())
                .asException());
        }
    }

    @Override
    public void getTemplates(GetTemplatesRequest request,
                             StreamObserver<Template> responseObserver) {
        try {
            for (Template template : templatesDao.getAllTemplates()) {
                responseObserver.onNext(template);
            }
            responseObserver.onCompleted();
        } catch (DataAccessException e) {
            responseObserver.onError(Status.INTERNAL
                .withDescription("Failed to get all templates.")
                .asException());
        }
    }

    @Override
    public void getTemplate(GetTemplateRequest request,
                            StreamObserver<Template> responseObserver) {
        if (!request.hasTemplateId()) {
            logger.error("Missing template ID for get template.");
            responseObserver.onError(Status.INVALID_ARGUMENT
                .withDescription("Get template by template ID must have an template ID").asException());
            return;
        }
        try {
            Optional<Template> templateOptional = templatesDao.getTemplate(request.getTemplateId());
            if (templateOptional.isPresent()) {
                responseObserver.onNext(templateOptional.get());
                responseObserver.onCompleted();
            } else {
                responseObserver.onError(Status.NOT_FOUND
                    .withDescription("Template ID " + Long.toString(request.getTemplateId()) + " not found.")
                    .asException());
            }
        } catch (DataAccessException e) {
            responseObserver.onError(Status.INTERNAL
                .withDescription("Failed to get template " + request.getTemplateId() + ".")
                .asException());
        }
    }

    @Override
    public void createTemplate(CreateTemplateRequest request,
                               StreamObserver<Template> responseObserver) {
        try {
            final Template template = templatesDao.createTemplate(request.getTemplateInfo());
            responseObserver.onNext(template);
            responseObserver.onCompleted();
        } catch (DataAccessException e) {
            responseObserver.onError(Status.INTERNAL
                    .withDescription("Failed to create template.")
                    .asException());
        } catch (DuplicateTemplateException e) {
            responseObserver.onError(Status.ALREADY_EXISTS
                    .withDescription(e.getLocalizedMessage())
                    .asException());
        }
    }

    @Override
    public void editTemplate(EditTemplateRequest request,
                             StreamObserver<Template> responseObserver) {
        if (!request.hasTemplateId()) {
            logger.error("Missing template ID for edit templates.");
            responseObserver.onError(Status.INVALID_ARGUMENT
                .withDescription("Edit templates must have an template ID").asException());
            return;
        }
        try {
            final Template template = templatesDao.editTemplate(request.getTemplateId(),
                request.getTemplateInfo());
            responseObserver.onNext(template);
            responseObserver.onCompleted();
        } catch (NoSuchObjectException e) {
            responseObserver.onError(Status.NOT_FOUND
                .withDescription("Template ID " + request.getTemplateId() + " not found.")
                .asException());
        } catch (DataAccessException e) {
            responseObserver.onError(Status.INTERNAL
                .withDescription("Failed to update template " + request.getTemplateId() + ".")
                .asException());
        } catch (IllegalTemplateOperationException e) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription(e.getLocalizedMessage())
                    .asException());
        } catch (DuplicateTemplateException e) {
            responseObserver.onError(Status.ALREADY_EXISTS
                    .withDescription(e.getLocalizedMessage())
                    .asException());
        }
    }

    @Override
    public void deleteTemplate(DeleteTemplateRequest request,
                               StreamObserver<Template> responseObserver) {
        if (!request.hasTemplateId()) {
            logger.error("Missing template ID for delete template.");
            responseObserver.onError(Status.INVALID_ARGUMENT
                .withDescription("Delete template must have an template ID").asException());
            return;
        }
        try {
            final Template template = templatesDao.deleteTemplateById(request.getTemplateId());
            responseObserver.onNext(template);
            responseObserver.onCompleted();
        } catch (NoSuchObjectException e) {
            responseObserver.onError(Status.NOT_FOUND
                .withDescription("Template ID " + request.getTemplateId() + " not found.")
                .asException());
        } catch (DataAccessException e) {
            responseObserver.onError(Status.INTERNAL
                .withDescription("Failed to delete template " + request.getTemplateId() + ".")
                .asException());
        } catch (IllegalTemplateOperationException e) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                .withDescription(e.getLocalizedMessage())
                .asException());
        }
    }

    @Override
    public void getTemplatesByType(GetTemplatesByTypeRequest request,
                                   StreamObserver<Template> responseObserver) {
        if (!request.hasEntityType()) {
            logger.error("Missing entity type for get template.");
            responseObserver.onError(Status.INVALID_ARGUMENT
                .withDescription("Get template by type must have an entity type").asException());
            return;
        }
        try {
            for (Template template : templatesDao.getTemplatesByEntityType(request.getEntityType())) {
                responseObserver.onNext(template);
            }
            responseObserver.onCompleted();
        } catch (DataAccessException e) {
            responseObserver.onError(Status.INTERNAL
                .withDescription("Failed to get template by entity type " + request.getEntityType() + ".")
                .asException());
        }
    }

    @Override
    public void getTemplatesByIds(GetTemplatesByIdsRequest request,
                                  StreamObserver<Template> responseObserver) {
        try {
            final Set<Long> templateIds = new HashSet<>(request.getTemplateIdsList());
            for (Template template : templatesDao.getTemplates(templateIds)) {
                responseObserver.onNext(template);
            }
            responseObserver.onCompleted();
        } catch (DataAccessException e) {
            logger.error("Failed to find templates.", e);
            responseObserver.onError(Status.INTERNAL
                .withDescription("Failed to get templates.")
                .asException());
        }
    }

    /**
     * Gets templates by template name.
     *
     * @param request The request that contains the template name.
     * @param responseObserver response observer
     */
    @Override
    public void getTemplatesByName(final GetTemplatesByNameRequest request,
                                   final StreamObserver<Template> responseObserver) {
        if (!request.hasTemplateName()) {
            logger.error("Failed to get templates because template name is missing in the request.");
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("Get template by name must have a template name in the request.")
                    .asException());
            return;
        }

        try {
            for (Template template : templatesDao.getTemplatesByName(request.getTemplateName())) {
                responseObserver.onNext(template);
            }
            responseObserver.onCompleted();
        } catch (DataAccessException e) {
            logger.error("Database error occurred while getting templates by name.", e);
            responseObserver.onError(Status.INTERNAL
                    .withDescription("Database error occurred while getting templates by name.")
                    .asException());
        }
    }
}
