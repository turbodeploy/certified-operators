package com.vmturbo.plan.orchestrator.templates;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.Iterators;

import com.vmturbo.common.protobuf.plan.ReservationDTO;
import com.vmturbo.plan.orchestrator.reservation.ReservationDao;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.exception.DataAccessException;

import com.vmturbo.common.protobuf.plan.DeploymentProfileDTO.DeploymentProfile;
import com.vmturbo.common.protobuf.plan.TemplateDTO.CreateTemplateRequest;
import com.vmturbo.common.protobuf.plan.TemplateDTO.DeleteTemplateRequest;
import com.vmturbo.common.protobuf.plan.TemplateDTO.DeleteTemplatesByTargetRequest;
import com.vmturbo.common.protobuf.plan.TemplateDTO.EditTemplateRequest;
import com.vmturbo.common.protobuf.plan.TemplateDTO.GetHeadroomTemplateRequest;
import com.vmturbo.common.protobuf.plan.TemplateDTO.GetHeadroomTemplateResponse;
import com.vmturbo.common.protobuf.plan.TemplateDTO.GetTemplateRequest;
import com.vmturbo.common.protobuf.plan.TemplateDTO.GetTemplatesRequest;
import com.vmturbo.common.protobuf.plan.TemplateDTO.GetTemplatesResponse;
import com.vmturbo.common.protobuf.plan.TemplateDTO.SingleTemplateResponse;
import com.vmturbo.common.protobuf.plan.TemplateDTO.Template;
import com.vmturbo.common.protobuf.plan.TemplateDTO.UpdateHeadroomTemplateRequest;
import com.vmturbo.common.protobuf.plan.TemplateDTO.UpdateHeadroomTemplateResponse;
import com.vmturbo.common.protobuf.plan.TemplateServiceGrpc.TemplateServiceImplBase;
import com.vmturbo.plan.orchestrator.deployment.profile.DeploymentProfileDaoImpl;
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

    private final DeploymentProfileDaoImpl deploymentProfileDao;

    private final int templateChunkSize;

    private final ReservationDao reservationDao;

    TemplatesRpcService(@Nonnull final TemplatesDao templatesDao,
                        @Nonnull final DeploymentProfileDaoImpl deploymentProfileDao,
                        @Nonnull final ReservationDao reservationDao,
                        final int templateChunkSize) {
        this.templatesDao = Objects.requireNonNull(templatesDao);
        this.deploymentProfileDao = Objects.requireNonNull(deploymentProfileDao);
        this.reservationDao = Objects.requireNonNull(reservationDao);
        this.templateChunkSize = templateChunkSize;
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
                             StreamObserver<GetTemplatesResponse> responseObserver) {
        try {
            final Set<Template> templates = templatesDao.getFilteredTemplates(request.getFilter());
            final Map<Long, Set<DeploymentProfile>> profilesByTemplateId;
            if (request.getIncludeDeploymentProfiles()) {
                profilesByTemplateId = deploymentProfileDao.getDeploymentProfilesForTemplates(templates.stream()
                    .map(Template::getId)
                    .collect(Collectors.toSet()));
            } else {
                profilesByTemplateId = Collections.emptyMap();
            }

            Iterators.partition(templates.iterator(), templateChunkSize)
                .forEachRemaining(templateChunk -> {
                    GetTemplatesResponse.Builder response = GetTemplatesResponse.newBuilder();
                    templateChunk.forEach(template -> {
                        response.addTemplates(SingleTemplateResponse.newBuilder()
                            .setTemplate(template)
                            .addAllDeploymentProfile(
                                profilesByTemplateId.getOrDefault(template.getId(),
                                    Collections.emptySet())))
                            .build();
                    });
                    responseObserver.onNext(response.build());
                });
            responseObserver.onCompleted();
        } catch (DataAccessException e) {
            responseObserver.onError(Status.INTERNAL
                .withDescription("Failed to get all templates.")
                .asException());
        }
    }

    @Override
    public void getTemplate(GetTemplateRequest request,
                            StreamObserver<SingleTemplateResponse> responseObserver) {
        if (!request.hasTemplateId()) {
            logger.error("Missing template ID for get template.");
            responseObserver.onError(Status.INVALID_ARGUMENT
                .withDescription("Get template by template ID must have an template ID").asException());
            return;
        }
        try {
            Optional<Template> templateOptional = templatesDao.getTemplate(request.getTemplateId());
            if (templateOptional.isPresent()) {
                final Template template = templateOptional.get();
                final Set<DeploymentProfile> profiles =
                    deploymentProfileDao.getDeploymentProfilesForTemplates(
                            Collections.singleton(template.getId()))
                        .getOrDefault(template.getId(), Collections.emptySet());
                responseObserver.onNext(SingleTemplateResponse.newBuilder()
                    .setTemplate(templateOptional.get())
                    .addAllDeploymentProfile(profiles)
                    .build());
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

        if (templateUsedByReservation(request.getTemplateId())) {
            responseObserver.onError(Status.FAILED_PRECONDITION
                    .withDescription("Delete the reservations used by this template first.").asException());
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

    private boolean templateUsedByReservation(long templateId) {
        final Set<ReservationDTO.Reservation> reservationsByTemplates =
                reservationDao.getReservationsByTemplates(Collections.singleton(templateId));
        if (!reservationsByTemplates.isEmpty()) {
            logger.error("This template {} is being used by reservations {}.", templateId,
                    reservationsByTemplates.stream().map(ReservationDTO.Reservation::getName).collect(Collectors.toSet()));
            return true;
        }
        return false;
    }

    @Override
    public void getHeadroomTemplateForCluster(GetHeadroomTemplateRequest request,
                                  StreamObserver<GetHeadroomTemplateResponse> responseObserver) {
        if (!request.hasGroupId()) {
            final String errMsg = "Group ID is missing.";
            logger.error(errMsg);
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(errMsg).asRuntimeException());
            return;
        }

        try {
            Optional<Template> template =
                templatesDao.getClusterHeadroomTemplateForGroup(request.getGroupId());
            GetHeadroomTemplateResponse.Builder response = GetHeadroomTemplateResponse.newBuilder();
            template.ifPresent(response::setHeadroomTemplate);
            responseObserver.onNext(response.build());
            responseObserver.onCompleted();
        } catch (DataAccessException e) {
            logger.error("Failed to headroom because of data access exception: Group `{}`",
                request.getGroupId(), e);
            responseObserver.onError(Status.INTERNAL
                .withDescription(e.getLocalizedMessage()).asException());
        }
    }

    @Override
    public void updateHeadroomTemplateForCluster(UpdateHeadroomTemplateRequest request,
                                 StreamObserver<UpdateHeadroomTemplateResponse> responseObserver) {
        if (!request.hasGroupId() || !request.hasTemplateId()) {
            final String errMsg = "Group ID or cluster headroom template ID is missing.";
            logger.error(errMsg);
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(errMsg).asRuntimeException());
            return;
        }

        logger.info("Updating cluster headroom template ID for group `{}` with `{}`",
            request.getGroupId(), request.getTemplateId());

        try {
            templatesDao.setOrUpdateHeadroomTemplateForCluster(request.getGroupId(),
                request.getTemplateId());
            responseObserver.onNext(UpdateHeadroomTemplateResponse.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (DataAccessException e) {
            logger.error("Failed to update group because of data access exception: Group `{}`",
                request.getGroupId(), e);
            responseObserver.onError(Status.INTERNAL
                .withDescription(e.getLocalizedMessage()).asException());
        }
    }
}
