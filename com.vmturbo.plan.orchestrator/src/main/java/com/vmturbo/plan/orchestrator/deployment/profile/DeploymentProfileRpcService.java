package com.vmturbo.plan.orchestrator.deployment.profile;

import java.util.Optional;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.exception.DataAccessException;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.plan.DeploymentProfileDTO.CreateDeploymentProfileRequest;
import com.vmturbo.common.protobuf.plan.DeploymentProfileDTO.DeleteDeploymentProfileRequest;
import com.vmturbo.common.protobuf.plan.DeploymentProfileDTO.DeploymentProfile;
import com.vmturbo.common.protobuf.plan.DeploymentProfileDTO.EditDeploymentProfileRequest;
import com.vmturbo.common.protobuf.plan.DeploymentProfileDTO.GetDeploymentProfileRequest;
import com.vmturbo.common.protobuf.plan.DeploymentProfileDTO.GetDeploymentProfilesRequest;
import com.vmturbo.common.protobuf.plan.DeploymentProfileServiceGrpc.DeploymentProfileServiceImplBase;
import com.vmturbo.plan.orchestrator.plan.DiscoveredNotSupportedOperationException;
import com.vmturbo.plan.orchestrator.plan.NoSuchObjectException;

public class DeploymentProfileRpcService extends DeploymentProfileServiceImplBase {

    private final Logger logger = LogManager.getLogger();

    private final DeploymentProfileDaoImpl deploymentProfileDao;

    public DeploymentProfileRpcService(@Nonnull DeploymentProfileDaoImpl deploymentProfileDao) {
        this.deploymentProfileDao = deploymentProfileDao;
    }

    @Override
    public void getDeploymentProfiles(GetDeploymentProfilesRequest request,
                                      StreamObserver<DeploymentProfile> responseObserver) {
        try {
            for (DeploymentProfile deploymentProfile : deploymentProfileDao.getAllDeploymentProfiles()) {
                responseObserver.onNext(deploymentProfile);
            }
            responseObserver.onCompleted();
        } catch (DataAccessException e) {
            responseObserver.onError(Status.INTERNAL
                .withDescription("Failed to get all deployment profiles.")
                .asException());
        }
    }

    @Override
    public void getDeploymentProfile(GetDeploymentProfileRequest request,
                                     StreamObserver<DeploymentProfile> responseObserver) {
        if (!request.hasDeploymentProfileId()) {
            logger.error("Missing deployment profile ID for get deployment profile.");
            responseObserver.onError(Status.INVALID_ARGUMENT
                .withDescription("Get deployment profile must provide an deployment profile ID")
                .asException());
            return;
        }
        try {
            Optional<DeploymentProfile> deploymentProfileOptional = deploymentProfileDao
                .getDeploymentProfile(request.getDeploymentProfileId());
            if (deploymentProfileOptional.isPresent()) {
                responseObserver.onNext(deploymentProfileOptional.get());
            }
            else {
                responseObserver.onNext(DeploymentProfile.newBuilder().build());
            }
            responseObserver.onCompleted();
        } catch (DataAccessException e) {
            responseObserver.onError(Status.INTERNAL
                .withDescription("Failed to get deployment profile " + request.getDeploymentProfileId() + ".")
                .asException());
        }
    }

    @Override
    public void createDeploymentProfile(CreateDeploymentProfileRequest request,
                                        StreamObserver<DeploymentProfile> responseObserver) {
        try {
            final DeploymentProfile deploymentProfile = deploymentProfileDao.createDeploymentProfile(request.getDeployInfo());
            responseObserver.onNext(deploymentProfile);
            responseObserver.onCompleted();
        } catch (DataAccessException e) {
            responseObserver.onError(Status.INTERNAL
                .withDescription("Failed to create deployment profile.")
                .asException());
        }
    }

    @Override
    public void editDeploymentProfile(EditDeploymentProfileRequest request,
                                      StreamObserver<DeploymentProfile> responseObserver) {
        if (!request.hasDeploymentProfileId()) {
            logger.error("Missing deployment profile ID for edit deployment profile.");
            responseObserver.onError(Status.INVALID_ARGUMENT
                .withDescription("Edit deployment profile must provide an deployment profile ID")
                .asException());
            return;
        }
        try {
            final DeploymentProfile deploymentProfile = deploymentProfileDao
                .editDeploymentProfile(request.getDeploymentProfileId(), request.getDeployInfo());
            responseObserver.onNext(deploymentProfile);
            responseObserver.onCompleted();
        } catch (NoSuchObjectException e) {
            responseObserver.onError(Status.NOT_FOUND
                .withDescription("Deployment profile ID " + request.getDeploymentProfileId() + " not found.")
                .asException());
        } catch (DiscoveredNotSupportedOperationException e) {
            responseObserver.onError(Status.PERMISSION_DENIED
                .withDescription("Edit discovered deployment profile is not allowed.")
                .asException());
        } catch (DataAccessException e) {
            responseObserver.onError(Status.INTERNAL
                .withDescription("Failed to edit deployment profile.")
                .asException());
        }
    }

    @Override
    public void deleteDeploymentProfile(DeleteDeploymentProfileRequest request,
                                        StreamObserver<DeploymentProfile> responseObserver) {
        if (!request.hasDeploymentProfileId()) {
            logger.error("Missing deployment profile ID for delete deployment profile.");
            responseObserver.onError(Status.INVALID_ARGUMENT
                .withDescription("Delete deployment profile must provide an deployment profile ID")
                .asException());
            return;
        }
        try {
            final DeploymentProfile deploymentProfile = deploymentProfileDao.deleteDeploymentProfile(request.getDeploymentProfileId());
            responseObserver.onNext(deploymentProfile);
            responseObserver.onCompleted();
        } catch (NoSuchObjectException e) {
            responseObserver.onError(Status.NOT_FOUND
                .withDescription("Deployment profile ID " + request.getDeploymentProfileId() + " not found.")
                .asException());
        } catch (DiscoveredNotSupportedOperationException e) {
            responseObserver.onError(Status.PERMISSION_DENIED
                .withDescription("Delete discovered deployment profile is not allowed.")
                .asException());
        } catch (DataAccessException e) {
            responseObserver.onError(Status.INTERNAL
                .withDescription("Failed to delete deployment profile.")
                .asException());
        }
    }
}
