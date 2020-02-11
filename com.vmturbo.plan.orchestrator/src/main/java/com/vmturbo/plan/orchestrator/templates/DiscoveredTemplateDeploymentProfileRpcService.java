package com.vmturbo.plan.orchestrator.templates;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.exception.DataAccessException;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.plan.DeploymentProfileDTO.DeploymentProfileInfo;
import com.vmturbo.common.protobuf.plan.DeploymentProfileDTO.EntityProfileToDeploymentProfile;
import com.vmturbo.common.protobuf.plan.DeploymentProfileDTO.UpdateDiscoveredTemplateDeploymentProfileResponse;
import com.vmturbo.common.protobuf.plan.DeploymentProfileDTO.UpdateTargetDiscoveredTemplateDeploymentProfileRequest;
import com.vmturbo.common.protobuf.plan.DiscoveredTemplateDeploymentProfileServiceGrpc.DiscoveredTemplateDeploymentProfileServiceImplBase;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateInfo;
import com.vmturbo.plan.orchestrator.templates.DiscoveredTemplateDeploymentProfileDaoImpl.TemplateInfoToDeploymentProfileMap;
import com.vmturbo.plan.orchestrator.templates.exceptions.NoMatchingTemplateSpecException;

public class DiscoveredTemplateDeploymentProfileRpcService extends DiscoveredTemplateDeploymentProfileServiceImplBase {

    private final Logger logger = LogManager.getLogger();

    private final TemplateSpecParser templateSpecParser;

    private final DiscoveredTemplateDeploymentProfileDaoImpl templateDeploymentProfileDao;

    public DiscoveredTemplateDeploymentProfileRpcService(@Nonnull TemplateSpecParser templateSpecParser,
                                                         @Nonnull DiscoveredTemplateDeploymentProfileDaoImpl templateDeploymentProfileDao) {
        this.templateSpecParser = templateSpecParser;
        this.templateDeploymentProfileDao = templateDeploymentProfileDao;
    }

    @Override
    public StreamObserver<UpdateTargetDiscoveredTemplateDeploymentProfileRequest> updateDiscoveredTemplateDeploymentProfile(

        StreamObserver<UpdateDiscoveredTemplateDeploymentProfileResponse> responseObserver) {

        return new StreamObserver<UpdateTargetDiscoveredTemplateDeploymentProfileRequest>() {

            Set<UpdateTargetDiscoveredTemplateDeploymentProfileRequest> requestSet = new HashSet<>();

            @Override
            public void onNext(final UpdateTargetDiscoveredTemplateDeploymentProfileRequest request) {
                // Aggregate all received templates
                requestSet.add(request);
            }

            @Override
            public void onError(final Throwable throwable) {
                logger.error("Error uploading discovered templates and deployment profile {}.",
                    throwable.getMessage());
            }

            @Override
            public void onCompleted() {
                logger.info("Finished streaming deployment templates");
                try {
                    final Map<Long, TemplateInfoToDeploymentProfileMap> targetProfileMap = new HashMap<>();
                    final Map<Long, List<DeploymentProfileInfo>> orphanedDeploymentProfile = new HashMap<>();
                    for (UpdateTargetDiscoveredTemplateDeploymentProfileRequest targetRequest : requestSet) {
                        final TemplateInfoToDeploymentProfileMap templateInfoSetMap = new TemplateInfoToDeploymentProfileMap(targetRequest.getDataAvailable());
                        for (EntityProfileToDeploymentProfile profile : targetRequest.getEntityProfileToDeploymentProfileList()) {
                            try {
                                final TemplateInfo templateInfo = TemplatesMapper.createTemplateInfo(profile.getEntityProfile(),
                                    templateSpecParser.getTemplateSpecMap());
                                templateInfoSetMap.put(templateInfo, profile.getDeploymentProfileList());
                            } catch (NoMatchingTemplateSpecException e) {
                                logger.warn("Could not find template spec for discovered template {} with entity type {}",
                                    profile.getEntityProfile().getDisplayName(),
                                    profile.getEntityProfile().getEntityType());
                            }
                        }
                        targetProfileMap.put(targetRequest.getTargetId(), templateInfoSetMap);
                        orphanedDeploymentProfile.put(targetRequest.getTargetId(), targetRequest.getDeploymentProfileWithoutTemplatesList());
                    }
                    UpdateDiscoveredTemplateDeploymentProfileResponse response =
                        templateDeploymentProfileDao
                                   .setDiscoveredTemplateDeploymentProfile(targetProfileMap,
                                                                           orphanedDeploymentProfile);
                    responseObserver.onNext(response);
                    responseObserver.onCompleted();
                    requestSet.clear();
                } catch (DataAccessException e) {
                    responseObserver.onError(Status.ABORTED
                        .withDescription("Failed to update discovered templates and deployment profile")
                        .asException());
                }
            }
        };
    }
}
