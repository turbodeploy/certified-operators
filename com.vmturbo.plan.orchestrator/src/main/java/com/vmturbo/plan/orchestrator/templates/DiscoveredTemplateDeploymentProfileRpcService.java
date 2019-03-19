package com.vmturbo.plan.orchestrator.templates;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

import org.jooq.exception.DataAccessException;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.plan.DeploymentProfileDTO.DeploymentProfileInfo;
import com.vmturbo.common.protobuf.plan.DeploymentProfileDTO.EntityProfileToDeploymentProfile;
import com.vmturbo.common.protobuf.plan.DeploymentProfileDTO.SetDiscoveredTemplateDeploymentProfileRequest;
import com.vmturbo.common.protobuf.plan.DeploymentProfileDTO.SetDiscoveredTemplateDeploymentProfileResponse;
import com.vmturbo.common.protobuf.plan.DeploymentProfileDTO.SetTargetDiscoveredTemplateDeploymentProfileRequest;
import com.vmturbo.common.protobuf.plan.DiscoveredTemplateDeploymentProfileServiceGrpc.DiscoveredTemplateDeploymentProfileServiceImplBase;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateInfo;
import com.vmturbo.plan.orchestrator.templates.DiscoveredTemplateDeploymentProfileDaoImpl.TemplateInfoToDeploymentProfileMap;
import com.vmturbo.plan.orchestrator.templates.exceptions.NoMatchingTemplateSpecException;

public class DiscoveredTemplateDeploymentProfileRpcService extends DiscoveredTemplateDeploymentProfileServiceImplBase {

    private final TemplateSpecParser templateSpecParser;

    private final DiscoveredTemplateDeploymentProfileDaoImpl templateDeploymentProfileDao;

    public DiscoveredTemplateDeploymentProfileRpcService(@Nonnull TemplateSpecParser templateSpecParser,
                                                         @Nonnull DiscoveredTemplateDeploymentProfileDaoImpl templateDeploymentProfileDao) {
        this.templateSpecParser = templateSpecParser;
        this.templateDeploymentProfileDao = templateDeploymentProfileDao;
    }

    @Override
    public void setDiscoveredTemplateDeploymentProfile(
        SetDiscoveredTemplateDeploymentProfileRequest request,
        StreamObserver<SetDiscoveredTemplateDeploymentProfileResponse> responseObserver) {
        try {
            final Map<Long, TemplateInfoToDeploymentProfileMap> targetProfileMap = new HashMap<>();
            final Map<Long, List<DeploymentProfileInfo>> orphanedDeploymentProfile = new HashMap<>();
            for (SetTargetDiscoveredTemplateDeploymentProfileRequest targetRequest : request.getTargetRequestList()) {
                final TemplateInfoToDeploymentProfileMap templateInfoSetMap = new TemplateInfoToDeploymentProfileMap();
                for (EntityProfileToDeploymentProfile profile : targetRequest.getEntityProfileToDeploymentProfileList()) {
                    final TemplateInfo templateInfo = TemplatesMapper.createTemplateInfo(profile.getEntityProfile(),
                        templateSpecParser.getTemplateSpecMap());
                    templateInfoSetMap.put(templateInfo, profile.getDeploymentProfileList());
                }
                targetProfileMap.put(targetRequest.getTargetId(), templateInfoSetMap);
                orphanedDeploymentProfile.put(targetRequest.getTargetId(), targetRequest.getDeploymentProfileWithoutTemplatesList());
            }
            templateDeploymentProfileDao.setDiscoveredTemplateDeploymentProfile(targetProfileMap, orphanedDeploymentProfile);
            responseObserver.onNext(SetDiscoveredTemplateDeploymentProfileResponse.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (DataAccessException e) {
            responseObserver.onError(Status.ABORTED
                .withDescription("Failed to update discovered templates and deployment profile")
                .asException());
        } catch (NoMatchingTemplateSpecException e) {
            responseObserver.onError(Status.ABORTED
                .withDescription("Failed to find match template spec")
                .asException());
        }
    }
}
