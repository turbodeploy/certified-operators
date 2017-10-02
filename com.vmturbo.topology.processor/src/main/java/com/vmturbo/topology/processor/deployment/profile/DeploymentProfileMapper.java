package com.vmturbo.topology.processor.deployment.profile;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.plan.DeploymentProfileDTO.DeploymentProfileInfo;
import com.vmturbo.common.protobuf.plan.DeploymentProfileDTO.DeploymentProfileInfo.DeploymentProfileContextData;
import com.vmturbo.common.protobuf.plan.DeploymentProfileDTO.DeploymentProfileInfo.Scope;
import com.vmturbo.common.protobuf.plan.DeploymentProfileDTO.DeploymentProfileInfo.ScopeAccessType;
import com.vmturbo.platform.common.dto.ProfileDTO.DeploymentProfileDTO;
import com.vmturbo.topology.processor.entity.EntityStore;

/**
 * A Mapper class for convert SDK discovered deployment profile to XL model.
 */
public class DeploymentProfileMapper {

    private static final Logger logger = LogManager.getLogger();

    public static DeploymentProfileInfo convertToDeploymentProfile(@Nonnull long targetId,
                                                                   @Nonnull EntityStore entityStore,
                                                                   @Nonnull DeploymentProfileDTO deploymentProfileDTO) {
        DeploymentProfileInfo.Builder builder = DeploymentProfileInfo.newBuilder()
            .setName(deploymentProfileDTO.getProfileName())
            .setProbeDeploymentProfileId(deploymentProfileDTO.getId())
            .setDiscovered(true);

        deploymentProfileDTO.getContextDataList().stream()
            .forEach(context -> {
                DeploymentProfileContextData contextData = DeploymentProfileContextData.newBuilder()
                    .setKey(context.getContextKey())
                    .setValue(context.getContextValue())
                    .build();
                builder.addContextData(contextData);
            });

        addScopeIds(targetId, builder, entityStore, deploymentProfileDTO.getRelatedScopeIdList(),
            deploymentProfileDTO.getAccessibleScopeIdList());
        return builder.build();
    }

    private static void addScopeIds(long targetId,
                             DeploymentProfileInfo.Builder builder,
                             EntityStore entityStore,
                             List<String> relatedScopeIds,
                             List<String> accessibleScopeIds) {
        final Optional<Map<String, Long>> targetEntityMapOpt = entityStore.getTargetEntityIdMap(targetId);
        if (!targetEntityMapOpt.isPresent()) {
            logger.warn("No entity ID map available for target {}", targetId);
            return;
        }
        Map<String, Long> targetEntityMap = targetEntityMapOpt.get();
        builder.addScopes(Scope.newBuilder()
            .addAllIds(relatedScopeIds.stream()
                .map(targetEntityMap::get)
                .collect(Collectors.toList()))
            .setScopeAccessType(ScopeAccessType.And));
        builder.addScopes(Scope.newBuilder()
            .addAllIds(accessibleScopeIds.stream()
                .map(targetEntityMap::get)
                .collect(Collectors.toList()))
            .setScopeAccessType(ScopeAccessType.Or));
    }
}
