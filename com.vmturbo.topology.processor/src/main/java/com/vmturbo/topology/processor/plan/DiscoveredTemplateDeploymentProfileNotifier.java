package com.vmturbo.topology.processor.plan;

import java.util.Collection;

import javax.annotation.Nonnull;

import com.vmturbo.communication.CommunicationException;
import com.vmturbo.platform.common.dto.ProfileDTO.DeploymentProfileDTO;
import com.vmturbo.platform.common.dto.ProfileDTO.EntityProfileDTO;

/**
 * Responsible for upload and delete discovered templates and deployment profile. We should upload
 * templates and deployment profiles together, because we need to update the relationship between
 * templates and deployment profile as well.
 */
public interface DiscoveredTemplateDeploymentProfileNotifier {
    /**
     * Store discovered templates and deployment profile in memory after discovery finish, then will
     * upload them before broadcast.
     *
     * @param targetId Id of target object.
     * @param profiles A list of {@link EntityProfileDTO} object.
     */
    void setTargetsTemplateDeploymentProfile(long targetId,
                                             @Nonnull Collection<EntityProfileDTO> profiles,
                                             @Nonnull Collection<DeploymentProfileDTO> deploymentProfile);

    /**
     * Upload discovered templates and deployment profile to plan component before broadcast.
     *
     * @throws CommunicationException if upload failure.
     */
    void sendTemplateDeploymentProfileData() throws CommunicationException;

    /**
     * Delete related discovered templates and deployment profile when some target is removed.
     *
     * @param targetId Id of target object.
     */
    void deleteTemplateDeploymentProfileByTarget(long targetId);
}
