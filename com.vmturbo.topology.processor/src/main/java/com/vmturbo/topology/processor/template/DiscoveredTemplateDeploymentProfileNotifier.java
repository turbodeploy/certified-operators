package com.vmturbo.topology.processor.template;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.communication.CommunicationException;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.ProfileDTO.DeploymentProfileDTO;
import com.vmturbo.platform.common.dto.ProfileDTO.EntityProfileDTO;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.processor.template.DiscoveredTemplateDeploymentProfileUploader.UploadException;

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
     * @param deploymentProfile A collection of {@link DeploymentProfileDTO} objects
     * @param discoveredEntities A list of {@link EntityDTO} discovered for this target. We will use
     *                           this to fill-in-the-blanks on info not in the profile, such as
     *                           estimating CPU speed for a VM profile, where speed is not explicitly
     *                           specified in the template.
     */
    void recordTemplateDeploymentInfo(long targetId,
                                      @Nonnull Collection<EntityProfileDTO> profiles,
                                      @Nonnull Collection<DeploymentProfileDTO> deploymentProfile,
                                      @Nonnull List<EntityDTO> discoveredEntities);

    /**
     * Upload discovered templates and deployment profile to plan component before broadcast.
     *
     * @throws UploadException if upload failure.
     * @throws InterruptedException if thread interrupted during upload.
     */
    void sendTemplateDeploymentProfileData() throws UploadException, InterruptedException;

    /**
     * Delete related discovered templates and deployment profile when some target is removed.
     *
     * @param targetId Id of target object.
     */
    void deleteTemplateDeploymentProfileByTarget(long targetId);

    /**
     * Get the generated profile oid by the external string id.
     * This information will only be available upon successful completion of sendTemplateDeploymentProfileData().
     *
     * @param targetId target identifier
     * @param profileVendorId profile external identifier
     * @return null if not found
     */
    @Nullable
    Long getProfileId(long targetId, @Nonnull String profileVendorId);

    /**
     * Get the generated deployment profile oid by the external string id.
     * This information will only be available upon successful completion of sendTemplateDeploymentProfileData().
     *
     * @param targetId target identifier
     * @param deploymentProfileVendorId deployment profile external identifier
     * @return null if not found
     */
    @Nullable
    Long getDeploymentProfileId(long targetId, @Nonnull String deploymentProfileVendorId);

    /**
     * Fix the topology references to template identifiers, once the latter have been resolved.
     *
     * @param topology entities to be adjusted
     */
    void patchTopology(@Nonnull Map<Long, TopologyEntity.Builder> topology);
}
