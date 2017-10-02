package com.vmturbo.plan.orchestrator.deployment.profile;

import static com.vmturbo.plan.orchestrator.db.Tables.DEPLOYMENT_PROFILE;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.jooq.DSLContext;

import com.vmturbo.common.protobuf.plan.DeploymentProfileDTO;
import com.vmturbo.common.protobuf.plan.DeploymentProfileDTO.DeploymentProfileInfo;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.plan.orchestrator.db.tables.pojos.DeploymentProfile;
import com.vmturbo.plan.orchestrator.plan.DiscoveredNotSupportedOperationException;
import com.vmturbo.plan.orchestrator.plan.NoSuchObjectException;

/**
 * Implementation of deployment profiles using database connection. And it is only for generating
 * usr created deployment profile, not discovered ones.
 * {@link com.vmturbo.plan.orchestrator.templates.DiscoveredTemplateDeploymentProfileDaoImpl} class
 * is used for uploading discovered templates and deployment profiles.
 */
public class DeploymentProfileDaoImpl {

    private final DSLContext dsl;

    public DeploymentProfileDaoImpl(@Nonnull final DSLContext dsl) {
        this.dsl = dsl;
    }

    public Set<DeploymentProfileDTO.DeploymentProfile> getAllDeploymentProfiles() {
        final List<DeploymentProfile> allDeploymentProfiles = dsl.selectFrom(DEPLOYMENT_PROFILE)
            .fetch()
            .into(DeploymentProfile.class);
        return convertToProtoDeploymentProfileList(allDeploymentProfiles);
    }

    /**
     * Get one deployment profile by id.
     *
     * @param id of deployment profile.
     * @return Optional of deployment profile.
     */
    public Optional<DeploymentProfileDTO.DeploymentProfile> getDeploymentProfile(long id) {
        return getDeploymentProfile(dsl, id);
    }

    /**
     * Create a new deployment profile.
     *
     * @param deploymentProfileInfo contains new deployment profile need to created.
     * @return Created new deployment profile.
     */
    public DeploymentProfileDTO.DeploymentProfile createDeploymentProfile(@Nonnull DeploymentProfileInfo deploymentProfileInfo) {
        DeploymentProfile deploymentProfile = new DeploymentProfile(IdentityGenerator.next(), null, null,
            deploymentProfileInfo.getName(), deploymentProfileInfo);
        dsl.newRecord(DEPLOYMENT_PROFILE, deploymentProfile).store();
        return DeploymentProfileDTO.DeploymentProfile.newBuilder()
            .setId(deploymentProfile.getId())
            .setDeployInfo(deploymentProfile.getDeploymentProfileInfo())
            .build();
    }

    /**
     * Update a existing deployment profile.
     *
     * @param id of existing deployment profile.
     * @param deploymentProfileInfo contains new deployment profile need to updated.
     * @return new updated deployment profile.
     * @throws NoSuchObjectException
     */
    public DeploymentProfileDTO.DeploymentProfile editDeploymentProfile(long id,
                                                                         @Nonnull DeploymentProfileInfo deploymentProfileInfo)
        throws NoSuchObjectException, DiscoveredNotSupportedOperationException {
        DeploymentProfileDTO.DeploymentProfile deploymentProfile = getDeploymentProfile(dsl, id)
            .orElseThrow(() -> noSuchObjectException(id));
        if (deploymentProfile.hasTargetId()) {
            throw new DiscoveredNotSupportedOperationException("Edit discovered deployment profile " +
                "is not supported");
        }
        dsl.update(DEPLOYMENT_PROFILE)
            .set(DEPLOYMENT_PROFILE.NAME, deploymentProfileInfo.getName())
            .set(DEPLOYMENT_PROFILE.DEPLOYMENT_PROFILE_INFO, deploymentProfileInfo)
            .where(DEPLOYMENT_PROFILE.ID.eq(id))
            .execute();

        DeploymentProfileDTO.DeploymentProfile.Builder builder = DeploymentProfileDTO.DeploymentProfile
            .newBuilder()
            .setId(deploymentProfile.getId())
            .setDeployInfo(deploymentProfileInfo);
        if (deploymentProfile.hasTargetId()) {
            builder.setTargetId(deploymentProfile.getTargetId());
        }
        return builder.build();
    }

    /**
     * Deletes an existing deployment profile.
     *
     * @param id ID of the deployment profile to be deleted.
     * @return new updated deployment profile.
     * @throws NoSuchObjectException
     */
    public DeploymentProfileDTO.DeploymentProfile deleteDeploymentProfile(long id)
        throws NoSuchObjectException, DiscoveredNotSupportedOperationException {
        DeploymentProfileDTO.DeploymentProfile deploymentProfile = getDeploymentProfile(dsl, id)
            .orElseThrow(() -> noSuchObjectException(id));
        if (deploymentProfile.hasTargetId()) {
            throw new DiscoveredNotSupportedOperationException("Delete discovered deployment profile " +
                "is not supported");
        }
        dsl.deleteFrom(DEPLOYMENT_PROFILE).where(DEPLOYMENT_PROFILE.ID.eq(id)).execute();
        return deploymentProfile;
    }

    private Optional<DeploymentProfileDTO.DeploymentProfile> getDeploymentProfile(DSLContext dsl,
                                                                                  long id) {
        final DeploymentProfile deploymentProfile = dsl.selectFrom(DEPLOYMENT_PROFILE)
            .where(DEPLOYMENT_PROFILE.ID.eq(id)).fetchOne().into(DeploymentProfile.class);

        return Optional.ofNullable(deploymentProfile)
            .map(profile -> convertToProtoDeploymentProfile(profile));
    }

    private Set<DeploymentProfileDTO.DeploymentProfile> convertToProtoDeploymentProfileList(
        @Nonnull List<DeploymentProfile> deploymentProfiles) {
        return deploymentProfiles.stream()
            .map(deploymentProfile -> convertToProtoDeploymentProfile(deploymentProfile))
            .collect(Collectors.toSet());
    }

    private DeploymentProfileDTO.DeploymentProfile convertToProtoDeploymentProfile(@Nonnull DeploymentProfile deploymentProfile) {
        DeploymentProfileDTO.DeploymentProfile.Builder builder = DeploymentProfileDTO.DeploymentProfile
            .newBuilder()
            .setId(deploymentProfile.getId())
            .setDeployInfo(deploymentProfile.getDeploymentProfileInfo());
        if (deploymentProfile.getTargetId() != null) {
            builder.setTargetId(deploymentProfile.getTargetId());
        }
        return builder.build();
    }

    private static NoSuchObjectException noSuchObjectException(long id) {
        return new NoSuchObjectException("Deployment profile with id " + id + " not found");
    }
}
