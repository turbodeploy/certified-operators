package com.vmturbo.plan.orchestrator.deployment.profile;

import static com.vmturbo.plan.orchestrator.db.Tables.DEPLOYMENT_PROFILE;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import com.google.gson.JsonParseException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.exception.DataAccessException;

import com.vmturbo.common.protobuf.plan.DeploymentProfileDTO;
import com.vmturbo.common.protobuf.plan.DeploymentProfileDTO.DeploymentProfileInfo;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.components.common.diagnostics.StringDiagnosable;
import com.vmturbo.components.common.diagnostics.DiagnosticsAppender;
import com.vmturbo.components.common.diagnostics.DiagnosticsException;
import com.vmturbo.components.common.diagnostics.DiagsRestorable;
import com.vmturbo.plan.orchestrator.db.Tables;
import com.vmturbo.plan.orchestrator.db.tables.pojos.DeploymentProfile;
import com.vmturbo.plan.orchestrator.plan.DiscoveredNotSupportedOperationException;
import com.vmturbo.plan.orchestrator.plan.NoSuchObjectException;

/**
 * Implementation of deployment profiles using database connection. And it is only for generating
 * usr created deployment profile, not discovered ones.
 * {@link com.vmturbo.plan.orchestrator.templates.DiscoveredTemplateDeploymentProfileDaoImpl} class
 * is used for uploading discovered templates and deployment profiles.
 */
public class DeploymentProfileDaoImpl implements DiagsRestorable {

    @VisibleForTesting
    static final Gson GSON = ComponentGsonFactory.createGsonNoPrettyPrint();

    private final Logger logger = LogManager.getLogger();

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
     * Get the deployment profiles associated with specific templates.
     *
     * @param templateIds The OIDs of the target templates. If empty, the response will be empty.
     * @return A map of (template id, set of {@link DeploymentProfile}s associated with the template).
     *         Each template ID in the input that has associated profiles will have an entry in the
     *         map.
     */
    @Nonnull
    public Map<Long, Set<DeploymentProfileDTO.DeploymentProfile>> getDeploymentProfilesForTemplates(
            @Nonnull final Set<Long> templateIds) {
        if (templateIds.isEmpty()) {
            return Collections.emptyMap();
        }

        final Map<Long, Set<DeploymentProfileDTO.DeploymentProfile>> retMap = new HashMap<>();
        dsl.selectFrom(DEPLOYMENT_PROFILE
            .innerJoin(Tables.TEMPLATE_TO_DEPLOYMENT_PROFILE).onKey())
            .where(Tables.TEMPLATE_TO_DEPLOYMENT_PROFILE.TEMPLATE_ID.in(templateIds))
            .fetch()
            .forEach(record -> {
                DeploymentProfile deploymentProfile = record.into(DeploymentProfile.class);
                final long templateId = record.get(Tables.TEMPLATE_TO_DEPLOYMENT_PROFILE.TEMPLATE_ID);
                retMap.computeIfAbsent(templateId, k -> new HashSet<>())
                    .add(convertToProtoDeploymentProfile(deploymentProfile));
            });
        return retMap;
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

    private Optional<DeploymentProfileDTO.DeploymentProfile> getDeploymentProfile(DSLContext dsl,
                                                                                  long id) {
        final DeploymentProfile deploymentProfile = dsl.selectFrom(DEPLOYMENT_PROFILE)
            .where(DEPLOYMENT_PROFILE.ID.eq(id)).fetchOne().into(DeploymentProfile.class);

        return Optional.ofNullable(deploymentProfile)
            .map(this::convertToProtoDeploymentProfile);
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
     * @throws NoSuchObjectException If no deployment profile with the id exists.
     * @throws DiscoveredNotSupportedOperationException If the deployment profile was discovered.
     *     Discovered profiles are immutable.
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
     * @throws NoSuchObjectException If the deployment profile doesn't exist.
     * @throws DiscoveredNotSupportedOperationException If the deployment profile is discovered.
     *     Discovered profiles are immutable.
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

    private Set<DeploymentProfileDTO.DeploymentProfile> convertToProtoDeploymentProfileList(
        @Nonnull List<DeploymentProfile> deploymentProfiles) {
        return deploymentProfiles.stream()
            .map(this::convertToProtoDeploymentProfile)
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

    /**
     * {@inheritDoc}
     *
     * This method retrieves all deployment profiles and serializes them as JSON strings.
     *
     * @throws DiagnosticsException on exceptions occurred
     */
    @Override
    public void collectDiags(@Nonnull DiagnosticsAppender appender)
            throws DiagnosticsException {
        final Set<DeploymentProfileDTO.DeploymentProfile> profiles = getAllDeploymentProfiles();
        logger.info("Collecting diagnostics for {} deployment profiles", profiles.size());
        for (DeploymentProfileDTO.DeploymentProfile profile : profiles) {
            appender.appendString(
                    GSON.toJson(profile, DeploymentProfileDTO.DeploymentProfile.class));
        }
    }

    /**
     * {@inheritDoc}
     *
     * This method clears all existing deployment profiles, then deserializes and adds a list of
     * serialized deployment profiles from diagnostics.
     *
     * @param collectedDiags The diags collected from a previous call to
     *      {@link StringDiagnosable#collectDiags(DiagnosticsAppender)}. Must be in the same order.
     * @throws DiagnosticsException if the db already contains deployment profiles, or in response
     *                              to any errors that may occur deserializing or restoring a
     *                              deployment profile.
     */
    @Override
    public void restoreDiags(@Nonnull final List<String> collectedDiags) throws DiagnosticsException {

        final List<String> errors = new ArrayList<>();

        final Set<DeploymentProfileDTO.DeploymentProfile> preexisting = getAllDeploymentProfiles();
        if (!preexisting.isEmpty()) {
            final int numPreexisting = preexisting.size();
            final String clearingMessage = "Clearing " + numPreexisting +
                " preexisting deployment profiles: " + preexisting.stream()
                    .map(deploymentProfile -> deploymentProfile.getDeployInfo().getName())
                    .collect(Collectors.toList());
            errors.add(clearingMessage);
            logger.warn(clearingMessage);

            final int deleted = deleteAllDeploymentProfiles();
            if (deleted != numPreexisting) {
                final String deletedMessage = "Failed to delete " + (numPreexisting - deleted) +
                    " preexisting plan projects: " + getAllDeploymentProfiles().stream()
                        .map(deploymentProfile -> deploymentProfile.getDeployInfo().getName())
                        .collect(Collectors.toList());
                logger.error(deletedMessage);
                errors.add(deletedMessage);
            }
        }

        logger.info("Loading {} serialized deployment profiles from diags", collectedDiags.size());

        final long count = collectedDiags.stream().map(serial -> {
            try {
                return GSON.fromJson(serial, DeploymentProfileDTO.DeploymentProfile.class);
            } catch (JsonParseException e) {
                errors.add("Failed to deserialize deployment profile " + serial +
                    " because of parse exception " + e.getMessage());
                return null;
            }
        }).filter(Objects::nonNull).map(this::restoreDeploymentProfile).filter(optional -> {
            optional.ifPresent(errors::add);
            return !optional.isPresent();
        }).count();

        logger.info("Loaded {} deployment profiles from diags", count);

        if (!errors.isEmpty()) {
            throw new DiagnosticsException(errors);
        }
    }

    @Nonnull
    @Override
    public String getFileName() {
        return "DeploymentProfiles";
    }

    /**
     * Add a deployment profile to the database. Note that this is used for restoring deployment
     * profiles from diagnostics and should NOT be used for normal operations.
     *
     * @param profile the deployment profile to add.
     * @return an optional of a string representing any error that may have occurred
     */
    private Optional<String> restoreDeploymentProfile(
                                @Nonnull final DeploymentProfileDTO.DeploymentProfile profile) {
        final DeploymentProfile record = new DeploymentProfile(profile.getId(), null, null,
            profile.getDeployInfo().getName(), profile.getDeployInfo());
        try {
            int r = dsl.newRecord(DEPLOYMENT_PROFILE, record).store();
            return r == 1 ? Optional.empty() :
                Optional.of("Failed to restore deployment profile " + profile);
        } catch (DataAccessException e) {
            return Optional.of("Could not restore deployment profile " + profile +
                " because of DataAccessException " + e.getMessage());
        }
    }

    /**
     * Deletes all deployment profiles. Note: this is only used when restoring deployment profiles
     * from diagnostics and should NOT be used during normal operations.
     *
     * @return the number of records deleted
     */
    private int deleteAllDeploymentProfiles() {
        try {
            return dsl.deleteFrom(DEPLOYMENT_PROFILE).execute();
        } catch (DataAccessException e) {
            return 0;
        }
    }
}
