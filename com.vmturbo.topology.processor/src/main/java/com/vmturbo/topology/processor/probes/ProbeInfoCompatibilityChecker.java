package com.vmturbo.topology.processor.probes;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.IdentityMetadata.EntityIdentityMetadata;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;

/**
 * Utility class to check {@link ProbeInfo} objects for compatibility.
 *
 * When upgrading a probe, changes in the {@link ProbeInfo} object signals that associated
 * data would have to be migrated. Since we do not yet have any migrations or any process
 * around them, we treat changes in these fields as indicating an incompatibility.
 *
 * Note that of the data stored in the probe info is order-insensitive, so we cannot
 * rely on protobuf's {@link #equals(Object)} method for these.
 *
 */
public class ProbeInfoCompatibilityChecker {
    private static final Logger logger = LogManager.getLogger();

    /**
     * Create a new ProbeInfoCompatibilityChecker.
     */
    public ProbeInfoCompatibilityChecker() {

    }

    /**
     * The list of {@link CompatibilityCheck}s to apply when comparing an existing probe info to
     * a newly registered one.
     */
    private static final Collection<CompatibilityCheck> CHECKS;

    /**
     * A compatibility check to apply to a new {@link ProbeInfo}.
     */
    @FunctionalInterface
    private interface CompatibilityCheck {

        /**
         * Check whether the new probe info is compatible with the existing one.
         *
         * @param existingInfo The existing {@link ProbeInfo}.
         * @param newInfo The new {@link ProbeInfo}.
         * @return An {@link Optional} containing the error if they are not compatible.
         *         An empty optional otherwise.
         */
        Optional<String> checkCompatibility(@Nonnull final ProbeInfo existingInfo,
                                            @Nonnull final ProbeInfo newInfo);
    }

    /**
     * Changes in these fields on a {@link ProbeInfo} indicate a compatibility problem.
     *
     * When a probe registers with its info, if that probe previously registered with
     * different info, we need to check if the new info is compatible with the old info.
     *
     * For example, because we persist identity metadata, a change in identity metadata
     * might require a data migration for all associated entities. Because this migration
     * has not been implemented yet, we consider a change in this field to be an
     * incompatibility.
     */
    static {
        final List<CompatibilityCheck> checks = new ArrayList<>();

        checks.add(new EqualityCheck(ProbeInfo::getProbeType, "type"));
        checks.add(new EqualityCheck(ProbeInfo::getProbeCategory, "category"));
        // Lists are converted to Sets so that ordering does not affect equality checking
        checks.add(new EqualityCheck(probe -> new HashSet<>(probe.getTargetIdentifierFieldList()),
            "targetIdentifierFieldList"));
        checks.add(new IdentityMetadataCheck());
        CHECKS = Collections.unmodifiableList(checks);
    }



    /**
     * Compare 2 {@link ProbeInfo} objects for compatibility.
     * Compatibility is reflexive, symmetric and transitive.
     *
     * @param existingInfo Existing {@link ProbeInfo} to compare to.
     * @param newInfo The newly registered {@link ProbeInfo} to compare.
     * @return {@code true} if probe infos are compatible, {@code false} otherwise.
     */
    public boolean areCompatible(@Nonnull final ProbeInfo existingInfo,
                                 @Nonnull final ProbeInfo newInfo) {

        Objects.requireNonNull(existingInfo);
        Objects.requireNonNull(newInfo);
        if (existingInfo == newInfo) {
            return true; // Fast check for reference equality.
        }

        final List<String> errors = CHECKS.stream()
            .map(check -> check.checkCompatibility(existingInfo, newInfo))
            .filter(Optional::isPresent)
            .map(Optional::get)
            .collect(Collectors.toList());
        if (!errors.isEmpty()) {
            logger.error("New probe info incompatible with existing probe info. Errors: " + errors);
            return false;
        } else {
            return true;
        }
    }

    /**
     * A compatibility check that compares certain probe fields for equality.
     *
     * Used for fields where changes indicate a compatibility problem.
     */
    private static class EqualityCheck implements CompatibilityCheck {
        private final Function<ProbeInfo, Object> extractor;
        private final String fieldName;

        private EqualityCheck(@Nonnull final Function<ProbeInfo, Object> extractor,
                              @Nonnull final String fieldName) {
            this.extractor = extractor;
            this.fieldName = fieldName;
        }

        @Override
        public Optional<String> checkCompatibility(@Nonnull final ProbeInfo existingInfo, @Nonnull final ProbeInfo newInfo) {
            final Object existingField = extractor.apply(existingInfo);
            final Object newField = extractor.apply(newInfo);
            if (Objects.equals(existingField, newField)) {
                return Optional.empty();
            } else {
                return Optional.of("Incompatible field: " + fieldName + "! Existing: " +
                    existingField + ". New: " + newField);
            }
        }
    }

    /**
     * A compatibility check for {@link EntityIdentityMetadata} reported by the probe.
     */
    private static class IdentityMetadataCheck implements CompatibilityCheck {

        @Override
        public Optional<String> checkCompatibility(@Nonnull final ProbeInfo existingInfo,
                                                   @Nonnull final ProbeInfo newInfo) {
            final Map<EntityType, EntityIdentityMetadata> existingByType =
                existingInfo.getEntityMetadataList().stream()
                    .collect(Collectors.toMap(EntityIdentityMetadata::getEntityType, Function.identity()));
            final Map<EntityType, EntityIdentityMetadata> newByType =
                newInfo.getEntityMetadataList().stream()
                    .collect(Collectors.toMap(EntityIdentityMetadata::getEntityType, Function.identity()));

            final Set<EntityType> removedEntityTypes = existingByType.keySet().stream()
                .filter(type -> !newByType.containsKey(type))
                .collect(Collectors.toSet());

            final Set<EntityType> modifiedEntityTypes = newInfo.getEntityMetadataList().stream()
                .map(newMetadata -> {
                    final EntityIdentityMetadata existingMetadata = existingByType.get(newMetadata.getEntityType());
                    if (existingMetadata != null && !existingMetadata.equals(newMetadata)) {
                        return newMetadata.getEntityType();
                    } else {
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());

            if (!removedEntityTypes.isEmpty()) {
                // Log a warning and continue.
                logger.warn("Identity metadata for existing entity types '{}' removed for '{}' probe type!",
                                removedEntityTypes, newInfo.getProbeType());
            }
            // TODO (roman, May 2 2019): OM-45657 - Support modifying entity types.
            // For probes that we own we may still require server-side migrations to preserve
            // the IDs. For third-party probes we may need to clear out all entities of these
            // types known to the IdentityProvider and re-assign IDs.
            if (!modifiedEntityTypes.isEmpty()) {
                return Optional.of(
                                String.format("Identity metadata for existing entity types %s changed! Need migration.",
                                                modifiedEntityTypes));
            }
            return Optional.empty();
        }
    }
}
