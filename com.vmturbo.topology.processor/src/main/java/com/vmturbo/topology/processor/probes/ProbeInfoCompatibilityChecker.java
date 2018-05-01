package com.vmturbo.topology.processor.probes;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

import javax.annotation.Nonnull;

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

    /**
     * Create a new ProbeInfoCompatibilityChecker.
     */
    public ProbeInfoCompatibilityChecker() {
        
    }

    private static final Collection<Function<ProbeInfo, Object>> PROPERTY_EXTRACTORS;

    /**
     * Changes in these fields on a {@link ProbeInfo} indicate a compatibility problem.
     *
     * When a probe registers with its info, if that probe previously registered with
     * different info, we need to check if the new info is compatible with the old info.
     *
     * For example, because we persist identity metadata, a change in identity metadata
     * would require a data migration for all associated entities. Because this migration
     * has not been implemented yet, we consider a change in this field to be an
     * incompatibility.
     */
    static {
        final List<Function<ProbeInfo, Object>> extractors = new ArrayList<>();
        extractors.add(ProbeInfo::getProbeType);
        extractors.add(ProbeInfo::getProbeCategory);
        extractors.add(ProbeInfo::getAccountDefinitionList);
        extractors.add(probe -> new HashSet<>(probe.getTargetIdentifierFieldList()));
        extractors.add(probe -> new HashSet<>(probe.getEntityMetadataList()));
        PROPERTY_EXTRACTORS = Collections.unmodifiableList(extractors);
    }

    /**
     * Compare 2 {@link ProbeInfo} objects for compatibility.
     * Compatibility is reflexive, symmetric and transitive.
     *
     * @param left left {@link ProbeInfo} to compare
     * @param right right {@link ProbeInfo} to compare
     * @return {@code true} if probe infos are equal, {@code false} otherwise.
     */
    public boolean areCompatible(@Nonnull final ProbeInfo left,
                                 @Nonnull final ProbeInfo right) {
        Objects.requireNonNull(left);
        Objects.requireNonNull(right);
        if (left == right) {
            return true; // Fast check for reference equality.
        }

        return PROPERTY_EXTRACTORS.stream()
            .allMatch(extractor -> {
                final Object leftField = extractor.apply(left);
                final Object rightField = extractor.apply(right);
                return Objects.equals(leftField, rightField);
            });
    }
}
