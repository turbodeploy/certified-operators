package com.vmturbo.stitching;

import java.util.Collection;
import java.util.Collections;
import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;

/**
 * A {@link StitchingPoint} represents a match between an internal entity and one or more external entities.
 *
 * {@Link StitchingPoint}s should be stitched together to combine separate graphs into a combined graph.
 *
 * A {@link StitchingPoint} consists of an internal entity and zero or more external entities.
 *
 * {@link com.vmturbo.stitching.StitchingOperation}s are run on a per-target basis. An internal entity
 * represents the entity discovered by that target that should be stitched with entities discovered by
 * other targets. External matches are external entities (that is, entities discovered by other targets)
 * that should be stitched with the internal entity.
 *
 * For a {@link com.vmturbo.stitching.StitchingOperation} that does not stitch with any external entities,
 * the list of external matches will always be empty.
 *
 * For a {@link com.vmturbo.stitching.StitchingOperation} that does stitch with external entities,
 * the list of external matches will always contain at least one entry.
 */
@Immutable
public class StitchingPoint {
    /**
     * For a {@link com.vmturbo.stitching.StitchingOperation} being run for a specific target, represents
     * the entity discovered by that target that should be stitched with entities discoverd by other targets.
     */
    private final EntityDTO.Builder internalEntity;

    /**
     * For a {@link com.vmturbo.stitching.StitchingOperation} being run for a specific target, represents
     * the entity discovered by that target that should be stitched with entities discoverd by other targets.
     */
    private final Collection<EntityDTO.Builder> externalMatches;

    /**
     * Create a new {@link StitchingPoint} without external matches.
     *
     * @param internalEntity The entity discovered by the target for which a
     *                       {@link com.vmturbo.stitching.StitchingOperation} that should be stitched.
     */
    public StitchingPoint(@Nonnull final EntityDTO.Builder internalEntity) {
        this.internalEntity = Objects.requireNonNull(internalEntity);
        this.externalMatches = Collections.emptyList();
    }


    /**
     * Create a new {@link StitchingPoint} with external matches.
     *
     * @param internalEntity The entity discovered by the target for which a
     *                       {@link com.vmturbo.stitching.StitchingOperation} that should be stitched.
     * @param externalMatches The entities discovered by other targets to be stitched with the internal entity.
     */
    public StitchingPoint(@Nonnull final EntityDTO.Builder internalEntity,
                          @Nonnull final Collection<EntityDTO.Builder> externalMatches) {
        this.internalEntity = Objects.requireNonNull(internalEntity);
        this.externalMatches = Objects.requireNonNull(externalMatches);
    }

    /**
     * Get the internal entity for this {@link StitchingPoint}.
     *
     * @return The entity discovered by the target for which a
     *         {@link com.vmturbo.stitching.StitchingOperation} that should be stitched.
     */
    public EntityDTO.Builder getInternalEntity() {
        return internalEntity;
    }

    /**
     * Get the external entities for this {@link StitchingPoint}.
     *
     * @return For a {@link com.vmturbo.stitching.StitchingOperation} being run for a specific target, represents
     *         the entity discovered by that target that should be stitched with entities discoverd by other targets.
     */
    public Collection<EntityDTO.Builder> getExternalMatches() {
        return externalMatches;
    }
}
