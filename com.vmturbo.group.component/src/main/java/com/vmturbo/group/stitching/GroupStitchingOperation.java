package com.vmturbo.group.stitching;

import java.util.Collection;

import javax.annotation.Nonnull;

/**
 * An operation which takes the groups discovered from different targets and combine them into a
 * single group.
 *
 * <p>Stitching is applied after getting all groups uploaded from topology processor and before
 * saving to DB in group component to ensure that other components in the XL system only ever see
 * valid and combined groups.
 */
public interface GroupStitchingOperation {

    /**
     * Get the scope groups for this {@link GroupStitchingOperation}. It returns the groups which
     * we should perform operations on.
     *
     * @param groupStitchingContext the context which contains data necessary for stitching
     * @return collection of related {@link StitchingGroup} for this operation
     */
    Collection<StitchingGroup> getScope(@Nonnull GroupStitchingContext groupStitchingContext);

    /**
     * Stitch a collection of {@link StitchingGroup}s. It may modify or remove groups in the input
     * groupStitchingContext during stitching, and will return the updated context. The updated
     * context only contains unified groups, e.g. same groups from different targets have been
     * merged into one unified group.
     *
     * @param groups collection of {@link StitchingGroup}s to perform stitching operation on
     * @param groupStitchingContext the context which contains data necessary for stitching
     * @return updated version of input {@link GroupStitchingContext} containing all stitched groups
     */
    GroupStitchingContext stitch(@Nonnull Collection<StitchingGroup> groups,
                                 @Nonnull GroupStitchingContext groupStitchingContext);
}