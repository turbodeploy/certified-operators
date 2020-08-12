package com.vmturbo.stitching.utilities;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;

/**
 * Strategy of merging entity properties. The strategy defines how to merge properties
 * ({@link EntityDTO.EntityProperty}) of entities that are discovered by multiple targets.
 */
public enum MergePropertiesStrategy {
    /**
     * "Keep onto" strategy means that properties of the "onto" entity are preserved and no merging
     * is applied. This strategy should be used when we know for sure that all targets discover the
     * same set of properties for each shared entity.
     */
    KEEP_ONTO,

    /**
     * "Join" strategy means that resulting property list is a union of properties from all
     * {@code EntityDTO}s.
     * Uniqueness of property namespace + property name is preserved. That is if 2
     * {@code EntityDTO}s contain the same property it is not duplicated in the resulting list. In
     * this case property value is retrieved from the first (random) {@code EntityDTO}.
     */
    JOIN
}
