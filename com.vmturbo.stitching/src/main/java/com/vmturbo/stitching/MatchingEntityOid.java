package com.vmturbo.stitching;

import java.util.Collection;
import java.util.Collections;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * A {@link MatchingEntityOid} represents the OID of internal Entity is used for entity matching.
 */
public class MatchingEntityOid implements MatchingPropertyOrField<String> {

    private static final Logger logger = LogManager.getLogger();

    /**
     * Extract the entity OID as the matching value.
     *
     * @param entity Entity to extract the matching value from.
     * @return Optional of the OID, or Optional empty if the wrong casting.
     */
    @Nonnull
    @Override
    public Collection<String> getMatchingValue(@Nonnull final StitchingEntity entity) {
        try {
            return Collections.singleton(Long.toString(entity.getOid()));
        } catch (ClassCastException cce) {
            logger.error("While extracting OID for entity {}, extracted value of wrong "
                            + " class.  Exception: {}",
                    entity.getDisplayName(), cce);
            return Collections.emptySet();
        }
    }
}
