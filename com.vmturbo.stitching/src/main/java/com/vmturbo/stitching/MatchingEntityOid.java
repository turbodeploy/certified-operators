package com.vmturbo.stitching;

import java.util.List;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.Lists;
import com.google.protobuf.MessageOrBuilder;

import com.vmturbo.stitching.utilities.DTOFieldAndPropertyHandler;

/**
 * A {@link MatchingEntityOid} represents the OID of internal Entity is used for entity matching.
 *
 * @param <RETURN_TYPE>
 */
public class MatchingEntityOid<RETURN_TYPE> implements MatchingPropertyOrField<RETURN_TYPE> {

    private static final Logger logger = LogManager.getLogger();


    /**
     * Empty constructor since we only need to extract the OID of stitching entity without any other
     * parameter.
     */
    public MatchingEntityOid() { }

    /**
     * Extract the entity OID as the matching value.
     *
     * @param entity Entity to extract the matching value from.
     * @return Optional of the OID, or Optional empty if the wrong casting.
     */
    @Override
    public Optional<RETURN_TYPE> getMatchingValue(@Nonnull final StitchingEntity entity) {
        try {
            return Optional.of((RETURN_TYPE) Long.toString(entity.getOid()));
        } catch (ClassCastException cce) {
            logger.error("While extracting OID for entity {}, extracted value of wrong "
                            + " class.  Exception: {}",
                    entity.getDisplayName(), cce);
            return Optional.empty();
        }
    }
}
