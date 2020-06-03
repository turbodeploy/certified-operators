package com.vmturbo.stitching;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import javax.annotation.Nonnull;

import com.google.common.collect.Lists;
import com.google.protobuf.MessageOrBuilder;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.stitching.utilities.DTOFieldAndPropertyHandler;

/**
 * A {@link MatchingField} represents a field of the EntityDTO that can be used for entity matching.
 * Its getMatchingValue method extracts the field and returns it for use in matching entities. Its
 * constructor takes a list of Strings which represent the sequence of messagePath needed to get to the
 * matching value.  For example, for Storage Stitching we use the externalName value which is
 * in the storage_data field of the EntityDTO.  For this we pass in {"storage_data", "externalName"}.
 *
 * @param <RETURN_TYPE>
 */
public class MatchingField<RETURN_TYPE> implements MatchingPropertyOrField<RETURN_TYPE> {

    private static final Logger logger = LogManager.getLogger();

    final private List<String> messagePath;

    /**
     * Construct an instance of MatchingField that retrieves the value of the field represented
     * by the sequence of structures in the EntityDTO protobuf represented by the list of Strings
     * passed in.
     *
     * @param messagePath List of protobuf messagePath to call in order to get the matching value.  This is
     *               the simple, unqualified name of the field.  For example, "storage_data" and
     *               not "com.vmturbo.platform.common.dto.EntityDTO.storage_data"
     */
    public MatchingField(@Nonnull List<String> messagePath, @Nonnull String fieldName) {
        this.messagePath = Lists.newArrayList(messagePath);
        this.messagePath.add(fieldName);
    }

    /**
     * Iterate over the messagePath extracting each from the protobuf.  Return the last field.
     *
     * @param entity Entity to extract the matching value from.
     * @return Optional of the value of the matching field, or Optional empty if the field does not
     * exist.
     */
    @Override
    public Collection<RETURN_TYPE> getMatchingValue(@Nonnull final StitchingEntity entity) {
        Object nextObject = entity.getEntityBuilder();
        for (String nextFieldName : messagePath) {
            if (!(nextObject instanceof MessageOrBuilder)) {
                logger.error("Could not find field {} for entity {}.  Skipping this entity",
                        nextFieldName, nextObject);
                return Collections.emptySet();
            }
            try {
                nextObject = DTOFieldAndPropertyHandler
                        .getFieldFromMessageOrBuilder((MessageOrBuilder) nextObject, nextFieldName);
            } catch (NoSuchFieldException e) {
                logger.error("Could not find field {} for entity {}.  Skipping this entity",
                        nextFieldName, nextObject);
                return Collections.emptySet();
            }
        }
        try {
            if (nextObject instanceof Collection) {
                @SuppressWarnings("unchecked")
                final Collection<RETURN_TYPE> result = (Collection<RETURN_TYPE>)nextObject;
                return result;
            }
            @SuppressWarnings("unchecked")
            final RETURN_TYPE retVal = (RETURN_TYPE)nextObject;
            if (retVal == null) {
                return Collections.emptySet();
            }
            return Collections.singleton(retVal);
        } catch (ClassCastException cce) {
            logger.error("While extracting matching field for entity {} extracted value of wrong "
                            + " class.  For field {} retrieved value of type {}.  Exception: {}",
                    entity.getDisplayName(), messagePath.toString(), nextObject.getClass(), cce);
            return Collections.emptySet();
        }
    }
}
