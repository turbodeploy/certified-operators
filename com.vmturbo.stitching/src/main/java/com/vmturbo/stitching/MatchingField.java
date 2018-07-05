package com.vmturbo.stitching;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.Lists;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.MessageOrBuilder;

/**
 * A {@link MatchingField} represents a field of the EntityDTO that can be used for entity matching.
 * Its getMatchingValue method extracts the field and returns it for use in matching entities. Its
 * constructor takes a list of Strings which represent the sequence of fields needed to get to the
 * matching value.  For example, for Storage Stitching we use the externalName value which is
 * in the storage_data field of the EntityDTO.  For this we pass in {"storage_data", "externalName"}.
 *
 * @param <RETURN_TYPE>
 */
public class MatchingField<RETURN_TYPE> implements MatchingPropertyOrField<RETURN_TYPE> {

    private static final Logger logger = LogManager.getLogger();

    final private List<String> fields;

    /**
     * Construct an instance of MatchingField that retrieves the value of the field represented
     * by the sequence of structures in the EntityDTO protobuf represented by the list of Strings
     * passed in.
     *
     * @param fields List of protobuf fields to call in order to get the matching value.  This is
     *               the simple, unqualified name of the field.  For example, "storage_data" and
     *               not "com.vmturbo.platform.common.dto.EntityDTO.storage_data"
     */
    public MatchingField(@Nonnull List<String> fields) {
        this.fields = Lists.newArrayList(fields);
    }

    /**
     * Return the named field from the {@link MessageOrBuilder} that is passed in.
     *
     * @param message   {@link MessageOrBuilder} to extract the field from.
     * @param fieldName {@link String} giving the name of the field to extract.
     * @return {@link Object} giving the value of the field.
     * @throws NoSuchFieldException if no {@FieldDescriptor} if found with the name fieldName.
     */
    private Object getNamedFieldFromMessage(@Nonnull final MessageOrBuilder message,
                                            @Nonnull final String fieldName)
            throws NoSuchFieldException {
        final FieldDescriptor nextField = message.getDescriptorForType()
                .findFieldByName(fieldName);
        if (null == nextField) {
            throw new NoSuchFieldException("Field named " + fieldName + " not found in message "
                    + message.toString());
        }
        return message.getField(nextField);
    }

    /**
     * Iterate over the fields extracting each from the protobuf.  Return the last field.
     *
     * @param entity Entity to extract the matching value from.
     * @return Optional of the value of the matching field, or Optional empty if the field does not
     * exist.
     */
    @Override
    public Optional<RETURN_TYPE> getMatchingValue(@Nonnull final StitchingEntity entity) {
        Object nextObject = entity.getEntityBuilder();
        for (String nextFieldName : fields) {
            if (!(nextObject instanceof MessageOrBuilder)) {
                logger.error("Could not find field {} for entity {}.  Skipping this entity",
                        nextFieldName, nextObject);
                return Optional.empty();
            }
            try {
                nextObject = getNamedFieldFromMessage((MessageOrBuilder) nextObject, nextFieldName);
            } catch (NoSuchFieldException e) {
                logger.error("Could not find field {} for entity {}.  Skipping this entity",
                        nextFieldName, nextObject);
                return Optional.empty();
            }
        }
        try {
            RETURN_TYPE retVal = (RETURN_TYPE) nextObject;
            return Optional.of(retVal);
        } catch (ClassCastException cce) {
            logger.error("While extracting matching field for entity {} extracted value of wrong "
                            + " class.  For field {} retrieved value of type {}.  Exception: {}",
                    entity.getDisplayName(), fields.toString(), nextObject.getClass(), cce);
            return Optional.empty();
        }
    }
}
