package com.vmturbo.stitching.utilities;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message.Builder;
import com.google.protobuf.MessageOrBuilder;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityProperty;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTOOrBuilder;
import com.vmturbo.platform.sdk.common.supplychain.SupplyChainConstants;
import com.vmturbo.platform.sdk.common.util.SDKUtil;
import com.vmturbo.stitching.DTOFieldSpec;

/**
 * Utility class for manipulating fields in a DTO by simple name. For example, you can get or set
 * the value of the EntityDTO.storage_data field with just a reference to the enclosing Message
 * and the string "storage_data".
 */
public class DTOFieldAndPropertyHandler {

    private static final FieldDescriptor USED_FIELD_FIELD_DESCRIPTOR =
            CommodityDTO.getDescriptor().findFieldByNumber(CommodityDTO.USED_FIELD_NUMBER);
    private static final Logger logger = LogManager.getLogger();
    private static final FieldDescriptor UTILIZATION_DATA_FIELD_DESCRIPTOR =
            CommodityDTO.getDescriptor().findFieldByNumber(
                    CommodityDTO.UTILIZATIONDATA_FIELD_NUMBER);

    /**
     * Private constructor since class just provides static utility method
     */
    private DTOFieldAndPropertyHandler() {}

    /**
     * Return a {@link String} that returns the named property from the passed in
     * {@link EntityDTOOrBuilder} or the empty string if the property does not exist.
     *
     * @param {@link EntityDTOOrBuilder} representing the entity to extract the property from.
     * @param propertyName The name of the property to merge.
     * @return A {@link String} giving the value of the property or the empty String.
     */
    public static String getPropertyFromEntity(@Nonnull final EntityDTOOrBuilder entity,
                                               @Nonnull final String propertyName) {
        // get the named property
        return entity.getEntityPropertiesList()
                .stream()
                .filter(ep -> ep.getName().equals(propertyName))
                .map(EntityProperty::getValue)
                .findFirst().orElse("");
    }

    /**
     * Set the value of the named property to the passed in {@link Builder}.
     *
     * @param entityBuilder {@link EntityDTO.Builder} representing the entityBuilder to extract the
     *                      property from.
     * @param propertyName  The name of the property to merge.
     * @param newValue      {@link String} giving the new value of the property.
     */
    public static void setPropertyOfEntity(@Nonnull final EntityDTO.Builder entityBuilder,
                                           @Nonnull final String propertyName,
                                           @Nonnull final String newValue) {
        // see if the named property is already there and replace it.  If it is not there, create it
        // and add it.
        Optional<EntityProperty.Builder> entityProp =
                entityBuilder.getEntityPropertiesBuilderList().stream()
                        .filter(epb -> epb.getName().equals(propertyName))
                        .findFirst();
        if (entityProp.isPresent()) {
            entityProp.get().setValue(newValue).build();
        } else {
            EntityProperty.Builder newBuilder = EntityProperty.newBuilder();
            newBuilder.setName(propertyName)
                    .setValue(newValue)
                    .setNamespace(SDKUtil.DEFAULT_NAMESPACE);
            entityBuilder.addEntityProperties(newBuilder.build());
        }
    }

    /**
     * Retrieve the value of named field from the passed in MessageOrBuilder. If the field exists,
     * but the value is not set, it will return null.
     *
     * @param message {@link MessageOrBuilder} object from which we extract the field value.
     * @param fieldName {@link String} simple name of the field to retrieve.
     * @return {@link Object} the value of the field which may be another {@link MessageOrBuilder}
     * if the field is itself a protobuf message, or null if the field is not set.
     */
    @Nullable
    public static Object getFieldFromMessageOrBuilder(@Nonnull final MessageOrBuilder message,
                                                      @Nonnull final String fieldName)
            throws NoSuchFieldException {
        final FieldDescriptor fieldDescriptor = getFieldDescriptor(message, fieldName);
        if (fieldDescriptor.isRepeated() || message.hasField(fieldDescriptor)) {
            return message.getField(fieldDescriptor);
        }
        return null;
    }

    /**
     * Extract the named field from a message builder.
     * @param builder the {@link Builder} to extract the field from.
     * @param fieldName {@link String} giving the simple name of the field
     * @return {@link FieldDescriptor} for the named field.
     * @throws NoSuchFieldException if there is no field matching fieldName.
     */
    public static FieldDescriptor getFieldDescriptor(@Nonnull final MessageOrBuilder builder,
                                                     @Nonnull final String fieldName)
            throws NoSuchFieldException {
        FieldDescriptor nextField = builder.getDescriptorForType().findFieldByName(fieldName);
        if (nextField == null) {
            throw new NoSuchFieldException("No field named " + fieldName + " found in builder.");
        }
        return nextField;
    }

    /**
     * Take a protobuf {@link MessageOrBuilder} and a sequence of simple field names ending in the
     * field whose value should be returned and return that value by traversing the field names in
     * the sequence. If the value of the field is not set, it will return null.
     *
     * @param msgOrBuilder {@link MessageOrBuilder} object to begin the traversal at.
     * @param fieldSpec {@link DTOFieldSpec} specifying the field to return.
     * @return {@link Object} with the value of the field that was obtained from the traversal, or
     * null if the field is not set.
     * @throws NoSuchFieldException if any field in the sequence cannot be found
     */
    @Nullable
    public static Object getValueFromFieldSpec(@Nonnull final MessageOrBuilder msgOrBuilder,
                                               @Nonnull final DTOFieldSpec fieldSpec)
            throws NoSuchFieldException {
        Object nxtObject = msgOrBuilder;
        // Iterate over protobuf Message fields to get to Message that actually contains the value
        // we want
        for (String nextFieldName : fieldSpec.getMessagePath()) {
            // if the field exists, but value is not set, return null immediately
            if (nxtObject == null) {
                return null;
            }
            if (!(nxtObject instanceof MessageOrBuilder)) {
                throw new NoSuchFieldException("Could not find field " + nextFieldName +
                        " in Object " + nxtObject);
            }
            nxtObject = getFieldFromMessageOrBuilder((MessageOrBuilder) nxtObject, nextFieldName);
        }
        return nxtObject == null ? null : getFieldFromMessageOrBuilder(
                (MessageOrBuilder) nxtObject, fieldSpec.getFieldName());
    }

    /**
     * Set a value to a field in a DTO.
     *
     * @param builder The {@link Builder} to set the field in.
     * @param fieldSpec the field to set.
     * @param newValue the new value to set the field to.
     * @throws NoSuchFieldException if the field doesn't exist or any Message on the path to the
     * field doesn't exist
     */
    public static void setValueToFieldSpec(@Nonnull final Builder builder,
                                           @Nonnull final DTOFieldSpec fieldSpec,
                                           @Nullable Object newValue)
            throws NoSuchFieldException {
        Builder nxtBuilder = builder;
        for (String fieldName : fieldSpec.getMessagePath()) {
            FieldDescriptor nextField = getFieldDescriptor(nxtBuilder, fieldName);
            nxtBuilder = nxtBuilder.getFieldBuilder(nextField);
        }
        // at this point, we've reached the Builder for the field we want to set
        nxtBuilder.setField(getFieldDescriptor(nxtBuilder, fieldSpec.getFieldName()), newValue);
        nxtBuilder.build();
    }

    /**
     * Take all the populated fields from one Builder and push them onto the other, overwriting
     * fields if necessary. If patchedFields is not empty, it will only patch those specified
     * fields, otherwise it will patch all available fields.
     *
     * @param from the MessageOrBuilder to take the fields from
     * @param onto the Builder to write the values onto
     * @param patchedFields the list of fields to patch, patch all if empty
     */
    public static <T extends Builder> T mergeBuilders(@Nonnull final T from,
                                                      @Nonnull final T onto,
                                                      @Nonnull List<DTOFieldSpec> patchedFields) {
        if (patchedFields.isEmpty()) {
            final Map<FieldDescriptor, ?> allFromFields = from.getAllFields();
            for (Entry<FieldDescriptor, ?> entry : allFromFields.entrySet()) {
                final FieldDescriptor key = entry.getKey();
                onto.setField(key, entry.getValue());
                // "Used" and "utilizationData" are two fields that depend on each other, so
                // if we change one of them, we must also change the second one, and if the probe
                // contains information about only one field and overwrites the information of another
                // probe, then the other field must be cleared.
                clearConnectedField(onto, allFromFields, key, USED_FIELD_FIELD_DESCRIPTOR,
                        UTILIZATION_DATA_FIELD_DESCRIPTOR);
                clearConnectedField(onto, allFromFields, key, UTILIZATION_DATA_FIELD_DESCRIPTOR,
                        USED_FIELD_FIELD_DESCRIPTOR);
            }
        } else {
            patchedFields.forEach(fieldSpec -> {
                try {
                    final Object newValue = DTOFieldAndPropertyHandler.getValueFromFieldSpec(from,
                        fieldSpec);
                    if (newValue != null) {
                        DTOFieldAndPropertyHandler.setValueToFieldSpec(onto, fieldSpec, newValue);
                    }
                } catch (NoSuchFieldException e) {
                    logger.error("Unable to patch field {} with path {} from {} to {}",
                        fieldSpec.getFieldName(), fieldSpec.getMessagePath(), from, onto, e);
                }
            });
        }
        return onto;
    }

    private static <T extends Builder> void clearConnectedField(@Nonnull T onto,
            Map<FieldDescriptor, ?> allFromFields, FieldDescriptor currentField,
            FieldDescriptor firstTiedField, FieldDescriptor secondTiedField) {
        if (currentField.getFullName().equals(firstTiedField.getFullName())) {
            if (!allFromFields.containsKey(secondTiedField)) {
                onto.clearField(secondTiedField);
            }
        }
    }

    /**
     * Extract the vendor identifier (legacy 'local name') from entity properties.
     *
     * @param entity the entity for which the local name is to be extracted
     * @return external identity, may be null as probes are only required to provide them
     *         if they have action execution
     */
    @Nullable
    public static String getVendorId(@Nonnull final EntityDTOOrBuilder entity) {
        return entity.getEntityPropertiesList().stream()
            .filter(property -> SDKUtil.DEFAULT_NAMESPACE.equals(property.getNamespace()))
            .filter(property -> SupplyChainConstants.LOCAL_NAME.equals(property.getName()))
            .map(EntityProperty::getValue)
            .findAny().orElse(null);
    }

}



