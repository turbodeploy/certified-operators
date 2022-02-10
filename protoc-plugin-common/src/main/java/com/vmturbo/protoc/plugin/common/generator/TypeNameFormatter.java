package com.vmturbo.protoc.plugin.common.generator;

import javax.annotation.Nonnull;

/**
 * Formats names for types of fields. Used by {@link FieldDescriptor} when generating type names for fields.
 * Can be used to transform names of types (ie TopologyEntityDTO -> TopologyEntityPojo)
 */
public interface TypeNameFormatter {

    /**
     * Formatting the IDENTITY type (returns the original type name).
     */
    TypeNameFormatter IDENTITY = new IdentityTypeNameFormatter();

    /**
     * Format the type name for a field.
     *
     * @param unformattedTypeName The unformatted type name.
     * @return The formatted type name.
     */
    @Nonnull
    String formatTypeName(@Nonnull String unformattedTypeName);

    /**
     * The identity type name formatter for returning the original name of types.
     */
    class IdentityTypeNameFormatter implements TypeNameFormatter {

        @Nonnull
        @Override
        public String formatTypeName(@Nonnull String unformattedTypeName) {
            return unformattedTypeName;
        }
    }
}
