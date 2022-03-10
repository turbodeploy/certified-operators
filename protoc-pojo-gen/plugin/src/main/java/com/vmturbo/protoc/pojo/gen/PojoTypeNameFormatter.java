package com.vmturbo.protoc.pojo.gen;

import static com.vmturbo.protoc.pojo.gen.PojoCodeGenerator.IMPLEMENTATION_SUFFIX;

import javax.annotation.Nonnull;

import com.vmturbo.protoc.plugin.common.generator.TypeNameFormatter;

/**
 * Formatter for POJO type nammes.
 */
public class PojoTypeNameFormatter implements TypeNameFormatter {
    @Nonnull
    @Override
    public String formatTypeName(@Nonnull String unformattedTypeName) {
        if (unformattedTypeName.endsWith("DTO")) {
            return unformattedTypeName.substring(0, unformattedTypeName.length() - 3) + IMPLEMENTATION_SUFFIX;
        } else if (unformattedTypeName.endsWith(IMPLEMENTATION_SUFFIX)) {
            return unformattedTypeName;
        } else {
            return unformattedTypeName + IMPLEMENTATION_SUFFIX;
        }
    }
}
