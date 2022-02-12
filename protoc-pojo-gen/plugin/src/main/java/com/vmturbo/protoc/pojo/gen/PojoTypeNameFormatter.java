package com.vmturbo.protoc.pojo.gen;

import javax.annotation.Nonnull;

import com.vmturbo.protoc.plugin.common.generator.TypeNameFormatter;

/**
 * Formatter for POJO type nammes.
 */
public class PojoTypeNameFormatter implements TypeNameFormatter {
    @Nonnull
    @Override
    public String formatTypeName(@Nonnull String unformattedTypeName) {
        return unformattedTypeName.endsWith("DTO")
            ? unformattedTypeName.substring(0, unformattedTypeName.length() - 3) + PojoCodeGenerator.POJO_SUFFIX
            : unformattedTypeName;
    }
}
