package com.vmturbo.api.component.external.api.util;

import com.google.common.collect.ImmutableSet;
import com.vmturbo.common.protobuf.utils.StringConstants;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nullable;
import org.springframework.util.Assert;
import com.vmturbo.api.enums.EntityType;

public class ValidationUtils {

    private static final Set<String> EXTERNAL_ENTITY_TYPES;
    static {
        ImmutableSet.Builder<String> builder = new ImmutableSet.Builder<>();

        Arrays.stream(EntityType.values()).map(Enum::toString).forEach(builder::add);
        builder.add(StringConstants.WORKLOAD);

        EXTERNAL_ENTITY_TYPES = builder.build();
    }

    /**
     * Validates that all elements in the supplied {@link Collection} are supported entity types
     *
     * @param entityTypes a collection of entity type strings
     * @throws IllegalArgumentException if at least one element is invalid
     */
    public static void validateExternalEntityTypes(@Nullable final Collection<String> entityTypes) {
        Optional.ofNullable(entityTypes).orElse(Collections.emptyList())
                .forEach(ValidationUtils::validateExternalEntityType);
    }

    /**
     * Validates that the supplied string is a supported entity type
     *
     * @param entityType the entity type string
     * @throws IllegalArgumentException if the entity type is invalid
     */
    public static void validateExternalEntityType(final String entityType) {
        Assert.isTrue(isValidExternalEntityType(entityType),
                () -> String.format("'%s' is not a valid entity type", entityType));
    }

    private static boolean isValidExternalEntityType(final String entityType) {
        return EXTERNAL_ENTITY_TYPES.contains(entityType);
    }

}
