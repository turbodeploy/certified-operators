package com.vmturbo.api.component.external.api.util;

import static com.vmturbo.api.component.external.api.mapper.GroupMapper.API_GROUP_TYPE_TO_GROUP_TYPE;

import java.util.Arrays;
import java.util.Collection;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableSet;

import org.apache.commons.collections4.CollectionUtils;
import org.springframework.util.Assert;

import com.vmturbo.api.component.external.api.mapper.MarketMapper;
import com.vmturbo.api.enums.EntityType;
import com.vmturbo.common.protobuf.utils.StringConstants;

/**
 * A utility class with methods to perform validation checks.
 */
public class ValidationUtils {

    private static final Set<String> EXTERNAL_ENTITY_TYPES;
    private static final Set<String> GROUP_ENTITY_TYPES;
    private static final Set<String> SEARCHABLE_OBJ_TYPES;

    static {
        ImmutableSet.Builder<String> builder = new ImmutableSet.Builder<>();

        // build Entity Types set
        Arrays.stream(EntityType.values()).map(Enum::toString).forEach(builder::add);
        builder.add(StringConstants.WORKLOAD);
        EXTERNAL_ENTITY_TYPES = builder.build();

        // build Group Entity Types set (groups, entities)
        API_GROUP_TYPE_TO_GROUP_TYPE.keySet().forEach(builder::add);
        GROUP_ENTITY_TYPES = builder.build();

        // build Searchable generic Types set (groups, entities, market, target)
        builder.add(MarketMapper.MARKET);
        builder.add(StringConstants.TARGET);
        SEARCHABLE_OBJ_TYPES = builder.build();
    }

    private ValidationUtils() {}

    /**
     * Validates that all elements in the supplied {@link Collection} are supported entity types.
     *
     * @param entityTypes a collection of entity type strings
     * @throws IllegalArgumentException if at least one element is invalid
     */
    public static void validateExternalEntityTypes(@Nullable final Collection<String> entityTypes) {
        assertIsSubset(entityTypes, EXTERNAL_ENTITY_TYPES, "Invalid entity type(s)");
    }

    /**
     * Validates that all elements in the supplied {@link Collection} are supported searchable group
     * types.
     *
     * @param groupTypes a collection of group or entity type strings, refers to the members of a group
     * @throws IllegalArgumentException if at least one element is invalid
     */
    public static void validateGroupEntityTypes(@Nullable final Collection<String> groupTypes) {
        assertIsSubset(groupTypes, GROUP_ENTITY_TYPES, "Invalid group type(s)");
    }

    /**
     * Validates that all elements in the supplied {@link Collection} are supported searchable
     * object types. Used with GET /search
     *
     * @param types a collection of searchable object type strings
     * @throws IllegalArgumentException if at least one element is invalid
     */
    public static void validateGetSearchableObjTypes(@Nullable final Collection<String> types) {
        assertIsSubset(types, SEARCHABLE_OBJ_TYPES, "Invalid type(s)");
    }

    /**
     * Validates that all elements in the supplied {@link Collection} are supported searchable
     * object types. Used with POST /search
     *
     * @param types a collection of searchable object type strings
     * @throws IllegalArgumentException if at least one element is invalid
     */
    public static void validatePostSearchableObjTypes(@Nullable final Collection<String> types) {
        assertIsSubset(types, GROUP_ENTITY_TYPES, "Invalid type(s)");
    }

    /**
     * Validates that the supplied {@link Collection} contains only expected values.
     * @param actual a collection of strings to be validated
     * @param expected a static set of allowable strings
     * @param message error message used upon failure
     * @throws IllegalArgumentException if at least one element is invalid
     */
    private static void assertIsSubset(@Nullable Collection<String> actual, @Nonnull Collection<String> expected,
            String message) throws IllegalArgumentException {
        Collection<String> invalidValues = CollectionUtils.subtract(CollectionUtils.emptyIfNull(actual), expected);
        Assert.isTrue(invalidValues.isEmpty(), String.format("%s: %s", message,
                String.join(", ", invalidValues)));
    }
}
