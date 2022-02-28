package com.vmturbo.api.enums;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import com.vmturbo.common.protobuf.topology.ApiEntityType;

/**
 * Tests to ensure consistency between entity type enums.
 */
public class EntityTypeConsistencyTest {

    /**
     * Ensures that {@link EntityType} and {@link ApiEntityType} enums have exactly the same string
     * representations.
     * In order to add a new entity type, {@link EntityType} must first be updated in the api
     * repository.
     */
    @Test
    public void testEntityTypesAreConsistent() {
        Set<String> apiEntityTypeStrings = Arrays.stream(ApiEntityType.values())
                .map(ApiEntityType::apiStr)
                .collect(Collectors.toSet());

        Set<String> entityTypeStrings = Arrays.stream(EntityType.values()).map(EntityType::toString)
                .collect(Collectors.toSet());

        Assertions.assertThat(apiEntityTypeStrings).containsOnlyElementsOf(entityTypeStrings);
    }

}
