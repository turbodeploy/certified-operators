package com.vmturbo.topology.processor.stitching;

import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableSet;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityProperty;
import com.vmturbo.stitching.utilities.MergePropertiesStrategy;

/**
 * Unit tests for {@link PropertiesMerger}.
 */
public class PropertiesMergerTest {

    private static final String EXISTING_NAMESPACE = "existingNamespace";
    private static final String NEW_NAMESPACE = "newNamespace";
    private static final String PROPERTY_1 = "property1";
    private static final String PROPERTY_2 = "property2";
    private static final String PROPERTY_3 = "property2";
    private static final String VALUE = "value";
    private static final String NEW_VALUE = "newValue";

    private final EntityDTO.Builder from = EntityDTO.newBuilder()
            .addEntityProperties(EntityProperty.newBuilder()
                    .setNamespace(EXISTING_NAMESPACE)
                    .setName(PROPERTY_1)
                    .setValue(NEW_VALUE)
                    .build())
            .addEntityProperties(EntityProperty.newBuilder()
                    .setNamespace(EXISTING_NAMESPACE)
                    .setName(PROPERTY_2)
                    .setValue(VALUE)
                    .build())
            .addEntityProperties(EntityProperty.newBuilder()
                    .setNamespace(NEW_NAMESPACE)
                    .setName(PROPERTY_3)
                    .setValue(VALUE)
                    .build());

    private final EntityDTO.Builder onto = EntityDTO.newBuilder()
            .addEntityProperties(EntityProperty.newBuilder()
                    .setNamespace(EXISTING_NAMESPACE)
                    .setName(PROPERTY_1)
                    .setValue(VALUE)
                    .build());

    /**
     * Test merging entity properties using
     * {@link com.vmturbo.stitching.utilities.MergePropertiesStrategy#KEEP_ONTO} strategy.
     */
    @Test
    public void testMergeKeepOnto() {
        final PropertiesMerger propertiesMerger = new PropertiesMerger(
                MergePropertiesStrategy.KEEP_ONTO);
        propertiesMerger.merge(from, onto);

        // Resulting entity should preserve properties from "onto" DTO
        Assert.assertEquals(1, onto.getEntityPropertiesCount());
        final EntityProperty property = onto.getEntityProperties(0);
        Assert.assertEquals(EXISTING_NAMESPACE, property.getNamespace());
        Assert.assertEquals(PROPERTY_1, property.getName());
    }

    /**
     * Test merging entity properties using
     * {@link com.vmturbo.stitching.utilities.MergePropertiesStrategy#JOIN} strategy.
     */
    @Test
    public void testMergeJoin() {
        final PropertiesMerger propertiesMerger = new PropertiesMerger(
                MergePropertiesStrategy.JOIN);
        propertiesMerger.merge(from, onto);

        // Resulting entity should contain properties from both "onto" and "from" DTOs
        final Set<String> resultingProperties = onto.getEntityPropertiesList().stream()
                .map(property -> property.getNamespace() + property.getName())
                .collect(Collectors.toSet());
        final Set<String> expectedProperties = ImmutableSet.of(
                EXISTING_NAMESPACE + PROPERTY_1,
                EXISTING_NAMESPACE + PROPERTY_2,
                NEW_NAMESPACE + PROPERTY_3
        );
        Assert.assertEquals(expectedProperties, resultingProperties);
    }
}
