package com.vmturbo.topology.processor.stitching;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableSet;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityProperty;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata.MergePropertiesStrategy;
import com.vmturbo.platform.sdk.common.util.SDKUtil;

/**
 * Unit tests for {@link PropertiesMerger}.
 */
public class PropertiesMergerTest {

    private static final String EXISTING_NAMESPACE = "existingNamespace";
    private static final String NEW_NAMESPACE = "newNamespace";
    private static final String PROPERTY_1 = "property1";
    private static final String PROPERTY_2 = "property2";
    private static final String PROPERTY_3 = "property3";
    private static final String PROPERTY_4 = "property4";
    private static final String PROPERTY_5 = "property5";
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
                    .build())
            .addEntityProperties(EntityProperty.newBuilder()
                    .setNamespace(SDKUtil.VC_TAGS_NAMESPACE)
                    .setName(PROPERTY_5)
                    .setValue(VALUE)
                    .build());

    private final EntityDTO.Builder onto = EntityDTO.newBuilder()
            .addEntityProperties(EntityProperty.newBuilder()
                    .setNamespace(EXISTING_NAMESPACE)
                    .setName(PROPERTY_1)
                    .setValue(VALUE)
                    .build())
            .addEntityProperties(EntityProperty.newBuilder()
                    .setNamespace(EXISTING_NAMESPACE)
                    .setName(PROPERTY_4)
                    .setValue(VALUE)
                    .build());

    private final EntityDTO.Builder duplicateKeyOnto = EntityDTO.newBuilder()
            .addEntityProperties(EntityProperty.newBuilder()
                    .setNamespace(SDKUtil.VC_TAGS_NAMESPACE)
                    .setName(PROPERTY_1)
                    .setValue(VALUE)
                    .build())
            .addEntityProperties(EntityProperty.newBuilder()
                    .setNamespace(SDKUtil.VC_TAGS_NAMESPACE)
                    .setName(PROPERTY_1)
                    .setValue(NEW_VALUE)
                    .build())
            .addEntityProperties(EntityProperty.newBuilder()
                    .setNamespace(EXISTING_NAMESPACE)
                    .setName(PROPERTY_2)
                    .setValue(VALUE)
                    .build());

    /**
     * Test merging entity properties using
     * {@link com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata.MergePropertiesStrategy#MERGE_NOTHING}
     * strategy.
     */
    @Test
    public void testMergeNothing() {
        final PropertiesMerger propertiesMerger = new PropertiesMerger(
                MergePropertiesStrategy.MERGE_NOTHING);
        final Map<String, Integer> ontoPropertyMap = propertiesMerger.ontoPropertyMap(onto);
        propertiesMerger.merge(from, onto, ontoPropertyMap);

        // Resulting entity should contain properties from both "onto" and "from" DTOs
        final Set<String> resultingProperties = onto.getEntityPropertiesList()
                .stream()
                .map(property -> property.getNamespace() + property.getName())
                .collect(Collectors.toSet());
        final Set<String> expectedProperties = ImmutableSet.of(EXISTING_NAMESPACE + PROPERTY_1,
                EXISTING_NAMESPACE + PROPERTY_4);
        Assert.assertEquals(expectedProperties, resultingProperties);
    }

    /**
     * Test merging entity properties using
     * {@link com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata.MergePropertiesStrategy#MERGE_IF_NOT_PRESENT}
     * strategy.
     */
    @Test
    public void testMergeIfNotPresent() {
        final PropertiesMerger propertiesMerger = new PropertiesMerger(
                MergePropertiesStrategy.MERGE_IF_NOT_PRESENT);
        final Map<String, Integer> ontoPropertyMap = propertiesMerger.ontoPropertyMap(onto);
        propertiesMerger.merge(from, onto, ontoPropertyMap);

        // Resulting entity should contain properties from both "onto" and "from" DTOs
        final Set<String> resultingProperties = onto.getEntityPropertiesList()
                .stream()
                .map(property -> property.getNamespace() + property.getName())
                .collect(Collectors.toSet());
        final Set<String> expectedProperties = ImmutableSet.of(EXISTING_NAMESPACE + PROPERTY_1,
                EXISTING_NAMESPACE + PROPERTY_2, NEW_NAMESPACE + PROPERTY_3,
                EXISTING_NAMESPACE + PROPERTY_4, SDKUtil.VC_TAGS_NAMESPACE + PROPERTY_5);
        Assert.assertEquals(expectedProperties, resultingProperties);

        // Verify that the property value was NOT overridden
        Optional<EntityProperty> property = onto.getEntityPropertiesList()
                .stream()
                .filter(p -> PROPERTY_1.equals(p.getName()))
                .findFirst();
        Assert.assertTrue(property.isPresent());
        Assert.assertEquals(VALUE, property.get().getValue());
    }

    /**
     * Test merging entity properties using
     * {@link com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata.MergePropertiesStrategy#MERGE_AND_OVERWRITE}
     * strategy.
     */
    @Test
    public void testMergeAndOverwrite() {
        final PropertiesMerger propertiesMerger = new PropertiesMerger(
                MergePropertiesStrategy.MERGE_AND_OVERWRITE);
        final Map<String, Integer> ontoPropertyMap = propertiesMerger.ontoPropertyMap(onto);
        propertiesMerger.merge(from, onto, ontoPropertyMap);

        // Resulting entity should contain properties from both "onto" and "from" DTOs
        final Set<String> resultingProperties = onto.getEntityPropertiesList()
                .stream()
                .map(property -> property.getNamespace() + property.getName())
                .collect(Collectors.toSet());
        final Set<String> expectedProperties = ImmutableSet.of(EXISTING_NAMESPACE + PROPERTY_1,
                EXISTING_NAMESPACE + PROPERTY_2, NEW_NAMESPACE + PROPERTY_3,
                EXISTING_NAMESPACE + PROPERTY_4, SDKUtil.VC_TAGS_NAMESPACE + PROPERTY_5);
        Assert.assertEquals(expectedProperties, resultingProperties);

        // Verify that the property value was overridden
        Optional<EntityProperty> property = onto.getEntityPropertiesList()
                .stream()
                .filter(p -> PROPERTY_1.equals(p.getName()))
                .findFirst();
        Assert.assertTrue(property.isPresent());
        Assert.assertEquals(NEW_VALUE, property.get().getValue());
    }

    /**
     * Test merging entity properties with duplicate property keys for VCTAGS namespace.
     */
    @Test
    public void testMergeDuplicatePropertyKeys() {
        final PropertiesMerger propertiesMerger = new PropertiesMerger(
                MergePropertiesStrategy.MERGE_AND_OVERWRITE);
        final Map<String, Integer> ontoPropertyMap = propertiesMerger.ontoPropertyMap(
                duplicateKeyOnto);
        Assert.assertTrue(ontoPropertyMap.size() == 1);
        Assert.assertEquals(Integer.valueOf(2), ontoPropertyMap.get(EXISTING_NAMESPACE.concat("::").concat(PROPERTY_2)));
    }
}
