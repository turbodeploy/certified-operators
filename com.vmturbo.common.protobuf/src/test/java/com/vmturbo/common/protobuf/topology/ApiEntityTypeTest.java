package com.vmturbo.common.protobuf.topology;

import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.util.Set;

import com.google.common.collect.ImmutableSet;

import org.hamcrest.collection.IsIterableContainingInAnyOrder;
import org.junit.Test;

import com.vmturbo.platform.common.dto.CommonDTOREST.EntityDTO.EntityType;

/**
 * Testing class for {@link ApiEntityType} enums.
 */
public class ApiEntityTypeTest {

    /**
     * Test converting existing {@link ApiEntityType} string to {@link EntityType} numeric value.
     */
    @Test
    public void testFromStringToSdkType() {
        //GIVEN
        String pm = ApiEntityType.PHYSICAL_MACHINE.apiStr();

        //THEN
        assertEquals(EntityType.PHYSICAL_MACHINE.getValue(), ApiEntityType.fromStringToSdkType(pm));
    }

    /**
     * Test null {@link ApiEntityType} string to {@link EntityType} numeric value.
     *
     * <p>Null should return EntityType.Unknown value</p>
     */
    @Test
    public void testFromStringToSdkTypeWithNullType() {
        assertEquals(EntityType.UNKNOWN.getValue(), ApiEntityType.fromStringToSdkType(null));
    }

    /**
     * Tests converting {@link EntityType} numeric value to {@link ApiEntityType} string.
     */
    @Test
    public void testFromSdkTypeToEntityTypeString() {
        //GIVEN
        int pm = EntityType.PHYSICAL_MACHINE.getValue();

        //THEN
        assertEquals(ApiEntityType.PHYSICAL_MACHINE.apiStr(),
                ApiEntityType.fromSdkTypeToEntityTypeString(pm));
    }

    /**
     * Tests unsupported {@link EntityType} numeric value to {@link ApiEntityType} string.
     *
     * <p>Unsupported value should return {@link ApiEntityType} UNKNOWN</p>
     */
    @Test
    public void testFromSdkTypeToEntityTypeStringWithUnsupportedValue() {
        assertEquals(ApiEntityType.UNKNOWN.apiStr(), ApiEntityType.fromSdkTypeToEntityTypeString(-1));
    }

    /**
     * Set of entity types that should not be expanded.
     */
    private static final Set<ApiEntityType> ENTITY_TYPES_NOT_TO_EXPAND = ImmutableSet.of(
            ApiEntityType.VIRTUAL_MACHINE,
            ApiEntityType.PHYSICAL_MACHINE,
            ApiEntityType.STORAGE,
            ApiEntityType.CONTAINER,
            ApiEntityType.COMPUTE_TIER,
            ApiEntityType.STORAGE_TIER,
            ApiEntityType.DATABASE_TIER,
            ApiEntityType.DATABASE_SERVER_TIER
    );

    /**
     * Test in case that someone modify {@link ApiEntityType#ENTITY_TYPES_TO_EXPAND} by mistake.
     */
    @Test
    public void testEntityTypesThatShouldNotBeExpanded() {
        assertThat(ApiEntityType.ENTITY_TYPES_TO_EXPAND.keySet(),
                not(IsIterableContainingInAnyOrder.containsInAnyOrder(ENTITY_TYPES_NOT_TO_EXPAND.toArray())));
    }
}
