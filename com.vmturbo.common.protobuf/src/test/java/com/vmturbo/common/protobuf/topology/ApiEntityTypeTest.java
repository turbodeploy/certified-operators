package com.vmturbo.common.protobuf.topology;

import static org.junit.Assert.assertEquals;

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
}
