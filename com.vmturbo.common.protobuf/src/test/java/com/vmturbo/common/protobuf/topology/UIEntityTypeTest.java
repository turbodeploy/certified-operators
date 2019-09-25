package com.vmturbo.common.protobuf.topology;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.vmturbo.platform.common.dto.CommonDTOREST.EntityDTO.EntityType;

/**
 * Testing class for {@link UIEntityType} enums.
 */
public class UIEntityTypeTest {

    /**
     * Test converting existing {@link UIEntityType} string to {@link EntityType} numeric value.
     */
    @Test
    public void testFromStringToSdkType() {
        //GIVEN
        String pm = UIEntityType.PHYSICAL_MACHINE.apiStr();

        //THEN
        assertEquals(EntityType.PHYSICAL_MACHINE.getValue(), UIEntityType.fromStringToSdkType(pm));
    }

    /**
     * Test null {@link UIEntityType} string to {@link EntityType} numeric value.
     *
     * <p>Null should return EntityType.Unknown value</p>
     */
    @Test
    public void testFromStringToSdkTypeWithNullType() {
        assertEquals(EntityType.UNKNOWN.getValue(), UIEntityType.fromStringToSdkType(null));
    }

    /**
     * Tests converting {@link EntityType} numeric value to {@link UIEntityType} string.
     */
    @Test
    public void testFromSdkTypeToEntityTypeString() {
        //GIVEN
        int pm = EntityType.PHYSICAL_MACHINE.getValue();

        //THEN
        assertEquals(UIEntityType.PHYSICAL_MACHINE.apiStr(),
                UIEntityType.fromSdkTypeToEntityTypeString(pm));
    }

    /**
     * Tests unsupported {@link EntityType} numeric value to {@link UIEntityType} string.
     *
     * <p>Unsupported value should return {@link UIEntityType} UNKNOWN</p>
     */
    @Test
    public void testFromSdkTypeToEntityTypeStringWithUnsupportedValue() {
        assertEquals(UIEntityType.UNKNOWN.apiStr(), UIEntityType.fromSdkTypeToEntityTypeString(-1));
    }
}
