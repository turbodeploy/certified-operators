package com.vmturbo.api.component.external.api.util;

import com.vmturbo.api.enums.EntityType;
import com.vmturbo.common.protobuf.utils.StringConstants;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import org.assertj.core.api.Assertions;
import org.junit.Test;

public class ValidationUtilsTest {

    /**
     * Test that {@link ValidationUtils#validateExternalEntityType(String)} does not throw an
     * exception when a valid {@link EntityType} string is passed to the function
     */
    @Test
    public void testApiEntityTypeStringIsValid() {
        ValidationUtils.validateExternalEntityType(EntityType.VirtualMachine.toString());
    }

    /**
     * Test that {@link ValidationUtils#validateExternalEntityType(String)} does not throw an
     * exception when the value {@link StringConstants#WORKLOAD} is passed to the function
     */
    @Test
    public void testWorkloadStringIsValid() {
        ValidationUtils.validateExternalEntityType(StringConstants.WORKLOAD);
    }

    /**
     * Test that {@link ValidationUtils#validateExternalEntityType(String)} throws an
     * {@link IllegalArgumentException} when an invalid string is passed to the function
     */
    @Test
    public void testExceptionThrownWhenEntityTypeStringIsInvalid() {
        Assertions.assertThatThrownBy(() -> ValidationUtils.validateExternalEntityType("invalid"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * Test that {@link ValidationUtils#validateExternalEntityType(String)} throws an
     * {@link IllegalArgumentException} when null string is passed to the function
     */
    @Test
    public void testExceptionThrownWhenEntityTypesStringIsNull() {
        Assertions.assertThatThrownBy(() -> ValidationUtils.validateExternalEntityType(null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * Test that {@link ValidationUtils#validateExternalEntityTypes(Collection)} does not throw an
     * exception when a collection of valid strings is passed to the function
     */
    @Test
    public void testValidEntityTypesCollection() {
        ValidationUtils.validateExternalEntityTypes(
                Arrays.asList(EntityType.VirtualMachine.toString(), StringConstants.WORKLOAD,
                        EntityType.Container.toString()));
    }

    /**
     * Test that {@link ValidationUtils#validateExternalEntityTypes(Collection)} does not throw an
     * exception when an empty collection is passed to the function
     */
    @Test
    public void testEmptyEntityTypesCollectionIsValid() {
        ValidationUtils.validateExternalEntityTypes(Collections.emptyList());
    }

    /**
     * Test that {@link ValidationUtils#validateExternalEntityTypes(Collection)} throws an
     * {@link IllegalArgumentException} when a collection containing at least one invalid string
     * is passed to the function
     */
    @Test
    public void testExceptionThrownWhenEntityTypesCollectionHasInvalidElement() {
        Assertions.assertThatThrownBy(() -> ValidationUtils.validateExternalEntityTypes(
                Arrays.asList(EntityType.VirtualMachine.toString(), "invalid"))).isInstanceOf(
                IllegalArgumentException.class);
    }

    /**
     * Test that {@link ValidationUtils#validateExternalEntityTypes(Collection)} throws an
     * {@link IllegalArgumentException} when a collection containing at least one null string
     * is passed to the function
     */
    @Test
    public void testExceptionThrownWhenEntityTypesCollectionHasNullElement() {
        Assertions.assertThatThrownBy(() -> ValidationUtils.validateExternalEntityTypes(
                Arrays.asList(EntityType.VirtualMachine.toString(), null))).isInstanceOf(
                IllegalArgumentException.class);
    }
}
