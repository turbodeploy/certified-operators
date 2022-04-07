package com.vmturbo.api.component.external.api.util;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import com.vmturbo.api.component.external.api.mapper.MarketMapper;
import com.vmturbo.api.enums.EntityType;
import com.vmturbo.api.enums.GroupType;
import com.vmturbo.common.protobuf.utils.StringConstants;

/**
 * Unit tests for {@link ValidationUtils}.
 */
public class ValidationUtilsTest {

    /**
     * Test that {@link ValidationUtils#validateExternalEntityTypes(Collection)} does not throw an
     * exception when a valid {@link EntityType} string is passed to the function.
     */
    @Test
    public void testApiEntityTypeStringIsValid() {
        ValidationUtils.validateExternalEntityTypes(
                Collections.singleton(EntityType.VirtualMachine.toString()));
    }

    /**
     * Test that {@link ValidationUtils#validateExternalEntityTypes(Collection)} does not throw an
     * exception when the value {@link StringConstants#WORKLOAD} is passed to the function.
     */
    @Test
    public void testWorkloadStringIsValid() {
        ValidationUtils.validateExternalEntityTypes(Collections.singleton(StringConstants.WORKLOAD));
    }

    /**
     * Test that {@link ValidationUtils#validateExternalEntityTypes(Collection)} throws an
     * {@link IllegalArgumentException} when an invalid string is passed to the function.
     */
    @Test
    public void testExceptionThrownWhenEntityTypeStringIsInvalid() {
        Assertions.assertThatThrownBy(() -> ValidationUtils.validateExternalEntityTypes(Collections.singleton("invalid")))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * Test that {@link ValidationUtils#validateExternalEntityTypes(Collection)} throws an
     * {@link IllegalArgumentException} when null string is passed to the function.
     */
    @Test
    public void testExceptionThrownWhenEntityTypesStringIsNull() {
        Assertions.assertThatThrownBy(() -> ValidationUtils.validateExternalEntityTypes(Collections.singleton(null)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * Test that {@link ValidationUtils#validateExternalEntityTypes(Collection)} does not throw an
     * exception when a collection of valid strings is passed to the function.
     */
    @Test
    public void testValidEntityTypesCollection() {
        ValidationUtils.validateExternalEntityTypes(
                Arrays.asList(EntityType.VirtualMachine.toString(), StringConstants.WORKLOAD,
                        EntityType.Container.toString()));
    }

    /**
     * Test that {@link ValidationUtils#validateExternalEntityTypes(Collection)} does not throw an
     * exception when an empty collection is passed to the function.
     */
    @Test
    public void testEmptyEntityTypesCollectionIsValid() {
        ValidationUtils.validateExternalEntityTypes(Collections.emptyList());
    }

    /**
     * Test that {@link ValidationUtils#validateExternalEntityTypes(Collection)} does not throw an
     * exception when null is passed to the function.
     */
    @Test
    public void testNullEntityTypesCollectionIsValid() {
        ValidationUtils.validateExternalEntityTypes(null);
    }

    /**
     * Test that {@link ValidationUtils#validateExternalEntityTypes(Collection)} throws an
     * {@link IllegalArgumentException} when a collection containing at least one invalid string
     * is passed to the function.
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
     * is passed to the function.
     */
    @Test
    public void testExceptionThrownWhenEntityTypesCollectionHasNullElement() {
        Assertions.assertThatThrownBy(() -> ValidationUtils.validateExternalEntityTypes(
                Arrays.asList(EntityType.VirtualMachine.toString(), null))).isInstanceOf(
                IllegalArgumentException.class);
    }

    /**
     * Test that {@link ValidationUtils#validateGroupEntityTypes(Collection)} does not throw an
     * exception when an empty collection is passed to the function.
     */
    @Test
    public void testEmptyGroupEntityTypesCollectionIsValid() {
        ValidationUtils.validateGroupEntityTypes(Collections.emptyList());
    }

    /**
     * Test that {@link ValidationUtils#validateGroupEntityTypes(Collection)} does not throw an
     * exception when null is passed to the function.
     */
    @Test
    public void testNullGroupEntityTypesCollectionIsValid() {
        ValidationUtils.validateGroupEntityTypes(null);
    }

    /**
     * Test that {@link ValidationUtils#validateGroupEntityTypes(Collection)} does not throw an
     * exception when a valid {@link EntityType} string is passed to the function.
     */
    @Test
    public void testEntityTypeAsValidGroupEntityType() {
        ValidationUtils.validateGroupEntityTypes(Collections.singleton(EntityType.VirtualMachine.toString()));
    }

    /**
     * Test that {@link ValidationUtils#validateGroupEntityTypes(Collection)} does not throw an
     * exception when a valid {@link GroupType} string is passed to the function.
     */
    @Test public void testGroupTypeAsValidGroupEntityType() {
        ValidationUtils.validateGroupEntityTypes(Collections.singleton(GroupType.Group.toString()));
    }

    /**
     * Test that {@link ValidationUtils#validateGroupEntityTypes(Collection)} throws an
     * {@link IllegalArgumentException} when a collection containing an invalid string
     * is passed to the function.
     */
    @Test
    public void testExceptionThrownWhenGroupEntityTypeStringIsInvalid() {
        Assertions.assertThatThrownBy(() -> ValidationUtils.validateGroupEntityTypes(Collections.singleton("invalid")))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * Test that {@link ValidationUtils#validateGroupEntityTypes(Collection)} throws an
     * {@link IllegalArgumentException} when null string is passed to the function.
     */
    @Test
    public void testExceptionThrownWhenGroupEntityTypesStringIsNull() {
        Assertions.assertThatThrownBy(() -> ValidationUtils.validateGroupEntityTypes(Collections.singleton(null)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * Test that {@link ValidationUtils#validateGroupEntityTypes(Collection)} throws an
     * {@link IllegalArgumentException} when a collection contains both valid and invalid strings.
     */
    @Test
    public void testExceptionThrowWhenGroupEntityTypesIncludesInvalid() {
        Assertions.assertThatThrownBy(() -> ValidationUtils.validateGroupEntityTypes(
                Arrays.asList(GroupType.Group.toString(), EntityType.VirtualMachine.toString(), null)
        )).isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * Test that {@link ValidationUtils#validateSearchableObjTypes(Collection)} does not throw an
     * exception when an empty collection is passed to the funciton.
     */
    @Test
    public void testEmptyObjTypeCollectionIsValid() {
        ValidationUtils.validateSearchableObjTypes(Collections.emptyList());
    }

    /**
     * Test that {@link ValidationUtils#validateSearchableObjTypes(Collection)} does not throw an
     * exception when null is passed to the function.
     */
    @Test
    public void testNullObjTypeCollectionIsValid() {
        ValidationUtils.validateSearchableObjTypes(null);
    }

    /**
     * Test that {@link ValidationUtils#validateSearchableObjTypes(Collection)} does not throw an
     * exception when a valid {@link EntityType} string is passed to the function.
     */
    @Test
    public void testEntityTypeAsValidObjType() {
        ValidationUtils.validateSearchableObjTypes(Collections.singleton(EntityType.VirtualMachine.toString()));
    }

    /**
     * Test that {@link ValidationUtils#validateSearchableObjTypes(Collection)} does not throw an
     * exception when a valid {@link GroupType} string is passed to the function.
     */
    @Test
    public void testGroupTypeAsValidObjType() {
        ValidationUtils.validateSearchableObjTypes(Collections.singleton(GroupType.Group.toString()));
    }

    /**
     * Test that {@link ValidationUtils#validateSearchableObjTypes(Collection)} does not throw an
     * exception when the values {@link StringConstants#WORKLOAD} or {@link MarketMapper#MARKET} is
     * passed to the function.
     */
    @Test
    public void testConstantsAsValidObjTypes() {
        ValidationUtils.validateSearchableObjTypes(Arrays.asList(MarketMapper.MARKET, StringConstants.TARGET));
    }

    /**
     * Test that {@link ValidationUtils#validateSearchableObjTypes(Collection)} throws an
     * {@link IllegalArgumentException} when a collection containing an invalid string
     * is passed to the function.
     */
    @Test
    public void testExceptionThrownWhenObjTypeStringIsInvalid() {
        Assertions.assertThatThrownBy(() -> ValidationUtils.validateSearchableObjTypes(Collections.singleton("invalid")))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * Test that {@link ValidationUtils#validateSearchableObjTypes(Collection)} throws an
     * {@link IllegalArgumentException} when null string is passed to the function.
     */
    @Test
    public void testExceptionThrownWhenObjTypesStringIsNull() {
        Assertions.assertThatThrownBy(() -> ValidationUtils.validateSearchableObjTypes(Collections.singleton(null)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * Test that {@link ValidationUtils#validateSearchableObjTypes(Collection)} throws an
     * {@link IllegalArgumentException} when a collection contains both valid and invalid strings.
     */
    @Test
    public void testExceptionThrowWhenObjTypesIncludesInvalid() {
        Assertions.assertThatThrownBy(() -> ValidationUtils.validateSearchableObjTypes(
                Arrays.asList(GroupType.Group.toString(), EntityType.VirtualMachine.toString(), null)
        )).isInstanceOf(IllegalArgumentException.class);
    }
}
