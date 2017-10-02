package com.vmturbo.topology.processor.identity;

/**
 * The VMTPropertyDescriptor implements property descriptor.
 */
public interface PropertyDescriptor {
    /**
     * Returns the property value as a String.
     *
     * @return the property value as a String.
     */
    String getValue();

    /**
     * Returns a relative rank of the property type.
     * The rank is {@code 1}-based (i.e. >= 1), with the {@code 1} being the highest.
     * The properties of the property type with the highest rank will be examined first, and will
     * have biggest impact on the
     * matching.
     *
     * @return The {@code 1}-based rank of the property subclass.
     */
    Integer getPropertyTypeRank();
}
