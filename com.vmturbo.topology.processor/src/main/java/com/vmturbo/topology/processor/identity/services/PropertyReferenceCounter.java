package com.vmturbo.topology.processor.identity.services;

/**
 * The PropertyReferenceCounter implements integer property reference counter.
 * The reason behind its existence is twofold:
 * <ul>
 * <li>Avoid creation of new object on every increment.
 * <li>Provide an easy identification of what it is in the complex generic type.
 * </ul>
 */
final class PropertyReferenceCounter extends Number {

    /**
     * The serial version UUID.
     */
    private static final long serialVersionUID = -5547707968601659412L;

    private int value;

    PropertyReferenceCounter() {
        value = 0;
    }

    @Override
    public int intValue() {
        return value;
    }

    @Override
    public long longValue() {
        return value;
    }

    @Override
    public float floatValue() {
        return value;
    }

    @Override
    public double doubleValue() {
        return value;
    }

    int increment() {
        return value++;
    }
}
