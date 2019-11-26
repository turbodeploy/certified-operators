package com.vmturbo.securekvstore;

import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * A wrapper object to interact with HashiCorp Vault with Spring-Vault library.
 */
public class ObjectWrapper {

    private String key;
    private String value;

    /**
     * Default constructor is needed for auto serialization/deserialization.
     */
    ObjectWrapper() {}

    /**
     * Constructor for this wrapper.
     *
     * @param key key
     * @param value value
     */
    public ObjectWrapper(@Nonnull String key, @Nonnull String value) {
        this.key = Objects.requireNonNull(key);
        this.value = Objects.requireNonNull(value);
    }

    /**
     * Getter.
     *
     * @return the key.
     */
    @Nullable
    public String getKey() {
        return key;
    }

    /**
     * Getter.
     *
     * @return return the value.
     */
    @Nullable
    public String getValue() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ObjectWrapper that = (ObjectWrapper)o;
        return Objects.equals(key, that.key) && Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {

        return Objects.hash(key, value);
    }
}
