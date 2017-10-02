package com.vmturbo.components.test.utilities.topology.conversion;

import java.util.function.Consumer;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * An interface for a class that can be converted into an SDK protobuf.
 * @param <T> The proto type that the class can be converted into.
 */
public interface IntoSdkProto<T extends com.google.protobuf.MessageOrBuilder> {
    /**
     * Generate an SDK protobuf equivalent of this object.
     * @return The SDK protobuf equivalent of this object.
     */
    @Nonnull
    T toSdkProto();

    /**
     * Set a value on a protobuf builder if the value is not null. If the value is null,
     * do not call the setter method.
     *
     * @param setter The setter for setting a protobuf builder value.
     * @param value The value to be set if not null.
     * @param <VALUE> The type of the value.
     * @return true if the value was set, false otherwise.
     */
    default <VALUE> boolean conditionallySet(@Nonnull final Consumer<VALUE> setter,
                                             @Nullable final VALUE value) {
        if (value != null) {
            setter.accept(value);
            return true;
        } else {
            return false;
        }
    }
}
