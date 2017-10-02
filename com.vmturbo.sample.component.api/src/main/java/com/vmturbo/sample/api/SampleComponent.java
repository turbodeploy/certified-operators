package com.vmturbo.sample.api;

import javax.annotation.Nonnull;

/**
 * The {@link SampleComponent} is the Java interface that allows
 * other components to register for notifications that the sample
 * component emits.
 */
public interface SampleComponent extends AutoCloseable {

    void addEchoListener(@Nonnull final EchoListener listener);

}
