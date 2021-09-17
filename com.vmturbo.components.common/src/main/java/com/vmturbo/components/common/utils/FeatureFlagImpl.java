package com.vmturbo.components.common.utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;

import com.google.common.annotations.VisibleForTesting;

import org.jetbrains.annotations.NotNull;
import org.springframework.context.EnvironmentAware;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

/**
 * Class to implement feature flags that can be used throughout the XL code base.
 *
 * <p>A {@link FeatureFlagImpl} for a given product feature can be used to manage execution of
 * feature-related code while the feature is in initial development, generally extending until it
 * has reached GA status, often having first gone through a successful preview period with selected
 * customers. The general rule for features in development is that feature-related code must have no
 * visible impact in installations where the feature is not explicitly enabled.</p>
 *
 * <p>Once a feature has reached GA status, its feature flag should be scheduled for removal,
 * by configuring a release number that will be its target release for retirement.</p>
 */

public class FeatureFlagImpl {

    protected static final List<FeatureFlagImpl> ALL_FEATURE_FLAGS = new ArrayList<>();
    private static CountDownLatch readyLatch = new CountDownLatch(1);

    static final String FEATURE_FLAG_PREFIX = "featureFlags.";
    private static Environment environment;
    private final String name;
    private final String enablementProperty;

    // attempts to test enablement will block until this is set properly from the spring
    // environment, so initialization here is immaterial
    private boolean enabled;

    /**
     * Create a new instance with the given feature name and enablement property name.
     *
     * <p>If we have received an {@link Environment} to resolve enablement, this new flag
     * is resolved immediately. Otherwise, it will be resolved when an {@link Environment}
     * arrives.</p>
     *
     * @param name               feature flag name
     * @param enablementProperty boolean property that conveys this flag's enablement
     */
    protected FeatureFlagImpl(String name, String enablementProperty) {
        this.name = name;
        this.enablementProperty = enablementProperty;
        if (environment != null) {
            resolve(environment);
        }
        ALL_FEATURE_FLAGS.add(this);
    }


    public static List<FeatureFlagImpl> getAllFeatureFlags() {
        return Collections.unmodifiableList(ALL_FEATURE_FLAGS);
    }


    /**
     * Test whether this feature flag is enabled.
     *
     * <p>If we have not yet received the Spring {@link Environment} to resolve feature flag
     * enablement, this method will block until it arrives.</p>
     *
     * @return true if this feature flag is enabled
     */
    public boolean isEnabled() {
        try {
            readyLatch.await();
            return enabled;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    /**
     * Execute the given code if this feature flag is enabled.
     *
     * <p>This will block if the enablement has not yet been resolved.</p>
     *
     * @param code code to be executed if enabled
     */
    public void ifEnabled(Runnable code) {
        if (isEnabled()) {
            code.run();
        }
    }

    /**
     * Execute the given code and return the value it produces, if this feature flag is enabled.
     *
     * <p>If enablement has not yet been resolved, this method will block waiting for that.</p>
     *
     * @param code            code to  be executed to produce a value, if enabled
     * @param valueIfDisabled value to return if the feature is disabled
     * @param <T>             type of value produced
     * @return computed value if enabled, else valueIfDisabled
     * @throws Exception if the code throws an exception
     */
    public <T> T ifEnabled(Callable<T> code, T valueIfDisabled) throws Exception {
        return isEnabled() ? code.call() : valueIfDisabled;
    }

    /**
     * Execute the given code and return the value it produces, if this feature flag is enabled,
     * else return null.
     *
     * <p>If enablement has not yet been resolved, this method will block waiting for that.</p>
     *
     * @param code code to  be executed to produce a value, if enabled
     * @param <T>  type of value produced
     * @return computed value if enabled, else null
     * @throws Exception if the code throws an exception
     */
    public <T> T ifEnabled(Callable<T> code) throws Exception {
        return ifEnabled(code, null);
    }

    public String getName() {
        return name;
    }

    static void resolveAll(Environment environment) {
        getAllFeatureFlags().forEach(ff -> ff.resolve(environment));
        FeatureFlagImpl.environment = environment;
        readyLatch.countDown();

    }

    /**
     * This method can be used in a @Before-method in a test class to ensure that resolution
     * performed in one test does not affect a subsequent test.
     */
    @VisibleForTesting
    static void clearEnvironment() {
        FeatureFlagImpl.environment = null;
        readyLatch = new CountDownLatch(1);
    }


    private void resolve(Environment environment) {
        this.enabled = environment.getProperty(FEATURE_FLAG_PREFIX + enablementProperty,
                Boolean.class, false);
    }

    @Override
    public String toString() {
        return String.format("FF<%s=%s>", name, isEnabled());
    }

    /**
     * Class that supplies the Spring {@link Environment} needed for enablement resolution when it is
     * provided by Spring.
     */
    @Configuration
    public static class FeatureFlagConfig implements EnvironmentAware {

        @Override
        public void setEnvironment(@NotNull final Environment environment) {
            FeatureFlagImpl.resolveAll(environment);
        }
    }
}
