package com.vmturbo.components.common.utils;

import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Before;
import org.junit.Test;
import org.springframework.mock.env.MockEnvironment;

import com.vmturbo.components.common.utils.FeatureFlagImpl.FeatureFlagConfig;

/**
 * Tests of {@link FeatureFlagImpl} classes.
 */
public class FeatureFlagTest {

    // ensure that the FeatureFlag class is fully initialized when executing this test class, so
    // that FeatureFlagImpl.ALL_FEATURE_FLAGS will not be empty
    @SuppressWarnings("unused")
    private static final FeatureFlag DUMMY = FeatureFlag.DUMMY;

    /**
     * Make sure that feature flag resolutions do not leak from one test to another.
     */
    @Before
    public void before() {
        FeatureFlagImpl.clearEnvironment();
    }

    /**
     * Ensure that feature flag definitions are in place, else a few other tests in this class would
     * be pointless.
     */
    @Test
    public void testThatFeatureFlagDefinitionsArePresent() {
        FeatureFlagImpl.resolveAll(new MockEnvironment());
        assertThat(DUMMY, in(FeatureFlagImpl.ALL_FEATURE_FLAGS));
    }

    /**
     * Test that when an environment is injected via {@link FeatureFlagConfig}, its effects are seen
     * in feature flag enablement.
     */
    @Test
    public void testThatEnvironmentInjectionWorks() {
        new FeatureFlagConfig().setEnvironment(new MockEnvironment()
                .withProperty(FeatureFlag.FEATURE_FLAG_PREFIX + "dummy", "true"));
        assertThat(DUMMY.isEnabled(), is(true));
    }

    /**
     * Make sure that resolving feature flags with configured properties will leave them with
     * correct enablement. This includes the case of a missing property, which should leave the
     * corresponding feature flag to be disabled.
     */
    @Test
    public void testThatFeatureEnablementWorks() {
        final FeatureFlag enabled = new FeatureFlag("Enabled", "enabled");
        final FeatureFlag disabled = new FeatureFlag("Enabled", "disabled");
        final FeatureFlag unset = new FeatureFlag("Unset", "unset");
        FeatureFlagImpl.resolveAll(new MockEnvironment()
                .withProperty(FeatureFlagImpl.FEATURE_FLAG_PREFIX + "enabled", "true")
                .withProperty(FeatureFlagImpl.FEATURE_FLAG_PREFIX + "disabled", "false")
        );
        assertThat(enabled.isEnabled(), is(true));
        assertThat(disabled.isEnabled(), is(false));
        assertThat(unset.isEnabled(), is(false));
    }

    /**
     * Test that code executing under the various #ifEnabled(...) methods obey enablement.
     *
     * @throws Exception if executed code throws an exception
     */
    @Test
    public void testThatConditionalCodeExecutionWorks() throws Exception {
        final FeatureFlag enabled = new FeatureFlag("Enabled", "enabled");
        final FeatureFlag disabled = new FeatureFlag("Enabled", "disabled");
        FeatureFlagImpl.resolveAll(new MockEnvironment()
                .withProperty(FeatureFlagImpl.FEATURE_FLAG_PREFIX + "enabled", "true")
        );
        AtomicInteger counter = new AtomicInteger(0);
        enabled.ifEnabled((Runnable)counter::incrementAndGet);
        assertThat(counter.get(), is(1));
        disabled.ifEnabled((Runnable)counter::incrementAndGet);
        assertThat(counter.get(), is(1));

        assertThat(enabled.ifEnabled(() -> 1, 0), is(1));
        assertThat(disabled.ifEnabled(() -> 1, 0), is(0));

        assertThat(enabled.ifEnabled(() -> 1), is(1));
        assertThat(disabled.ifEnabled(() -> 1), is(nullValue()));
    }

    /**
     * Make sure that accessors return correct values.
     */
    @Test
    public void testThatBasicAccessorsWork() {
        FeatureFlag.resolveAll(new MockEnvironment());
        final FeatureFlag ff1 = new FeatureFlag("FF1", "ff1");
        final FeatureFlag ff2 = new FeatureFlag("FF2", "ff2");
        assertThat(ff1.getName(), is("FF1"));
        assertThat(ff2.getName(), is("FF2"));
        assertThat(ff1.toString(), is("FF<FF1=false>"));
        assertThat(ff2.toString(), is("FF<FF2=false>"));
    }

    /**
     * Test tha the blocking and interruption-handling logic in {@link FeatureFlagImpl#isEnabled()}
     * works as intended.
     *
     * @throws InterruptedException if this thread is interrupted while sleeping
     */
    @Test
    public void testThatBlockedEnablementCheckWorks() throws InterruptedException {
        final AtomicReference<Boolean> enabled = new AtomicReference<>();
        final AtomicReference<Boolean> interrupted = new AtomicReference<>();

        // first try interrupting a blocked enablement-checking flag before resolution occurs
        FeatureFlag testFF = new FeatureFlag("Test", "test");
        Thread thread = new Thread(() -> {
            enabled.set(testFF.isEnabled());
            interrupted.set(Thread.currentThread().isInterrupted());
        });
        thread.start();
        Thread.sleep(100L);
        assertThat(enabled.get(), is(nullValue()));
        assertThat(thread.isAlive(), is(true));
        thread.interrupt();
        Thread.sleep(100L);
        assertThat(enabled.get(), is(false));
        assertThat(interrupted.get(), is(true));
        assertThat(thread.isAlive(), is(false));

        // reset for another test without interrupting the blocked thread
        enabled.set(null);
        interrupted.set(null);
        FeatureFlagImpl.clearEnvironment();
        // and then test that scenario
        thread = new Thread(() -> {
            enabled.set(testFF.isEnabled());
            interrupted.set(Thread.currentThread().isInterrupted());
        });
        thread.start();
        Thread.sleep(100L);
        assertThat(thread.isAlive(), is(true));
        FeatureFlag.resolveAll(new MockEnvironment()
                .withProperty(FeatureFlag.FEATURE_FLAG_PREFIX + "test", "true"));
        Thread.sleep(100L);
        assertThat(enabled.get(), is(true));
        assertThat(interrupted.get(), is(false));
        assertThat(thread.isAlive(), is(false));
    }
}
