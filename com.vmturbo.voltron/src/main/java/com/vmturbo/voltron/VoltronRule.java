package com.vmturbo.voltron;

import javax.annotation.Nonnull;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * Class-level rule that can be used to set up a Voltron instance in a system/integration test.
 */
public class VoltronRule implements TestRule {

    private final VoltronConfiguration configuration;

    private VoltronsContainer voltronsContainer = null;

    /**
     * Create the rule. Do this in a @ClassRule annotated field.
     *
     * @param voltronConfiguration The configuration for the voltron instance.
     */
    public VoltronRule(@Nonnull final VoltronConfiguration voltronConfiguration) {
        this.configuration = voltronConfiguration;
    }

    @Override
    public Statement apply(Statement base, Description description) {

        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                try {
                    setup(description);
                    base.evaluate();
                } finally {
                    teardown();
                }
            }
        };
    }

    public VoltronsContainer getVoltronsContainer() {
        return voltronsContainer;
    }

    private void teardown() {
        voltronsContainer.demolish();
    }

    private void setup(Description description) {
        String namespace = description.getTestClass().getSimpleName();
        voltronsContainer = Voltron.start(namespace, configuration);
    }
}
