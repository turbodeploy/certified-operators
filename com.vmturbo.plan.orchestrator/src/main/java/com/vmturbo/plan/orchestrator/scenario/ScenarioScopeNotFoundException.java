package com.vmturbo.plan.orchestrator.scenario;

import java.util.Set;

import javax.annotation.Nonnull;

/**
 * Exception to be thrown when the scope entries provided in the scenario does not exist in the
 * system.
 */
public class ScenarioScopeNotFoundException extends Exception {

    /**
     * Constructor for ScenarioScopeNotFoundException.
     *
     * @param scopesNotFound set of scope ids which can not be found.
     */
    public ScenarioScopeNotFoundException(@Nonnull Set<Long> scopesNotFound) {
        super("These entries provided in the scenario scope list do not exist: " + scopesNotFound);
    }
}
