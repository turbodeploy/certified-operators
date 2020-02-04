package com.vmturbo.mediation.actionscript.parameter;

import java.util.Objects;

import javax.annotation.Nonnull;

/**
 * The default variables that are exposed to all scripts, if the corresponding data is available.
 * If the data is not available or not applicable for a particular action, the corresponding
 * variable is not injected.
 */
public enum ActionScriptParameterDefinition {

    VMT_ACTION_INTERNAL("The UUID of the action"),
    VMT_ACTION_NAME("The name of the action"),
    VMT_CURRENT_INTERNAL("The name of the current provider"),
    VMT_CURRENT_NAME("The display name of the current provider"),
    //VMT_CURRENT_COMMODITY("The name of the current commodity"),
    VMT_NEW_INTERNAL("The name of the new provider"),
    VMT_NEW_NAME("The display name of the new provider"),
    //VMT_NEW_COMMODITY("The name of the new commodity"),
    VMT_TARGET_INTERNAL("The name of the entity being acted upon"),
    VMT_TARGET_NAME("The display name of the entity being acted upon"),
    VMT_TARGET_UUID("The UUID of the entity being acted upon");

    private final String description;

    ActionScriptParameterDefinition(@Nonnull final String description) {
        this.description = Objects.requireNonNull(description);
    }

    public String getDescription() {
        return description;
    }
}
