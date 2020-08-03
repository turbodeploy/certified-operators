package com.vmturbo.action.orchestrator.action;

import javax.annotation.Nullable;

/**
 * Enumeration of action visibility levels. The following levels are available:
 * <ol>
 * <li>actions that are always visible</li>
 * <li>action that are not visible by default but can be queried with a special request</li>
 * <li>actions that are always hidden.</li>
 * </ol>
 */
public enum VisibilityLevel {
    /**
     * Defines an action that is visible to all components.
     */
    ALWAYS_VISIBLE,

    /**
     * Defines an action that is not visible to other components by default. However such action
     * can be visible if request explicitly specifies that result must include hidden actions.
     */
    HIDDEN_BY_DEFAULT,

    /**
     * Defines an action that is only visible inside of Action Orchestrator and is never exposed
     * to other components.
     */
    ALWAYS_HIDDEN;

    /**
     * Check if visibility level applies to requested visibility flag.
     *
     * @param visible Visibility flag. If it is {@code true} only visible actions are returned.
     * If it is {@code false} only invisible by default actions are returned. If it is null then
     * both visible and invisible by default actions are returned.
     * @return True if visibility level applies to requested visibility flag.
     */
    public boolean checkVisibility(final @Nullable Boolean visible) {
        if (visible == null) {
            return this == ALWAYS_VISIBLE || this == HIDDEN_BY_DEFAULT;
        } else if (visible) {
            return this == ALWAYS_VISIBLE;
        } else {
            return this == HIDDEN_BY_DEFAULT;
        }
    }
}
