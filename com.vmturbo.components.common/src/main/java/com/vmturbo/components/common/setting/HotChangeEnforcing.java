/*
 * (C) Turbonomic 2020.
 */

package com.vmturbo.components.common.setting;

/**
 * {@link HotChangeEnforcing} describe possible values for hot change enforcing settings.
 */
public enum HotChangeEnforcing {
    /**
     * Resize actions will execute in a disruptive way unless “Enforce non disruptive mode” is true
     * and action occurs outside of a Scheduled Policy Window.
     */
    DISRUPTIVE,
    /**
     * Resize actions will execute in a non-disruptive way.
     */
    NON_DISRUPTIVE,
    /**
     * Resize actions will execute using discovered hot add/remove capabilities, unless the action
     * will be disruptive and “Enforce non disruptive mode” is true and action occurs outside of a
     * Scheduled Policy Window.
     */
    DISCOVERED;
}
