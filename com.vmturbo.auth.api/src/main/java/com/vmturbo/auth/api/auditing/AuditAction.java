package com.vmturbo.auth.api.auditing;

/**
 * Audit actions. They indicate what action is being executed.
 * For new audit entry, please use these actions or add new actions if needed.
 */
public enum AuditAction {
    LOGIN("Login"),
    LOGOUT("Logout"),
    CREATE_USER("Create User"),
    DELETE_USER("Delete User"),
    MODIFY_USER("Modify User"),
    CHANGE_SCOPE("Change Scope"),
    CHANGE_ROLE("Change Role"),
    CHANGE_PASSWORD("Change Password"),
    CHANGE_TYPE("Change Type"),
    UNKNOWN("UNKNOWN"),
    POWER_ON("Power On"),
    SUSPEND_POWER("Suspend Power"),
    POWER_OFF("Power Off"),
    DELETE_CRITICAL_ENTITY("Delete Critical Entity"),
    REMOVE_ENTIRY("Remove Entity"),
    NOTIFY_EXCEPTION("Notify Exception"),
    CHANGE_NOTIFICATION("Change Notification"),
    CLEAR_NOTIFICATION("Clear Notification"),
    DEPLOY_ACTION("Deploy Action"),
    MOVE_VM("Move VM"),
    EXECUTE_ACTION("Execute Action"),
    START_APPLICATION("Start Application"),
    DELETE_APPLICATION("Delete Application"),
    ACCEPT_ACTION("Accept Action"),
    SET_LDAP("Configure LDAP"),
    CHECK_AUTHORIZATION("Check Permission"),
    ACCESS_USER("Access User"),
    SYSTEM_INIT("System Initialization"),
    CREATE_GROUP("Create External Group"),
    CHANGE_GROUP("Change External Group Role"),
    DELETE_GROUP("Delete External Group"),
    CREATE_POLICY("Create Policy"),
    DELETE_POLICY("Delete Policy"),
    CHANGE_POLICY("Change Policy");

    private String displayName;

    AuditAction(String displayName) {
        this.displayName = displayName;
    }

    public String displayName() {
        return displayName;
    }
}