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
    CREATE_WIDGETSET("Create Widget Set"),
    UPDATE_WIDGETSET("Update Widget Set"),
    DELETE_WIDGETSET("Delete Widget Set"),
    TRANSFER_WIDGETSET("Transfer Widget Set"),
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
    /**
     * Successfully accepted action will start execution immediately.
     */
    ACCEPT_ACTION("Accept Action"),
    /**
     * Successfully accepted action will be executed when execution schedule will be active.
     */
    ACCEPT_SCHEDULED_ACTION("Accept Scheduled Action"),
    SET_LDAP("Configure LDAP"),
    CHECK_AUTHORIZATION("Check Permission"),
    ACCESS_USER("Access User"),
    SYSTEM_INIT("System Initialization"),
    CREATE_GROUP("Create External Group"),
    CHANGE_GROUP("Change External Group Role"),
    DELETE_GROUP("Delete External Group"),
    CREATE_POLICY("Create Policy"),
    DELETE_POLICY("Delete Policy"),
    CHANGE_POLICY("Change Policy"),
    ADD_LICENSE("Add License"),
    DELETE_LICENSE("Delete License"),
    ENABLE_EXTERNAL_LICENSE_SYNC("An external license synchronization service was enabled"),
    SET_SAML_AUTH("Configure SAML authentication and authorization"),
    SET_DEFAULT_AUTH("Configure local and AD authentication and authorization"),
    SET_AD_ONLY_AUTH("Configure AD only authentication and authorization"),
    SET_LOCAL_ONLY_AUTH("Configure Local only authentication and authorization"),
    SET_HEADER_AUTH("Configure header authentication and authorization"),
    SET_AD_MULTI_GROUP_AUTH("Configure AD to support multiple external groups");


    private String displayName;

    AuditAction(String displayName) {
        this.displayName = displayName;
    }

    public String displayName() {
        return displayName;
    }
}
