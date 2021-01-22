package com.vmturbo.extractor.grafana.model;

import com.google.gson.annotations.SerializedName;

/**
 * Represents a user in an organization returned by the Grafana API.
 *
 * <p/>See: https://grafana.com/docs/grafana/latest/http_api/org/#get-users-in-organization
 */
public class OrganizationUser {
    private static final String GRAFANA_GLOBAL_ADMIN = "admin";

    @SerializedName("login")
    private String username;

    @SerializedName("userId")
    private long userId;

    @SerializedName("role")
    private Role role;

    public String getUsername() {
        return username;
    }

    public long getUserId() {
        return userId;
    }

    public Role getRole() {
        return role;
    }

    public boolean isGlobalAdmin() {
        return username.equalsIgnoreCase(GRAFANA_GLOBAL_ADMIN);
    }
}
