package com.vmturbo.extractor.grafana.model;

import javax.annotation.Nonnull;

import com.google.gson.annotations.SerializedName;

/**
 * Input DTO for the user creation endpoint.
 *
 * <p/>See: https://grafana.com/docs/grafana/latest/http_api/admin/#global-users.
 */
public class UserInput {
    @SerializedName("name")
    private String displayName;

    @SerializedName("login")
    private String username;

    @SerializedName("password")
    private String password;

    // The default organization.
    @SerializedName("OrgId")
    private int organization = 1;

    /**
     * Create a new instance of this DTO.
     *
     * @param name The display name.
     * @param username The username.
     * @param password The password.
     */
    public UserInput(@Nonnull final String name,
            @Nonnull final String username,
            @Nonnull final String password) {
        this.displayName = name;
        this.username = username;
        this.password = password;
    }

    public String getUsername() {
        return username;
    }

    public int getOrgId() {
        return organization;
    }
}
