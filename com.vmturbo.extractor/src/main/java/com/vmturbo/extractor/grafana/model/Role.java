package com.vmturbo.extractor.grafana.model;

import java.util.Optional;

import javax.annotation.Nonnull;

import com.google.gson.annotations.SerializedName;

/**
 * A role for a user inside an organization in Grafana.
 * See: https://grafana.com/docs/grafana/latest/permissions/organization_roles
 */
public enum Role {
    /**
     * Can do everything scoped to the organization.
     */
    @SerializedName("Admin")
    ADMIN("Admin"),

    /**
     * Can edit dashboards, panels, etc.
     */
    @SerializedName("Editor")
    EDITOR("Editor"),

    /**
     * Can look at stuff.
     */
    @SerializedName("Viewer")
    VIEWER("Viewer");

    private final String name;

    Role(String name) {
        this.name = name;
    }

    @Nonnull
    public String getName() {
        return name;
    }

    /**
     * Parse the string representation of the role (from a Grafana object).
     *
     * @param name The string.
     * @return The role, or an empty optional.
     */
    public static Optional<Role> parse(String name) {
        for (Role role : Role.values()) {
            if (name.equalsIgnoreCase(role.getName())) {
                return Optional.of(role);
            }
        }
        return Optional.empty();
    }

}
