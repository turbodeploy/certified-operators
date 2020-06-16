package com.vmturbo.extractor.grafana.model;

import com.google.gson.annotations.SerializedName;

/**
 * See: https://grafana.com/docs/grafana/latest/http_api/dashboard_versions/.
 */
public class DashboardVersion {
    @SerializedName("id")
    private long id;

    @SerializedName("dashboardId")
    private long dashboardId;

    @SerializedName("parentVersion")
    private int parentVersion;

    @SerializedName("version")
    private int version;

    @SerializedName("message")
    private String message;

    @SerializedName("createdBy")
    private String createdBy;

    /**
     * Get the commit message used when saving this version.
     *
     * @return The message.
     */
    public String getMessage() {
        return message;
    }

    /**
     * Get the user that created this version of the dashboard.
     *
     * @return The user.
     */
    public String getCreatedBy() {
        return createdBy;
    }

    /**
     * Get the version of the dashboard. Versions start at 1 and get incremented every save.
     *
     * @return The version.
     */
    public int getVersion() {
        return version;
    }

    /**
     * Get the parent version (i.e. the version before this version).
     *
     * @return The parent version.
     */
    public int getParentVersion() {
        return parentVersion;
    }

    /**
     * Get the ID of the dashboard the version refers to.
     *
     * @return The dashboard id.
     */
    public long getDashboardId() {
        return dashboardId;
    }
}
