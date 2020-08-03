package com.vmturbo.extractor.grafana.model;

import java.util.OptionalLong;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.annotations.SerializedName;

import com.vmturbo.components.api.FormattedString;

/**
 * Spec for a dashboard.
 *
 * <p/>We don't actually send the spec to Grafana. It wraps the "dashboard" JSON object, which is
 * the JSON for the dashboard, and provides some utilities for working with that nested JSON
 * object.
 *
 * <p/>We don't want to fully define the dashboard DTO, because we don't need 99% of its fields,
 * and we would need to keep it up to date as we change Grafana versions. All we want is to save
 * it from Grafana to file, load it from file, and send it back to Grafana.
 */
public class DashboardSpec {

    /**
     * The actual dashboard JSON model.
     */
    private final JsonObject dashboard;

    /**
     * Create a new {@link DashboardSpec}.
     *
     * @param dashboardJson The JSON model for the dashboard.
     */
    public DashboardSpec(@Nonnull final JsonObject dashboardJson) {
        this.dashboard = dashboardJson;
    }

    /**
     * Get the UID of the dashboard.
     * UIDs are the same across deployments.
     * See: https://grafana.com/docs/grafana/latest/http_api/dashboard/#identifier-id-vs-unique-identifier-uid
     *
     * @return The dashboard UID.
     */
    @Nonnull
    public String getUid() {
        JsonElement uid = dashboard.get("uid");
        if (uid == null) {
            return "";
        } else {
            return uid.getAsString();
        }
    }

    /**
     * Get the title of the dashboard (i.e. its display name).
     *
     * @return The dashboard title.
     */
    @Nonnull
    public String getTitle() {
        JsonElement title = dashboard.get("title");
        if (title == null) {
            return "";
        } else {
            return title.getAsString();
        }
    }

    /**
     * Get the id of the dashboard, if it has an id.
     * Ids are unique per deployment.
     * See: https://grafana.com/docs/grafana/latest/http_api/dashboard/#identifier-id-vs-unique-identifier-uid
     *
     * @return The id, if this dashboard has an id set.
     */
    public OptionalLong getId() {
        JsonElement id = dashboard.get("id");
        if (id == null) {
            return OptionalLong.empty();
        } else {
            return OptionalLong.of(id.getAsLong());
        }
    }

    /**
     * Create a new {@link UpsertDashboardRequest} from this {@link DashboardSpec}.
     *
     * @return The {@link UpsertDashboardRequest}.
     */
    @Nonnull
    public UpsertDashboardRequest creationRequest() {
        return new UpsertDashboardRequest(this);
    }

    /**
     * See: https://grafana.com/docs/grafana/latest/http_api/dashboard/#create-update-dashboard.
     */
    public static class UpsertDashboardRequest {

        @SerializedName("dashboard")
        private JsonObject dashboard;

        @SerializedName("folderId")
        private Long folderId = null;

        // Overwrite false by default - we will set the latest version explicitly.
        @SerializedName("overwrite")
        private boolean overwrite = false;

        @SerializedName("message")
        private String message;

        private UpsertDashboardRequest(DashboardSpec dashboard) {
            this.dashboard = dashboard.dashboard;
        }

        /**
         * This transitions the request from a "create" to an "update", and set the relevant
         * properties on the request object.
         *
         * @param id The id of the dashboard.
         * @param curVersion The current dashboard version on the Grafana server.
         * @return The request, for method chaining.
         */
        @Nonnull
        public UpsertDashboardRequest setUpdateProperties(final long id,
                @Nullable final DashboardVersion curVersion) {
            dashboard.addProperty("id", id);
            if (curVersion != null) {
                dashboard.addProperty("version", curVersion.getVersion());
            } else {
                // This shouldn't happen, because if there is an existing dashboard we should always
                // have at least one version. But just in case, we will set "overwrite" to true,
                // which will make Grafana ignore the input version parameters.
                overwrite = true;
            }
            return this;
        }

        /**
         * Return whether or not the request has an ID set.
         *
         * @return True if there is an id set in the dashboard in the request.
         */
        public boolean hasId() {
            return dashboard.has("id");
        }

        /**
         * Set the "commit message" for this upsert. This message will appear when looking at
         * the version history.
         *
         * @param message The message.
         * @return The {@link UpsertDashboardRequest}.
         */
        @Nonnull
        public UpsertDashboardRequest setMessage(@Nonnull final String message) {
            this.message = message;
            return this;
        }

        /**
         * Set the folder that this dashboard should go into.
         *
         * @param parentFolder The {@link Folder}.
         * @return The {@link UpsertDashboardRequest}.
         */
        public UpsertDashboardRequest setParentFolder(Folder parentFolder) {
            this.folderId = parentFolder.getId();
            return this;
        }

        @Override
        public String toString() {
            return DashboardSpec.toString(dashboard);
        }

    }

    @Override
    public String toString() {
        return toString(dashboard);
    }

    private static String toString(JsonObject dashboard) {
        return FormattedString.format("{} (uid: {})",
                dashboard.get("title"),
                dashboard.get("uid"));
    }
}
