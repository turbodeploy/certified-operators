package com.vmturbo.extractor.grafana.client;

import com.google.gson.JsonObject;

import com.vmturbo.extractor.grafana.model.DashboardSpec.UpsertDashboardRequest;
import com.vmturbo.extractor.grafana.model.Datasource;
import com.vmturbo.extractor.grafana.model.Folder;

/**
 * Helper interface to track operations done by the {@link GrafanaClient}.
 *
 * <p/>Helpful for callers of {@link GrafanaClient} that want to track a multi-step operation.
 * (e.g. create a folder, a datasource, and then a dashboard).
 */
public interface OperationSummary {

    /**
     * Datasource was successfully created.
     *
     * @param newDatasource The newly created {@link Datasource}.
     * @param success Whether or not the connection tests ran successfully. If this is false,
     *                there may be configuration issues with the datasource.
     */
    default void recordDatasourceCreation(Datasource newDatasource, boolean success) {}

    /**
     * Datasource was successfully updated.
     *
     * @param existing The {@link Datasource} before the change.
     * @param datasource The updated {@link Datasource}.
     */
    default void recordDatasourceUpdate(Datasource existing, Datasource datasource) {}

    /**
     * Datasource was not modified, because the input was the same as what was already in Grafana.
     *
     * @param existing The existing {@link Datasource}.
     */
    default void recordDatasourceUnchanged(Datasource existing) {}

    /**
     * A folder was updated successfully.
     *
     * @param existing The old {@link Folder}.
     * @param folder The updated {@link Folder}.
     */
    default void recordFolderUpdate(Folder existing, Folder folder) {}

    /**
     * A folder was created successfully.
     *
     * @param createdFolder The created {@link Folder}.
     */
    default void recordFolderCreate(Folder createdFolder) {}

    /**
     * A folder was not updated because the input was the same as what was already in Grafana.
     *
     * @param existing The existing {@link Folder}.
     */
    default void recordFolderUnchanged(Folder existing) {}

    /**
     * Permissions set on folder.
     *
     * @param folderUid The UID of the folder.
     * @param permissions The input permissions.
     * @param response The response from the server.
     */
    default void recordFolderPermissions(String folderUid, JsonObject permissions, JsonObject response) {}

    /**
     * A folder was deleted successfully.
     *
     * @param folder The {@link Folder}.
     */
    default void recordFolderDelete(Folder folder) {}

    /**
     * A dashboard was upserted (created or updated) successfully.
     *
     * @param request The request.
     * @param response The response.
     */
    default void recordDashboardUpsert(UpsertDashboardRequest request, JsonObject response) {}

    /**
     * A user already exists.
     *
     * @param username Username.
     * @param userId The user ID.
     */
    default void recordUserUnchanged(String username, long userId) {}

    /**
     * A user was created successfully.
     *
     * @param username Username.
     * @param userId The user ID.
     */
    default void recordUserCreate(String username, long userId) {}
}
