package com.vmturbo.extractor.grafana;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import javax.annotation.Nonnull;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.components.api.FormattedString;
import com.vmturbo.extractor.grafana.client.OperationSummary;
import com.vmturbo.extractor.grafana.model.DashboardSpec;
import com.vmturbo.extractor.grafana.model.DashboardSpec.UpsertDashboardRequest;
import com.vmturbo.extractor.grafana.model.Datasource;
import com.vmturbo.extractor.grafana.model.Folder;

/**
 * A helper class to track the calls made during one run of {@link Grafanon#refreshGrafana(RefreshSummary)}.
 * Purely for logging and debugging purposes.
 *
 * <p/>This is particularly useful because the Grafana refresh/configuration process is just
 * a series of API calls, and it's good to know how many we executed successfully.
 */
public class RefreshSummary implements OperationSummary {

    private static final Gson GSON = ComponentGsonFactory.createGson();

    private final List<Operation> operations = new ArrayList<>();

    @Override
    public void recordDatasourceCreation(Datasource input, boolean success) {
        Operation operation = Operation.withSummary(
                FormattedString.format("Created new datasource \"{}\". Connection test {}", input, success ? "succeeded" : "failed"))
            .andDetails(() -> GSON.toJson(input));
        operations.add(operation);
    }

    @Override
    public void recordDatasourceUpdate(Datasource existing, Datasource datasource) {
        final Operation operation = Operation.withSummary(FormattedString.format(
                "Updated existing datasource \"{}\".", existing))
            .andDetails(new FormattedString("Old: {}\n New: {}",
                () -> GSON.toJson(existing), () -> GSON.toJson(datasource)));
        operations.add(operation);
    }

    @Override
    public void recordDatasourceUnchanged(Datasource existing) {
        final Operation operation = Operation.withSummary(FormattedString.format(
                "Datasource \"{}\" unchanged.", existing))
            .andDetails(() -> GSON.toJson(existing));
        operations.add(operation);
    }

    @Override
    public void recordFolderUpdate(Folder existing, Folder updated) {
        final Operation operation = Operation.withSummary(FormattedString.format(
                "Folder \"{}\" updated.", existing))
            .andDetails(new FormattedString("Old: \"{}\"\n New: \"{}\"",
                () -> GSON.toJson(existing), () -> GSON.toJson(updated)));
        operations.add(operation);
    }

    @Override
    public void recordFolderCreate(Folder createdFolder) {
        final Operation operation = Operation.withSummary(FormattedString.format(
                "Folder \"{}\" created.", createdFolder))
                .andDetails(() -> GSON.toJson(createdFolder));
        operations.add(operation);
    }

    @Override
    public void recordFolderUnchanged(Folder existing) {
        final Operation operation = Operation.withSummary(FormattedString.format(
                "Folder \"{}\" unchanged.", existing))
                .andDetails(() -> GSON.toJson(existing));
        operations.add(operation);
    }

    @Override
    public void recordFolderPermissions(String folderUid, JsonObject permissions, JsonObject response) {
        final Operation operation = Operation.withSummary(FormattedString.format(
            "Set permissions for folder with uid \"{}\".", folderUid))
            .andDetails(new FormattedString("Request: {}\nResponse:{}",
                () -> GSON.toJson(permissions), () -> GSON.toJson(response)));
        operations.add(operation);
    }

    @Override
    public void recordFolderDelete(Folder folder) {
        final Operation operation = Operation.withSummary(FormattedString.format(
                "Deleted folder \"{}\".", folder))
                .andDetails(() -> GSON.toJson(folder));
        operations.add(operation);
    }

    @Override
    public void recordDashboardUpsert(UpsertDashboardRequest request, JsonObject response) {
        final Operation operation = Operation.withSummary(FormattedString.format(
            "{} dashboard {}", (request.hasId() ? "Updated" : "Created"), request))
            .andDetails(new FormattedString("Full request: {}\nFull response: {}",
                    () -> GSON.toJson(request),
                    () -> GSON.toJson(response)));
        operations.add(operation);
    }

    /**
     * Record that we are not updating a dashboard because it's not changed from the version
     * already in grafana.
     *
     * @param existingDashboardId The existing dashboard ID.
     * @param dashboardSpec The spec for the dashboard (for verbose logging).
     */
    public void recordDashboardUnchanged(Long existingDashboardId, DashboardSpec dashboardSpec) {
        final Operation operation = Operation.withSummary(FormattedString.format(
                "Dashboard {} (id: {}) unchanged", dashboardSpec, existingDashboardId))
            .andDetails(() -> GSON.toJson(dashboardSpec));
        operations.add(operation);
    }

    /**
     * Record an error encountered during Grafana operations.
     * @param e The error.
     */
    public void recordError(Exception e) {
        final Operation operation = Operation.withSummary(
            FormattedString.format("Encountered error: {}", e.getMessage()));
        operations.add(operation);
    }

    /**
     * Create a summary string containing all the changes in this summary.
     *
     * @param withDetails If true, print the long form - this includes all the involved JSON objects,
     *                    and can get quite long.
     * @return The string, to log or return to the caller.
     */
    @Nonnull
    public String summarize(boolean withDetails) {
        StringBuilder retBldr = new StringBuilder();
        retBldr.append(operations.size() + " operations:");
        operations.forEach(op -> {
            retBldr.append("\n");
            op.summarize(retBldr, withDetails);
        });
        return retBldr.toString();
    }

    @Override
    public String toString() {
        return summarize(false);
    }

    /**
     * An operation is just a summary, with a supplier that can provide additional details if
     * necessary.
     */
    private static class Operation {
        private final String summary;
        private Supplier<String> details = () -> "";

        private Operation(String summary) {
            this.summary = summary;
        }

        public static Operation withSummary(String summary) {
            return new Operation(summary);
        }

        Operation andDetails(Supplier<String> details) {
            this.details = details;
            return this;
        }

        void summarize(StringBuilder bldr, boolean withDetails) {
            bldr.append(summary);
            if (withDetails) {
                String detailsStr;
                try {
                    detailsStr = details.get();
                } catch (RuntimeException e) {
                    detailsStr = e.toString();
                }
                bldr.append("Details:\n")
                    .append(detailsStr);
            }
        }
    }
}
