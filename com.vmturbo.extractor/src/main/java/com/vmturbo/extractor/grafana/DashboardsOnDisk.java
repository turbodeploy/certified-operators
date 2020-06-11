package com.vmturbo.extractor.grafana;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.components.api.FormattedString;
import com.vmturbo.extractor.grafana.model.DashboardSpec;
import com.vmturbo.extractor.grafana.model.FolderInput;

/**
 * Utility class to visit and validate the dashboards stored on disk (i.e. in the resources folder).
 */
public class DashboardsOnDisk {
    private static final Gson GSON = ComponentGsonFactory.createGson();
    /**
     * The name of the folder under resources/.
     */
    private static final String DASHBOARDS_FOLDER = "dashboards";
    private static final String PERMISSIONS_FILE = "permissions.json";
    private static final String FOLDER_FILE = "folder.json";

    /**
     * Helper object representing all the data in a particular folder.
     * Each of these folders should be created as a folder in Grafana.
     */
    public static class FolderData {
        private final String fileName;
        private final FolderInput folder;
        private final List<DashboardSpec> dashboards;
        private final JsonObject permissions;

        private FolderData(@Nonnull final String fileName,
                @Nonnull final FolderInput folder,
                @Nonnull final List<DashboardSpec> dashboards,
                @Nullable final JsonObject permissions) {
            this.fileName = fileName;
            this.folder = folder;
            this.dashboards = dashboards;
            this.permissions = permissions;
        }

        /**
         * Get the folder name in the file system - this is just for debugging.
         *
         * @return The folder name.
         */
        public String getFileName() {
            return fileName;
        }

        /**
         * Get the specifications for the folder to create in Grafana.
         *
         * @return The {@link FolderInput} object, which can be serialized and sent to the Grafana API.
         */
        @Nonnull
        public FolderInput getFolderSpec() {
            return folder;
        }

        /**
         * Get the permissions object for this folder.
         *
         * @return If this folder has specific permissions, a {@link JsonObject} which can be sent
         *         to the Grafana permissions API. An empty optional if we want default permissions.
         */
        public Optional<JsonObject> getPermissions() {
            return Optional.ofNullable(permissions);
        }

        /**
         * Return the dashboards in this folder, by UID.
         *
         * @return Map of (uid) -> ({@link DashboardSpec}).
         */
        @Nonnull
        public Map<String, DashboardSpec> getDashboardsByUid() {
            return dashboards.stream()
                .collect(Collectors.toMap(DashboardSpec::getUid, Function.identity()));
        }
    }

    /**
     * Visit the dashboards on disk. See: {@link DashboardVisitor}.
     *
     * @param visitor The visitor.
     * @throws IllegalArgumentException If there is some invalid state in the on-disk dashboards.
     */
    public void visit(DashboardVisitor visitor) {
        File dashboardsDir = new File(this.getClass().getClassLoader().getResource(DASHBOARDS_FOLDER).getFile());
        Preconditions.checkArgument(dashboardsDir.exists() && dashboardsDir.isDirectory());
        JsonParser parser = new JsonParser();
        // Should produce NPE because we verified it exists and is a directory.
        for (File file : dashboardsDir.listFiles()) {
            if (file.isDirectory()) {
                final String fileName = file.getName();
                final List<DashboardSpec> dashboards  = new ArrayList<>();
                JsonObject permissions = null;
                FolderInput folder = null;
                // Shouldn't produce NPE because we verified it's a directory.
                for (File jsonFile : file.listFiles()) {
                    if (jsonFile.getName().equals(PERMISSIONS_FILE)) {
                        // Override folder permissions.
                        try {
                            permissions = parser.parse(new FileReader(jsonFile)).getAsJsonObject();
                        } catch (FileNotFoundException e) {
                            // This shouldn't happen, because we are going over existing files.
                        }
                    } else if (jsonFile.getName().equals(FOLDER_FILE)) {
                        try {
                            folder = GSON.fromJson(new FileReader(jsonFile), FolderInput.class);
                        } catch (FileNotFoundException e) {
                            // This shouldn't happen, because we are going over existing files.
                        }
                    } else if (jsonFile.getName().endsWith(".json")) {
                        try {
                            JsonObject dashboardJson = parser.parse(new FileReader(jsonFile)).getAsJsonObject();
                            DashboardSpec dashboard = new DashboardSpec(dashboardJson);
                            validateDashboard(jsonFile, dashboard);
                            dashboards.add(dashboard);
                        } catch (FileNotFoundException e) {
                            e.printStackTrace();
                        }
                    } else {
                        // Skip irrelevant file. Probably something useless, like documentation.
                    }
                }
                if (folder == null) {
                    throw new IllegalArgumentException(
                        FormattedString.format("No {} found in folder {}", FOLDER_FILE, file.getPath()));
                }

                final FolderData folderData = new FolderData(fileName, folder, dashboards, permissions);
                visitor.receiveFolderData(folderData);
            }
        }
    }

    private void validateDashboard(File file, DashboardSpec dashboard) {
        dashboard.getId()
            .ifPresent(id -> {
                throw new IllegalArgumentException(FormattedString.format(
                    "Saved dashboard at {} (title: {}, uid: {}) has an id explicitly set.",
                    file.getPath(), dashboard.getTitle(), dashboard.getUid()));
            });
        if (dashboard.getUid().length() >= 40) {
            throw new IllegalArgumentException("Dashboard UIDs must be less than 40 characters. Grafana limit.");
        }
    }

    /**
     * Visitor interface for the on-disk dashboards.
     */
    public interface DashboardVisitor {

        /**
         * Receive the {@link FolderData} for a specific folder.
         *
         * @param folderData The {@link FolderData}.
         */
        void receiveFolderData(FolderData folderData);
    }
}
