package com.vmturbo.extractor.grafana;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.components.api.FormattedString;
import com.vmturbo.extractor.bin.ConvertJsonToYaml;
import com.vmturbo.extractor.grafana.model.DashboardSpec;
import com.vmturbo.extractor.grafana.model.FolderInput;

/**
 * Utility class to visit and validate the dashboards stored on disk (i.e. in the resources folder).
 */
public class DashboardsOnDisk {
    private static final Logger logger = LogManager.getLogger();
    private static final Gson GSON = ComponentGsonFactory.createGson();
    private static final ObjectReader yamlReader = new YAMLMapper().reader();
    private static final ObjectWriter jsonWriter = new ObjectMapper().writer();
    /**
     * The name of the folder under resources/.
     */
    private static final String PERMISSIONS_FILE = "permissions.yaml";
    private static final String FOLDER_FILE = "folder.yaml";
    private static final String GENERAL_FOLDER = "general";
    private static final String PROTOTYPE_FOLDER = "prototype";

    private final String dashboardsPath;
    private final boolean includePrototypes;

    /**
     * Create a new instance.
     * @param dashboardsPath The path to the dashboards folder.
     * @param includePrototypes Whether to include the prototype folder.
     */
    public DashboardsOnDisk(@Nonnull final String dashboardsPath, final boolean includePrototypes) {
        this.dashboardsPath = dashboardsPath;
        this.includePrototypes = includePrototypes;
    }

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
                @Nullable final FolderInput folder,
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
         *         May return an optional folder, in which case all the dashboards should go into
         *         the "general" folder.
         */
        @Nonnull
        public Optional<FolderInput> getFolderSpec() {
            return Optional.ofNullable(folder);
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

        @Override
        public String toString() {
            return FormattedString.format("Folder: {} with {} dashboards."
                            + " Permissions override {}.",
                    fileName, dashboards.size(), permissions == null ? "absent" : "present");
        }

    }

    private Optional<String> shouldScanFolder(String folderName) {
        if (folderName.equals(PROTOTYPE_FOLDER) && !includePrototypes) {
            return Optional.of("Prototype reports disabled."
                + " Set \"includePrototypes\" property to \"true\" in the CR to enable.");
        } else {
            return Optional.empty();
        }
    }

    /**
     * Visit the dashboards on disk. See: {@link DashboardVisitor}.
     *
     * @param visitor The visitor.
     * @throws IllegalArgumentException If there is some invalid state in the on-disk dashboards.
     */
    public void visit(DashboardVisitor visitor) {
        logger.info("Looking through {} for dashboard folders.", dashboardsPath);
        File dashboardsDir = new File(dashboardsPath);
        Preconditions.checkArgument(dashboardsDir.exists() && dashboardsDir.isDirectory(),
            FormattedString.format("Dashboard path {} not pointing to a valid dashboard directory."));
        JsonParser parser = new JsonParser();
        // Should produce NPE because we verified it exists and is a directory.
        for (File file : dashboardsDir.listFiles()) {
            if (file.isDirectory()) {
                final String fileName = file.getName();
                final Optional<String> skipReason = shouldScanFolder(fileName);
                if (skipReason.isPresent()) {
                    logger.info("Skipping folder {}.", skipReason.get());
                    continue;
                }
                logger.info("Scanning folder {}", fileName);
                final List<DashboardSpec> dashboards  = new ArrayList<>();
                JsonObject permissions = null;
                FolderInput folder = null;
                // Shouldn't produce NPE because we verified it's a directory.
                for (File yamlFile : file.listFiles()) {
                    logger.debug("Found file {}", yamlFile.getName());
                    if (yamlFile.getName().equals(PERMISSIONS_FILE)) {
                        // Override folder permissions.
                        try {
                            permissions = parser.parse(yamlToJson(yamlFile)).getAsJsonObject();
                        } catch (IOException e) {
                            // This shouldn't happen, because we are going over existing files.
                        }
                    } else if (yamlFile.getName().equals(FOLDER_FILE)) {
                        try {
                            folder = GSON.fromJson(yamlToJson(yamlFile), FolderInput.class);
                        } catch (IOException e) {
                            // This shouldn't happen, because we are going over existing files.
                        }
                    } else if (yamlFile.getName().endsWith(".yaml")) {
                        try {
                            JsonObject dashboardJson = parser.parse(yamlToJson(yamlFile)).getAsJsonObject();
                            DashboardSpec dashboard = new DashboardSpec(dashboardJson);
                            validateDashboard(yamlFile, dashboard);
                            dashboards.add(dashboard);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    } else {
                        // Skip irrelevant file. Probably something useless, like documentation.
                    }
                }
                if (folder == null && !fileName.equalsIgnoreCase(GENERAL_FOLDER)) {
                    throw new IllegalArgumentException(
                        FormattedString.format("No {} found in folder {}", FOLDER_FILE, file.getPath()));
                }

                final FolderData folderData = new FolderData(fileName, folder, dashboards, permissions);
                logger.info("Completed scan: {}", folderData);
                visitor.receiveFolderData(folderData);
            } else {
                logger.info("Skipping non-folder {}", file.getName());
            }
        }
    }

    private String yamlToJson(File yamlFile) throws IOException {
        final String yaml = FileUtils.readFileToString(yamlFile, Charsets.UTF_8);
        return ConvertJsonToYaml.yamlToJson(yaml);
    }

    private void validateDashboard(File file, DashboardSpec dashboard) {
        dashboard.getId()
            .ifPresent(id -> {
                throw new IllegalArgumentException(FormattedString.format(
                    "Saved dashboard at {} (title: {}, uid: {}) has an id explicitly set.",
                    file.getPath(), dashboard.getTitle(), dashboard.getUid()));
            });
        if (dashboard.getUid().length() > 40) {
            throw new IllegalArgumentException(FormattedString.format(
                    "Saved dashboard at {} (title: {}, uid: {}) has a uid exceeding grafana's 40 character limit.",
                    file.getPath(), dashboard.getTitle(), dashboard.getUid()));
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
