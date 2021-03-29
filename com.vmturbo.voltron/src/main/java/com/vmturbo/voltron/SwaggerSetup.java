package com.vmturbo.voltron;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Setup for serving swagger UI for the external API in Voltron.
 * <p/>
 * Swagger resources are not correctly put in the correct places during a normal build in order
 * to be able to serve them properly through Voltron. Some resources are placed in
 * com.vmturbo.api.component/target/generated-docs/swagger/external and some are placed in
 * com.vmturbo.api.component/target/apidoc/. Resources from both of these directories
 * must be combined into a single directory in order to be properly served.
 */
public class SwaggerSetup {
    private final String externalGeneratedDocsPath;
    private final String apiDocsPath;

    /**
     * The swagger directory in the data path.
     */
    public static final String VOLTRON_SWAGGER_DIRECTORY = "swagger";

    /**
     * The swagger JSON file.
     */
    public static final String SWAGGER_JSON_FILE = "swagger.json";

    private static final Logger logger = LogManager.getLogger();

    /**
     * Data from the externalGeneratedDocsPath and apiDocsPath are both copied into a new
     * directory we create in the dataPath.
     *
     * @param apiComponentPath com.vmturbo.api.component location
     */
    public SwaggerSetup(@Nonnull final String apiComponentPath) {
        this.externalGeneratedDocsPath = Paths.get(apiComponentPath, "target/generated-docs/swagger/external").toString();
        this.apiDocsPath = Paths.get(apiComponentPath, "target/apidoc/").toString();
    }

    /**
     * Copy swagger resources into the data path so that they can be served for swagger-ui.
     * Resources will only actually be copied if they are not already there or they are
     * older than the most recent build as determined by the timestamp on swagger.json.
     *
     * @param dataPath The data path (ie Voltron workspace directory).
     * @return The location of the copied swagger resources.
     */
    public String copyResourcesIntoDataPath(@Nonnull final String dataPath) {
        return copyResourcesIntoDataPath(dataPath, null, false);
    }

    /**
     * Copy swagger resources into the data path so that they can be served for swagger-ui.
     *
     * @param dataPath The data path (ie Voltron workspace directory).
     * @param swaggerDirectoryOffset An offset in the data path in which we will copy the files.
     *                               Set to /swagger if this parameter is null.
     * @param force Whether to force the copy of resources. If false, we first check if resources
     *              actually need to be copied and only if they need to be copied do we do the copying.
     * @return The location of the copied swagger resources.
     */
    public String copyResourcesIntoDataPath(@Nonnull final String dataPath,
                                            @Nullable final String swaggerDirectoryOffset,
                                            final boolean force) {
        final Path destinationPath = Paths.get(dataPath,
            swaggerDirectoryOffset == null ? VOLTRON_SWAGGER_DIRECTORY : swaggerDirectoryOffset);
        final Path generatedDocsPath = Paths.get(externalGeneratedDocsPath);
        final Path apiDocsPath = Paths.get(this.apiDocsPath);

        if (shouldCopyFiles(destinationPath, generatedDocsPath, force)) {
            deleteDirectory(destinationPath);
            copyFolder(generatedDocsPath, destinationPath);
            copyFolder(apiDocsPath, destinationPath);
        }

        return destinationPath.toString();
    }

    /**
     * Check if we should copy files. Files should be copied if:
     * 1. The destinationPath does not exist.
     * 2. The swagger.json in the externalGeneratedDocsPath is newer than the one at the destinationPath
     * 3. The "force" parameter is true.
     *
     * @param destinationPath The destination directory for the swagger files.
     * @param generatedDocsPath The directory where swagger files are written by the build.
     * @param force Whether to force the copy.
     *
     * @return Whether we should copy swagger files.
     */
    private boolean shouldCopyFiles(@Nonnull final Path destinationPath,
                                    @Nonnull final Path generatedDocsPath,
                                    final boolean force) {
        if (force) {
            return true;
        }

        if (!Files.exists(destinationPath)) {
            return true;
        }

        try {
            Path dstSwaggerJson = Paths.get(destinationPath.toString(), SWAGGER_JSON_FILE);
            if (!Files.exists(dstSwaggerJson)) {
                return true;
            }

            final BasicFileAttributes destFileAttrs = Files.readAttributes(dstSwaggerJson,
                BasicFileAttributes.class);
            final BasicFileAttributes srcFileAttrs = Files.readAttributes(Paths.get(
                generatedDocsPath.toString(), SWAGGER_JSON_FILE), BasicFileAttributes.class);

            return srcFileAttrs.lastModifiedTime().compareTo(destFileAttrs.lastModifiedTime()) > 0;
        } catch (IOException e) {
            logger.error("Error getting file attributes: ", e);
            return false;
        }
    }

    private void deleteDirectory(Path toDelete) {
        if (Files.exists(toDelete)) {
            final File dir = new File(toDelete.toString());
            if (dir.isDirectory()) {
                try {
                    FileUtils.forceDelete(dir);
                    logger.info("Deleted external API swagger directory '{}'", dir);
                } catch (IOException e) {
                    logger.error(String.format("Unable to delete directory %s: ", dir.toString()), e);
                }
            }
        }
    }

    private void copyFolder(Path src, Path dest) {
        // Ensure the directory exists.
        final File directory = new File(dest.toString());
        if (!directory.exists()) {
            directory.mkdir();
        }

        try (Stream<Path> stream = Files.walk(src)) {
            stream.forEach(source -> {
                if (!Files.isDirectory(source)) {
                    copy(source, dest.resolve(src.relativize(source)));
                }
            });
        } catch (IOException e) {
            final String message = String.format("Unable to copy folder '%s' to '%s': ", src, dest);
            logger.error(message, e);
        }

        logger.info(String.format("Copied files from %s into %s", src.toString(), dest.toString()));
    }

    private void copy(Path source, Path dest) {
        try {
            Files.copy(source, dest, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
        } catch (Exception e) {
            final String message = String.format("Unable to copy file '%s' to '%s': ", source, dest);
            logger.error(message, e);
        }
    }
}
