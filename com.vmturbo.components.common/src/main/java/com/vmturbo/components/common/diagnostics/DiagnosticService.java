package com.vmturbo.components.common.diagnostics;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.zip.ZipOutputStream;

import com.google.common.annotations.VisibleForTesting;

import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Service to collect diagnostic information on the current Component.
 **/
@Component
public class DiagnosticService {

    /**
     * The logger.
     */
    private Logger logger_ = LogManager.getLogger(DiagnosticService.class);

    /**
     * The pre-set diags URL that connects to an http server that is initialized when the containers
     * startup in diags.py
     */
    private static final String DIAGS_URL = "http://127.128.129.130:58888/diags";

    /**
     * The time to wait for the diags to be collected = 30 minutes
     */
    private static final int DIAGS_WAIT_SEC = 30 * 60;

    /**
     * The diags destination directory.
     */
    private static final String SYSTEM_DIAGS_DIRECTORY = "/tmp/diags/system-data";

    /**
     * The diags completion flag file.
     */
    private static final String DIAGS_COMPLETION_FILE = "/tmp/diags_done";

    /**
     * The time to sleep between attempts to check the diags collection completion.
     */
    private static final long DIAGS_WAIT_SLEEP_MS = 1000L;

    @Value("${component_type:unknown-component}")
    private String componentType;

    @Value("${instance_id:unknown-component_instance}")
    private String instanceId;

    @Autowired
    private FileFolderZipper fileFolderZipper;

    /**
     * Retrieves the diagnostics.
     *
     * @return {code true} always. This is for mocking.
     * @throws IOException In case of an error doing so. For example, if the HTTP server is not
     *                     available.
     */
    @VisibleForTesting
    boolean getSystemDiags() throws IOException {
        URL url = new URL(DIAGS_URL);
        URLConnection connection = null;
        try {
            connection = url.openConnection();
        } finally {
            if (connection != null) {
                IOUtils.closeQuietly(connection.getInputStream());
            }
        }
        // Wait for it to appear
        for (int i = 0; i < DIAGS_WAIT_SEC; i++) {
            File f = new File(DIAGS_COMPLETION_FILE);
            if (f.isFile() && f.canRead()) {
                if (!f.delete()) {
                    logger_.warn("Unable to delete " + DIAGS_COMPLETION_FILE);
                }
                break;
            }
            try {
                Thread.sleep(DIAGS_WAIT_SLEEP_MS);
            } catch (InterruptedException e) {
                // Do nothing.
            }
        }
        return true;
    }

    /**
     * Call the shell script to collect diagnostic files into a temp folder, and then dump the
     * resulting files from the predefined temp folder onto a ZipOutputStream.
     *
     * @param diagnosticZip zip output stream that the individual files are written onto
     * @throws DiagnosticsException if there is an error writing data to the diagnostics zip stream
     */
    public void dumpSystemDiags(ZipOutputStream diagnosticZip) throws DiagnosticsException {
        Path diagsDir = Paths.get(SYSTEM_DIAGS_DIRECTORY);
        // zip files in the temp directory onto the diagnosticZip stream
        try {
            getSystemDiags();
            // We've collected all diagnostics, now zip and ship.
            fileFolderZipper.zipFilesInFolder(instanceId, diagnosticZip, diagsDir);

            // Delete the results if they are available.
            // During the testing they will not be.
            if (!Files.isDirectory(diagsDir)) {
                return;
            }
            // After we've uploaded the information, delete the temporary files.
            Files.walk(diagsDir)
                 .sorted(Comparator.reverseOrder())
                 .map(Path::toFile)
                 .peek(s -> logger_.info("File/Directory {} is being deleted", s))
                 .forEach(File::delete);
        } catch (IOException e) {
            logger_.error("Error zipping temp diag files", e);
            throw new DiagnosticsException(
                    "Error zipping temp diagnostic files onto diagnosticZip stream", e);
        }
    }
}
