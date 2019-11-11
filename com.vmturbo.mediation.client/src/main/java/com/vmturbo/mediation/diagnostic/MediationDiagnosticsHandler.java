package com.vmturbo.mediation.diagnostic;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.stream.Stream;
import java.util.zip.ZipOutputStream;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.components.common.diagnostics.DiagnosticsWriter;

/**
 * Class for handling the mediation components diagnostics.
 */
public class MediationDiagnosticsHandler {

    private final Logger logger = LogManager.getLogger();
    private final DiagnosticsWriter diagnosticsWriter;
    private final String envTmpDiagsDir;

    public MediationDiagnosticsHandler(@Nonnull final DiagnosticsWriter diagnosticsWriter,
                                       @Nonnull final String envTmpDiagsDir) {
        this.diagnosticsWriter = Objects.requireNonNull(diagnosticsWriter);
        this.envTmpDiagsDir = Objects.requireNonNull(envTmpDiagsDir);
    }

    /**
     * Dumping the diags on the zip stream arg.
     *
     * @param diagnosticZip stream to collect all the diags.
     */
    public void dump(@Nonnull final ZipOutputStream diagnosticZip) {
        iterateOverAllInPath(diagnosticZip, new File(envTmpDiagsDir));
    }

    /**
     * Recursive function to iterate over all files in folder or sub-folder.
     * So if azure and aws probes run in the same container this will go into all sub folders of
     * /tmp/diags/azure/.. and tmp/diags/aws.. and extract all the files in them.
     *
     * @param diagnosticZip response stream to add all the diagnostic files.
     * @param path that contains all the diagnostic files.
     */
    private void iterateOverAllInPath(final ZipOutputStream diagnosticZip, File path) {
        File[] probeDiagsFiles = path.listFiles();
        if (probeDiagsFiles != null) {
            for (File probeDiagsFile : probeDiagsFiles) {
                if (probeDiagsFile.isDirectory()) {
                    iterateOverAllInPath(diagnosticZip, probeDiagsFile);
                } else {
                    writeDiagsFileToStream(probeDiagsFile.toString(), diagnosticZip);
                }
            }
        }
    }

    private void writeDiagsFileToStream(String diagsFileName, ZipOutputStream diagsZip) {
        try {
            Stream<String> streamValuesToWrite = Files.lines(Paths.get(diagsFileName));
            diagnosticsWriter.writeZipEntry(diagsFileName, streamValuesToWrite, diagsZip);
        } catch (Exception e) {
            logger.error("Failed to collect diags {}", diagsFileName, e);
        }
    }
}
