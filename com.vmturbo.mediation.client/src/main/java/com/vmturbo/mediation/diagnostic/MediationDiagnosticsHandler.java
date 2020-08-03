package com.vmturbo.mediation.diagnostic;

import java.io.File;
import java.util.Objects;
import java.util.zip.ZipOutputStream;

import javax.annotation.Nonnull;

import com.vmturbo.components.common.diagnostics.DiagnosticsHandler;
import com.vmturbo.components.common.diagnostics.DiagnosticsWriter;

/**
 * Class for handling the mediation components diagnostics.
 */
public class MediationDiagnosticsHandler {

    private final String envTmpDiagsDir;

    /**
     * Constructs diagnostics handler.
     *
     * @param envTmpDiagsDir environment temporary directory to use
     */
    public MediationDiagnosticsHandler(@Nonnull final String envTmpDiagsDir) {
        this.envTmpDiagsDir = Objects.requireNonNull(envTmpDiagsDir);
    }

    /**
     * Dumping the diags on the zip stream arg.
     *
     * @param diagnosticZip stream to collect all the diags.
     */
    public void dump(@Nonnull final ZipOutputStream diagnosticZip) {
        final DiagnosticsWriter diagsWriter = new DiagnosticsWriter(diagnosticZip);
        iterateOverAllInPath(diagsWriter, new File(envTmpDiagsDir));
        if (!diagsWriter.getErrors().isEmpty()) {
            diagsWriter.writeZipEntry(DiagnosticsHandler.ERRORS_FILE,
                    diagsWriter.getErrors().iterator());
        }
    }

    /**
     * Recursive function to iterate over all files in folder or sub-folder.
     * So if azure and aws probes run in the same container this will go into all sub folders of
     * /tmp/diags/azure/.. and tmp/diags/aws.. and extract all the files in them.
     *
     * @param diagnosticZip response stream to add all the diagnostic files.
     * @param path that contains all the diagnostic files.
     */
    private void iterateOverAllInPath(@Nonnull final DiagnosticsWriter diagnosticZip,
            @Nonnull File path) {
        File[] probeDiagsFiles = path.listFiles();
        if (probeDiagsFiles != null) {
            for (File probeDiagsFile : probeDiagsFiles) {
                if (probeDiagsFile.isDirectory()) {
                    iterateOverAllInPath(diagnosticZip, probeDiagsFile);
                } else {
                    diagnosticZip.dumpDiagnosable(new FileDiagnosticsProvider(probeDiagsFile));
                }
            }
        }
    }
}
