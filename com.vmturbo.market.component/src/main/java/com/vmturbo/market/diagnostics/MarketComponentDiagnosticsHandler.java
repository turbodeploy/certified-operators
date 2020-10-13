package com.vmturbo.market.diagnostics;

import java.io.File;
import java.io.FileInputStream;
import java.util.Objects;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.components.common.diagnostics.IDiagnosticsHandler;

/**
 * Handle diagnostics for {@link com.vmturbo.market.MarketComponent}.
 */
public class MarketComponentDiagnosticsHandler implements IDiagnosticsHandler {

    private static final int WRITE_CHUNK_SIZE = 64 * 1024;

    private static final Logger logger = LogManager.getLogger();

    /**
     * Dump Market-component diagnostics.
     *
     * @param zipStream the ZipOutputStream to dump diags to
     */
    @Override
    public void dump(@Nonnull ZipOutputStream zipStream) {
        try {
            File dumpDirectory = new File("/" + AnalysisDiagnosticsCollector.ANALYSIS_DIAGS_DIRECTORY);
            final String[] allDiscoveryDumpFiles = dumpDirectory.list();
            for (String filename : allDiscoveryDumpFiles) {
                if (filename.contains(AnalysisDiagnosticsCollector.ANALYSIS_DIAGS_SUFFIX)) {
                    File file = new File(Objects.requireNonNull(dumpDirectory), filename);
                    ZipEntry ze = new ZipEntry(filename);
                    zipStream.putNextEntry(ze);
                    FileInputStream fis = new FileInputStream(file);
                    int length;
                    byte[] buffer = new byte[WRITE_CHUNK_SIZE];
                    while ((length = fis.read(buffer)) > 0) {
                        zipStream.write(buffer, 0, length);
                    }
                    zipStream.closeEntry();
                    fis.close();
                }
            }
            zipStream.close();
        } catch (Exception e) {
            logger.error("Exception trying to create directory ", e);
        }

    }
}
