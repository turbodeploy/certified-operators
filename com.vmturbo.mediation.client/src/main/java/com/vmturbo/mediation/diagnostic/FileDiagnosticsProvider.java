package com.vmturbo.mediation.diagnostic;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Objects;

import javax.annotation.Nonnull;

import org.apache.commons.io.IOUtils;

import com.vmturbo.components.common.diagnostics.BinaryDiagnosable;
import com.vmturbo.components.common.diagnostics.DiagnosticsException;

/**
 * Diagnostics provider suitable to grab file contents to a diagnostics zip.
 */
public class FileDiagnosticsProvider implements BinaryDiagnosable {

    private final File file;

    /**
     * Constructs diagnostics provider saving the specified file.
     *
     * @param file file to dump
     */
    public FileDiagnosticsProvider(@Nonnull File file) {
        this.file = Objects.requireNonNull(file);
    }

    @Override
    public void collectDiags(@Nonnull OutputStream appender)
            throws DiagnosticsException, IOException {
        if (!file.isFile()) {
            throw new DiagnosticsException("File requested to dump is not a regular file: " + file);
        }
        try (InputStream fis = new FileInputStream(file)) {
            IOUtils.copy(fis, appender);
        }
    }

    @Nonnull
    @Override
    public String getFileName() {
        return file.getPath();
    }
}
