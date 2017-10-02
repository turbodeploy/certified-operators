package com.vmturbo.components.common.diagnostics;

import java.io.InputStream;
import java.util.zip.ZipInputStream;

import javax.annotation.Nonnull;

/**
 * A factory for {@link RecursiveZipReader}s, mainly for unit testing purpose.
 */
public interface RecursiveZipReaderFactory {

    @Nonnull
    RecursiveZipReader createReader(@Nonnull final InputStream zis);

    /**
     * The default implementation of {@link RecursiveZipReader} - this is what should happen in
     * production.
     */
    class DefaultRecursiveZipReaderFactory implements RecursiveZipReaderFactory {
        @Nonnull
        @Override
        public RecursiveZipReader createReader(@Nonnull final InputStream zis) {
            return new RecursiveZipReader(zis);
        }
    }
}
