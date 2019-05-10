package com.vmturbo.components.common.diagnostics;

import java.io.InputStream;

import javax.annotation.Nonnull;

/**
 * A factory for {@link DiagsZipReader}s, mainly for unit testing purpose.
 */
public interface DiagsZipReaderFactory {

    @Nonnull
    DiagsZipReader createReader(@Nonnull final InputStream zis);

    /**
     * The default implementation of {@link DiagsZipReader} - this is what should happen in
     * production.
     */
    class DefaultDiagsZipReader implements DiagsZipReaderFactory {
        @Nonnull
        @Override
        public DiagsZipReader createReader(@Nonnull final InputStream zis) {
            return new DiagsZipReader(zis);
        }
    }
}
