package com.vmturbo.components.common.diagnostics;

import javax.annotation.Nonnull;

/**
 * Something that could produce a diagnostics dump for further investigations.
 */
public interface Diagnosable {
    /**
     * Returns the file name which will hold this diagnostics information in the resulting zip
     * archive.
     *
     * @return file name insude zip
     */
    @Nonnull
    String getFileName();
}
