package com.vmturbo.components.common.diagnostics;

import java.util.List;

import javax.annotation.Nullable;

/**
 * A diagnostics item. Usually the content of one zip entry in a zip file.
 * Use with {@link DiagsZipReader} to iterate over the content of a zip file.
 * The iterator returns an instance of Diags for every zipped file. It also
 * handles nested zip files.
 */
public class Diags {
    private final String name;
    private final List<String> lines;
    private final byte[] bytes;

    Diags(String name, List<String> lines) {
        this.name = name;
        this.lines = lines;
        this.bytes = null;
    }

    Diags(String name, byte[] bytes) {
        this.name = name;
        this.lines = null;
        this.bytes = bytes;
    }

    /**
     * The name of the diagnostics which is the name of the corresponding ZipEntry.
     * @return the name of the diagnostics
     */
    public String getName() {
        return name;
    }

    /**
     * The content of the diagnostics item as a list of lines.
     * @return the content of the diagnostics as a list of lines
     */
    @Nullable
    public List<String> getLines() {
        return lines;
    }

    @Nullable
    public byte[] getBytes() {
        return bytes;
    }

    /**
     * The content of the diagnostics item as one string.
     * @return the content of the diagnostics as one string
     */
    public String getContent() {
        return String.join("\n", lines);
    }

    @Override
    public String toString() {
        final String description;
        if (lines != null) {
            description = lines.toString();
        } else if (bytes != null) {
            description = bytes.length + "bytes";
        } else {
            description = null;
        }
        return name + " : " + description;
    }
}
