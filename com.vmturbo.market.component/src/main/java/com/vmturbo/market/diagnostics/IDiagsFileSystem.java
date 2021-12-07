package com.vmturbo.market.diagnostics;

import java.nio.file.Path;
import java.util.stream.Stream;

/**
 * Abstraction around the file system to help with the testability of code which interacts with file system.
 */
public interface IDiagsFileSystem {

    /**
     * Deletes a file if it exists.
     * @param fileToDelete file {@link Path} to delete
     */
    void deleteIfExists(Path fileToDelete);

    /**
     * List the files in a directory.
     * @param directoryPath directoryPath to list contents of
     * @return Stream of Paths representing the directory contents
     */
    Stream<Path> listFiles(Path directoryPath);

    /**
     * Checks if the path is a directory.
     * @param path path to check
     * @return true if directory, false otherwise
     */
    boolean isDirectory(Path path);

    /**
     * Gets the last modified date.
     * @param path path to check
     * @return Gets the last modified date
     */
    long getLastModified(Path path);
}
