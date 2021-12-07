package com.vmturbo.market.diagnostics;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * {@inheritDoc}.
 */
public class DiagsFileSystem implements IDiagsFileSystem {

    private static final Logger logger = LogManager.getLogger();

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteIfExists(Path fileToDelete) {
        try {
            if (Files.deleteIfExists(fileToDelete)) {
                logger.info("Deleted file " + fileToDelete.getFileName().toString());
            }
        } catch (IOException e) {
            logger.error("Could not delete analysis digas file " + fileToDelete.getFileName().toString() + " :", e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Stream<Path> listFiles(Path path) {
        try {
            return Files.list(path);
        } catch (IOException e) {
            logger.error("Could not list files in " + path + ": ", e);
            return Stream.empty();
        }
    }

    @Override
    public boolean isDirectory(Path path) {
        return Files.isDirectory(path);
    }

    @Override
    public long getLastModified(Path path) {
        return path.toFile().lastModified();
    }
}
