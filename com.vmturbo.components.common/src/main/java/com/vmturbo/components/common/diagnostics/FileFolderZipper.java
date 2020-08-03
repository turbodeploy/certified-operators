package com.vmturbo.components.common.diagnostics;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import javax.annotation.Nonnull;

import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Dump all the files in a given folder into a ZipOutputStream. The file path for each file in
 * the zip will be prefixed with a base folder name. The ZipOutputStream will be
 * {@link ZipOutputStream#}, which means the underlying stream will <em>not</em> be closed.
 */
public class FileFolderZipper {
    /**
     * The logger.
     */
    private Logger logger_ = LogManager.getLogger();

    /**
     * Write all the files in a given directory onto a {@link ZipOutputStream}. Calculate a file
     * path name for the
     * zipped file based on the node and a given "base" path.
     * If we throw {@link IOException} then any open zipEntry is closed.
     *
     * @param basePath      base file path to annotate each zip file
     * @param diagnosticZip the destination ZipOutputStream to write zip files onto
     * @param src           the source directory from which files will be zipped
     * @throws IOException if an I/O error occurs
     */
    public void zipFilesInFolder(@Nonnull String basePath,
                                 @Nonnull ZipOutputStream diagnosticZip,
                                 @Nonnull Path src)
            throws IOException {
        try (DirectoryStream<Path> paths = Files.newDirectoryStream(src)) {
            for (Path diagnosticFilePath : paths) {
                File fPath = diagnosticFilePath.toFile();
                String srcName = fPath.getName();
                String srcPath = fPath.getCanonicalPath();
                String zipFilePath = basePath + "/" + srcName;
                logger_.debug("adding zip file entry for: " + zipFilePath);

                if (diagnosticFilePath.toFile().isDirectory()) {
                    // recursively handle the directory.
                    logger_.info("recurse into directory {}", Paths.get(srcPath));
                    zipFilesInFolder(zipFilePath, diagnosticZip, Paths.get(srcPath));
                } else {
                    // handle a single file
                    diagnosticZip.putNextEntry(new ZipEntry(zipFilePath));
                    try (FileInputStream diagnosticFile = new FileInputStream(srcPath)) {
                        IOUtils.copy(diagnosticFile, diagnosticZip);
                        diagnosticZip.closeEntry();
                    } catch (IOException e) {
                        logger_.error("Unable to add " + fPath + " to archive", e);
                    }
                }
            }
        }
    }
}