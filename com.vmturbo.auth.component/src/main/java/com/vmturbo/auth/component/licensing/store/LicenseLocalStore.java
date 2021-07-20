package com.vmturbo.auth.component.licensing.store;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Collections;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.api.crypto.CryptoFacility;
import com.vmturbo.common.protobuf.licensing.Licensing.LicenseDTO;

/**
 * Implements {@link ILicenseStore} by storing license in docker volume.
 *
 * It's immutable and thus thread safe.
 */
@ThreadSafe
public class LicenseLocalStore implements ILicenseStore {
    private static final Logger logger = LogManager.getLogger();

    /**
     * The license location property
     */
    private static final String VMT_LICENSE_DIR_PARAM = "com.vmturbo.license";

    /**
     * The default license file location
     */
    private static final String VMT_LICENSE_DIR = "/home/turbonomic/data/license";

    /**
     * The charset for the license
     */
    private static final String CHARSET_CRYPTO = "UTF-8";

    /**
     * The license file name prefix
     */
    private String VMT_LICENSE_FILE_PREFIX = "vmt_license.";

    /**
     * The license file suffix
     */
    private String VMT_LICENSE_FILE_SUFFIX = ".license";

    /**
     * Given a license uuid, generate the license file name it should be stored with.
     *
     * @param uuid the uuid for the license.
     * @return The filename that the license should be stored in.
     */
    private String generateLicenseFilename(String uuid) {
        return VMT_LICENSE_FILE_PREFIX + uuid + VMT_LICENSE_FILE_SUFFIX;
    }

    /**
     * Get the path where the license files should be kept.
     * @return the path where the license files should be kept.
     */
    private String getLicenseFileDirectory() {
        final String rootPath = System.getProperty(VMT_LICENSE_DIR_PARAM, VMT_LICENSE_DIR);
        return rootPath;
    }

    /**
     * Given a license uuid, generate the full path to the file it should be saved in.
     *
     * @param uuid the uuid of the license.
     * @return the full path to the file that this license should be stored in.
     */
    private String generateLicenseFilepath(String uuid) {
        return getLicenseFileDirectory() + File.separator + generateLicenseFilename(uuid);
    }

    @Override
    public LicenseDTO getLicense(final String uuid) throws IOException {
        Path licensePath = Paths.get(generateLicenseFilepath(uuid));
        return readLicenseFile(licensePath);
    }

    /**
     * Retrieves the existing licenses.
     * The information may be retrieved by anyone.
     *
     * @return A collection of the licenses currently in storage.
     */
    @Nonnull
    @Override
    public Collection<LicenseDTO> getLicenses() throws IOException {
        Path licensePath = Paths.get(getLicenseFileDirectory() + File.separator);

        if (Files.exists(licensePath)) {
            try {
                return Files.walk(licensePath)
                        .filter(path -> {
                            String filename = path.getFileName().toString();
                            return (filename.startsWith(VMT_LICENSE_FILE_PREFIX)
                                    && filename.endsWith(VMT_LICENSE_FILE_SUFFIX));
                        })
                        .map(path -> {
                            try {
                                return this.readLicenseFile(path);
                            } catch (IOException e) {
                                // throw as RTE so we can catch outside the lambda.
                                throw new RuntimeException(e);
                            }
                        })
                        .collect(Collectors.toList());
            } catch (RuntimeException rte) {
                if (rte.getCause() instanceof IOException) {
                    throw (IOException) rte.getCause();
                }
                throw rte;
            }
        }
        // no files to read -- return nothing
        return Collections.emptyList();
    }

    /**
     * Given a file path, read, decrypt and return the license contained in the file.
     * @param licenseFilePath the path to the license file to read
     * @return the LicenseDTO stored in the file, if it exists. Null, if empty or not found.
     */
    private LicenseDTO readLicenseFile(Path licenseFilePath) throws IOException {
        // read the file and decrypt the license in it.
        logger.info("Reading license file {}", licenseFilePath.getFileName());
        if (Files.notExists(licenseFilePath)) {
            logger.info("License file {} not found.", licenseFilePath.toString());
            return null;
        }

        byte[] encryptedBytes = Files.readAllBytes(licenseFilePath);
        byte[] decryptedBytes = CryptoFacility.decrypt(encryptedBytes);
        return LicenseDTO.parseFrom(decryptedBytes);
    }

    /**
     * Store the license to file. The license is encrypted in docker volume.
     *
     * @param licenseDTO The license protobuf to store.
     */
    @Override
    public void storeLicense(@Nonnull LicenseDTO licenseDTO) throws IOException {
        Path licenseFile = Paths.get(generateLicenseFilepath(licenseDTO.getUuid()));
        Path outputDir = Paths.get(getLicenseFileDirectory());
        if (!Files.exists(outputDir)) {
            Files.createDirectories(outputDir);
        }
        // Write the license file
        Files.write(licenseFile, CryptoFacility.encrypt(licenseDTO.toByteArray()));
    }

    /**
     * Remove the license w/the specified key, if it exists in storage.
     *
     * @param key the key of the license to remove.
     */
    @Override
    public boolean removeLicense(final String key) throws IOException {
        Path licenseFilePath = Paths.get(generateLicenseFilepath(key));
        if (Files.exists(licenseFilePath)) {
            Files.delete(licenseFilePath);
            return true;
        }
        return false;
    }

}
