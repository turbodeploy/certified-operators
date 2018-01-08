package com.vmturbo.auth.component.store;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import org.springframework.security.access.prepost.PreAuthorize;

import com.vmturbo.auth.api.authorization.AuthorizationException;
import com.vmturbo.components.crypto.CryptoFacility;

/**
 * Implements {@link ILicenseStore} by storing license in docker volume.
 *
 * It's immutable and thus thread safe.
 */
@ThreadSafe
public class LicenseLocalStore implements ILicenseStore {
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
     * The license file name
     */
    private String VMT_LICENSE_FILE = "vmt_license.inout";


    /**
     * Retrieves the license.
     * The information may be retrieved by anyone.
     *
     * @return The license if it exists.
     */
    @Nonnull
    @Override
    public Optional<String> getLicense() {
        final String location = System.getProperty(VMT_LICENSE_DIR_PARAM, VMT_LICENSE_DIR);
        Path licenseFile = Paths.get(location + "/" + VMT_LICENSE_FILE);
        try {
            if (Files.exists(licenseFile)) {
                byte[] bytes = Files.readAllBytes(licenseFile);
                String licenseCipherText = new String(bytes, CHARSET_CRYPTO);
                String license = CryptoFacility.decrypt(licenseCipherText);
                return Optional.of(license);
            }
        } catch (IOException e) {
            throw new SecurityException(e);
        }
        return Optional.empty();
    }

    /**
     * Store the license to file. The license is encrypted in docker volume.
     *
     * @param license The license in plain text.
     * @throws AuthorizationException In case of user doesn't have Administrator role.
     */
    @PreAuthorize("hasRole('ADMINISTRATOR')")
    @Override
    public void populateLicense(@Nonnull final String license) throws AuthorizationException {
        final String location = System.getProperty(VMT_LICENSE_DIR_PARAM, VMT_LICENSE_DIR);
        Path licenseFile = Paths.get(location + "/" + VMT_LICENSE_FILE);
        Path outputDir = Paths.get(location);
        try {
            if (!Files.exists(outputDir)) {
                Files.createDirectories(outputDir);
            }
            // Persist
            Files.write(licenseFile,
                    CryptoFacility.encrypt(license).getBytes(CHARSET_CRYPTO));
        } catch (IOException e) {
            throw new SecurityException(e);
        }

    }
}
