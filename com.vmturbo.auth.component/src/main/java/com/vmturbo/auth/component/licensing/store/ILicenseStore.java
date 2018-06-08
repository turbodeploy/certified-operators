package com.vmturbo.auth.component.licensing.store;

import java.io.IOException;
import java.util.Collection;

import com.vmturbo.auth.api.authorization.AuthorizationException;
import com.vmturbo.common.protobuf.licensing.Licensing.LicenseDTO;

/**
 * Implements license secure storage.
 */
public interface ILicenseStore {

    /**
     * Retrieves the stored licenses in raw (string xml) format.
     * The information may be retrieved by anyone.
     *
     * @return a collection of the licenses currently in storage, in XML format
     * @throws IOException if there was a problem reading the licenses
     */
    Collection<LicenseDTO> getLicenses() throws IOException;

    /**
     * Finds a stored license by key
     * @param key the license key to search for
     * @return The license data, if found. Null if not found.
     * @throws IOException if there is an error loading the license
     */
//    String getLicense(String key) throws IOException;

    /**
     * Finds a stored license by uuid
     * @param uuid the license uuid to search for
     * @return The license object, if found. Null if not found.
     * @throws IOException if there is an error loading the license
     */
    LicenseDTO getLicense(String uuid) throws IOException;

    /**
     * Add a new license or update an existing license in storage, based on a LicenseDTO object
     * passed to this function.
     *
     * @param license The license object to add or replace.
     * @throws IOException if there is an error saving the license.
     */
    void storeLicense(LicenseDTO license) throws IOException;

    /**
     * Remove the license identified by it's key.
     *
     * @param uuid the uuid of the license to remove.
     * @return true, if a license was removed. false if a license wasn't removed.
     * @throws java.io.IOException if there is an error removing the license
     */
    boolean removeLicense(String uuid) throws IOException;
}
