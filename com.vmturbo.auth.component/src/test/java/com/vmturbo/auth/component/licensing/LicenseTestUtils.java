package com.vmturbo.auth.component.licensing;

import static org.junit.Assert.assertEquals;

import java.util.Collection;
import java.util.Date;
import java.util.SortedSet;

import org.junit.Test;

import com.vmturbo.api.dto.license.ILicense;
import com.vmturbo.api.dto.license.ILicense.CountedEntity;
import com.vmturbo.licensing.License;
import com.vmturbo.licensing.utils.CWOMLicenseEdition;
import com.vmturbo.licensing.utils.LicenseUtil;

/**
 * Utility methods for testing turbo licenses
 */
public class LicenseTestUtils {

    /**
     * Utility function for creating a license.
     * @return
     */
    static protected ILicense createLicense(Date expirationDate, String email, Collection<String> features, int workloadCount) {
        License newLicense = new License();
        newLicense.setEdition("Test");
        newLicense.setEmail(email);
        newLicense.setExpirationDate(ILicense.formatDateToISO(expirationDate));
        newLicense.addFeatures(features);
        newLicense.setCountedEntity(CountedEntity.VM);
        newLicense.setNumLicensedEntities(workloadCount);
        newLicense.setLicenseKey(LicenseUtil.generateLicenseKey(newLicense));
        return newLicense;
    }
}
