package com.vmturbo.integrations.intersight.licensing;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.cisco.intersight.client.model.LicenseLicenseInfo;
import com.cisco.intersight.client.model.LicenseLicenseInfo.LicenseStateEnum;
import com.cisco.intersight.client.model.LicenseLicenseInfo.LicenseTypeEnum;

import com.vmturbo.common.protobuf.licensing.Licensing.LicenseDTO;

/**
 * static utilities for testing
 */
public class IntersightLicenseTestUtils {

    /**
     * utility function for creating mock iwo licenses
     * @param moid desired license moid
     * @param type desired license type
     * @param state desired license state
     * @return desired LicenseDTO
     */
    public static LicenseLicenseInfo createIwoLicense(String moid, LicenseTypeEnum type, LicenseStateEnum state) {
        LicenseLicenseInfo iwoLicense = mock(LicenseLicenseInfo.class);
        when(iwoLicense.getMoid()).thenReturn(moid);
        when(iwoLicense.getLicenseType()).thenReturn(type);
        when(iwoLicense.getLicenseState()).thenReturn(state);
        return iwoLicense;
    }

    /**
     * utility function for creating a proxy license based on an IWO license with the specified
     * properties.
     * @param moid desired license moid
     * @param type desired license type
     * @param state desired license state
     * @return desired LicenseDTO
     */
    public static LicenseDTO createProxyLicense(String moid, LicenseTypeEnum type, LicenseStateEnum state) {
        return IntersightLicenseUtils.toProxyLicense(createIwoLicense(moid, type, state));
    }
}
