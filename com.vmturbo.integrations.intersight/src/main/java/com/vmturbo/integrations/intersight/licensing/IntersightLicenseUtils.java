package com.vmturbo.integrations.intersight.licensing;

import java.util.Set;

import javax.annotation.Nonnull;

import com.cisco.intersight.client.model.LicenseLicenseInfo;
import com.cisco.intersight.client.model.LicenseLicenseInfo.LicenseStateEnum;
import com.google.common.collect.ImmutableSet;

import org.apache.commons.lang.StringUtils;

import com.vmturbo.api.dto.license.ILicense;
import com.vmturbo.api.dto.license.ILicense.CountedEntity;
import com.vmturbo.common.protobuf.licensing.Licensing.LicenseDTO;
import com.vmturbo.licensing.License;
import com.vmturbo.licensing.utils.LicenseUtil;

/**
 * Utility methods for working with Intersight Proxy Licenses
 */
public class IntersightLicenseUtils {

    private IntersightLicenseUtils() {}

    /**
     * These are the license states an Intersight license can be in to be considered active.
     */
    public static final Set<LicenseStateEnum> INTERSIGHT_ACTIVE_LICENSE_STATES = ImmutableSet.of(
            LicenseStateEnum.COMPLIANCE, LicenseStateEnum.TRIALPERIOD, LicenseStateEnum.OUTOFCOMPLIANCE);


    /**
     * Check if the given {@link LicenseDTO} is an intersight license.
     * @param licenseDTO the license to check.
     * @return true if the license is recognized as an intersight license. false otherwise.
     */
    public static boolean isIntersightLicense(LicenseDTO licenseDTO) {
        try {
            return (IntersightLicenseEdition.valueOf(licenseDTO.getEdition()) != null);
        } catch (IllegalArgumentException iae) {
            return false;
        }
    }

    /**
     * Given an intersight {@link LicenseLicenseInfo}, create a matching proxy license, which captures the essential
     * aspects of the intersight license info in XL licensing terms.
     *
     * @param intersightLicense the license to convert.
     * @return a proxy {@link LicenseDTO} representing the original intersight license.
     */
    public static LicenseDTO toProxyLicense(@Nonnull LicenseLicenseInfo intersightLicense) {
        // determine the IWO license edition to map to
        IntersightLicenseEdition iwoEdition = IntersightLicenseEdition.fromIntersightLicenseType(intersightLicense.getLicenseType());

        // We will not track license expiration for intersight licenses -- that is the job of the
        // smart licensing server. In XL, we will interpret the Intersight LicenseState as an
        // expired/non-expired state by setting the expiration date. We do not want to trigger our
        // usual XL expiration warnings either. So we will simply set the expiration date to the
        // "permanent license" date if the license is "active", and clear the date if it's not active.
        // This should result in immediate expired / non-expired treatment in XL, without the "about
        // to expire" logic ever kicking-in.
        String expirationDate = INTERSIGHT_ACTIVE_LICENSE_STATES.contains(intersightLicense.getLicenseState())
                ? ILicense.PERM_LIC : "";

        License proxyLicense = new License();
        proxyLicense.setExternalLicense(true);
        proxyLicense.setExternalLicenseKey(intersightLicense.getMoid());
        proxyLicense.setEmail("support@cisco.com");
        proxyLicense.setEdition(iwoEdition.name());
        // we don't know how many licensed entities there are in Intersight yet -- hopefully this
        // is something that we can get in the future. In the meantime, we'll hardcode a limit. We
        // can also disable the entity count enforcement in the license check service, but I'd rather
        // not do that yet.
        proxyLicense.setCountedEntity(CountedEntity.VM);
        proxyLicense.setNumLicensedEntities(100000);
        proxyLicense.setExpirationDate(expirationDate);
        proxyLicense.addFeatures(iwoEdition.getFeatures());
        proxyLicense.setLicenseKey(LicenseUtil.generateLicenseKey(proxyLicense));
        // now that we have a keyed license, converted it to a protobuf license for sending to the
        // license manager.
        return LicenseDTO.newBuilder()
                .setExternalLicenseKey(proxyLicense.getExternalLicenseKey())
                .setEmail(proxyLicense.getEmail())
                .setEdition(proxyLicense.getEdition())
                .setExpirationDate(proxyLicense.getExpirationDate())
                .setCountedEntity(proxyLicense.getCountedEntity().name())
                .setNumLicensedEntities(proxyLicense.getNumLicensedEntities())
                .addAllFeatures(proxyLicense.getFeatures())
                .setLicenseKey(proxyLicense.getLicenseKey())
                .build();
    }

    /**
     * Determine if two proxy licenses are equivalent, apart from the oid field.
     *
     * @param a first proxy license to compare
     * @param b second proxy license to compare
     * @return true if the proxy licenses are considered equal.
     */
    public static boolean areProxyLicensesEqual(LicenseDTO a, LicenseDTO b) {
        if (a == b) {
            return true;
        }

        if (a == null || b == null) {
            return false;
        }

        if (!StringUtils.equals(a.getExternalLicenseKey(), b.getExternalLicenseKey())) {
            return false;
        }

        // the license key should cover the rest of the attributes, and also exclude the oid field.
        // this is because the license key is generated independently from the oid.
        if (!StringUtils.equals(a.getLicenseKey(), b.getLicenseKey())) {
            return false;
        }
        return true;
    }

}
