package com.vmturbo.integrations.intersight.licensing;

import java.util.Comparator;
import java.util.Set;

import javax.annotation.Nonnull;

import com.cisco.intersight.client.JSON;
import com.cisco.intersight.client.model.LicenseLicenseInfo;
import com.cisco.intersight.client.model.LicenseLicenseInfo.LicenseStateEnum;
import com.cisco.intersight.client.model.LicenseLicenseInfo.LicenseTypeEnum;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableSet;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import com.vmturbo.api.dto.license.ILicense;
import com.vmturbo.api.dto.license.ILicense.CountedEntity;
import com.vmturbo.common.protobuf.licensing.Licensing.LicenseDTO;
import com.vmturbo.licensing.License;
import com.vmturbo.licensing.utils.LicenseUtil;

/**
 * Utility methods for working with Intersight Proxy Licenses
 */
public class IntersightLicenseUtils {
    private static final Logger logger = LogManager.getLogger();

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
            return (IntersightProxyLicenseEdition.valueOf(licenseDTO.getEdition()) != null);
        } catch (IllegalArgumentException iae) {
            return false;
        }
    }

    /**
     * Check if the intersight license is an active license. This would be determined by the license
     * state. If the license state is in any of the INTERSIGHT_ACTIVE_LICENSE_STATES, the license is
     * considered active.
     * @param intersightLicense the intersight license info to check
     * @return true, if the license was deemed active.
     */
    public static boolean isActiveIntersightLicense(@Nonnull LicenseLicenseInfo intersightLicense) {
        return INTERSIGHT_ACTIVE_LICENSE_STATES.contains(intersightLicense.getLicenseState());
    }

    /**
     * Check if the {@link LicenseDTO} is an "active" proxy license or not. It will be considered
     * an active proxy license as long as it's both an intersight license and has a non-empty
     * expiration date field.
     * @param licenseDTO the license to check.
     * @return true if the license is both a proxy license and has not expired.
     */
    public static boolean isActiveProxyLicense(LicenseDTO licenseDTO) {
        // validate that it's an intersight proxy license
        if (!isIntersightLicense(licenseDTO)) {
            return false;
        }
        // validate the expiration date field
        return !StringUtils.isBlank(licenseDTO.getExpirationDate());
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
        IntersightProxyLicenseEdition iwoEdition = IntersightProxyLicenseEdition.fromIntersightLicenseType(intersightLicense.getLicenseType());

        // We will not track license expiration for intersight licenses -- that is the job of the
        // smart licensing server. In XL, we will interpret the Intersight LicenseState as an
        // expired/non-expired state by setting the expiration date. We do not want to trigger our
        // usual XL expiration warnings either. So we will simply set the expiration date to the
        // "permanent license" date if the license is "active", and clear the date if it's not active.
        // This should result in immediate expired / non-expired treatment in XL, without the "about
        // to expire" logic ever kicking-in.
        String expirationDate = isActiveIntersightLicense(intersightLicense) ? ILicense.PERM_LIC : "";
        return createProxyLicense(intersightLicense.getMoid(), expirationDate, iwoEdition);
    }

    /**
     * utility function for creating an IWO LicenseLicenseInfo using JSON deserialization.
     *
     * @param json the JSON parser to use.
     * @param moid desired license moid
     * @param type desired license type
     * @param state desired license state
     * @return desired LicenseDTO
     * @throws JsonProcessingException if the deserializer encounters a problem processing the json
     */
    public static LicenseLicenseInfo createIwoLicense(JSON json, String moid, LicenseTypeEnum type, LicenseStateEnum state)
            throws JsonProcessingException {
        JSONObject licenseJson = new JSONObject();
        licenseJson.put("Moid", moid);
        licenseJson.put("LicenseType", type.name());
        licenseJson.put("LicenseState", state.name());
        licenseJson.put("ObjectType", "license.LicenseInfo");
        logger.info("new license json: "+ licenseJson.toString());
        LicenseLicenseInfo newIwoLicenseInfo = json.getMapper().readValue(licenseJson.toString(),
                LicenseLicenseInfo.class);
        return newIwoLicenseInfo;
    }

    /**
     * Create a proxy license using the specified properties.
     * @param moid the moid of the source license -- will be set as the external license key.
     * @param expirationDate the expiration date of the proxy license
     * @param iwoEdition the proxy license edition name
     * @return the proxy {@link LicenseDTO}
     */
    public static LicenseDTO createProxyLicense(String moid, String expirationDate, IntersightProxyLicenseEdition iwoEdition) {
        License proxyLicense = new License();
        proxyLicense.setExternalLicense(true);
        proxyLicense.setExternalLicenseKey(moid);
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

    /**
     * Sort licenses so that the "best" license is the highest-ranked. The sorting will be based on
     * the following criteria:
     * <ul>
     *     <li>Valid/active licenses are all ranked higher than invalid/expired licenses</li>
     *     <li>Licenses with higher-tier editions are ranked higher than lower-tier editions.</li>
     *     <li>final tie-breaker: order by moid<li>
     * </ul>
     */
    public static class BestAvailableIntersightLicenseComparator implements Comparator<LicenseLicenseInfo> {

        @Override
        public int compare(final LicenseLicenseInfo a, final LicenseLicenseInfo b) {
            if (a == null || b == null) {
                throw new NullPointerException();
            }

            // if one license is active and the other is not, use this as the ranking
            boolean isAActive = isActiveIntersightLicense(a);
            boolean isBActive = isActiveIntersightLicense(b);
            if (isAActive != isBActive) {
                return isAActive ? 1 : -1;
            }

            // both licenses have the same validity. Let's compare license types
            IntersightProxyLicenseEdition edition1 = IntersightProxyLicenseEdition.fromIntersightLicenseType(a.getLicenseType());
            IntersightProxyLicenseEdition edition2 = IntersightProxyLicenseEdition.fromIntersightLicenseType(b.getLicenseType());
            if (edition1.getTierNumber() != edition2.getTierNumber()) {
                // higher-tiered license wins
                return (edition1.getTierNumber() > edition2.getTierNumber()) ? 1 : -1;
            }

            // ok, same validity and tier -- let's tie-break using moids
            return StringUtils.compare(a.getMoid(), b.getMoid());
        }
    }
}
