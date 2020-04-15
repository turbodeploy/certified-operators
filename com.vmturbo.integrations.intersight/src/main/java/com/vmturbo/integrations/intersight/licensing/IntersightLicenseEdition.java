package com.vmturbo.integrations.intersight.licensing;

import java.util.SortedSet;

import com.cisco.intersight.client.model.LicenseLicenseInfo.LicenseTypeEnum;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.licensing.utils.CWOMLicenseEdition;

/**
 * An Intersight License Edition is a special classification of XL license that is only used when xl
 * is run as part of an Intersight configuration.
 *
 * <p>It's related to the {@link com.vmturbo.licensing.utils.CWOMLicenseEdition} in that the license
 * types are similar, and the feature set mapping is shared. They are used to license different
 * product variations, however.
 */
public enum IntersightLicenseEdition {
    IWO_ESSENTIALS(CWOMLicenseEdition.CWOM_ESSENTIALS),
    IWO_ADVANTAGE(CWOMLicenseEdition.CWOM_ADVANCED),
    IWO_PREMIER(CWOMLicenseEdition.CWOM_PREMIER);

    private static final Logger logger = LogManager.getLogger();

    private final CWOMLicenseEdition relatedCWOMLicenseEdition;

    IntersightLicenseEdition(CWOMLicenseEdition relatedCWOMLicenseEdition) {
        this.relatedCWOMLicenseEdition = relatedCWOMLicenseEdition;
    }

    /**
     * Convert from {@link LicenseTypeEnum} to {@link IntersightLicenseEdition}.
     *
     * @param intersightLicenseType the intersight license type to convert
     * @return the equivalent {@link IntersightLicenseEdition}, if one is found. If not, IWO_ESSENTIALS
     * will be returned. (this may be changed in the future)
     */
    public static IntersightLicenseEdition fromIntersightLicenseType(LicenseTypeEnum intersightLicenseType) {
        switch (intersightLicenseType) {
            case ESSENTIAL:
                return IWO_ESSENTIALS;
            case ADVANTAGE:
                return IWO_ADVANTAGE;
            case PREMIER:
                return IWO_PREMIER;
            default:
                // unrecognized license type. We will map this to essentials for now and log a warning.
                // in the future we may not want to map it to a license edition at all.
                logger.warn("Unrecognized Intersight license type: {}", intersightLicenseType);
                return IWO_ESSENTIALS;
        }
    }

    /**
     * Get the set of features this edition enables.
     *
     * @return a {@link SortedSet} of features available in this license edition.
     */
    public SortedSet<String> getFeatures() {
        return relatedCWOMLicenseEdition.getFeatures();
    }

}
