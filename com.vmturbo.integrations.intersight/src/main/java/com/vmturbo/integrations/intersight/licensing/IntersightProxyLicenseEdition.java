package com.vmturbo.integrations.intersight.licensing;

import java.util.SortedSet;

import com.cisco.intersight.client.model.LicenseLicenseInfo.LicenseTypeEnum;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.licensing.utils.CWOMLicenseEdition;

/**
 * An Intersight Proxy License Edition is a special classification of XL license that is only used when xl
 * is run as part of an Intersight configuration. A Proxy License Edition represents an XL license
 * type that maps to an Intersight License Edition.
 *
 * <p>It's related to the {@link com.vmturbo.licensing.utils.CWOMLicenseEdition} in that the license
 * types are similar, and the feature set mapping is shared.
 *
 * <p>The IWO_FALLBACK license edition is a special "minimal" license that may be used when there
 * are no active normal licenses available from Intersight. It's just intended to "keep the lights on"
 * in XL but without enabling major features.
 */
public enum IntersightProxyLicenseEdition {
    IWO_BASE(CWOMLicenseEdition.CWOM_ESSENTIALS.getFeatures(), 1),
    IWO_ESSENTIALS(CWOMLicenseEdition.CWOM_ESSENTIALS.getFeatures(), 2),
    IWO_ADVANTAGE(CWOMLicenseEdition.CWOM_ADVANCED.getFeatures(), 3),
    IWO_PREMIER(CWOMLicenseEdition.CWOM_PREMIER.getFeatures(), 4),
    IWO_FALLBACK(CWOMLicenseEdition.CWOM_ESSENTIALS.getFeatures(), 0);

    private static final Logger logger = LogManager.getLogger();

    private final int tierNumber;

    private final SortedSet<String> features;

    IntersightProxyLicenseEdition(SortedSet<String> features, int tierNumber) {
        this.tierNumber = tierNumber;
        this.features = features;
    }

    /**
     * Convert from {@link LicenseTypeEnum} to {@link IntersightProxyLicenseEdition}.
     *
     * @param intersightLicenseType the intersight license type to convert
     * @return the equivalent {@link IntersightProxyLicenseEdition}, if one is found. If not, IWO_ESSENTIALS
     * will be returned. (this may be changed in the future)
     */
    public static IntersightProxyLicenseEdition fromIntersightLicenseType(LicenseTypeEnum intersightLicenseType) {
        switch (intersightLicenseType) {
            case BASE:
                return IWO_BASE;
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
                return IWO_FALLBACK;
        }
    }

    /**
     * Get a numeric representation of the license tier. Used to compare which license edition is
     * "higher" than another. A higher number represents a higher tier.
     * @return the license tier number.
     */
    public int getTierNumber() {
        return tierNumber;
    }

    /**
     * Get the set of features this edition enables.
     *
     * @return a {@link SortedSet} of features available in this license edition.
     */
    public SortedSet<String> getFeatures() {
        return features;
    }

}
