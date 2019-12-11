package com.vmturbo.auth.api.licensing;

import java.util.concurrent.ExecutorService;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.licensing.Licensing.LicenseSummary;
import com.vmturbo.components.api.client.ApiClientException;
import com.vmturbo.components.api.client.ComponentNotificationReceiver;
import com.vmturbo.components.api.client.IMessageReceiver;

/**
 * The LicenseCheckClient provides access to the latest license verification information, including
 * what features are available in the installed licenses, current workload count and related limit,
 * if there is one.
 *
 * The client will listen for new license summary events in kafka and provides a callback mechanism
 * if you need to react to changes in license information.
 */
public class LicenseCheckClient extends ComponentNotificationReceiver<LicenseSummary> {

    public static final String LICENSE_SUMMARY_TOPIC = "license-summary";

    private final Logger logger = LogManager.getLogger();

    // cache the most recent license summary
    private LicenseSummary lastLicenseSummary;

    public LicenseCheckClient(@Nonnull final IMessageReceiver<LicenseSummary> messageReceiver,
                              @Nonnull final ExecutorService executorService) {
        super(messageReceiver, executorService);
    }

    /**
     * Check if a specific feature is available in the registered licenses. If a license summary is
     * not available, this check will always return false.
     *
     * TODO: consider throwing something if the license summary is not available yet, vs. no licenses
     * found.
     *
     * @param feature the feature to check for
     * @return true, if the feature is available. False if not available, or a license summary is
     * not available.
     */
    public boolean isFeatureAvailable(LicenseFeature feature) {
        if (lastLicenseSummary != null) {
            return lastLicenseSummary.getFeatureList().contains(feature.getKey());
        }
        return false;
    }

    public boolean hasValidLicense() {
        if (lastLicenseSummary != null) {
            return lastLicenseSummary.getIsValid();
        }
        return false;
    }

    /**
     * Is license check ready? Internally, it will check if last license summary is available.
     * @return true if summary is available.
     */
    public boolean isReady() {
        return lastLicenseSummary != null;
    }

    /**
     * Check if this license is for developer freemium edition.
     * @return true if "visibility_only" feature exists in a valid license.
     */
    public boolean isDevFreemium() {
        return hasValidLicense() && isFeatureAvailable(LicenseFeature.VISIBILITY_ONLY);
    }

    @Override
    protected void processMessage(@Nonnull final LicenseSummary message) {
        if (isLicenseDifferent(lastLicenseSummary, message)) {
            logger.info("Got a new license summary generated at {}", message.getGenerationDate());
        }
        lastLicenseSummary = message;
        // trigger update
    }

    /**
     * Checks to see if the new license is different than the previous one.
     *
     * @param before The previous license (possibly null)
     * @param after The new license
     *
     * @return true if the licenses are different in any way.
     */
    private static boolean isLicenseDifferent(@Nullable LicenseSummary before, @Nonnull LicenseSummary after) {
        if (before == null) {
            // no previous license, this is a change
            return true;
        }

        // If any license fields are different, we'll call the license different
        return !before.equals(after);
    }
}
