package com.vmturbo.auth.api.licensing;

import java.util.concurrent.ExecutorService;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.swagger.models.License;

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
    LicenseSummary lastLicenseSummary;

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
     * @return true iff summary is available.
     */
    public boolean isReady() {
        return lastLicenseSummary != null;
    }

    @Override
    protected void processMessage(@Nonnull final LicenseSummary message) throws ApiClientException, InterruptedException {
        logger.info("Got a new license summary generated at {}", message.getGenerationDate());
        lastLicenseSummary = message;
        // trigger update
    }
}
