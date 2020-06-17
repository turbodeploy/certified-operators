package com.vmturbo.auth.api.licensing;

import java.time.OffsetDateTime;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ExecutorService;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import com.google.protobuf.Empty;
import com.google.protobuf.ProtocolStringList;

import io.grpc.Channel;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

import com.vmturbo.common.protobuf.licensing.LicenseCheckServiceGrpc;
import com.vmturbo.common.protobuf.licensing.LicenseCheckServiceGrpc.LicenseCheckServiceBlockingStub;
import com.vmturbo.common.protobuf.licensing.Licensing.GetLicenseSummaryResponse;
import com.vmturbo.common.protobuf.licensing.Licensing.LicenseSummary;
import com.vmturbo.components.api.client.ComponentNotificationReceiver;
import com.vmturbo.components.api.client.IMessageReceiver;
import com.vmturbo.platform.sdk.common.util.ProbeLicense;

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

    private final Channel authChannel;

    // how long to wait for the first license summary to be retrieved before throwing a
    // LicenseCheckNotReady exception
    private long licenseSummaryTimeoutMs;

    // cache the most recent license summary
    @GuardedBy("this")
    private LicenseSummary lastLicenseSummary;

    /**
     * Provides access to a stream of license summary updates.
     */
    private final Flux<LicenseSummary> updateEventFlux;

    /**
     * The statusEmitter is used to push updates to the statusFlux subscribers.
     */
    private FluxSink<LicenseSummary> updateEventEmitter;

    public LicenseCheckClient(@Nonnull final IMessageReceiver<LicenseSummary> messageReceiver,
                              @Nonnull final ExecutorService executorService,
                              @Nullable final Channel authChannel,
                              final long licenseSummaryTimeoutMs) {
        super(messageReceiver, executorService);
        this.authChannel = authChannel;
        this.licenseSummaryTimeoutMs = licenseSummaryTimeoutMs;
        // create a flux that a listener can subscribe to LicenseSummary update events on.
        Flux<LicenseSummary> primaryFlux = Flux.create(emitter -> updateEventEmitter = emitter);
        updateEventFlux = primaryFlux.share(); // create a shareable flux for multicasting.
        // start publishing immediately w/o waiting for a consumer to signal demand.
        updateEventFlux.publish().connect();
        // bootstrap the license check client by trying to get the latest available license summary
        // from the auth service.
        final Thread bootstrapTread = new Thread(this::requestLatestLicenseSummary);
        bootstrapTread.start();
    }

    /**
     * Request the latest available license summary from the LicenseCheckService.
     */
    protected void requestLatestLicenseSummary() {
        if (authChannel == null) {
            logger.info("No auth channel available -- will not request current license summary.");
            return;
        }
        // if we have an auth channel, then lets' try to fetch the latest available license summary.
        // create a grpc client
        LicenseCheckServiceBlockingStub client = LicenseCheckServiceGrpc.newBlockingStub(authChannel)
                .withWaitForReady();
        logger.info("Requesting latest available license summary.");
        new Thread(() -> {
            GetLicenseSummaryResponse response = client.getLicenseSummary(Empty.getDefaultInstance());
            if (response.hasLicenseSummary()) {
                updateLicenseSummary(response.getLicenseSummary());
            }
        }).start();
    }

    /**
     * Get the update event stream. A listener can .subscribe() to the {@link Flux} that
     * is returned and get realtime notification of new LicenseSummary objects as soon as they are
     * available.
     *
     * @return a {@link Flux} of {@link LicenseSummary} objects
     */
    public Flux<LicenseSummary> getUpdateEventStream() {
        return updateEventFlux;
    }

    /**
     * get the latest license summary received by the check client.
     */
    public LicenseSummary getLicenseSummary() {
        if (lastLicenseSummary == null) {
            // see if we can wait for it
            try {
                updateEventFlux.blockFirstMillis(licenseSummaryTimeoutMs);
            } catch (IllegalStateException ise) {
                // this is just a timeout on the block -- no need to log anything. We'll throw the exception later.
            }
        }
        // if still no license, throw an error
        if (lastLicenseSummary == null) {
            throw new LicenseCheckNotReadyException();
        }
        return lastLicenseSummary;
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
    public boolean isFeatureAvailable(ProbeLicense feature) {
        return getLicenseSummary().getFeatureList().contains(feature.getKey());
    }

    /**
     * Check if a collection of features are available in the registered licenses. If a license summary is
     * not available, this check will always return false.
     *
     * <p>TODO: consider throwing something if the license summary is not available yet, vs. no licenses
     * found.
     *
     * @param features the collection of features to check for
     * @return true, if all of the feature are available. False if any are not available, or a
     * license summary is not available.
     */
    public boolean areFeaturesAvailable(Collection<ProbeLicense> features) {
        ProtocolStringList licenseFeatures = getLicenseSummary().getFeatureList();
        // return false if any of the requested features are not present in the license summarya
        for (ProbeLicense feature : features) {
            if (!licenseFeatures.contains(feature.getKey())) {
                return false;
            }
        }
        return true;
    }

    /**
     * Utility method that will check if a feature is available using {@link #isFeatureAvailable(ProbeLicense)},
     * and throws a {@link LicenseFeaturesRequiredException} if it is not.
     *
     * @param feature the feature to check for
     * @throws LicenseFeaturesRequiredException if any features are not present in the active licenses
     */
    public void checkFeatureAvailable(ProbeLicense feature)
            throws LicenseFeaturesRequiredException {
        if (!isFeatureAvailable(feature)) {
            throw new LicenseFeaturesRequiredException(Collections.singleton(feature));
        }
    }


    /**
     * Utility method that will check if a collection of features are available using {@link #areFeaturesAvailable(Collection)},
     * and throws a {@link LicenseFeaturesRequiredException} if any are not present.
     *
     * @param features the set of features to check for
     * @throws LicenseFeaturesRequiredException if any features are not present in the active licenses
     */
    public void checkFeaturesAvailable(Collection<ProbeLicense> features)
            throws LicenseFeaturesRequiredException {
        if (!areFeaturesAvailable(features)) {
            throw new LicenseFeaturesRequiredException(features);
        }
    }

    /**
     * Return true if we know that there is at least one valid installed. It may be expired though.
     *
     * @return true if the license is valid. false if no valid license was detected, or the license
     * check client hasn't finished initialization yet.
     */
    public boolean hasValidLicense() {
        if (lastLicenseSummary != null) {
            return lastLicenseSummary.getIsValid();
        }
        return false;
    }

    /**
     * Return true if we know that there is at least one valid and non-expired license installed.
     * @return true if a valid and non-expired license is known to be installed. false if not
     * found, or the license check client hasn't finished initialization yet.
     */
    public boolean hasValidNonExpiredLicense() {
        if (lastLicenseSummary != null) {
            return lastLicenseSummary.getIsValid() && (!lastLicenseSummary.getIsExpired());
        }
        return false;
    }

    /**
     * Get the current license summary.
     *
     * @return
     */
    public LicenseSummary geCurrentLicenseSummary() {
        return lastLicenseSummary;
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
        return hasValidLicense() && isFeatureAvailable(ProbeLicense.VISIBILITY_ONLY);
    }

    @Override
    protected void processMessage(@Nonnull final LicenseSummary message) {
        updateLicenseSummary(message);
    }

    /**
     * Evaluate an incoming license summary to see if it should replace the last known license summary.
     *
     * The new summary can replace the last known license summary as long as one of the following
     * is true:
     * <ul>
     *     <li>The new license summary has a newer generation date than the last known.</li>
     *     <li>There is no "last known" license summary yet.</li>
     * </ul>
     *
     * @param incomingLicenseSummary the new license summary to check
     * @return true, if the summary was updated. false otherwise.
     */
    protected synchronized boolean updateLicenseSummary(@Nonnull final LicenseSummary incomingLicenseSummary) {
        if (isLicenseSummaryDifferentAndNewer(lastLicenseSummary, incomingLicenseSummary)) {
            logger.info("Got a new license summary generated at {}", incomingLicenseSummary.getGenerationDate());
            lastLicenseSummary = incomingLicenseSummary;
            // push to flux too
            updateEventEmitter.next(lastLicenseSummary);
            return true;
        }
        return false;
    }

    /**
     * Checks to see if the new license is different than the previous one.
     *
     * @param before The previous license (possibly null)
     * @param after The new license
     *
     * @return true if the licenses are different in any way.
     */
    protected static boolean isLicenseSummaryDifferentAndNewer(@Nullable LicenseSummary before, @Nonnull LicenseSummary after) {
        if (before == null) {
            // no previous license, this is a change
            return true;
        }

        // If any license fields are different, we'll call the license different
        if (before.equals(after)) {
            return false;
        }
        // The licenses are different, now let's make sure the "after" is actually "after".
        // if the before license has no generation date, we'll return true since we can't actually
        // compare the dates anyways.
        if (StringUtils.isBlank(before.getGenerationDate())) {
            return true;
        }

        // if "after" has no generation date, we have to assume "before" is later.
        if (StringUtils.isBlank(after.getGenerationDate())) {
            return false;
        }

        // both license summaries should have generation dates -- compare them.
        return OffsetDateTime.parse(before.getGenerationDate()).isBefore(OffsetDateTime.parse(after.getGenerationDate()));
    }
}
