package com.vmturbo.topology.processor.probes;

import javax.annotation.Nullable;

import com.google.common.base.Strings;
import com.vdurmont.semver4j.Semver;
import com.vdurmont.semver4j.SemverException;

import com.vmturbo.api.enums.healthCheck.HealthState;
import com.vmturbo.platform.sdk.common.util.Pair;

/**
 * A factory related to probe versions.
 */
public class ProbeVersionFactory {
    /**
     * Error message for when the probe version is just one behind the platform's.
     */
    static final String PROBE_VERSION_ONE_BEHIND_MESSAGE = "Probe is one version behind; please consider upgrade";
    /**
     * Error message for when the probe version is too far from the platform's.
     */
    static final String PROBE_VERSION_INCOMPATIBLE_MESSAGE = "An incompatible probe version is in use; please upgrade.";
    /**
     * Error message for when the probe version is newer than the platform's.
     */
    static final String PROBE_VERSION_NEWER_MESSAGE = "Probe version is newer than the platform's; please ensure this is expected.";

    private ProbeVersionFactory() {}

    /**
     * Compare the given probe and platform versions, and return the deduced health state of the
     * probe based on the versions and the associated error message.
     *
     * @param probeVersion the version of the probe
     * @param platformVersion the version of the platform represented by the topology processor
     * @return the health state as well as the error message if any
     */
    public static Pair<HealthState, String> deduceProbeHealth(
            @Nullable final String probeVersion, @Nullable final String platformVersion) {
        final Semver probeSemver;
        try {
            probeSemver = new Semver(Strings.nullToEmpty(probeVersion)).toStrict();
        } catch (SemverException e) {
            return Pair.create(HealthState.MAJOR, "Probe has " + e.getMessage().toLowerCase());
        }

        final Semver platformSemver;
        try {
            platformSemver = new Semver(Strings.nullToEmpty(platformVersion)).toStrict();
        } catch (SemverException e) {
            return Pair.create(HealthState.MAJOR, "Platform has " + e.getMessage().toLowerCase());
        }

        if (probeSemver.isEquivalentTo(platformSemver)) {
            return Pair.create(HealthState.NORMAL, "");
        }
        if (probeSemver.isGreaterThan(platformSemver)) {
            return Pair.create(HealthState.MINOR, PROBE_VERSION_NEWER_MESSAGE);
        }
        switch (probeSemver.diff(platformSemver)) {
            case PATCH:
                if (probeSemver.getPatch() + 1 == platformSemver.getPatch()) {
                    return Pair.create(HealthState.MINOR, PROBE_VERSION_ONE_BEHIND_MESSAGE);
                }
            default:
                return Pair.create(HealthState.MAJOR, PROBE_VERSION_INCOMPATIBLE_MESSAGE);
        }
    }

}
