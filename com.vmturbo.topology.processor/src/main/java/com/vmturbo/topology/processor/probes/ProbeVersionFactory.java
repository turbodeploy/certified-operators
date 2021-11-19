package com.vmturbo.topology.processor.probes;

import javax.annotation.Nullable;

import com.google.common.base.Strings;
import com.vdurmont.semver4j.Semver;
import com.vdurmont.semver4j.SemverException;

import com.vmturbo.platform.sdk.common.util.Pair;

import common.HealthCheck.HealthState;

/**
 * A factory related to probe versions.
 */
public class ProbeVersionFactory {
    /**
     * Error messages related to probe version errors.
     */
    enum ProbeVersionErrorMessage {
        MISSING("Probe version is missing%s; please upgrade probe to version '%s'."),
        CUSTOM("Probe version ('%s') is custom; please confirm image being used supports server version '%s'."),
        OLDER("Probe version ('%s') is not current; please upgrade probe to version '%s'."),
        NEWER("Probe version ('%s') is newer than the server version ('%s'); please ensure this is expected.");

        private final String format;

        /**
         * The enum constructor.
         *
         * @param format the format according to which the error message will be constructed.
         */
        ProbeVersionErrorMessage(String format) {
            this.format = format;
        }

        /**
         * Return the corresponding error message constructed based on the probe and the server versions.
         *
         * @param probeVersion the version of the probe
         * @param serverVersion the version of the server
         * @return the constructed error message
         */
        String getMessage(final String probeVersion, final String serverVersion) {
            return String.format(format, Strings.nullToEmpty(probeVersion), Strings.nullToEmpty(serverVersion));
        }
    }

    /**
     * Error messages related to server version errors.
     */
    enum ServerVersionErrorMessage {
        MISSING("Server version%s is missing; please check with your administrator."),
        CUSTOM("Server version '%s' is a custom version; please ensure this is expected.");

        private final String format;

        /**
         * The enum constructor.
         *
         * @param format the format according to which the error message will be constructed.
         */
        ServerVersionErrorMessage(String format) {
            this.format = format;
        }

        /**
         * Return the corresponding error message constructed based on the server version.
         *
         * @param serverVersion the version of the server
         * @return the constructed error message
         */
        String getMessage(final String serverVersion) {
            return String.format(format,  Strings.nullToEmpty(serverVersion));
        }
    }

    private ProbeVersionFactory() {}

    /**
     * Compare the given probe and server versions, and return the deduced health state of the
     * probe based on the versions and the associated error message.
     *
     * @param probeVersion the version of the probe
     * @param serverVersion the version of the server represented by the topology processor
     * @return the health state as well as the error message if any
     */
    public static Pair<HealthState, String> deduceProbeHealth(
            @Nullable final String probeVersion, @Nullable final String serverVersion) {
        if (Strings.isNullOrEmpty(serverVersion)) {
            return Pair.create(HealthState.MAJOR,
                    ServerVersionErrorMessage.MISSING.getMessage(serverVersion));
        }
        final Semver serverSemver;
        try {
            serverSemver = new Semver(serverVersion).toStrict().withClearedSuffixAndBuild();
        } catch (SemverException e) {
            return Pair.create(HealthState.MAJOR,
                    ServerVersionErrorMessage.CUSTOM.getMessage(serverVersion));
        }

        if (Strings.isNullOrEmpty(probeVersion)) {
            return Pair.create(HealthState.MAJOR,
                    ProbeVersionErrorMessage.MISSING.getMessage(probeVersion, serverSemver.toString()));
        }
        final Semver probeSemver;
        try {
            probeSemver = new Semver(probeVersion).toStrict().withClearedSuffixAndBuild();
        } catch (SemverException e) {
            return Pair.create(HealthState.MINOR,
                    ProbeVersionErrorMessage.CUSTOM.getMessage(probeVersion, serverVersion));
        }

        if (probeSemver.isEquivalentTo(serverSemver)) {
            return Pair.create(HealthState.NORMAL, "");
        }
        if (probeSemver.isGreaterThan(serverSemver)) {
            return Pair.create(HealthState.MINOR,
                    ProbeVersionErrorMessage.NEWER.getMessage(probeVersion, serverVersion));
        }
        return Pair.create(HealthState.MAJOR,
                ProbeVersionErrorMessage.OLDER.getMessage(probeVersion, serverSemver.toString()));
    }

}
