package com.vmturbo.topology.processor.probes;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Strings;
import com.vdurmont.semver4j.Semver;
import com.vdurmont.semver4j.SemverException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.platform.sdk.common.MediationMessage;
import com.vmturbo.platform.sdk.common.util.Pair;

import common.HealthCheck.HealthState;

/**
 * A factory related to probe versions.
 */
public class ProbeVersionFactory {

    private static final Logger logger = LogManager.getLogger();
    private static final String CLOUD_NATIVE_PROBE_NAME = "kubeturbo";

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
     *  Error messages specific to CLoud Native probes.
     */
    enum CloudNativeAdditionalErrorMessage {
        CPU_THROTTLING_BREAKING_CHANGE_MESSAGE("Container CPU limit resize actions are currently disabled.");
        private final String message;

        CloudNativeAdditionalErrorMessage(String message) {
            this.message = message;
        }

        public String get() {
            return message;
        }
    }

    private static final String CPU_THROTTLING_BREAKING_CHANGE_VERSION = "8.7.5";

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

    /**
     * Enum representing the result of probe version comparison.
     */
    enum ProbeVersionCompareResult {
        EQUAL,
        SMALLER,
        GREATER,
        UNKNOWN
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
    public static Pair<HealthState, String>  deduceProbeHealth(
            @Nullable final String probeVersion, @Nullable final String serverVersion, @Nullable final MediationMessage.ProbeInfo probeInfo) {
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

        String msg = ProbeVersionErrorMessage.OLDER.getMessage(probeVersion, serverSemver.toString());

        // append additional message for kubeturbo versions that would cause Container CPU resize actions to be disabled
        if (probeInfo != null
                && probeInfo
                    .getProbeTargetInfo()
                    .getInputValuesList()
                    .stream()
                    .anyMatch(accountValue -> accountValue.getStringValue().contains(CLOUD_NATIVE_PROBE_NAME))
                && serverSemver.isGreaterThanOrEqualTo(CPU_THROTTLING_BREAKING_CHANGE_VERSION)
                && probeSemver.isLowerThan(CPU_THROTTLING_BREAKING_CHANGE_VERSION)) {
            msg = msg + " " + CloudNativeAdditionalErrorMessage.CPU_THROTTLING_BREAKING_CHANGE_MESSAGE.get();
        }

        return Pair.create(HealthState.MAJOR, msg);
    }

    /**
     * Compare two probe versions.
     * - If both versions are missing (e.g., an empty string), return unknown result
     * - If one of the versions is missing , then the missing version is considered smaller
     * - If the two version strings are literally the same (ignore case), return equal result
     * - Otherwise compare two versions following Semantic Versioning scheme
     * - If any of the versions is not a semantic version, return unknown result
     *
     * @param probeVersion1 The first probe version
     * @param probeVersion2 The second probe version
     * @return the probe version comparison result
     */
    @Nonnull
    public static ProbeVersionCompareResult compareProbeVersion(@Nullable final String probeVersion1,
                                                                @Nullable final String probeVersion2) {
        if (Strings.isNullOrEmpty(probeVersion1) && Strings.isNullOrEmpty(probeVersion2)) {
            return ProbeVersionCompareResult.UNKNOWN;
        }
        if (Strings.isNullOrEmpty(probeVersion1)) {
            return ProbeVersionCompareResult.SMALLER;
        }
        if (Strings.isNullOrEmpty(probeVersion2)) {
            return ProbeVersionCompareResult.GREATER;
        }
        if (probeVersion1.equalsIgnoreCase(probeVersion2)) {
            return ProbeVersionCompareResult.EQUAL;
        }
        try {
            final Semver probeSemver1 = new Semver(probeVersion1).toStrict().withClearedSuffixAndBuild();
            final Semver probeSemver2 = new Semver(probeVersion2).toStrict().withClearedSuffixAndBuild();
            if (probeSemver1.isEquivalentTo(probeSemver2)) {
                return ProbeVersionCompareResult.EQUAL;
            }
            if (probeSemver1.isGreaterThan(probeSemver2)) {
                return ProbeVersionCompareResult.GREATER;
            }
            return ProbeVersionCompareResult.SMALLER;
        } catch (SemverException e) {
            logger.warn("Cannot compare between version {} and {}: {}",
                        probeVersion1, probeVersion2, e.getMessage());
            return ProbeVersionCompareResult.UNKNOWN;
        }
    }
}
