package com.vmturbo.components.common.setting;

/**
 * Enum definitions for OS Migration Settings.
 */
public class OsMigrationSettingsEnum {

    /**
     * Available options for os migration.
     */
    public enum OsMigrationProfileOption {
        /**
         * Match selected OS and license with target OS and license for cost, size and reservations.
         */
        MATCH_SOURCE_TO_TARGET_OS,
        /**
         * Match the selected OS to an unlicensed, compatible target, assuming that license fees
         * will be paid to a third party.
         */
        BYOL,
        /**
         * Use the specified Target OS and license for cost, size and reservations per source OS.
         */
        CUSTOM_OS
    }

    /**
     * Types of operating systems.
     */
    public enum OperatingSystem {
        /**
         * Linux operating system.
         */
        LINUX,
        /**
         * Red Hat Enterprise Linux.
         */
        RHEL,
        /**
         * SUSE Linux Enterprise Server.
         */
        SLES,
        /**
         * Windows operating system.
         */
        WINDOWS
    }
}
