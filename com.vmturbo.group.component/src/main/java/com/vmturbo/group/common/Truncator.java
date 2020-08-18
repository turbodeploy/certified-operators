package com.vmturbo.group.common;

import static com.vmturbo.group.db.tables.GroupTags.GROUP_TAGS;
import static com.vmturbo.group.db.tables.Grouping.GROUPING;
import static com.vmturbo.group.db.tables.Policy.POLICY;
import static com.vmturbo.group.db.tables.SettingPolicy.SETTING_POLICY;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.platform.sdk.common.util.SDKUtil;

/**
 * This is utility class that provides truncation required for fields to fit in DB table.
 */
public class Truncator {
    private static final int HASH_LENGTH = 40;

    private static final Logger logger = LogManager.getLogger();

    private Truncator() {}

    /**
     * Truncates a group source identifier to fit in DB table.
     *
     * @param sourceIdentifier the group source identifier.
     * @param showLogMessage if true logs a message that truncation happened.
     * @return the truncated source identifier.
     */
    public static String truncateGroupSourceIdentifier(String sourceIdentifier,
                                                       boolean showLogMessage) {
        return truncateString(sourceIdentifier,
            GROUPING.ORIGIN_DISCOVERED_SRC_ID.getDataType().length(),
                showLogMessage ? "Group Source Identifier" : null);
    }

    /**
     * Truncates a group display name to fit in DB table.
     *
     * @param displayName the group display name.
     * @param showLogMessage if true logs a message that truncation happened.
     * @return the truncated display name.
     */
    public static String truncateGroupDisplayName(String displayName,
                                                       boolean showLogMessage) {
        return truncateString(displayName, GROUPING.DISPLAY_NAME.getDataType().length(),
            showLogMessage ? "Group display name" : null);
    }

    /**
     * Truncates a policy name to fit in DB table.
     *
     * @param name the policy name.
     * @param showLogMessage if true logs a message that truncation happened.
     * @return the truncated policy name.
     */
    public static String truncatePolicyName(String name,
                                                       boolean showLogMessage) {
        return truncateString(name, POLICY.NAME.getDataType().length(),
             showLogMessage ? "Policy name" : null);
    }

    /**
     * Truncates a policy display name to fit in DB table.
     *
     * @param displayName the policy display name.
     * @param showLogMessage if true logs a message that truncation happened.
     * @return the truncated display name.
     */
    public static String truncatePolicyDisplayName(String displayName,
                                                  boolean showLogMessage) {
        return truncateString(displayName, POLICY.DISPLAY_NAME.getDataType().length(),
            showLogMessage ? "Policy display name" : null);
    }

    /**
     * Truncates a setting policy name to fit in DB table.
     *
     * @param name the setting policy name.
     * @param showLogMessage if true logs a message that truncation happened.
     * @return the truncated policy name.
     */
    public static String truncateSettingPolicyName(String name,
                                            boolean showLogMessage) {
        return truncateString(name, SETTING_POLICY.NAME.getDataType().length(),
            showLogMessage ? "Setting policy name" : null);
    }

    /**
     * Truncates a setting policy display name to fit in DB table.
     *
     * @param displayName the setting policy display name.
     * @param showLogMessage if true logs a message that truncation happened.
     * @return the truncated display name.
     */
    public static String truncateSettingPolicyDisplayName(String displayName,
                                                   boolean showLogMessage) {
        return truncateString(displayName, SETTING_POLICY.DISPLAY_NAME.getDataType().length(),
            showLogMessage ? "Setting policy display name" : null);
    }

    /**
     * Truncates a tag key to fit in DB table.
     *
     * @param tagKey the group tag key.
     * @param showLogMessage if true logs a message that truncation happened.
     * @return the truncated tag key.
     */
    public static String truncateTagKey(String tagKey, boolean showLogMessage) {
        return truncateString(tagKey, GROUP_TAGS.TAG_KEY.getDataType().length(),
            showLogMessage ? "Group tag key" : null);
    }

    /**
     * Truncates a tag value to fit in DB table.
     *
     * @param tagValue the group tag value.
     * @param showLogMessage if true logs a message that truncation happened.
     * @return the truncated tag value.
     */
    public static String truncateTagValue(String tagValue, boolean showLogMessage) {
        return truncateString(tagValue, GROUP_TAGS.TAG_VALUE.getDataType().length(),
            showLogMessage ? "Group tag value" : null);
    }

    private static String truncateString(@Nonnull String inputString, int maxStringLength,
                                        @Nullable String logFieldMessage) {
        final String truncatedString = SDKUtil.fixUuid(inputString, maxStringLength,
            maxStringLength - HASH_LENGTH);

        if (logFieldMessage != null && !truncatedString.equals(inputString)) {
            logger.warn(logFieldMessage + String.format(
                " got truncated. The string %s has been truncated to %s.",
                inputString, truncatedString));
        }

        return truncatedString;
    }
}
