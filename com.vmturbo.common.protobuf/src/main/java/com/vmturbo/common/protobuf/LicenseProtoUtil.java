package com.vmturbo.common.protobuf;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.common.protobuf.licensing.Licensing.LicenseSummary;

/**
 * Various utilities methods for license related proto message.
 */
public final class LicenseProtoUtil {

    private LicenseProtoUtil() {}

    /**
     * Return the name to use for report editor users created by our platform in Grafana.
     * It's important that the same pattern be used across components, or else the users
     * created in Grafana won't match the usernames we try to log in with.
     *
     * @param editorPrefix The prefix to use.
     * @param editorNum The index of the editor.
     * @return The username to use.
     */
    @Nonnull
    public static String formatReportEditorUsername(@Nonnull final String editorPrefix, final int editorNum) {
        return String.format("%s-%d", editorPrefix, editorNum);
    }

    /**
     * Gets number of supported report editors for licenseSummary.
     * @param licenseSummary the licenseSummary
     * @return number of supported report editors or an empty optional if no Grafana license exists.
     */
    public static int numberOfSupportedReportEditors(@Nullable LicenseSummary licenseSummary) {
        if (licenseSummary != null) {
            return licenseSummary.getMaxReportEditorsCount();
        } else {
            return LicenseSummary.getDefaultInstance().getMaxReportEditorsCount();
        }
    }
}
