package com.vmturbo.extractor;

import com.vmturbo.components.common.featureflags.FeatureFlags;

/**
 * Document any cleanup required when it comes time to retire the SAAS_REPORTING feature flag, and
 * that would not cause compile errors if left undone when the flag definition is removed.
 */
class SaaSReportingChecklist {
    private SaaSReportingChecklist() {
        //noinspection StatementWithEmptyBody
        if (FeatureFlags.SAAS_REPORTING.isEnabled()) {
            /*
             * Don't forget:
             * <ul>
             *     <li>Remove dashboards from resources</li>
             *     <li>Remove mentions of feature flag in python code</li>
             * </ul>
             *
             */
        }
    }
}
