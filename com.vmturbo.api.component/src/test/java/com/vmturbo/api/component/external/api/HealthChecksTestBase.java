package com.vmturbo.api.component.external.api;

import java.util.List;

import com.vmturbo.common.protobuf.target.TargetDTO.TargetDetails;
import com.vmturbo.common.protobuf.target.TargetDTO.TargetHealth;
import com.vmturbo.common.protobuf.target.TargetDTO.TargetHealthSubCategory;
import com.vmturbo.platform.common.dto.Discovery.ErrorDTO.ErrorType;

/**
 * Provides some basic methods for health check tests.
 */
public class HealthChecksTestBase {
    protected static final int DEFAULT_FAILED_DISCOVERY_COUNT_THRESHOLD = 3;

    protected TargetDetails createTargetDetails(Long id, TargetHealth health, List<Long> derived,
                    List<Long> parents, boolean hidden) {
        return TargetDetails.newBuilder()
                        .setTargetId(id)
                        .setHealthDetails(health)
                        .addAllDerived(derived)
                        .addAllParents(parents)
                        .setHidden(hidden)
                        .build();
    }

    protected TargetHealth makeHealth(final TargetHealthSubCategory category,
                    final String targetDisplayName, final ErrorType errorType, final String errorText,
                    final long failureTime, final int failureTimes) {
        return TargetHealth.newBuilder()
                        .setSubcategory(category)
                        .setTargetName(targetDisplayName)
                        .setMessageText(errorText)
                        .setErrorType(errorType)
                        .setTimeOfFirstFailure(failureTime)
                        .setConsecutiveFailureCount(failureTimes)
                        .build();
    }

    protected TargetHealth makeHealth(final TargetHealthSubCategory category, final String targetDisplayName,
                    final String errorText) {
        return TargetHealth.newBuilder()
                        .setSubcategory(category)
                        .setTargetName(targetDisplayName)
                        .setMessageText(errorText)
                        .build();
    }

    protected TargetHealth makeHealth(final TargetHealthSubCategory category, final String targetDisplayName) {
        return TargetHealth.newBuilder()
                        .setSubcategory(category)
                        .setTargetName(targetDisplayName)
                        .build();
    }
}
