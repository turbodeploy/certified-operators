package com.vmturbo.api.component.external.api;

import java.util.List;

import com.vmturbo.common.protobuf.target.TargetDTO.TargetDetails;
import com.vmturbo.common.protobuf.target.TargetDTO.TargetHealth;
import com.vmturbo.common.protobuf.target.TargetDTO.TargetHealthSubCategory;
import com.vmturbo.platform.common.dto.Discovery.ErrorTypeInfo;

import common.HealthCheck.HealthState;

/**
 * Provides some basic methods for health check tests.
 */
public class HealthChecksTestBase {
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

    protected TargetHealth makeHealthCritical(final TargetHealthSubCategory category,
                                      final String targetDisplayName, final ErrorTypeInfo errorTypeInfo, final String errorText,
                                      final long failureTime, final int failureTimes) {
        return makeCriticalHealthBuilder(category, targetDisplayName, errorTypeInfo, errorText)
                        .setTimeOfFirstFailure(failureTime)
                        .setConsecutiveFailureCount(failureTimes)
                        .build();
    }

    protected TargetHealth makeHealthCritical(final TargetHealthSubCategory category,
                    final String targetDisplayName, final ErrorTypeInfo errorTypeInfo, final String errorText,
                    final long failureTime) {
        return makeCriticalHealthBuilder(category, targetDisplayName, errorTypeInfo, errorText)
                        .setTimeOfFirstFailure(failureTime)
                        .build();
    }

    protected TargetHealth makeHealthCritical(final TargetHealthSubCategory category,
                    final String targetDisplayName, final ErrorTypeInfo errorTypeInfo, final String errorText) {
        return makeCriticalHealthBuilder(category, targetDisplayName, errorTypeInfo, errorText)
                        .build();
    }

    private TargetHealth.Builder makeCriticalHealthBuilder(final TargetHealthSubCategory category,
                    final String targetDisplayName, final ErrorTypeInfo errorTypeInfo, final String errorText) {
        return TargetHealth.newBuilder()
                        .setHealthState(HealthState.CRITICAL)
                        .setSubcategory(category)
                        .setTargetName(targetDisplayName)
                        .setMessageText(errorText)
                        .addErrorTypeInfo(errorTypeInfo);
    }

    protected TargetHealth makeHealthMinor(final TargetHealthSubCategory category,
                    final String targetDisplayName, final String errorText) {
        return TargetHealth.newBuilder()
                        .setHealthState(HealthState.MINOR)
                        .setSubcategory(category)
                        .setTargetName(targetDisplayName)
                        .setMessageText(errorText)
                        .build();
    }

    protected TargetHealth makeHealthNormal(final TargetHealthSubCategory category, final String targetDisplayName) {
        return TargetHealth.newBuilder()
                        .setHealthState(HealthState.NORMAL)
                        .setSubcategory(category)
                        .setTargetName(targetDisplayName)
                        .build();
    }
}
