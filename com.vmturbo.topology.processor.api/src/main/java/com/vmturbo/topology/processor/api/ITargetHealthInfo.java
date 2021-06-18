package com.vmturbo.topology.processor.api;

import java.time.LocalDateTime;

import javax.annotation.Nonnull;

import com.vmturbo.platform.common.dto.Discovery.ErrorDTO.ErrorType;

/**
 * Represents target health info in topology-processor.
 */
public interface ITargetHealthInfo {
    /**
     * Subcategory of the health check done.
     */
    enum TargetHealthSubcategory {
        /**
         * Target health check: checked Validation.
         */
        VALIDATION,
        /**
         * Target health check: checked Discovery.
         */
        DISCOVERY,
        /**
         * Target health check: checked targets duplication.
         */
        DUPLICATION
    }

    /**
     * Get the id of checked target.
     * @return Long ID
     */
    @Nonnull Long getTargetId();

    /**
     * Get the name of the checked target.
     * @return String display name
     */
    @Nonnull String getDisplayName();

    /**
     * The result of which check we have here: just validation or discovery.
     * @return enum
     */
    @Nonnull TargetHealthSubcategory getSubcategory();

    /**
     * The type of the error that has happened during validation or discovery.
     * @return enum; received from probes; can be null if there was no error.
     */
    ErrorType getTargetErrorType();

    /**
     * The error text if there's anything to report.
     * @return String
     */
    String getErrorText();

    /**
     * The time of the first failure (provided that the target has failed validation or discovery).
     * @return LocalDateTime
     */
    LocalDateTime getTimeOfFirstFailure();

    /**
     * Number of consecutive failures (provided that the target has failed validation or discovery).
     * @return int; 0 if the target is in good state; 1 if the target has failed validation.
     */
    int getNumberOfConsecutiveFailures();
}
