package com.vmturbo.api.component.external.api.health;

import java.util.Date;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;

import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.component.external.api.logging.GlobalExceptionHandler;
import com.vmturbo.api.component.external.api.service.LicenseService;
import com.vmturbo.api.exceptions.UnauthorizedObjectException;

/**
 * API component helper class to restart a component by killing the JVM if the component GRPC requests
 * haven't succeeded for the given period of time.
 */
public class ApiComponentRestartHelper {

    private final GlobalExceptionHandler globalExceptionHandler;
    private final LicenseService licenseService;
    private final Logger logger = LogManager.getLogger();
    private final long failureMillisBeforeRestart;
    private final long startupTime = System.currentTimeMillis();
    private final long failureHoursBeforeRestart;

    @VisibleForTesting
    long getLastSuccessTime() {
        return lastSuccessTime;
    }

    private volatile long lastSuccessTime = -1;

    /**
     * Constructs the producer health monitor.
     *
     * @param globalExceptionHandler global exception handler
     * @param licenseService license service
     * @param failureHoursBeforeRestart failure hours before restart
     */
    public ApiComponentRestartHelper(@Nonnull final GlobalExceptionHandler globalExceptionHandler,
            @Nonnull final LicenseService licenseService, final long failureHoursBeforeRestart) {
        this.globalExceptionHandler = Objects.requireNonNull(globalExceptionHandler);
        this.licenseService = Objects.requireNonNull(licenseService);
        this.failureHoursBeforeRestart = failureHoursBeforeRestart;
        this.failureMillisBeforeRestart = TimeUnit.MILLISECONDS.convert(failureHoursBeforeRestart,
                TimeUnit.HOURS);
    }

    /**
     * Check the grpc request to determine current health status.
     *
     * @return the current health status
     */

    public boolean isHealthy() {
        boolean isNowHealthy = true;
        final long now = System.currentTimeMillis();
        // only return a new status object if the health status has changed.
        final boolean wasUnknownStatusRuntimeException =
                globalExceptionHandler.wasGrpcUnknownStatusError();
        if (wasUnknownStatusRuntimeException) {
            try {
                // double check to avoid transient exception
                licenseService.getLicensesSummary();
                // if it's recovered, set the flag back to false.
                globalExceptionHandler.resetGrpcUnknownStatusFlag();
                this.lastSuccessTime = now;
            } catch (UnauthorizedObjectException e) {
                logger.error("Failed to get license summary");
            } catch (StatusRuntimeException ex) {
                // for now only catch UNKNOWN exception which is known unrecoverable
                if (ex.getStatus().getCode() == Code.UNKNOWN) {
                    isNowHealthy = false;
                }
            }
        }
        return isNowHealthy;
    }

    /**
     * This method is invoked once an hour.
     */
    public void execute() {
        try {
            if (!isHealthy()) {
                final long delta;
                if (lastSuccessTime == -1) {
                    // grpc invoked, but never succeeded
                    delta = System.currentTimeMillis() - startupTime;
                } else {
                    delta = System.currentTimeMillis() - lastSuccessTime;
                }

                if (delta >= failureMillisBeforeRestart) {
                    logger.info("Restarting component since GRPC requests keep failing for {} hours"
                                    + ", last success time: {}", failureHoursBeforeRestart,
                            lastSuccessTime != -1 ? new Date(lastSuccessTime) : "None");
                    System.exit(1);
                }
            }
        } catch (Exception e) {
            logger.error("Error when checking restarting the component", e);
        }
    }
}