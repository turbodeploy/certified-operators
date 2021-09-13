package com.vmturbo.topology.processor.rest;

import java.util.Objects;

import javax.annotation.Nonnull;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.vmturbo.topology.processor.staledata.StaleDataManager;

/**
 * REST controller for stale data.
 */
@Api(value = "/staledata")
@RequestMapping(value = "/staledata")
@RestController
public class StaleDataController {

    private final StaleDataManager staleDataManager;

    /**
     * Builds an instance of the {@link StaleDataController}.
     *
     * @param staleDataManager used to perform stale data detection
     */
    public StaleDataController(@Nonnull final StaleDataManager staleDataManager) {
        this.staleDataManager = Objects.requireNonNull(staleDataManager);
    }

    /**
     * Triggers an immediate stale data check and notification.
     *
     * @return - true if the process succeeded
     */
    @RequestMapping(value = "/notify", method = RequestMethod.POST,
            produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
    @ApiOperation(value = "Notify on stale data",
            notes = "Triggers a check for stale data. This operation will scan the health of each"
                    + " target and notify all listeners (eg: logger, email notification)")
    @Nonnull
    public ResponseEntity<Boolean> checkStaleDataAndNotify() {
        final boolean isOperationScheduled = staleDataManager.notifyImmediately();
        return new ResponseEntity<>(isOperationScheduled, HttpStatus.OK);
    }
}
