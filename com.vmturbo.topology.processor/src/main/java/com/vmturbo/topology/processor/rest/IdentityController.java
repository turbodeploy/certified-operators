package com.vmturbo.topology.processor.rest;

import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import javax.annotation.Nonnull;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.vmturbo.topology.processor.entity.EntityStore;

/**
 * REST controller for oid management.
 */
@Api(value = "/identity")
@RequestMapping(value = "/identity")
@RestController
public class IdentityController {

    private final EntityStore entityStore;

    /**
     * Builds an instance of the {@link IdentityController}.
     * @param entityStore used to get the oids of the entities that are currently being discovered
     */
    public IdentityController(@Nonnull final EntityStore entityStore) {
        this.entityStore = Objects.requireNonNull(entityStore);
    }

    /**
     * Triggers a manual oid expiration. This process will expire oids in the assigned_oid table
     * and send the list of the expired oids to the other components
     *
     * @return The response entity containing the response.
     * @throws ExecutionException if the computation threw an
     * exception
     * @throws InterruptedException if the expiration oid thread was interrupted
     * @throws TimeoutException if the wait timed out
     */
    @RequestMapping(value = "/expire",
            method = RequestMethod.POST,
            produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
    @ApiOperation(value = "Expire the stale oids.",
    notes = "Triggers a manual oid expiration. This process will expire oids in the assigned_oid "
        + "table and send the list of the expired oids to the other components"
    )
    public ResponseEntity<String> expireOids() throws InterruptedException, ExecutionException, TimeoutException {
        final int nExpiredOids = entityStore.expireOids();
        return new ResponseEntity<>(
                String.format("Expired %d oids", nExpiredOids),
                HttpStatus.OK);
    }
}
