package com.vmturbo.topology.processor.rest;

import java.util.Collection;
import java.util.Objects;

import javax.annotation.Nonnull;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.vmturbo.topology.processor.api.impl.ProbeRESTApi.ProbeDescription;
import com.vmturbo.topology.processor.api.impl.ProbeRegistrationRESTApi.GetAllProbeRegistrations;
import com.vmturbo.topology.processor.api.impl.ProbeRegistrationRESTApi.ProbeRegistrationDescription;
import com.vmturbo.topology.processor.probes.ProbeStore;

/**
 * REST API to retrieve information about probe registrations.
 */
@Api(value = "/probe/registration")
@RequestMapping(value = "/probe/registration")
@RestController
public final class ProbeRegistrationController {

    private final ProbeStore probeStore;

    /**
     * Constructs a probe registration controller given the probe store.
     *
     * @param probeStore the probe store
     */
    public ProbeRegistrationController(@Nonnull final ProbeStore probeStore) {
        this.probeStore = Objects.requireNonNull(probeStore);
    }

    /**
     * Returns all the probe registrations.
     *
     * @return all the probe registrations
     */
    @RequestMapping(method = RequestMethod.GET,
            produces = { MediaType.APPLICATION_JSON_VALUE })
    @ApiOperation("Get all probe registrations.")
    public ResponseEntity<GetAllProbeRegistrations> getAllProbesRegistrations() {
        final Collection<ProbeRegistrationDescription> probeRegistrationDescriptions = probeStore.getAllProbeRegistrations();
        return new ResponseEntity<>(new GetAllProbeRegistrations(probeRegistrationDescriptions), HttpStatus.OK);
    }

    /**
     * Returns a probe registration by id.
     *
     * @param probeRegistrationId the id of the probe registration
     * @return a probe registration associated with the id
     */
    @RequestMapping(value = "/{probeRegistrationId}",
                    method = RequestMethod.GET,
                    produces = { MediaType.APPLICATION_JSON_VALUE })
    @ApiOperation("Get probe registration information by id.")
    @ApiResponses(value = { @ApiResponse(code = 404,
                    message = "If the probe registration doesn't exist in the topology processor.",
                    response = ProbeDescription.class) })
    public ResponseEntity<ProbeRegistrationDescription> getProbeRegistration(@ApiParam(
                    value = "The ID of the probe registration.") @PathVariable("probeId") final Long probeRegistrationId) {
        return probeStore.getProbeRegistrationById(probeRegistrationId)
                .map(probeRegistration -> new ResponseEntity<>(probeRegistration, HttpStatus.OK))
                .orElse(new ResponseEntity<>(new ProbeRegistrationDescription(), HttpStatus.NOT_FOUND));
    }
}
