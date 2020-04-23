package com.vmturbo.topology.processor.rest;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

import com.vmturbo.platform.common.dto.Discovery;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.topology.processor.actions.SdkToProbeActionsConverter;
import com.vmturbo.topology.processor.api.AccountDefEntry;
import com.vmturbo.topology.processor.api.impl.ProbeRESTApi.AccountField;
import com.vmturbo.topology.processor.api.impl.ProbeRESTApi.GetAllProbes;
import com.vmturbo.topology.processor.api.impl.ProbeRESTApi.ProbeDescription;
import com.vmturbo.topology.processor.probes.AccountValueAdaptor;
import com.vmturbo.topology.processor.probes.ProbeStore;

/**
 * REST API to retrieve information about registered probes.
 */
@Api(value = "/probe")
@RequestMapping(value = "/probe")
@RestController
public final class ProbeController {

    private final ProbeStore probeStore;

    public ProbeController(@Nonnull final ProbeStore probeStore) {
        this.probeStore = Objects.requireNonNull(probeStore);
    }

    @RequestMapping(method = RequestMethod.GET,
            produces = { MediaType.APPLICATION_JSON_UTF8_VALUE })
    @ApiOperation("Get all probes.")
    public ResponseEntity<GetAllProbes> getAllProbes() {
        final List<ProbeDescription> probeDescriptions = probeStore.getProbes().entrySet()
                        .stream()
                        .map(entry -> create(entry.getKey(), entry.getValue()))
                        .collect(Collectors.toList());
        return new ResponseEntity<>(new GetAllProbes(probeDescriptions), HttpStatus.OK);
    }

    @RequestMapping(value = "/{probeId}",
                    method = RequestMethod.GET,
                    produces = { MediaType.APPLICATION_JSON_UTF8_VALUE })
    @ApiOperation("Get probe information by id.")
    @ApiResponses(value = { @ApiResponse(code = 404,
                    message = "If the probe doesn't exist in the topology processor.",
                    response = ProbeDescription.class) })
    public ResponseEntity<ProbeDescription> getProbe(@ApiParam(
                    value = "The ID of the probe.") @PathVariable("probeId") final Long probeId) {
        final Optional<ProbeInfo> probeInfo = probeStore.getProbe(probeId);
        if (probeInfo.isPresent()) {
            return new ResponseEntity<>(create(probeId, probeInfo.get()), HttpStatus.OK);
        } else {
            return new ResponseEntity<>(new ProbeDescription("Probe not found by id " + probeId),
                            HttpStatus.NOT_FOUND);
        }
    }

    protected static AccountField create(@Nonnull final Discovery.AccountDefEntry accountDefEntry) {
        Objects.requireNonNull(accountDefEntry);
        final AccountDefEntry wrapper = AccountValueAdaptor.wrap(accountDefEntry);
        return new AccountField(
                    wrapper.getName(), wrapper.getDisplayName(),
                    wrapper.getDescription(), wrapper.isRequired(), wrapper.isSecret(),
                    wrapper.getValueType(), wrapper.getDefaultValue(), wrapper.getAllowedValues()
            , wrapper.getVerificationRegex());
    }

    protected static ProbeDescription create(final long probeId, @Nonnull final ProbeInfo probeInfo) {
        final List<AccountField> fields = probeInfo.getAccountDefinitionList().stream()
                        .map(ad -> create(ad)).collect(Collectors.toList());
        return new ProbeDescription(probeId, probeInfo.getProbeType(), probeInfo.getProbeCategory(),
            probeInfo.getCreationMode(), fields, probeInfo.getTargetIdentifierFieldList(),
                SdkToProbeActionsConverter.convert(probeInfo.getActionPolicyList()));
    }
}
