package com.vmturbo.topology.processor.api.impl;

import java.net.URI;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.springframework.http.HttpStatus;
import org.springframework.http.RequestEntity;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;

import com.vmturbo.communication.CommunicationException;
import com.vmturbo.components.api.client.ComponentApiConnectionConfig;
import com.vmturbo.components.api.client.ComponentRestClient;
import com.vmturbo.topology.processor.api.DiscoveryStatus;
import com.vmturbo.topology.processor.api.ProbeInfo;
import com.vmturbo.topology.processor.api.TargetData;
import com.vmturbo.topology.processor.api.TopologyProcessorException;
import com.vmturbo.topology.processor.api.ValidationStatus;
import com.vmturbo.topology.processor.api.dto.InputField;
import com.vmturbo.topology.processor.api.impl.OperationRESTApi.DiscoverAllResponse;
import com.vmturbo.topology.processor.api.impl.OperationRESTApi.OperationResponse;
import com.vmturbo.topology.processor.api.impl.OperationRESTApi.ValidateAllResponse;
import com.vmturbo.topology.processor.api.impl.ProbeRESTApi.GetAllProbes;
import com.vmturbo.topology.processor.api.impl.ProbeRESTApi.ProbeDescription;
import com.vmturbo.topology.processor.api.impl.TargetRESTApi.GetAllTargetsResponse;
import com.vmturbo.topology.processor.api.impl.TargetRESTApi.TargetInfo;
import com.vmturbo.topology.processor.api.impl.TargetRESTApi.TargetSpec;

/**
 * REST-specific part of topology processor client implementation.
 */
public class TopologyProcessorRestClient extends ComponentRestClient {

    private final String targetUri;
    private final String probeUri;
    private final String entityUri;
    private final String actionUri;

    private final NoExceptionsRestClient<GetAllProbes> getAllProbesClient;

    private final ProbeRestClient getProbeClient;

    private final SensitiveDataTargetRestClient addTargetClient;

    private final TargetRestClient getTargetClient;

    private final NoExceptionsRestClient<GetAllTargetsResponse> getAllTargetsClient;

    private final SensitiveDataTargetRestClient updateTargetsClient;

    private final TargetRestClient remoteTargetsClient;

    private final OperationResultRestClient targetOperationClient;

    private final NoExceptionsRestClient<ValidateAllResponse> validateAllClient;

    private final NoExceptionsRestClient<DiscoverAllResponse> discoverAllClient;


    public TopologyProcessorRestClient(@Nonnull final ComponentApiConnectionConfig connectionConfig) {
        super(connectionConfig);
        this.targetUri = restUri + "/target/";
        this.probeUri = restUri + "/probe/";
        this.entityUri = restUri + "/entity/";
        this.actionUri = restUri + "/action/";

        getAllProbesClient = new NoExceptionsRestClient<>(GetAllProbes.class);
        getProbeClient = new ProbeRestClient();
        addTargetClient = new SensitiveDataTargetRestClient(HttpStatus.BAD_REQUEST);
        getTargetClient = new TargetRestClient(HttpStatus.NOT_FOUND);
        getAllTargetsClient = new NoExceptionsRestClient<>(GetAllTargetsResponse.class);
        updateTargetsClient = new SensitiveDataTargetRestClient(HttpStatus.BAD_REQUEST, HttpStatus.NOT_FOUND,
                        HttpStatus.SERVICE_UNAVAILABLE);
        remoteTargetsClient =
                        new TargetRestClient(HttpStatus.NOT_FOUND, HttpStatus.SERVICE_UNAVAILABLE);
        targetOperationClient = new OperationResultRestClient();
        validateAllClient = new NoExceptionsRestClient<>(ValidateAllResponse.class);
        discoverAllClient = new NoExceptionsRestClient<>(DiscoverAllResponse.class);
    }

    @Nonnull
    public Set<ProbeInfo> getAllProbes() throws CommunicationException {
        final RequestEntity<?> request = RequestEntity.get(URI.create(probeUri)).build();
        final GetAllProbes result = getAllProbesClient.execute(request);
        return ImmutableSet.copyOf(result.getProbes());
    }

    @Nonnull
    public ProbeInfo getProbe(final long id) throws CommunicationException, TopologyProcessorException {
        final RequestEntity<?> request =
                        RequestEntity.get(URI.create(probeUri + Long.toString(id))).build();
        return getProbeClient.execute(request);
    }

    public long addTarget(final long probeId, @Nonnull final TargetData targetData)
                    throws CommunicationException, TopologyProcessorException {
        final TargetSpec spec = new TargetSpec(probeId,
                Objects.requireNonNull(targetData).getAccountData().stream()
                    .map(this::convert)
                    .collect(Collectors.toList()));
        final RequestEntity<?> request = RequestEntity.post(URI.create(targetUri)).body(spec);
        return addTargetClient.execute(request).getId();
    }

    @Nonnull
    public TargetInfo getTarget(final long id)
            throws CommunicationException, TopologyProcessorException {
        final RequestEntity<?> request =
                        RequestEntity.get(URI.create(targetUri + Long.toString(id))).build();
        return getTargetClient.execute(request);
    }

    @Nonnull
    public Set<TargetInfo> getAllTargets() throws CommunicationException {
        final RequestEntity<?> request = RequestEntity.get(URI.create(targetUri)).build();
        final GetAllTargetsResponse result = getAllTargetsClient.execute(request);
        return ImmutableSet.copyOf(result.getTargets());
    }

    public void removeTarget(final long target)
                    throws CommunicationException, TopologyProcessorException {
        final RequestEntity<?> request = RequestEntity
                        .delete(URI.create(targetUri + Long.toString(target))).build();
        remoteTargetsClient.execute(request);
    }

    public void modifyTarget(final long targetId, @Nonnull final TargetData newData)
                    throws CommunicationException, TopologyProcessorException {
        final RequestEntity<?> request =
                        RequestEntity.put(URI.create(targetUri + Long.toString(targetId)))
                                        .body(Objects.requireNonNull(newData).getAccountData());
        updateTargetsClient.execute(request);
    }

    @Nonnull
    private InputField convert(@Nonnull final com.vmturbo.topology.processor.api.AccountValue src) {
        return new InputField(src.getName(), src.getStringValue(),
                        Optional.of(src.getGroupScopeProperties()));
    }

    @Nonnull
    public ValidationStatus validateTarget(final long targetId)
                    throws CommunicationException, TopologyProcessorException {
        final RequestEntity<?> request = RequestEntity
                        .post(URI.create(targetUri + Long.toString(targetId) + "/validation"))
                        .build();
        final OperationResponse result = targetOperationClient.execute(request);
        return result.operation;
    }

    @Nonnull
    public Set<ValidationStatus> validateAllTargets() throws CommunicationException {
        final RequestEntity<?> request =
                        RequestEntity.post(URI.create(targetUri + "/validation")).build();
        final ValidateAllResponse result = validateAllClient.execute(request);
        return result.getResponses().stream().map(rslt -> rslt.operation)
                        .collect(Collectors.toSet());
    }

    @Nonnull
    public DiscoveryStatus discoverTarget(final long targetId)
                    throws CommunicationException, TopologyProcessorException {
        final RequestEntity<?> request = RequestEntity
                        .post(URI.create(targetUri + Long.toString(targetId) + "/discovery"))
                        .build();
        final OperationResponse result = targetOperationClient.execute(request);
        return result.operation;
    }

    @Nonnull
    public Set<DiscoveryStatus> discoverAllTargets() throws CommunicationException {
        final RequestEntity<?> request =
                RequestEntity.post(URI.create(targetUri + "/discovery")).build();
        final DiscoverAllResponse result = discoverAllClient.execute(request);
        return result.getResponses().stream().map(rslt -> rslt.operation)
                .collect(Collectors.toSet());
    }

    /**
     * Rest client, returning {@link TargetInfo} response.
     */
    private class TargetRestClient extends RestClient<TargetInfo, TopologyProcessorException> {

        TargetRestClient(HttpStatus... apiStatusCodes) {
            super(TargetInfo.class, apiStatusCodes);
        }

        @Override
        protected TopologyProcessorException createException(TargetInfo body) {
            return new TopologyProcessorException(Joiner.on(", ").join(body.getErrors()));
        }
    }

    /**
     * TargetRestClient that omits the body of the request in the message of any
     * CommunicationExceptions that are generated during execution.
     */
    private class SensitiveDataTargetRestClient extends TargetRestClient {
        SensitiveDataTargetRestClient(HttpStatus... apiStatusCodes) {
            super(apiStatusCodes);
        }

        @Override
        protected String createCommunicationExceptionMessage(final RequestEntity<?> request) {
            return "Error executing target request " + request.getUrl();
        }
    }
    /**
     * Rest client, returning {@link TargetInfo} response.
     */
    private class ProbeRestClient extends RestClient<ProbeDescription, TopologyProcessorException> {

        ProbeRestClient() {
            super(ProbeDescription.class, HttpStatus.NOT_FOUND);
        }

        @Override
        protected TopologyProcessorException createException(ProbeDescription body) {
            return new TopologyProcessorException(body.getError());
        }
    }

    /**
     * REST client for operation-related results (validation, discovery e.t.c.).
     */
    private class OperationResultRestClient extends RestClient<OperationResponse, TopologyProcessorException> {

        OperationResultRestClient() {
            super(OperationResponse.class, HttpStatus.NOT_FOUND, HttpStatus.INTERNAL_SERVER_ERROR);
        }

        @Override
        protected TopologyProcessorException createException(OperationResponse body) {
            return new TopologyProcessorException(body.error);
        }

    }

}
