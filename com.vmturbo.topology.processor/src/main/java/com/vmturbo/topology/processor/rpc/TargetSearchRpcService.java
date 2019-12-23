package com.vmturbo.topology.processor.rpc;

import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableMap;

import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.search.CloudType;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.StringFilter;
import com.vmturbo.common.protobuf.search.Search.SearchTargetsResponse;
import com.vmturbo.common.protobuf.search.SearchableProperties;
import com.vmturbo.common.protobuf.search.TargetSearchServiceGrpc.TargetSearchServiceImplBase;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.topology.processor.operation.IOperationManager;
import com.vmturbo.topology.processor.operation.validation.Validation;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetStore;

/**
 * gRPC service implementing various targets related searches. If fultills the request of one
 * single property filter. Now the following properties are supported: <ul>
 * <li>displayName - a regular expression to search for target's display name (use string
 * regexp)</li>
 * <li>validationStatus - select targets with the specified validation statuses (use options)</li>
 * <li>cloudProvider - select targets related to the specified cloud provider</li>
 * </ul>
 */
public class TargetSearchRpcService extends TargetSearchServiceImplBase {

    private final TargetStore targetStore;
    private final ProbeStore probeStore;
    private final IOperationManager operationManager;

    private final Map<String, TargetFetcher> targetFetcherMap;
    private final Logger logger = LogManager.getLogger(getClass());

    /**
     * Constructs targets search gRPC service.
     *
     * @param targetStore target store
     * @param probeStore probe store
     * @param operationManager operations manager
     */
    public TargetSearchRpcService(@Nonnull TargetStore targetStore, @Nonnull ProbeStore probeStore,
            @Nonnull IOperationManager operationManager) {
        this.targetStore = Objects.requireNonNull(targetStore);
        this.probeStore = Objects.requireNonNull(probeStore);
        this.operationManager = Objects.requireNonNull(operationManager);
        targetFetcherMap =
                ImmutableMap.<String, TargetFetcher>builder().put(SearchableProperties.DISPLAY_NAME,
                        this::getTargetsByName)
                        .put(SearchableProperties.TARGET_VALIDATION_STATUS,
                                this::getTargetsByStatus)
                        .put(SearchableProperties.CLOUD_PROVIDER, this::getTargetsByCloudProvider)
                        .build();
    }

    @Override
    public void searchTargets(PropertyFilter request,
            StreamObserver<SearchTargetsResponse> responseObserver) {
        if (!request.hasStringFilter()) {
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(
                    "StringFilter is expected for targets searching. Instead found: " +
                            request.getPropertyTypeCase()).asException());
            return;
        }
        final TargetFetcher targetFetcher = targetFetcherMap.get(request.getPropertyName());
        if (targetFetcher == null) {
            logger.info("Could not perform search targets for filter: " + request);
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(
                    "Searching for targets by property " + request.getPropertyName() +
                            " is not supported").asException());
            return;
        }
        try {
            final Set<Long> targets =
                    targetFetcher.getTargetsUsingFilter(request.getStringFilter());
            responseObserver.onNext(
                    SearchTargetsResponse.newBuilder().addAllTargets(targets).build());
            responseObserver.onCompleted();
        } catch (StatusException e) {
            responseObserver.onError(e);
        }
    }

    @Nonnull
    private Set<Long> getTargetsByName(@Nonnull StringFilter stringFilter) throws StatusException {
        if (!stringFilter.hasStringPropertyRegex()) {
            throw Status.INVALID_ARGUMENT.withDescription(
                    "Regular expression is expected for target by name filter: " + stringFilter)
                    .asException();
        }
        final int flags = stringFilter.getCaseSensitive() ? 0 : Pattern.CASE_INSENSITIVE;
        final Pattern pattern = Pattern.compile(stringFilter.getStringPropertyRegex(), flags);
        final Set<Long> result = new HashSet<>();
        for (Target target : targetStore.getAll()) {
            if (pattern.matcher(target.getDisplayName()).matches() ^
                    !stringFilter.getPositiveMatch()) {
                result.add(target.getId());
            }
        }
        return result;
    }

    @Nonnull
    private Set<Long> getTargetsByStatus(@Nonnull StringFilter stringFilter)
            throws StatusException {
        if (stringFilter.getOptionsCount() < 1) {
            throw Status.INVALID_ARGUMENT.withDescription(
                    "Explicit options are expected for target by status filter: " + stringFilter)
                    .asException();
        }
        final Set<String> requestedStates = new HashSet<>(stringFilter.getOptionsList());
        final Set<Long> result = new HashSet<>();
        for (Target target : targetStore.getAll()) {
            final String status = getValidationStatus(target.getId());
            if (requestedStates.contains(status) ^ !stringFilter.getPositiveMatch()) {
                result.add(target.getId());
            }
        }
        return result;
    }

    @Nullable
    private String getValidationStatus(long targetId) {
        return operationManager.getLastValidationForTarget(targetId)
                .map(Validation::getStatus)
                .map(Object::toString)
                .orElse(null);
    }

    @Nonnull
    private Set<Long> getTargetsByCloudProvider(@Nonnull StringFilter stringFilter)
            throws StatusException {
        if (stringFilter.getOptionsCount() < 1) {
            throw Status.INVALID_ARGUMENT.withDescription(
                    "Explicit options are expected for target by cloud provider filter: " +
                            stringFilter).asException();
        }
        final Set<Optional<CloudType>> requestedCloudTypes = stringFilter.getOptionsList()
                .stream()
                .map(CloudType::fromString)
                .filter(Optional::isPresent)
                .collect(Collectors.toSet());
        final Set<Long> probesToFetch = new HashSet<>();
        for (Entry<Long, ProbeInfo> entry : probeStore.getProbes().entrySet()) {
            final Optional<CloudType> cloudType =
                    CloudType.fromProbeType(entry.getValue().getProbeType());
            if (requestedCloudTypes.contains(cloudType) ^ !stringFilter.getPositiveMatch()) {
                probesToFetch.add(entry.getKey());
            }
        }
        return targetStore.getAll()
                .stream()
                .filter(target -> probesToFetch.contains(target.getProbeId()))
                .map(Target::getId)
                .collect(Collectors.toSet());
    }

    /**
     * An abstraction for a specific targets filter. It is able to apply the string filter and
     * fetch the matching targets.
     */
    private interface TargetFetcher {
        /**
         * Fetches targets matching the input filter.
         *
         * @param stringFilter filter to match targets against
         * @return set of target ids
         * @throws StatusException if some exception occurred
         */
        @Nonnull
        Set<Long> getTargetsUsingFilter(@Nonnull StringFilter stringFilter) throws StatusException;
    }
}
