package com.vmturbo.topology.processor.rpc;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.common.Pagination;
import com.vmturbo.common.protobuf.search.CloudType;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.StringFilter;
import com.vmturbo.common.protobuf.search.SearchableProperties;
import com.vmturbo.common.protobuf.target.TargetDTO.GetTargetDetailsRequest;
import com.vmturbo.common.protobuf.target.TargetDTO.GetTargetDetailsResponse;
import com.vmturbo.common.protobuf.target.TargetDTO.GetTargetsStatsRequest;
import com.vmturbo.common.protobuf.target.TargetDTO.GetTargetsStatsResponse;
import com.vmturbo.common.protobuf.target.TargetDTO.SearchTargetsRequest;
import com.vmturbo.common.protobuf.target.TargetDTO.SearchTargetsResponse;
import com.vmturbo.common.protobuf.target.TargetDTO.TargetDetailLevel;
import com.vmturbo.common.protobuf.target.TargetDTO.TargetDetails;
import com.vmturbo.common.protobuf.target.TargetDTO.TargetHealth;
import com.vmturbo.common.protobuf.target.TargetsServiceGrpc.TargetsServiceImplBase;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryType;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.topology.processor.operation.IOperationManager;
import com.vmturbo.topology.processor.operation.Operation;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetStatusOuterClass.TargetStatus;
import com.vmturbo.topology.processor.targets.TargetStore;
import com.vmturbo.topology.processor.targets.TargetsStatsBuckets;
import com.vmturbo.topology.processor.targets.status.TargetStatusTracker;

/**
 * Target gRPC service.
 */
public class TargetsRpcService extends TargetsServiceImplBase {

    private final Logger logger = LogManager.getLogger(getClass());

    private final TargetStore targetStore;
    private final ProbeStore probeStore;
    private final IOperationManager operationManager;
    private final TargetStatusTracker targetStatusTracker;
    private final TargetHealthRetriever targetHealthRetriever;

    /**
     * Constructs target gRPC service.
     *
     * @param targetStore The target store
     * @param probeStore The probe store.
     * @param operationManager The operation manager.
     * @param targetStatusTracker The tracker of the targets statuses.
     * @param targetHealthRetriever Retrieves health information for targets.
     */
    public TargetsRpcService(@Nonnull final TargetStore targetStore,
            @Nonnull final ProbeStore probeStore,
            @Nonnull final IOperationManager operationManager,
            @Nonnull final TargetStatusTracker targetStatusTracker,
            @Nonnull final TargetHealthRetriever targetHealthRetriever) {
        this.targetStore = Objects.requireNonNull(targetStore);
        this.probeStore = Objects.requireNonNull(probeStore);
        this.operationManager = Objects.requireNonNull(operationManager);
        this.targetStatusTracker = Objects.requireNonNull(targetStatusTracker);
        this.targetHealthRetriever = Objects.requireNonNull(targetHealthRetriever);
    }

    @Override
    public void getTargetsStats(GetTargetsStatsRequest request,
            StreamObserver<GetTargetsStatsResponse> responseObserver) {
        // only calculate stats on non-hidden targets
        final List<Target> targets = targetStore.getAll()
            .stream()
            .filter(t -> !t.getSpec().getIsHidden())
            .collect(Collectors.toList());
        final TargetsStatsBuckets targetsStatsBuckets = new TargetsStatsBuckets(request.getGroupByList());
        targetsStatsBuckets.addTargets(targets);
        responseObserver.onNext(GetTargetsStatsResponse.newBuilder()
                .addAllTargetsGroupStat(targetsStatsBuckets.getTargetsStats())
                .build());
        responseObserver.onCompleted();
    }

    @Override
    public void getTargetDetails(GetTargetDetailsRequest request,
            StreamObserver<GetTargetDetailsResponse> responseObserver) {
        final TargetDetailLevel detailLevel = request.getDetailLevel();
        // We always return health.

        final Set<Long> requestedTargets = new HashSet<>(request.getTargetIdsList());
        final Map<Long, TargetHealth> targetHealth = targetHealthRetriever.getTargetHealth(requestedTargets, request.getReturnAll());

        final Map<Long, TargetStatus> targetStatus;
        if (request.getDetailLevel() == TargetDetailLevel.FULL) {
            targetStatus = targetStatusTracker.getTargetsStatuses(requestedTargets, request.getReturnAll());
        } else {
            targetStatus = Collections.emptyMap();
        }

        final GetTargetDetailsResponse.Builder respBuilder = GetTargetDetailsResponse.newBuilder();
        targetHealth.forEach((targetId, health) -> {
            TargetDetails.Builder detailsBldr = TargetDetails.newBuilder()
                .setTargetId(targetId)
                .setHealthDetails(health);
            TargetStatus status = targetStatus.get(targetId);
            if (status != null) {
                detailsBldr.addAllLastDiscoveryDetails(status.getStageDetailsList());
            }
            detailsBldr.addAllParents(targetStore.getParentTargetIds(targetId));
            detailsBldr.setHidden(targetStore.getTarget(targetId).get().isHidden());
            detailsBldr.addAllDerived(targetStore.getDerivedTargetIds(targetId));
            respBuilder.putTargetDetails(targetId, detailsBldr.build());
        });
        responseObserver.onNext(respBuilder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void searchTargets(SearchTargetsRequest request,
            StreamObserver<SearchTargetsResponse> responseObserver) {
        try {
            final Predicate<Target> predicates = getPredicate(request.getPropertyFilterList());
            // apply the filter
            List<Target> targets = targetStore.getAll().stream().filter(predicates).collect(
                    Collectors.toList());

            final SearchTargetsResponse.Builder builder = SearchTargetsResponse.newBuilder();

            // Apply pagination if they exist
            //
            // Note: We do not set the pagination response if the pagination params are unset.
            if (request.hasPaginationParams()) {
                final Pagination.PaginationResponse.Builder paginationInfo =
                        Pagination.PaginationResponse.newBuilder().setTotalRecordCount(targets.size());
                Pagination.PaginationParameters paginationParams = request.getPaginationParams();

                if (paginationParams.hasOrderBy()) {
                    final Comparator<Target> sorting = getSorting(paginationParams.getOrderBy(),
                            paginationParams.getAscending());
                    targets.sort(sorting);
                }

                if (paginationParams.hasLimit() || paginationParams.hasCursor()) {
                    final int targetCount = targets.size();
                    final int currentCursor = getCursor(paginationParams);
                    final int nextCursor = currentCursor + getLimit(paginationParams, targets.size());
                    if (currentCursor < targetCount) {
                        targets = targets.subList(currentCursor, Math.min(nextCursor, targetCount));
                        if (nextCursor < targetCount) {
                            paginationInfo.setNextCursor(Integer.toString(nextCursor));
                        }
                    } else {
                        targets = Collections.emptyList();
                    }
                }

                builder.setPaginationResponse(paginationInfo);
            }

            // get the ids for the targets
            targets.forEach(t -> builder.addTargets(t.getId()));

            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
        } catch (IllegalArgumentException e) {
            logger.error("Illegal argument error", e);
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asException());
        } catch (StatusException e) {
            logger.error("An error occurred while searching for target.", e);
            responseObserver.onError(e);
        }
    }

    private Comparator<Target> getSorting(Pagination.OrderBy orderBy, boolean ascending)
            throws StatusException {
        if (orderBy.hasTarget()) {
            Comparator<Target> comparator;
            switch (orderBy.getTarget()) {
                case TARGET_DISPLAY_NAME:
                    comparator = Comparator.comparing(Target::getDisplayName);
                    break;
                case TARGET_VALIDATION_STATUS:
                    comparator = Comparator.comparing(t -> {
                        final String validationStatus = getValidationStatus(t.getId());
                        return validationStatus != null ? validationStatus : "";
                    });
                    break;
                default:
                    throw Status.INVALID_ARGUMENT.withDescription(
                            "Sorting field not supported for targets. Field: "
                                + orderBy.getTarget()).asException();
            }
            if (!ascending) {
                comparator = comparator.reversed();
            }
            return comparator;
        } else {
            throw Status.INVALID_ARGUMENT.withDescription(
                    "Expected that order by target to be populated. Instead found: "
                            + orderBy).asException();
        }
    }

    private int getLimit(Pagination.PaginationParameters paginationParams, int resultSize) {
        return paginationParams.getLimit() > 0 ? paginationParams.getLimit() : resultSize;
    }

    private int getCursor(Pagination.PaginationParameters paginationParams) {
        if (paginationParams.hasCursor()) {
            if (StringUtils.isNumeric(paginationParams.getCursor())) {
                int cursor = Integer.parseInt(paginationParams.getCursor());
                if (cursor < 0) {
                    throw new IllegalArgumentException("Cursor must be positive. Got: " + paginationParams.getCursor());
                }
                return cursor;
            } else {
                throw new IllegalArgumentException("Cursor must be numeric. Got: " + paginationParams.getCursor());
            }
        } else {
            return 0;
        }
    }

    /**
     * Calculates the latest validation status. Discovery is also treated as a validation (as
     * validation is a part of discovery). We get the latest validation/discovery operation and
     * get the status from it.
     *
     * @param targetId target to get status of
     * @return status, if any. Returns {@code null} if no validation/discovery operations
     *         finished on the target
     */
    @Nullable
    private String getValidationStatus(long targetId) {
        final List<Operation> operations = new ArrayList<>(DiscoveryType.values().length + 1);
        operationManager.getLastValidationForTarget(targetId).ifPresent(operations::add);
        // We don't take incremental discoveries into consideration.
        // VC incremental discoveries can return SUCCESS even if validation and normal discoveries
        // are failing (e.g. incorrect credentials).
        operationManager.getLastDiscoveryForTarget(targetId, DiscoveryType.FULL)
                .ifPresent(operations::add);
        if (operations.isEmpty()) {
            return null;
        }
        final Operation latest =
                Collections.max(operations, Comparator.comparing(Operation::getCompletionTime));
        return latest.getStatus().toString();
    }

    private Predicate<Target> getPredicate(List<PropertyFilter> propertyFilters) throws StatusException {
        List<Predicate<Target>> predicates = new ArrayList<>(propertyFilters.size());

        for (PropertyFilter propertyFilter : propertyFilters) {
            // all the property filters should be string
            if (!propertyFilter.hasStringFilter()) {
                throw Status.INVALID_ARGUMENT.withDescription(
                        "StringFilter is expected for searching for targets. Instead found: "
                                + propertyFilter.getPropertyTypeCase()).asException();
            }
            if (!propertyFilter.hasPropertyName()) {
                throw Status.INVALID_ARGUMENT.withDescription(
                        "The property filter is missing the property name. Property filter: "
                                + propertyFilter).asException();
            }
            switch (propertyFilter.getPropertyName()) {
                case SearchableProperties.DISPLAY_NAME:
                    predicates.add(new TargetsByNamePredicate(propertyFilter.getStringFilter()));
                    break;
                case SearchableProperties.PROBE_TYPE:
                    predicates.add(new TargetsByTypePredicate(propertyFilter.getStringFilter()));
                    break;
                case SearchableProperties.TARGET_VALIDATION_STATUS:
                    predicates.add(new TargetsByValidationStatusPredicate(this::getValidationStatus,
                            propertyFilter.getStringFilter()));
                    break;
                case SearchableProperties.IS_TARGET_HIDDEN:
                    predicates.add(new TargetsByHiddenStatePredicate(propertyFilter.getStringFilter()));
                    break;
                case SearchableProperties.CLOUD_PROVIDER:
                    predicates.add(new TargetsByCloudProviderPredicate(probeStore,
                            propertyFilter.getStringFilter()));
                    break;
                case SearchableProperties.K8S_CLUSTER:
                    predicates.add(new TargetsByK8sClusterPredicate(targetStore,
                            propertyFilter.getStringFilter()));
                    break;
                default:
                    logger.error("Could not perform search targets for filter: " + propertyFilters);
                    throw Status.INVALID_ARGUMENT.withDescription("Searching for targets by property "
                            + propertyFilter.getPropertyName() + " is not supported").asException();
            }
        }


        return target -> {
            for (Predicate<Target> predicate : predicates) {
                if (!predicate.test(target)) {
                    return false;
                }
            }
            return true;
        };
    }

    /**
     * The predicate for filtering targets by cloud provider.
     */
    private static class TargetsByCloudProviderPredicate implements Predicate<Target> {
        private final Set<Long> probesToFetch;

        TargetsByCloudProviderPredicate(@Nonnull ProbeStore probeStore,
                @Nonnull StringFilter stringFilter) throws StatusException {
            if (stringFilter.getOptionsCount() < 1) {
                throw Status.INVALID_ARGUMENT.withDescription(
                        "Explicit options are expected for target by cloud provider filter: "
                                + stringFilter).asException();
            }
            final Set<Optional<CloudType>> requestedCloudTypes = stringFilter.getOptionsList()
                    .stream()
                    .map(CloudType::fromString)
                    .filter(Optional::isPresent)
                    .collect(Collectors.toSet());
            probesToFetch = new HashSet<>();
            for (Entry<Long, ProbeInfo> entry : probeStore.getProbes().entrySet()) {
                final Optional<CloudType> cloudType =
                        CloudType.fromProbeType(entry.getValue().getProbeType());
                if (requestedCloudTypes.contains(cloudType) ^ !stringFilter.getPositiveMatch()) {
                    probesToFetch.add(entry.getKey());
                }
            }
        }

        @Override
        public boolean test(Target target) {
            return probesToFetch.contains(target.getProbeId());
        }
    }

    /**
     * The predicate for filtering targets by K8S cluster.
     */
    private static class TargetsByK8sClusterPredicate implements Predicate<Target> {
        private final TargetStore targetStore;
        private final StringFilter stringFilter;
        private final Pattern pattern;

        TargetsByK8sClusterPredicate(@Nonnull TargetStore targetStore,
                @Nonnull StringFilter stringFilter) throws StatusException {
            if (!stringFilter.hasStringPropertyRegex()) {
                throw Status.INVALID_ARGUMENT.withDescription(
                        "Regular expression is expected for cloud native target by k8s cluster filter: " + stringFilter)
                        .asException();
            }
            this.targetStore = targetStore;
            this.stringFilter = stringFilter;
            final int flags = stringFilter.getCaseSensitive() ? 0 : Pattern.CASE_INSENSITIVE;
            this.pattern = Pattern.compile(stringFilter.getStringPropertyRegex(), flags);
        }

        @Override
        public boolean test(Target target) {
            Optional<ProbeCategory> probeCategory = targetStore.getProbeCategoryForTarget(target.getId());
            // Skip non Cloud Native targets.
            if (!probeCategory.isPresent() || probeCategory.get() != ProbeCategory.CLOUD_NATIVE) {
                return false;
            }
            // Add targetId to results (based on the following XOR condition) if
            // 1. probe type matches the pattern, AND stringFilter is positive match;
            // 2. probe type does not match the pattern, AND stringFilter is not positive match;
            return pattern.matcher(target.getProbeInfo().getProbeType()).matches()
                    ^ !stringFilter.getPositiveMatch();
        }
    }

    /**
     * The predicate for filtering targets by display name.
     */
    private static class TargetsByNamePredicate implements Predicate<Target> {
        private final StringFilter stringFilter;
        private final Pattern pattern;

        TargetsByNamePredicate(@Nonnull StringFilter stringFilter) throws StatusException {
            if (!stringFilter.hasStringPropertyRegex()) {
                throw Status.INVALID_ARGUMENT.withDescription(
                        "Regular expression is expected for target by name filter: " + stringFilter)
                        .asException();
            }
            this.stringFilter = stringFilter;
            final int flags = stringFilter.getCaseSensitive() ? 0 : Pattern.CASE_INSENSITIVE;
            this.pattern = Pattern.compile(stringFilter.getStringPropertyRegex(), flags);
        }

        @Override
        public boolean test(Target target) {
            return pattern.matcher(target.getDisplayName()).matches()
                    ^ !stringFilter.getPositiveMatch();
        }
    }

    /**
     * The predicate for filtering targets by probe type.
     */
    private static class TargetsByTypePredicate implements Predicate<Target> {
        private final StringFilter stringFilter;
        private final Set<String> types;

        TargetsByTypePredicate(@Nonnull StringFilter stringFilter) throws StatusException {
            if (stringFilter.getOptionsCount() < 1) {
                throw Status.INVALID_ARGUMENT.withDescription(
                        "Explicit options are expected for target by type filter: " + stringFilter)
                        .asException();
            }
            this.stringFilter = stringFilter;
            this.types = new HashSet<>(stringFilter.getOptionsList());
        }

        @Override
        public boolean test(Target target) {
            return types.contains(target.getProbeInfo().getProbeType())
                    ^ !stringFilter.getPositiveMatch();
        }
    }

    /**
     * The predicate for filtering targets by cloud provider.
     */
    private static class TargetsByValidationStatusPredicate implements Predicate<Target> {
        private final StringFilter stringFilter;
        private final Function<Long, String> validationStatusGetter;
        private final Set<String> requestedStates;

        TargetsByValidationStatusPredicate(@Nonnull Function<Long, String> validationStatusGetter,
                @Nonnull StringFilter stringFilter) throws StatusException {
            if (stringFilter.getOptionsCount() < 1) {
                throw Status.INVALID_ARGUMENT.withDescription(
                        "Explicit options are expected for target by status filter: " + stringFilter)
                        .asException();
            }
            this.stringFilter = stringFilter;
            this.validationStatusGetter = validationStatusGetter;
            this.requestedStates = new HashSet<>(stringFilter.getOptionsList());
        }

        @Override
        public boolean test(Target target) {
            final String status = validationStatusGetter.apply(target.getId());
            return requestedStates.contains(status) ^ !stringFilter.getPositiveMatch();
        }
    }

    /**
     * The predicate for filtering targets if they are hidden.
     */
    private static class TargetsByHiddenStatePredicate implements  Predicate<Target> {
        private final StringFilter stringFilter;
        private final boolean includeHidden;
        private final boolean includeNonHidden;

        TargetsByHiddenStatePredicate(StringFilter stringFilter) throws StatusException {
            if (stringFilter.getOptionsCount() < 1) {
                throw Status.INVALID_ARGUMENT.withDescription(
                        "Explicit options are expected for target by hidden state filter: " + stringFilter)
                        .asException();
            }
            this.stringFilter = stringFilter;
            boolean includeHidden = false;
            boolean includeNonHidden = false;
            for (String option : stringFilter.getOptionsList()) {
                boolean value = Boolean.parseBoolean(option);
                if (value) {
                    includeHidden = true;
                } else {
                    includeNonHidden = true;
                }
            }

            this.includeHidden = includeHidden;
            this.includeNonHidden = includeNonHidden;
        }

        @Override
        public boolean test(Target target) {
            return ((target.getSpec().getIsHidden() && this.includeHidden)
                    || (!target.getSpec().getIsHidden() && this.includeNonHidden))
                    ^ !stringFilter.getPositiveMatch();
        }
    }


}
