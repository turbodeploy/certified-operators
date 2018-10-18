package com.vmturbo.cost.component.rpc;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TimeZone;
import java.util.function.Function;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.ImmutableSet;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.common.protobuf.cost.Cost.AccountExpenses;
import com.vmturbo.common.protobuf.cost.Cost.CreateDiscountRequest;
import com.vmturbo.common.protobuf.cost.Cost.CreateDiscountResponse;
import com.vmturbo.common.protobuf.cost.Cost.DeleteDiscountRequest;
import com.vmturbo.common.protobuf.cost.Cost.DeleteDiscountResponse;
import com.vmturbo.common.protobuf.cost.Cost.Discount;
import com.vmturbo.common.protobuf.cost.Cost.DiscountInfo;
import com.vmturbo.common.protobuf.cost.Cost.EntityCost;
import com.vmturbo.common.protobuf.cost.Cost.GetDiscountRequest;
import com.vmturbo.common.protobuf.cost.Cost.UpdateDiscountRequest;
import com.vmturbo.common.protobuf.cost.Cost.UpdateDiscountResponse;
import com.vmturbo.common.protobuf.cost.CostServiceGrpc.CostServiceImplBase;
import com.vmturbo.common.protobuf.stats.Stats.GetAveragedEntityStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.Builder;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord.StatValue;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter;
import com.vmturbo.cost.component.discount.DiscountNotFoundException;
import com.vmturbo.cost.component.discount.DiscountStore;
import com.vmturbo.cost.component.discount.DuplicateAccountIdException;
import com.vmturbo.cost.component.entity.cost.EntityCostStore;
import com.vmturbo.cost.component.expenses.AccountExpensesStore;
import com.vmturbo.cost.component.utils.BusinessAccountHelper;
import com.vmturbo.sql.utils.DbException;

/**
 * Implements the RPC calls supported by the cost component for retrieving Cloud cost data.
 */
public class CostRpcService extends CostServiceImplBase {

    /**
     *  Cloud service constant to match UI request, also used in test cases
     */
    public static final String CLOUD_SERVICE = "cloudService";

    /**
     * Cloud service provider constant to match UI request, also used in test cases
     */
    public static final String CSP = "CSP";

    /**
     * Cloud target constant to match UI request, also used in test case
     */
    public static final String TARGET = "target";

    /**
     * Cloud cost price constant to match UI request, also used in test case
     */
    public static final String COST_PRICE = "costPrice";

    /**
     * Cloud workload constant to match UI request, also used in test case
     */
    public static final String WORKLOAD = "workload";

    /**
     * Cloud VM constant to match UI request, also used in test case
     */
    public static final String VIRTUAL_MACHINE = "VirtualMachine";

    private static final String ERR_MSG = "Invalid discount deletion input: No associated account ID or discount ID specified";

    private static final String NO_ASSOCIATED_ACCOUNT_ID_OR_DISCOUNT_INFO_PRESENT = "No discount info present.";

    private static final String INVALID_ARGUMENTS_FOR_DISCOUNT_UPDATE = "Invalid arguments for discount update";

    private static final String CREATING_A_DISCOUNT_WITH_ASSOCIATED_ACCOUNT_ID = "Creating a discount: {} with associated account id: {}";

    private static final String DELETING_A_DISCOUNT = "Deleting a discount {}: {}";

    private static final String UPDATING_A_DISCOUNT = "Updating a discount: {}";

    private static final String FAILED_TO_UPDATE_DISCOUNT = "Failed to update discount ";

    private static final String FAILED_TO_FIND_THE_UPDATED_DISCOUNT = "Failed to find the updated discount";

    private static final Logger logger = LogManager.getLogger();

    private static final Set SUPPORT_GROUP_BY = ImmutableSet.of(CLOUD_SERVICE, CSP, TARGET);

    private final DiscountStore discountStore;

    private final AccountExpensesStore accountExpensesStore;

    private final EntityCostStore entityCostStore;

    private final BusinessAccountHelper businessAccountHelper;


    /**
     * Create a new RIAndExpenseUploadRpcService.
     *
     * @param discountStore        The store containing account discounts
     * @param accountExpensesStore The store containing account expenses
     */
    public CostRpcService(@Nonnull final DiscountStore discountStore,
                          @Nonnull final AccountExpensesStore accountExpensesStore,
                          @Nonnull final EntityCostStore costStoreHouse,
                          @Nonnull final BusinessAccountHelper businessAccountHelper) {
        this.discountStore = Objects.requireNonNull(discountStore);
        this.accountExpensesStore = Objects.requireNonNull(accountExpensesStore);
        this.entityCostStore = Objects.requireNonNull(costStoreHouse);
        this.businessAccountHelper = Objects.requireNonNull(businessAccountHelper);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void createDiscount(CreateDiscountRequest request,
                               StreamObserver<CreateDiscountResponse> responseObserver) {
        logger.info(CREATING_A_DISCOUNT_WITH_ASSOCIATED_ACCOUNT_ID,
                request.getDiscountInfo(), request.getId());
        if (!request.hasId() || !request.hasDiscountInfo()) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription(NO_ASSOCIATED_ACCOUNT_ID_OR_DISCOUNT_INFO_PRESENT).asRuntimeException());
            return;
        }
        try {
            final Discount discount = discountStore.persistDiscount(request.getId(), request.getDiscountInfo());
            responseObserver.onNext(CreateDiscountResponse.newBuilder()
                    .setDiscount(discount)
                    .build());
            responseObserver.onCompleted();
        } catch (DuplicateAccountIdException e) {
            responseObserver.onError(Status.ALREADY_EXISTS
                    .withDescription(e.getMessage())
                    .asException());
        } catch (DbException e) {
            responseObserver.onError(Status.INTERNAL
                    .withDescription(e.getMessage())
                    .asException());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteDiscount(DeleteDiscountRequest request,
                               StreamObserver<DeleteDiscountResponse> responseObserver) {
        // ensure request has either associated account or discount id
        if ((!request.hasAssociatedAccountId() && !request.hasDiscountId())) {
            logger.error(ERR_MSG);
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(ERR_MSG).asRuntimeException());
            return;
        }
        try {
            if (request.hasDiscountId()) {
                logger.info(DELETING_A_DISCOUNT, "by ID", request.getDiscountId());
                discountStore.deleteDiscountByDiscountId(request.getDiscountId());
            } else {
                logger.info(DELETING_A_DISCOUNT, "by associated account ID", request.getDiscountId());
                discountStore.deleteDiscountByAssociatedAccountId(request.getAssociatedAccountId());
            }
            responseObserver.onNext(DeleteDiscountResponse.newBuilder()
                    .setDeleted(true)
                    .build());
            responseObserver.onCompleted();
        } catch (DiscountNotFoundException e) {
            responseObserver.onError(Status.NOT_FOUND
                    .withDescription(e.getMessage())
                    .asException());
        } catch (DbException e) {
            responseObserver.onError(Status.INTERNAL
                    .withDescription(e.getMessage())
                    .asException());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void getDiscounts(GetDiscountRequest request,
                             StreamObserver<Discount> responseObserver) {
        try {
            if (request.hasFilter()) {
                request.getFilter().getAssociatedAccountIdList().forEach(id ->
                {
                    try {
                        discountStore.getDiscountByAssociatedAccountId(id)
                                .stream().forEach(responseObserver::onNext);
                    } catch (DbException e) {
                        throw new RuntimeException(e);
                    }
                });
            } else {
                discountStore.getAllDiscount().forEach(responseObserver::onNext);
            }
            responseObserver.onCompleted();
        } catch (DbException | RuntimeException e) {
            responseObserver.onError(Status.INTERNAL
                    .withDescription(e.getMessage())
                    .asException());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateDiscount(UpdateDiscountRequest request,
                               StreamObserver<UpdateDiscountResponse> responseObserver) {
        // require new discount info
        if (!request.hasNewDiscountInfo() || (!request.hasAssociatedAccountId() && !request.hasDiscountId())) {
            logger.error(INVALID_ARGUMENTS_FOR_DISCOUNT_UPDATE);
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription(INVALID_ARGUMENTS_FOR_DISCOUNT_UPDATE).asRuntimeException());
            return;
        }
        logger.info(UPDATING_A_DISCOUNT, request);

        try {
            final Discount existingDiscount = getDiscount(request);
            // copy the original discountInfo as base
            final DiscountInfo.Builder newBuilder = DiscountInfo.newBuilder(existingDiscount.getDiscountInfo());
            final DiscountInfo discountInfo = request.getNewDiscountInfo();

            if (discountInfo.hasServiceLevelDiscount()) {
                newBuilder.setServiceLevelDiscount(discountInfo.getServiceLevelDiscount());
            }

            if (discountInfo.hasTierLevelDiscount()) {
                newBuilder.setTierLevelDiscount(discountInfo.getTierLevelDiscount());
            }

            if (discountInfo.hasAccountLevelDiscount()) {
                newBuilder.setAccountLevelDiscount(discountInfo.getAccountLevelDiscount());
            }

            if (discountInfo.hasDisplayName()) {
                newBuilder.setDisplayName(discountInfo.getDisplayName());
            }
            if (request.hasAssociatedAccountId()) {
                discountStore.updateDiscountByAssociatedAccount(request.getAssociatedAccountId(), newBuilder.build());
            } else {
                discountStore.updateDiscount(request.getDiscountId(), newBuilder.build());

            }

            responseObserver.onNext(UpdateDiscountResponse.newBuilder()
                    .setUpdatedDiscount(getDiscount(request))
                    .build());
            responseObserver.onCompleted();
        } catch (DiscountNotFoundException e) {
            logger.error(FAILED_TO_UPDATE_DISCOUNT + getId(request), e);
            responseObserver.onError(Status.NOT_FOUND
                    .withDescription(e.getMessage()).asException());
        } catch (DbException e) {
            logger.error(FAILED_TO_UPDATE_DISCOUNT + getId(request), e);
            responseObserver.onError(Status.INTERNAL
                    .withDescription(e.getMessage()).asException());
        }
    }

    /**
     * Fetch a sequence of StatSnapshot's based on the time range and filters in the StatsRequest.
     * The stats will be provided from the Cost database.
     * <p>
     * Currently (Sep 26, 2019), it only supports requests for account expense with GroupBy
     * with CSP, target, and cloudService.
     * <p>
     * The 'entitiesList' in the {@link GetAveragedEntityStatsRequest} is not currently used.
     *
     * @param request          a set of parameters describing how to fetch the snapshot and what entities.
     * @param responseObserver the sync for each result value {@link StatSnapshot}
     */
    @Override
    public void getAveragedEntityStats(GetAveragedEntityStatsRequest request,
                                       StreamObserver<StatSnapshot> responseObserver) {
        final StatsFilter filter = request.getFilter();

        final Optional<String> relatedEntityType = getRelatedEntityType(filter);
        if (CLOUD_SERVICE.equals(relatedEntityType.orElse(null))) {
            final Optional<String> groupByOptional = getGroupByType(filter);
            processCloudServiceStats(request, responseObserver, filter, groupByOptional);
        } else if (WORKLOAD.equals(relatedEntityType.orElse(null))
                || VIRTUAL_MACHINE.equals(relatedEntityType.orElse(null))) {
            processBottomUpCostStats(request, responseObserver, filter);
        } else {
            responseOnError(request, responseObserver);
        }
    }

    // perform bottom up cost stats retrieval.
    private void processBottomUpCostStats(@Nonnull final GetAveragedEntityStatsRequest request,
                                          @Nonnull final StreamObserver<StatSnapshot> responseObserver,
                                          @Nonnull final StatsFilter filter) {
        try {
            final Map<Long, Map<Long, EntityCost>> snapshoptToEntityCostMap =
                    (filter.hasStartDate() && filter.hasEndDate()) ?
                            entityCostStore.getEntityCosts(getLocalDateTime(filter.getStartDate())
                                    , getLocalDateTime(filter.getEndDate())) :
                            entityCostStore.getLatestEntityCost();

            snapshoptToEntityCostMap.forEach((time, costsByEntity) -> {
                        final Builder snapshotBuilder = StatSnapshot.newBuilder();
                        snapshotBuilder.setSnapshotDate(DateTimeUtil.toString(time));
                        costsByEntity.values().forEach(entityCost -> {
                            entityCost.getComponentCostList().forEach(serviceExpenses -> {
                                        final float amount = (float) serviceExpenses.getAmount().getAmount();
                                        // build the record for this stat (commodity type)
                                        final StatRecord statRecord = buildStatRecord(
                                                entityCost.getAssociatedEntityId()
                                                , amount);
                                        // return add this record to the snapshot for this timestamp
                                        snapshotBuilder.addStatRecords(statRecord);
                                    }
                            );
                        });
                        responseObserver.onNext(snapshotBuilder.build());
                    }
            );
            responseObserver.onCompleted();
        } catch (Exception e) {
            logger.error("Error getting stats snapshots for {}", request);
            logger.error("    ", e);
            responseObserver.onError(Status.INTERNAL
                    .withDescription("Internal Error fetching stats for: " + request + ", cause: "
                            + e.getMessage())
                    .asException());
        }

    }

    // perform top down account expense stat retrieval
    private void processCloudServiceStats(@Nonnull final GetAveragedEntityStatsRequest request,
                                          @Nonnull final StreamObserver<StatSnapshot> responseObserver,
                                          @Nonnull final StatsFilter filter, final Optional<String> groupByOptional) {
        groupByOptional.ifPresent(groupByType -> {
                    try {
                        final Map<Long, Map<Long, AccountExpenses>> snapshoptToAccountExpensesMap =
                                (filter.hasStartDate() && filter.hasEndDate()) ?
                                        accountExpensesStore.getAccountExpenses(getLocalDateTime(filter.getStartDate())
                                                , getLocalDateTime(filter.getEndDate())) :
                                        accountExpensesStore.getLatestExpenses();
                        createStatSnapshots(snapshoptToAccountExpensesMap, groupByType)
                                .map(Builder::build)
                                .forEach(responseObserver::onNext);
                        responseObserver.onCompleted();
                    } catch (Exception e) {
                        logger.error("Error getting stats snapshots for {}", request);
                        logger.error("    ", e);
                        responseObserver.onError(Status.INTERNAL
                                .withDescription("Internal Error fetching stats for: " + request + ", cause: "
                                        + e.getMessage())
                                .asException());
                    }
                }
        );
        if (!groupByOptional.isPresent()) {
            responseOnError(request, responseObserver);
        }
    }

    private void responseOnError(final @Nonnull GetAveragedEntityStatsRequest request, final @Nonnull StreamObserver<StatSnapshot> responseObserver) {
        responseObserver.onError(Status.INTERNAL
                .withDescription("The request includes unsupported group by type: " + request
                        + ", currently only group by CSP, CloudService and target (Account) are supported")
                .asException());
    }


    // extract group by type.
    private Optional<String> getGroupByType(@Nonnull final StatsFilter filter) {
        final Function<String, Boolean> f = s -> filter.getCommodityRequestsList().stream().anyMatch(commodityRequest ->
                commodityRequest.getGroupByList().stream().anyMatch(str -> str.equals(s)));
        if (f.apply(CLOUD_SERVICE)) return Optional.of(CLOUD_SERVICE);
        if (f.apply(CSP)) return Optional.of(CSP);
        if (f.apply(TARGET)) return Optional.of(TARGET);
        return Optional.empty();

    }

    // get related entity type
    private Optional<String> getRelatedEntityType(@Nonnull final StatsFilter filter) {
        final Function<String, Boolean> f = s -> filter.getCommodityRequestsList().stream().anyMatch(commodityRequest ->
                commodityRequest.getRelatedEntityType().equalsIgnoreCase(s));
        if (f.apply(CLOUD_SERVICE)) return Optional.of(CLOUD_SERVICE);
        if (f.apply(WORKLOAD)) return Optional.of(WORKLOAD);
        if (f.apply(VIRTUAL_MACHINE)) return Optional.of(VIRTUAL_MACHINE);
        return Optional.empty();

    }

    // build stat snapshots
    private Stream<Builder> createStatSnapshots(@Nonnull final Map<Long, Map<Long, AccountExpenses>> snapshoptToExpenseMap,
                                                @Nonnull final String groupByType) {
        return snapshoptToExpenseMap.entrySet().stream().map(entry -> {
                    final Builder snapshotBuilder = StatSnapshot.newBuilder();
                    snapshotBuilder.setSnapshotDate(DateTimeUtil.toString(entry.getKey()));
                    entry.getValue().values().forEach(accountExpenses -> {
                        accountExpenses.getAccountExpensesInfo().getServiceExpensesList().forEach(serviceExpenses -> {
                                    final float amount = (float) serviceExpenses.getExpenses().getAmount();
                                    // build the record for this stat (commodity type)
                                    final StatRecord statRecord = buildStatRecord(
                                            groupByType.equals(CLOUD_SERVICE) ?
                                                    serviceExpenses.getAssociatedServiceId() : businessAccountHelper
                                                    .resolveTargetId(accountExpenses.getAssociatedAccountId())
                                            , amount);
                                    // return add this record to the snapshot for this timestamp
                                    snapshotBuilder.addStatRecords(statRecord);
                                }
                        );
                    });
                    return snapshotBuilder;
                }
        );
    }

    // populate the stat record
    // TODO support the MAX, MIN values when roll up data are available
    private StatRecord buildStatRecord(@Nullable final Long producerId,
                                       @Nullable final Float avgValue) {
        final StatRecord.Builder statRecordBuilder = StatRecord.newBuilder()
                .setName(COST_PRICE);
        if (producerId != null && producerId != 0) {
            // providerUuid, it's associated accountId except CloudService type which is serviceId
            statRecordBuilder.setProviderUuid(String.valueOf(producerId));
        }
        // hardcoded for now
        statRecordBuilder.setUnits("$/h");

        // values, used, peak
        StatValue.Builder statValueBuilder = StatValue.newBuilder();
        if (avgValue != null) {
            statValueBuilder.setAvg(avgValue);
        }

        statValueBuilder.setTotal(avgValue);

        // currentValue
        statRecordBuilder.setCurrentValue(avgValue);

        StatValue statValue = statValueBuilder.build();

        statRecordBuilder.setValues(statValue);
        statRecordBuilder.setUsed(statValue);
        statRecordBuilder.setPeak(statValue);

        return statRecordBuilder.build();
    }

    private long getId(final UpdateDiscountRequest request) {
        return request.hasDiscountId() ? request.getDiscountId() : request.getAssociatedAccountId();
    }

    private Discount getDiscount(final UpdateDiscountRequest request) throws DiscountNotFoundException, DbException {
        final List<Discount> discounts = request.hasAssociatedAccountId() ?
                discountStore.getDiscountByAssociatedAccountId(request.getAssociatedAccountId()) :
                discountStore.getDiscountByDiscountId(request.getDiscountId());
        return discounts.stream()
                .findFirst()
                .orElseThrow(() -> new DiscountNotFoundException(FAILED_TO_FIND_THE_UPDATED_DISCOUNT));
    }

    /**
     * Convert date time to local date time.
     *
     * @param dateTime date time with long type.
     * @return local date time with LocalDateTime type.
     */
    private LocalDateTime getLocalDateTime(long dateTime) {
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(dateTime),
                TimeZone.getDefault().toZoneId());
    }
}
