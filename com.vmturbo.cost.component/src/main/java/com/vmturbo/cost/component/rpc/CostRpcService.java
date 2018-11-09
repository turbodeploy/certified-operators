package com.vmturbo.cost.component.rpc;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalDouble;
import java.util.Set;
import java.util.TimeZone;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.common.protobuf.cost.Cost.AccountExpenses;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatRecord;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatRecord.StatRecord;
import com.vmturbo.common.protobuf.cost.Cost.CostCategory;
import com.vmturbo.common.protobuf.cost.Cost.CreateDiscountRequest;
import com.vmturbo.common.protobuf.cost.Cost.CreateDiscountResponse;
import com.vmturbo.common.protobuf.cost.Cost.DeleteDiscountRequest;
import com.vmturbo.common.protobuf.cost.Cost.DeleteDiscountResponse;
import com.vmturbo.common.protobuf.cost.Cost.Discount;
import com.vmturbo.common.protobuf.cost.Cost.DiscountInfo;
import com.vmturbo.common.protobuf.cost.Cost.EntityCost;
import com.vmturbo.common.protobuf.cost.Cost.GetCloudCostStatsRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetCloudCostStatsResponse;
import com.vmturbo.common.protobuf.cost.Cost.GetCloudExpenseStatsRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetCloudExpenseStatsRequest.GroupByType;
import com.vmturbo.common.protobuf.cost.Cost.GetDiscountRequest;
import com.vmturbo.common.protobuf.cost.Cost.UpdateDiscountRequest;
import com.vmturbo.common.protobuf.cost.Cost.UpdateDiscountResponse;
import com.vmturbo.common.protobuf.cost.CostServiceGrpc.CostServiceImplBase;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord.StatValue;
import com.vmturbo.cost.component.discount.DiscountNotFoundException;
import com.vmturbo.cost.component.discount.DiscountStore;
import com.vmturbo.cost.component.discount.DuplicateAccountIdException;
import com.vmturbo.cost.component.entity.cost.EntityCostStore;
import com.vmturbo.cost.component.entity.cost.ProjectedEntityCostStore;
import com.vmturbo.cost.component.expenses.AccountExpensesStore;
import com.vmturbo.cost.component.reserved.instance.TimeFrameCalculator;
import com.vmturbo.cost.component.reserved.instance.TimeFrameCalculator.TimeFrame;
import com.vmturbo.cost.component.util.AccountExpensesFilter;
import com.vmturbo.cost.component.util.CostFilter;
import com.vmturbo.cost.component.util.EntityCostFilter;
import com.vmturbo.cost.component.utils.BusinessAccountHelper;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.sql.utils.DbException;

/**
 * Implements the RPC calls supported by the cost component for retrieving Cloud cost data.
 */
public class CostRpcService extends CostServiceImplBase {

    /**
     * Cloud cost price constant to match UI request, also used in test case
     */
    public static final String COST_PRICE = "costPrice";

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

    private final DiscountStore discountStore;

    private final AccountExpensesStore accountExpensesStore;

    private final EntityCostStore entityCostStore;

    private final ProjectedEntityCostStore projectedEntityCostStore;

    private final BusinessAccountHelper businessAccountHelper;

    private final TimeFrameCalculator timeFrameCalculator;


    /**
     * Create a new RIAndExpenseUploadRpcService.
     *
     * @param discountStore        The store containing account discounts
     * @param accountExpensesStore The store containing account expenses
     */
    public CostRpcService(@Nonnull final DiscountStore discountStore,
                          @Nonnull final AccountExpensesStore accountExpensesStore,
                          @Nonnull final EntityCostStore costStoreHouse,
                          @Nonnull final ProjectedEntityCostStore projectedEntityCostStore,
                          @Nonnull final TimeFrameCalculator timeFrameCalculator,
                          @Nonnull final BusinessAccountHelper businessAccountHelper) {

        this.discountStore = Objects.requireNonNull(discountStore);
        this.accountExpensesStore = Objects.requireNonNull(accountExpensesStore);
        this.entityCostStore = Objects.requireNonNull(costStoreHouse);
        this.projectedEntityCostStore = Objects.requireNonNull(projectedEntityCostStore);
        this.businessAccountHelper = Objects.requireNonNull(businessAccountHelper);
        this.timeFrameCalculator = Objects.requireNonNull(timeFrameCalculator);
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
     * The 'entitiesList' in the {@link GetCloudExpenseStatsRequest} is not currently used.
     *
     * @param request          a set of parameters describing how to fetch the snapshot and what entities.
     * @param responseObserver the sync for each result value {@link StatSnapshot}
     */
    @Override
    public void getAccountExpenseStats(GetCloudExpenseStatsRequest request,
                                       StreamObserver<GetCloudCostStatsResponse> responseObserver) {
        if (request.hasGroupBy()) {
            try {
                final TimeFrame timeFrame = timeFrameCalculator.millis2TimeFrame(request.getStartDate());
                final Set<Long> filterIds = request.getEntityFilter().getEntityIdList().stream().collect(Collectors.toSet());
                final Set<Integer> entityTypeFilterIds = request.getEntityTypeFilter().getEntityTypeIdList().stream().collect(Collectors.toSet());
                final CostFilter entityCostFilter =
                        new AccountExpensesFilter(filterIds, entityTypeFilterIds, request.getStartDate(), request.getEndDate(), timeFrame);
                final Map<Long, Map<Long, AccountExpenses>> snapshoptToExpenseMap =
                        (request.hasStartDate() && request.hasEndDate()) ?
                                accountExpensesStore.getAccountExpenses(entityCostFilter) :
                                accountExpensesStore.getLatestExpenses(filterIds, entityTypeFilterIds);
                final List<CloudCostStatRecord> cloudStatRecords = Lists.newArrayList();
                snapshoptToExpenseMap.forEach((snapshopTime, accountIdToExpensesMap) -> {
                    final CloudCostStatRecord.Builder snapshotBuilder = CloudCostStatRecord.newBuilder();
                    snapshotBuilder.setSnapshotDate(DateTimeUtil.toString(snapshopTime));
                    final List<AccountExpenseStat> accountExpenseStats = Lists.newArrayList();
                    accountIdToExpensesMap.values().forEach(accountExpenses -> {
                        accountExpenses.getAccountExpensesInfo().getServiceExpensesList().forEach(serviceExpenses -> {
                                    final long amount = (long) serviceExpenses.getExpenses().getAmount();
                                    // build the record for this stat (commodity type)
                                    final AccountExpenseStat accountExpenseStat = new AccountExpenseStat(
                                            request.getGroupBy().equals(GroupByType.CLOUD_SERVICE) ?
                                                    serviceExpenses.getAssociatedServiceId() : businessAccountHelper
                                                    .resolveTargetId(accountExpenses.getAssociatedAccountId())
                                            , amount);
                                    // return add this record to the snapshot for this timestamp
                                    accountExpenseStats.add(accountExpenseStat);
                                }
                        );
                    });

                    snapshotBuilder.addAllStatRecords(aggregateStatRecords(accountExpenseStats));
                    cloudStatRecords.add(snapshotBuilder.build());
                });
                GetCloudCostStatsResponse response =
                        GetCloudCostStatsResponse.newBuilder()
                                .addAllCloudStatRecord(cloudStatRecords)
                                .build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();

            } catch (DbException e) {
                logger.error("Error getting stats snapshots for {}", request);
                logger.error("    ", e);
                responseObserver.onError(Status.INTERNAL
                        .withDescription("Internal Error fetching stats for: " + request + ", cause: "
                                + e.getMessage())
                        .asException());
            }
        } else {
            responseObserver.onError(Status.INTERNAL
                    .withDescription("The request includes unsupported group by type: " + request
                            + ", currently only group by CSP, CloudService and target (Account) are supported")
                    .asException());
        }


    }

    // aggregate account expense stats based to either targetId, or serviceId
    private Iterable<? extends StatRecord> aggregateStatRecords(@Nonnull final List<AccountExpenseStat> accountExpenseStats) {
        final Map<Long, List<AccountExpenseStat>> aggregatedMap = accountExpenseStats.stream()
                .collect(Collectors.groupingBy(statRecord -> statRecord.getAssociatedEntityId()));
        final List<StatRecord> aggregatedStatRecords = Lists.newArrayList();
        aggregatedMap.forEach((id, stats) -> {
            final CloudCostStatRecord.StatRecord.Builder statRecordBuilder = CloudCostStatRecord.StatRecord.newBuilder()
                    .setName(COST_PRICE);
            statRecordBuilder.setAssociatedEntityId(id);
            statRecordBuilder.setAssociatedEntityType(EntityType.CLOUD_SERVICE_VALUE);
            statRecordBuilder.setUnits("$/h");
            statRecordBuilder.setValues(CloudCostStatRecord.StatRecord.StatValue.newBuilder()
                    .setAvg((float) stats.stream().map(AccountExpenseStat::getValue).mapToLong(v -> v).average().orElse(0))
                    .setMax(stats.stream().map(AccountExpenseStat::getValue).mapToLong(v -> v).max().orElse(0))
                    .setMin(stats.stream().map(AccountExpenseStat::getValue).mapToLong(v -> v).min().orElse(0))
                    .setTotal(stats.stream().map(AccountExpenseStat::getValue).mapToLong(v -> v).sum())
                    .build());
            aggregatedStatRecords.add(statRecordBuilder.build());
        });
        return aggregatedStatRecords;
    }

    @Override
    public void getCloudCostStats(GetCloudCostStatsRequest request,
                                  StreamObserver<GetCloudCostStatsResponse> responseObserver) {
        try {
            if (!request.hasCostCategoryFilter()) {
                final TimeFrame timeFrame = timeFrameCalculator.millis2TimeFrame(request.getStartDate());
                final Set<Long> filterIds = request.getEntityFilter().getEntityIdList().stream().collect(Collectors.toSet());
                final Set<Integer> entityTypeFilterIds = request.getEntityTypeFilter().getEntityTypeIdList().stream().collect(Collectors.toSet());

                final CostFilter entityCostFilter =
                        new EntityCostFilter(filterIds, entityTypeFilterIds, request.getStartDate(), request.getEndDate(), timeFrame);
                Map<Long, Map<Long, EntityCost>> snapshotToEntityCostMap;
                if (request.hasStartDate() && request.hasEndDate()) {
                    snapshotToEntityCostMap = entityCostStore.getEntityCosts(entityCostFilter);
                    snapshotToEntityCostMap.getOrDefault(request.getEndDate(), new HashMap<>()).putAll(projectedEntityCostStore.getAllProjectedEntitiesCosts());
                } else {
                    snapshotToEntityCostMap = entityCostStore.getLatestEntityCost(filterIds, entityTypeFilterIds);
                }
                final List<CloudCostStatRecord> cloudStatRecords = Lists.newArrayList();
                snapshotToEntityCostMap.forEach((time, costsByEntity) -> {
                            final List<CloudCostStatRecord.StatRecord> statRecords = Lists.newArrayList();
                            // GroupBy Cost components, e.g. compute, IP, storage, license
                            if (request.hasGroupBy()
                                    && request.getGroupBy().equals(GetCloudCostStatsRequest.GroupByType.COSTCOMPONENT)) {
                                final CloudCostStatRecord.StatRecord.Builder builder = CloudCostStatRecord.StatRecord.newBuilder();
                                aggregateEntityCostByCostType(costsByEntity.values())
                                        .forEach(componentCost -> {
                                            builder.setAssociatedEntityType(EntityType.VIRTUAL_MACHINE_VALUE);
                                            builder.setCategory(componentCost.costCategory);
                                            builder.setName(COST_PRICE);
                                            builder.setUnits("$/h");
                                            builder.setValues(CloudCostStatRecord.StatRecord.StatValue.newBuilder()
                                                    .setAvg((float) componentCost.geAvg().orElse(0))
                                                    .setMax((float) componentCost.getMax().orElse(0))
                                                    .setMin((float) componentCost.getMin().orElse(0))
                                                    .setTotal((float) componentCost.getTotal())
                                                    .build());
                                            statRecords.add(builder.build());
                                        });
                            } else {  // populate cost per entity, and up to the caller to aggregate as needed.
                                costsByEntity.values().forEach(entityCost -> {
                                    entityCost.getComponentCostList().forEach(componentCost -> {
                                                final CloudCostStatRecord.StatRecord.Builder builder = CloudCostStatRecord.StatRecord.newBuilder();
                                                final float amount = (float) componentCost.getAmount().getAmount();
                                                builder.setAssociatedEntityId(entityCost.getAssociatedEntityId());
                                                builder.setAssociatedEntityType(entityCost.getAssociatedEntityType());
                                                builder.setCategory(componentCost.getCategory());
                                                builder.setName(COST_PRICE);
                                                builder.setUnits("$/h");
                                                builder.setValues(CloudCostStatRecord.StatRecord.StatValue.newBuilder()
                                                        .setAvg(amount)
                                                        .setMax(amount)
                                                        .setMin(amount)
                                                        .setTotal(amount)
                                                        .build());
                                                statRecords.add(builder.build());
                                            }
                                    );
                                });
                            }
                            final CloudCostStatRecord cloudStatRecord = CloudCostStatRecord.newBuilder()
                                    .setSnapshotDate(DateTimeUtil.toString(time))
                                    .addAllStatRecords(statRecords)
                                    .build();
                            cloudStatRecords.add(cloudStatRecord);

                        }
                );
                GetCloudCostStatsResponse response =
                        GetCloudCostStatsResponse.newBuilder()
                                .addAllCloudStatRecord(cloudStatRecords)
                                .build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
            } else {
                responseObserver.onError(Status.INTERNAL
                        .withDescription("The request includes unsupported filter type: " + request
                                + ", currently only workload type is supported")
                        .asException());
            }
        } catch (DbException e) {
            logger.error("Error getting stats snapshots for {}", request);
            logger.error("    ", e);
            responseObserver.onError(Status.INTERNAL
                    .withDescription("Internal Error fetching stats for: " + request + ", cause: "
                            + e.getMessage())
                    .asException());
        }
    }

    //TODO move them to helper class
    // aggregate all the value per cost type, e.g. IP, Compute, License
    private Set<AggregatedEntityCost> aggregateEntityCostByCostType(final Collection<EntityCost> values) {
        final AggregatedEntityCost costIP = new AggregatedEntityCost(CostCategory.IP);
        final AggregatedEntityCost costLicense = new AggregatedEntityCost(CostCategory.LICENSE);
        final AggregatedEntityCost costOnDemandCompute = new AggregatedEntityCost(CostCategory.ON_DEMAND_COMPUTE);
        final AggregatedEntityCost costStorage = new AggregatedEntityCost(CostCategory.STORAGE);
        final AggregatedEntityCost costRICompute = new AggregatedEntityCost(CostCategory.RI_COMPUTE);
        values.forEach(entityCost -> {
            entityCost.getComponentCostList().forEach(componentCost -> {
                switch(componentCost.getCategory()) {
                    case ON_DEMAND_COMPUTE:
                        costOnDemandCompute.addCost(componentCost.getAmount().getAmount());
                        break;
                    case IP:
                        costIP.addCost(componentCost.getAmount().getAmount());
                        break;
                    case LICENSE:
                        costLicense.addCost(componentCost.getAmount().getAmount());
                        break;
                    case STORAGE:
                        costStorage.addCost(componentCost.getAmount().getAmount());
                        break;
                    case RI_COMPUTE:
                        costRICompute.addCost(componentCost.getAmount().getAmount());
                        break;
                }
            });
        });
        return ImmutableSet.of(costIP, costOnDemandCompute, costLicense, costStorage, costRICompute);
    }

    private CloudCostStatRecord.StatRecord buildStatRecord(@Nullable final Long producerId,
                                                           @Nullable final Float avgValue) {
        final CloudCostStatRecord.StatRecord.Builder statRecordBuilder = CloudCostStatRecord.StatRecord.newBuilder()
                .setName(COST_PRICE);
        if (producerId != null && producerId != 0) {
            // providerUuid, it's associated accountId except CloudService type which is serviceId
            statRecordBuilder.setAssociatedEntityId(producerId);
        }
        // hardcoded for now
        statRecordBuilder.setUnits("$/h");

        // values, used, peak
        StatValue.Builder statValueBuilder = StatValue.newBuilder();
        if (avgValue != null) {
            statValueBuilder.setAvg(avgValue);
        }

        statValueBuilder.setTotal(avgValue);

        statRecordBuilder.setValues(CloudCostStatRecord.StatRecord.StatValue.newBuilder()
                .setAvg(avgValue)
                .setMax(avgValue)
                .setMin(avgValue)
                .setTotal(avgValue)
                .build());
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

    // helper class to do entity cost aggregation calculation per timestamp
    private class AggregatedEntityCost {
        final CostCategory costCategory;
        final List<Double> costList = Lists.newArrayList();

        AggregatedEntityCost(CostCategory costCategory) {
            this.costCategory = costCategory;
        }

        void addCost(double amount) {
            costList.add(amount);
        }

        OptionalDouble getMin() {
            return costList.stream()
                    .mapToDouble(v -> v)
                    .min();
        }

        OptionalDouble getMax() {
            return costList.stream()
                    .mapToDouble(v -> v)
                    .max();
        }

        OptionalDouble geAvg() {
            return costList.stream()
                    .mapToDouble(v -> v)
                    .average();
        }

        double getTotal() {
            return costList.stream()
                    .mapToDouble(v -> v)
                    .sum();
        }
    }

    // helper class to do account expense aggregation calculation per timestamp
    private class AccountExpenseStat {
        private final long associatedEntityId;
        private final long value;

        AccountExpenseStat(long associatedEntityId, long value) {
            this.associatedEntityId = associatedEntityId;
            this.value = value;
        }

        public long getAssociatedEntityId() {
            return associatedEntityId;
        }

        public long getValue() {
            return value;
        }
    }

}