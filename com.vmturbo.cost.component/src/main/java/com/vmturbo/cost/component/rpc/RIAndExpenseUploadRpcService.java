package com.vmturbo.cost.component.rpc;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import io.grpc.stub.StreamObserver;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;

import com.vmturbo.common.protobuf.cost.Cost.ChecksumResponse;
import com.vmturbo.common.protobuf.cost.Cost.ClearRIDataChecksumRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetAccountExpensesChecksumRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetRIDataChecksumRequest;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo;
import com.vmturbo.common.protobuf.cost.Cost.UploadAccountExpensesRequest;
import com.vmturbo.common.protobuf.cost.Cost.UploadAccountExpensesResponse;
import com.vmturbo.common.protobuf.cost.Cost.UploadRIDataRequest;
import com.vmturbo.common.protobuf.cost.Cost.UploadRIDataRequest.AccountRICoverageUpload;
import com.vmturbo.common.protobuf.cost.Cost.UploadRIDataRequest.EntityRICoverageUpload;
import com.vmturbo.common.protobuf.cost.Cost.UploadRIDataRequest.EntityRICoverageUpload.Coverage;
import com.vmturbo.common.protobuf.cost.Cost.UploadRIDataResponse;
import com.vmturbo.common.protobuf.cost.RIAndExpenseUploadServiceGrpc.RIAndExpenseUploadServiceImplBase;
import com.vmturbo.cost.component.expenses.AccountExpensesStore;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceBoughtStore;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceCoverageUpdate;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceSpecStore;
import com.vmturbo.cost.component.reserved.instance.filter.ReservedInstanceBoughtFilter;

/**
 * This class is an uploader service for RIs and expenses.
 */
public class RIAndExpenseUploadRpcService extends RIAndExpenseUploadServiceImplBase {
    private final Logger logger = LogManager.getLogger();

    private final DSLContext dsl;

    @Nonnull
    private final AccountExpensesStore accountExpensesStore;

    @Nonnull
    private final ReservedInstanceSpecStore reservedInstanceSpecStore;

    @Nonnull
    private final ReservedInstanceBoughtStore reservedInstanceBoughtStore;

    private final ReservedInstanceCoverageUpdate reservedInstanceCoverageUpdate;

    // remember the checksum of the last upload requests we successfully processed.
    private long lastProcessedRIDataChecksum = 0;

    private long lastProcessedAccountExpensesChecksum = 0;

    private final boolean riSupportInPartialCloudEnvironment;

    /**
     * Constructor.
     *
     * @param dsl database context
     * @param accountExpensesStore store for acount expenses
     * @param reservedInstanceSpecStore store for reserved instance specs
     * @param reservedInstanceBoughtStore store for reserved instance bought
     * @param reservedInstanceCoverageUpdate reserved instance coverage update
     * @param riSupportInPartialCloudEnvironment OM-58310: Improve handling partially
     *         discovered cloud environment feature flag. Calculate hourly reservation usage during
     *         upload if flag is set to {@code true}.
     */
    public RIAndExpenseUploadRpcService(@Nonnull final DSLContext dsl,
            @Nonnull final AccountExpensesStore accountExpensesStore,
            @Nonnull final ReservedInstanceSpecStore reservedInstanceSpecStore,
            @Nonnull final ReservedInstanceBoughtStore reservedInstanceBoughtStore,
            @Nonnull final ReservedInstanceCoverageUpdate reservedInstanceCoverageUpdate,
            final boolean riSupportInPartialCloudEnvironment) {
        this.dsl = dsl;
        this.accountExpensesStore = accountExpensesStore;
        this.reservedInstanceSpecStore = reservedInstanceSpecStore;
        this.reservedInstanceBoughtStore = reservedInstanceBoughtStore;
        this.reservedInstanceCoverageUpdate = reservedInstanceCoverageUpdate;
        this.riSupportInPartialCloudEnvironment = riSupportInPartialCloudEnvironment;
    }

    @Override
    public void getRIDataChecksum(final GetRIDataChecksumRequest request, final StreamObserver<ChecksumResponse> responseObserver) {
        responseObserver.onNext(ChecksumResponse.newBuilder()
                .setChecksum(lastProcessedRIDataChecksum).build());
        responseObserver.onCompleted();
    }

    @Override
    public void clearRIDataChecksum(final ClearRIDataChecksumRequest request,
                                    final StreamObserver<ChecksumResponse> responseObserver) {
        lastProcessedRIDataChecksum = 0;
        responseObserver.onNext(ChecksumResponse.newBuilder()
                .setChecksum(lastProcessedRIDataChecksum).build());
        responseObserver.onCompleted();
    }


    @Override
    public void getAccountExpensesChecksum(final GetAccountExpensesChecksumRequest request, final StreamObserver<ChecksumResponse> responseObserver) {
        responseObserver.onNext(ChecksumResponse.newBuilder()
                .setChecksum(lastProcessedAccountExpensesChecksum).build());
        responseObserver.onCompleted();
    }

    @Override
    public void uploadAccountExpenses(final UploadAccountExpensesRequest request, final StreamObserver<UploadAccountExpensesResponse> responseObserver) {
        logger.info("Processing account expenses for topology {}", request.getTopologyId());
        if (request.getAccountExpensesCount() > 0) {
            // update biz accounts
            logger.debug("Updating expenses for {} Business Accounts...", request.getAccountExpensesCount());
            request.getAccountExpensesList().forEach(accountExpenses -> {
                try {
                    accountExpensesStore.persistAccountExpenses(accountExpenses.getAssociatedAccountId(),
                            accountExpenses.getExpensesDate(),
                            accountExpenses.getAccountExpensesInfo());
                } catch (Exception e) {
                    logger.error("Error saving account {}", accountExpenses.getAssociatedAccountId(), e);
                    // TODO: Refine the error handling. Return an error for now.
                    responseObserver.onError(e);
                    return;
                }
            });
            // TODO: do we need to delete business accounts that were NOT updated? If so, how should
            // we do this?
        }
        lastProcessedAccountExpensesChecksum = request.getChecksum();
        responseObserver.onNext(UploadAccountExpensesResponse.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void uploadRIData(final UploadRIDataRequest request, final StreamObserver<UploadRIDataResponse> responseObserver) {
        logger.info("Processing RI data for topology {}", request.getTopologyContextId());
        // need to update reserved instance bought and spec first, because reserved instance coverage
        // data will use them later.
        storeRIBoughtAndSpecIntoDB(request);

        // need to update the reserved instance ID in each EntityRICoverageUpload's Coverage list
        // The topology-processor currently uploads ReservedInstanceBought instances with a TP
        // generated oid. However, the ReservedInstanceBoughtStore drops the TP's oid and assigns
        // its own. Therefore, the only way to map a Coverage instance to a ReservedInstanceBought
        // is through the ProbeReservedInstanceId. We normalize the Coverage records here to remove
        // the dependency on downstream consumers.
        final Map<String, Long> riProbeIdToOid = reservedInstanceBoughtStore
                .getReservedInstanceBoughtByFilter(ReservedInstanceBoughtFilter.SELECT_ALL_FILTER)
                .stream()
                .filter(ReservedInstanceBought::hasReservedInstanceBoughtInfo)
                .collect(Collectors.toMap(ri -> ri.getReservedInstanceBoughtInfo()
                        .getProbeReservedInstanceId(), ReservedInstanceBought::getId));
        final List<EntityRICoverageUpload> entityRiCoverageWithRIOid =
                updateEntityRICoverages(request.getReservedInstanceCoverageList(), riProbeIdToOid);
        reservedInstanceCoverageUpdate.storeEntityRICoverageOnlyIntoCache(request.getTopologyContextId(),
                entityRiCoverageWithRIOid);
        // Store the account coverage in cache
        final List<AccountRICoverageUpload> accountRiCoverageWithRIOid =
                updateAccountRICoverages(request.getAccountLevelReservedInstanceCoverageList(),
                        riProbeIdToOid);
        reservedInstanceCoverageUpdate.cacheAccountRICoverageData(request.getTopologyContextId(),
                accountRiCoverageWithRIOid);
        lastProcessedRIDataChecksum = request.getChecksum();
        responseObserver.onNext(UploadRIDataResponse.getDefaultInstance());
        responseObserver.onCompleted();
    }

    /**
     * Store the received reserved instance bought and specs into database.
     * @param request a {@link UploadRIDataRequest} contains RI bought and spec data.
     */
    private void storeRIBoughtAndSpecIntoDB(@Nonnull final UploadRIDataRequest request) {
        if (request.getReservedInstanceBoughtList().isEmpty()
            && request.getReservedInstanceSpecsList().isEmpty()) {
            logger.info("There is no RI bought and spec in uploaded data!");
        } else if (request.getReservedInstanceSpecsCount() > 0
            && request.getReservedInstanceBoughtCount() > 0) {
            // if uploaded data has both RI bought and spec, then update them in one transaction.
            logger.debug("Updating {} ReservedInstance bought and spec...", request.getReservedInstanceSpecsCount());
            try {
                dsl.transaction(configuration -> {
                    final DSLContext transactionContext = DSL.using(configuration);
                    final Map<Long, Long> riSpecIdMap =
                            reservedInstanceSpecStore.updateReservedInstanceSpec(transactionContext,
                                    request.getReservedInstanceSpecsList());
                    final List<ReservedInstanceBoughtInfo> riBoughtInfoWithNewSpecIds =
                            updateRIBoughtInfoWithNewSpecIds(request.getReservedInstanceBoughtList(), riSpecIdMap);
                    reservedInstanceBoughtStore.updateReservedInstanceBought(transactionContext,
                            riBoughtInfoWithNewSpecIds);
                });
            } catch (DataAccessException e) {
                logger.error("Error while updating RISpec or RIBought store", e);
            }
        } else if (!request.getReservedInstanceBoughtList().isEmpty()
                || !request.getReservedInstanceSpecsList().isEmpty()) {
            logger.error("There are {} RI bought, but there are {} RI spec!",
                    request.getReservedInstanceBoughtCount(), request.getReservedInstanceSpecsCount());
        }
    }

    /**
     * Generate a list of {@link ReservedInstanceBoughtInfo} which contains the real spec id.
     *
     * @param riBoughtList a list of {@link ReservedInstanceBought} which contains the probe assigned
     *                     RI spec id.
     * @param riSpecIdMap a Map which key is probe assigned RI spec id, value is the real spec id.
     * @return a list of {@link ReservedInstanceBoughtInfo}.
     */
    private List<ReservedInstanceBoughtInfo> updateRIBoughtInfoWithNewSpecIds(
            @Nonnull final List<ReservedInstanceBought> riBoughtList,
            @Nonnull final Map<Long, Long> riSpecIdMap) {
        return riBoughtList.stream()
                .map(riBought -> ReservedInstanceBought.newBuilder(riBought)
                    .getReservedInstanceBoughtInfoBuilder()
                        // riSpecIdMap should contains all old reserved instance spec id.
                        .setReservedInstanceSpec(riSpecIdMap.get(riBought.getReservedInstanceBoughtInfo()
                                .getReservedInstanceSpec()))
                    .build())
                .collect(Collectors.toList());
    }

    private List<EntityRICoverageUpload> updateEntityRICoverages(
            @Nonnull final List<EntityRICoverageUpload> entityRICoverageList,
            @Nonnull final Map<String, Long> riProbeIdToOid ) {
        return entityRICoverageList.stream().map(EntityRICoverageUpload::newBuilder)
                .peek(entityRICoverageUpload -> entityRICoverageUpload.getCoverageBuilderList()
                        .forEach(coverage -> updateCoverage(coverage, riProbeIdToOid)))
                .map(EntityRICoverageUpload.Builder::build).collect(Collectors.toList());
    }

    private List<AccountRICoverageUpload> updateAccountRICoverages(
            final List<AccountRICoverageUpload> accountLevelReservedInstanceCoverageList,
            @Nonnull final Map<String, Long> riProbeIdToOid) {
        return accountLevelReservedInstanceCoverageList.stream()
                .map(AccountRICoverageUpload::newBuilder)
                .peek(accountRICoverageUpload -> accountRICoverageUpload.getCoverageBuilderList()
                        .forEach(coverage -> updateCoverage(coverage, riProbeIdToOid)))
                .map(AccountRICoverageUpload.Builder::build).collect(Collectors.toList());
    }

    /**
     * 1. Sets the reserved instance oid for {@code coverage} with oid generated by
     * {@link ReservedInstanceBoughtStore}.
     * 2. Calculates hourly reservation usage if {@link this#riSupportInPartialCloudEnvironment} flag
     * is set to {@code true} .
     *
     * @param coverage the {@link Coverage.Builder} which needs to be updated
     * @param riProbeIdToOid mapping of the probe id to the cost component generated oid
     */
    private void updateCoverage(@Nonnull final Coverage.Builder coverage,
            @Nonnull final Map<String, Long> riProbeIdToOid) {
        // Update the reserved instance id for coverage record,
        // mapping through the probe reserved instance id.
        if (coverage.hasProbeReservedInstanceId()) {
            final Long oid = riProbeIdToOid.get(coverage.getProbeReservedInstanceId());
            if (oid != null) {
                coverage.setReservedInstanceId(oid);
            } else {
                logger.debug(
                        "Could not find reserved instance oid by id {}, reserved instance oid will not be set for coverage {}.",
                        coverage.getProbeReservedInstanceId(), coverage);
                coverage.clearReservedInstanceId();
            }
        }
        if (riSupportInPartialCloudEnvironment && coverage.hasCoveredCoupons()) {
            if (coverage.hasUsageStartTimestamp() && coverage.hasUsageEndTimestamp()) {
                final double hours = (double)Math.abs(
                        coverage.getUsageEndTimestamp() - coverage.getUsageStartTimestamp())
                        / TimeUnit.HOURS.toMillis(1);
                coverage.setCoveredCoupons(coverage.getCoveredCoupons() / hours);
            } else {
                logger.warn(
                        "Usage period for coverage {} is not specified, hourly reservation usage will not be calculated.",
                        coverage);
            }
        }
    }
}
