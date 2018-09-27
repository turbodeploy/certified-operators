package com.vmturbo.cost.component.rpc;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;

import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo;
import com.vmturbo.common.protobuf.cost.Cost.UploadRIAndExpenseDataRequest;
import com.vmturbo.common.protobuf.cost.Cost.UploadRIAndExpenseDataResponse;
import com.vmturbo.common.protobuf.cost.RIAndExpenseUploadServiceGrpc.RIAndExpenseUploadServiceImplBase;
import com.vmturbo.cost.component.expenses.AccountExpensesStore;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceBoughtStore;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceCoverageUpdate;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceSpecStore;

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

    public RIAndExpenseUploadRpcService(@Nonnull final DSLContext dsl,
                                        @Nonnull AccountExpensesStore accountExpensesStore,
                                        @Nonnull ReservedInstanceSpecStore reservedInstanceSpecStore,
                                        @Nonnull ReservedInstanceBoughtStore reservedInstanceBoughtStore,
                                        @Nonnull ReservedInstanceCoverageUpdate reservedInstanceCoverageUpdate) {
        this.dsl = dsl;
        this.accountExpensesStore = accountExpensesStore;
        this.reservedInstanceSpecStore = reservedInstanceSpecStore;
        this.reservedInstanceBoughtStore = reservedInstanceBoughtStore;
        this.reservedInstanceCoverageUpdate = reservedInstanceCoverageUpdate;
    }

    @Override
    public void uploadRIAndExpenseData(final UploadRIAndExpenseDataRequest request, final StreamObserver<UploadRIAndExpenseDataResponse> responseObserver) {
        logger.info("Processing cost data for topology {}", request.getTopologyId());
        if (request.getAccountExpensesCount() > 0) {
            // update biz accounts
            logger.debug("Updating expenses for {} Business Accounts...", request.getAccountExpensesCount());
            request.getAccountExpensesList().forEach(accountExpenses -> {
                try {
                    accountExpensesStore.persistAccountExpenses(accountExpenses.getAssociatedAccountId(),
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
        // need to update reserved instance bought and spec first, because reserved instance coverage
        // data will use them later.
        storeRIBoughtAndSpecIntoDB(request);
        reservedInstanceCoverageUpdate.storeEntityRICoverageOnlyIntoCache(request.getTopologyId(),
                request.getReservedInstanceCoverageList());
        responseObserver.onNext(UploadRIAndExpenseDataResponse.getDefaultInstance());
        responseObserver.onCompleted();
    }

    /**
     * Store the received reserved instance bought and specs into database.
     * @param request a {@link UploadRIAndExpenseDataRequest} contains RI bought and spec data.
     */
    private void storeRIBoughtAndSpecIntoDB(@Nonnull final UploadRIAndExpenseDataRequest request) {
        if (request.getReservedInstanceBoughtList().isEmpty() && request.getReservedInstanceSpecsList().isEmpty()) {
            logger.info("There is no RI bought and spec in uploaded data!");
        }
        else if (request.getReservedInstanceSpecsCount() > 0 && request.getReservedInstanceBoughtCount() > 0) {
            // if uploaded data has both RI bought and spec, then update them in one transaction.
            logger.debug("Updating {} ReservedInstance bought and spec...", request.getReservedInstanceSpecsCount());
            dsl.transaction(configuration -> {
                final DSLContext transactionContext = DSL.using(configuration);
                final Map<Long, Long> riSpecIdMap =
                        reservedInstanceSpecStore.updateReservedInstanceBoughtSpec(transactionContext,
                                request.getReservedInstanceSpecsList());
                final List<ReservedInstanceBoughtInfo> riBoughtInfoWithNewSpecIds =
                        updateRIBoughtInfoWithNewSpecIds(request.getReservedInstanceBoughtList(), riSpecIdMap);
                reservedInstanceBoughtStore.updateReservedInstanceBought(transactionContext,
                        riBoughtInfoWithNewSpecIds);
            });
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
}
