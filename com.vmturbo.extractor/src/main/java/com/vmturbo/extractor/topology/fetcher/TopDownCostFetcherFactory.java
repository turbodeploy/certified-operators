package com.vmturbo.extractor.topology.fetcher;

import java.util.Optional;
import java.util.function.Consumer;

import javax.annotation.Nonnull;

import io.grpc.StatusRuntimeException;

import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.cost.Cost.AccountExpenseQueryScope;
import com.vmturbo.common.protobuf.cost.Cost.AccountExpenses;
import com.vmturbo.common.protobuf.cost.Cost.GetAccountExpensesChecksumRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetCurrentAccountExpensesRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetCurrentAccountExpensesResponse;
import com.vmturbo.common.protobuf.cost.CostServiceGrpc.CostServiceBlockingStub;
import com.vmturbo.common.protobuf.cost.RIAndExpenseUploadServiceGrpc.RIAndExpenseUploadServiceBlockingStub;
import com.vmturbo.components.common.utils.MultiStageTimer;

/**
 * Factory class for {@link TopDownCostFetcher}s. Lives across topologies, and caches the most
 * recently retrieved {@link TopDownCostData} until the cost component's expense checksum changes.
 * This avoids excessive RPCs and database queries.
 */
public class TopDownCostFetcherFactory {
    private static final Logger logger = LogManager.getLogger();

    private final CostServiceBlockingStub costService;

    private final RIAndExpenseUploadServiceBlockingStub expenseUploadService;

    private volatile long curChecksum = 0;

    private volatile TopDownCostData cachedCostData = new TopDownCostData();

    /**
     * Constructor.
     *
     * @param costService To access the account expenses RPC.
     * @param expenseUploadService To access the account expense checksum RPC.
     */
    public TopDownCostFetcherFactory(@Nonnull final CostServiceBlockingStub costService,
            @Nonnull final RIAndExpenseUploadServiceBlockingStub expenseUploadService) {
        this.costService = costService;
        this.expenseUploadService = expenseUploadService;
    }

    /**
     * Create a new fetcher for a particular topology. The fetcher itself just passes through to
     * the factory, in order to utilize the cached {@link TopDownCostData}, updating it if
     * necessary.
     *
     * @param timer Timer. See {@link DataFetcher}
     * @param consumer Consumer for the resulting {@link TopDownCostData}. See {@link DataFetcher}.
     * @return The {@link TopDownCostFetcher}.
     */
    @Nonnull
    public TopDownCostFetcher newFetcher(MultiStageTimer timer, Consumer<TopDownCostData> consumer) {
        return new TopDownCostFetcher(timer, consumer, this);
    }

    @Nonnull
    synchronized TopDownCostData getMostRecentData() {
        long costChecksum = curChecksum;
        try {
            costChecksum = expenseUploadService.getAccountExpensesChecksum(GetAccountExpensesChecksumRequest.getDefaultInstance()).getChecksum();
        } catch (StatusRuntimeException e) {
            logger.error("Failed to fetch expenses checksum from cost component. Error: {}", e.getMessage());
        }

        if (costChecksum != curChecksum) {
            final TopDownCostData newCostData = new TopDownCostData();
            try {
                final GetCurrentAccountExpensesResponse response = costService.getCurrentAccountExpenses(
                        GetCurrentAccountExpensesRequest.newBuilder()
                                .setScope(AccountExpenseQueryScope.newBuilder().setAllAccounts(true))
                                .build());

                response.getAccountExpenseList().forEach(newCostData::addAccountExpense);
                this.curChecksum = costChecksum;
                this.cachedCostData = newCostData;
            } catch (StatusRuntimeException e) {
                logger.error("Failed to fetch top-down stats from cost component. Error: {}", e.getMessage());
            }
        }
        return cachedCostData;
    }

    /**
     * Utility object to make caching and using the top-down cost data easier.
     */
    public static class TopDownCostData {
        private final Long2ObjectMap<AccountExpenses> accountExpensesById = new Long2ObjectOpenHashMap<>();

        void addAccountExpense(final AccountExpenses accountExpenses) {
            final long accountId = accountExpenses.getAssociatedAccountId();
            accountExpensesById.put(accountId, accountExpenses);
        }

        /**
         * Get the expenses associated with a particular account.
         *
         * @param accountId The id of the account.
         * @return The {@link AccountExpenses} or an empty optional if there are no expenses for
         *         the account.
         */
        public Optional<AccountExpenses> getAccountExpenses(final long accountId) {
            return Optional.ofNullable(accountExpensesById.get(accountId));
        }
    }
}
