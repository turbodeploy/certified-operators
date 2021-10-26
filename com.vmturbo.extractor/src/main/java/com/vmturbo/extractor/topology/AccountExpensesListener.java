package com.vmturbo.extractor.topology;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.cost.Cost.AccountExpenses;
import com.vmturbo.common.protobuf.cost.Cost.AccountExpenses.AccountExpensesInfo.ServiceExpenses;
import com.vmturbo.common.protobuf.cost.CostNotificationOuterClass.CostNotification;
import com.vmturbo.components.common.utils.MultiStageTimer;
import com.vmturbo.components.common.utils.MultiStageTimer.Detail;
import com.vmturbo.components.common.utils.ThrowingConsumer;
import com.vmturbo.cost.api.CostNotificationListener;
import com.vmturbo.extractor.models.DslRecordSink;
import com.vmturbo.extractor.models.DslUpsertRecordSink;
import com.vmturbo.extractor.models.ModelDefinitions.CloudServiceCost;
import com.vmturbo.extractor.models.Table.Record;
import com.vmturbo.extractor.models.Table.TableWriter;
import com.vmturbo.extractor.topology.fetcher.TopDownCostFetcherFactory.TopDownCostData;
import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;

/**
 * TopologyWriter that records per-entity cost data obtained from the cost compoennt.
 */
public class AccountExpensesListener implements CostNotificationListener {
    private static final Logger logger = LogManager.getLogger();

    private final DataProvider dataProvider;
    private final DbEndpoint dbEndpoint;
    private final ExecutorService pool;
    private final WriterConfig config;
    private final boolean isBillingCostReportingEnabled;

    /**
     * Create a new instance.
     *
     * @param dataProvider {@link DataProvider} that will provide cost data
     * @param dbEndpoint   access to extractor database
     * @param pool         thread pool
     * @param config       writer config
     * @param isBillingCostReportingEnabled whether billing cost is enabled for reporting
     */
    public AccountExpensesListener(DataProvider dataProvider, final DbEndpoint dbEndpoint,
            final ExecutorService pool, WriterConfig config, boolean isBillingCostReportingEnabled) {
        this.dataProvider = dataProvider;
        this.dbEndpoint = dbEndpoint;
        this.pool = pool;
        this.config = config;
        this.isBillingCostReportingEnabled = isBillingCostReportingEnabled;
        logger.info("Created account expenses listener for endpoint {}.", dbEndpoint);
    }

    /**
     * Receives notification from cost component (RIAndExpenseUploadRpcService::uploadAccountExpenses)
     * that new cloud service expenses have been written to the account expenses store.
     * This triggers extractor to pull the latest top-down data and write it to cloud_service_cost
     * DB table, for all accounts.
     *
     * @param costNotification The account expense available notification.
     */
    @Override
    public void onCostNotificationReceived(@Nonnull final CostNotification costNotification) {
        if (costNotification.hasAccountExpensesAvailable()) {
            MultiStageTimer timer = new MultiStageTimer(logger);

            logger.info("Received AccountExpenses available notification, fetching top-down costs."
                    + "Billing cost reporting enabled? {}", isBillingCostReportingEnabled);
            // Trigger fetch of the latest billing data from cost component. We need to do this
            // if either extractor is enabled or embedded reporting billing costs flag is enabled.
            dataProvider.fetchTopDownCostData(timer);

            // Only write data if embedded reporting billing flag is enabled.
            if (isBillingCostReportingEnabled) {
                try {
                    timer.start("Write account expenses");

                    // Load the latest cost data just fetched.
                    final TopDownCostData expenseData = dataProvider.getTopDownCostData();

                    int numAccounts = writeAccountExpenses(expenseData);
                    timer.info(String.format("Wrote account expenses for %d accounts.", numAccounts),
                            Detail.STAGE_DETAIL);
                } catch (UnsupportedDialectException | SQLException e) {
                    logger.error("Failed writing account expense data", e);
                } catch (InterruptedException e) {
                    logger.error("Interrupted while writing account expense data");
                    Thread.currentThread().interrupt();
                } finally {
                    timer.stop();
                }
            }
        }
    }

    /**
     * Writes the latest top-down account data to the cloud_service_cost DB table.
     * Normally we receive new data at least once a day, in case we attempt to insert data multiple
     * times, then DB gets updated with the set of records that we tried to insert last.
     *
     * @param expenseData Top-down data read from cost component, to be written to extractor DB.
     * @return Number of accounts for which data was written.
     * @throws UnsupportedDialectException On DB insert error.
     * @throws InterruptedException Thread interrupt.
     * @throws SQLException General DB error.
     */
    int writeAccountExpenses(@Nullable final TopDownCostData expenseData)
            throws UnsupportedDialectException, InterruptedException, SQLException {
        if (expenseData == null || expenseData.size() == 0) {
            logger.warn("No account data available, so none persisted.");
            return 0;
        }
        logger.info("Writing cloud service expenses for {} accounts.", expenseData.size());

        final DslRecordSink sink = new DslUpsertRecordSink(dbEndpoint.dslContext(), CloudServiceCost.TABLE,
                config, pool, "temp_csc", ImmutableList.of(CloudServiceCost.TIME,
                CloudServiceCost.ACCOUNT_OID, CloudServiceCost.CLOUD_SERVICE_OID),
                ImmutableList.of(CloudServiceCost.COST));

        int numAccounts = 0;
        try (TableWriter writer = CloudServiceCost.TABLE.open(sink, "cloud_service_cost inserter",
                logger)) {
            numAccounts = consumeAccountExpenses(writer, expenseData);
        }
        return numAccounts;
    }

    /**
     * Writes relevant cloud service cost records to the specified record consumer (e.g TableWriter).
     *
     * @param consumer Consumer that accepts the records to be written.
     * @param expenseData Billing expense data to write.
     * @return Number of accounts for which records were written.
     * @throws InterruptedException when interrupted
     * @throws SQLException when failed to initiate data writing
     */
    @VisibleForTesting
    static int consumeAccountExpenses(ThrowingConsumer<Record, SQLException> consumer,
            @Nonnull final TopDownCostData expenseData) throws SQLException, InterruptedException {
        final AtomicInteger accountCount = new AtomicInteger();

        final Set<Timestamp> usageDates = new HashSet<>();
        final AtomicInteger serviceCount = new AtomicInteger();
        for (AccountExpenses accountExpense : expenseData.getAllAccountExpenses()) {
            if (!accountExpense.hasAccountExpensesInfo() || !accountExpense.hasAssociatedAccountId()
                    || !accountExpense.hasExpensesDate()) {
                continue;
            }
            final long accountId = accountExpense.getAssociatedAccountId();
            final Timestamp snapshotTimestamp = new Timestamp(accountExpense.getExpensesDate());
            accountCount.incrementAndGet();
            final List<ServiceExpenses> services =
                    accountExpense.getAccountExpensesInfo().getServiceExpensesList();

            for (ServiceExpenses service : services) {
                if (service.hasAssociatedServiceId() && service.hasExpenses()) {
                    consumer.accept(accountExpenseRecord(snapshotTimestamp, accountId, service));
                }
            }
            usageDates.add(snapshotTimestamp);
            serviceCount.addAndGet(services.size());
        }
        logger.info("Processed {} cloud service expenses for {} accounts. Dates: {}.",
                serviceCount.get(), accountCount.get(), usageDates);
        return accountCount.get();
    }

    /**
     * Creates a cloud service record to be inserted.
     *
     * @param accountId Business account oid.
     * @param snapshotTime Timestamp of the record. Service usage time.
     * @param cloudServiceExpense Info about service expense costs.
     * @return DB record.
     */
    private static Record accountExpenseRecord(final Timestamp snapshotTime, final long accountId,
            final ServiceExpenses cloudServiceExpense) {
        final Record record = new Record(CloudServiceCost.TABLE);
        record.set(CloudServiceCost.TIME, snapshotTime);
        record.set(CloudServiceCost.ACCOUNT_OID, accountId);
        record.set(CloudServiceCost.CLOUD_SERVICE_OID, cloudServiceExpense.getAssociatedServiceId());
        record.set(CloudServiceCost.COST, cloudServiceExpense.getExpenses().getAmount());
        return record;
    }
}
