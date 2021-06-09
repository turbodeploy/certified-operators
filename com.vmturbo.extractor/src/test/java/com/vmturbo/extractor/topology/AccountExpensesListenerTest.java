package com.vmturbo.extractor.topology;

import static org.junit.Assert.assertEquals;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;

import org.junit.Test;

import com.vmturbo.common.protobuf.cost.Cost.AccountExpenses;
import com.vmturbo.common.protobuf.cost.Cost.AccountExpenses.AccountExpensesInfo;
import com.vmturbo.common.protobuf.cost.Cost.AccountExpenses.AccountExpensesInfo.ServiceExpenses;
import com.vmturbo.extractor.models.ModelDefinitions.CloudServiceCost;
import com.vmturbo.extractor.models.Table.Record;
import com.vmturbo.extractor.topology.fetcher.TopDownCostFetcherFactory.TopDownCostData;
import com.vmturbo.platform.sdk.common.CommonCost.CurrencyAmount;

/**
 * Verifies that AccountExpensesListener is correctly persisting the billing expense costs.
 */
public class AccountExpensesListenerTest {

    /**
     * Checks if the TopDownCostData gets correctly converted into billing expense DB records.
     */
    @Test
    public void verifyBillingExpenseConversion() {
        final LocalDateTime today = LocalDate.now().atStartOfDay();
        final LocalDateTime yday = today.minusDays(1);
        final long todayMillis = today.toInstant(ZoneOffset.UTC).toEpochMilli();
        final long ydayMillis = yday.toInstant(ZoneOffset.UTC).toEpochMilli();
        List<String> expectedServices = new ArrayList<>();
        long accountOid1 = 101L;
        final List<ServiceExpenseData> services1 = ImmutableList.of(
                new ServiceExpenseData(1001L, 100.0d), new ServiceExpenseData(1002L, 10.0d)
        );

        long accountOid2 = 201L;
        final List<ServiceExpenseData> services2 = ImmutableList.of(
                new ServiceExpenseData(2001L, 200.0d), new ServiceExpenseData(2002L, 20.0d)
        );

        final TopDownCostData expenseData = new TopDownCostData();
        addServiceData(expenseData, todayMillis, accountOid1, services1, expectedServices);
        addServiceData(expenseData, ydayMillis, accountOid2, services2, expectedServices);

        expectedServices = expectedServices.stream().sorted().collect(Collectors.toList());
        final CloudServiceSink consumer = new CloudServiceSink();
        int numAccounts = AccountExpensesListener.consumeAccountExpenses(consumer, expenseData);
        assertEquals(2, numAccounts);
        final List<Record> serviceRecords = consumer.getAllRecords();
        assertEquals(4, serviceRecords.size());

        List<String> actualServices = serviceRecords.stream().map(record ->
                getFormattedService(record.get(CloudServiceCost.TIME).getTime(),
                record.get(CloudServiceCost.ACCOUNT_OID),
                record.get(CloudServiceCost.CLOUD_SERVICE_OID),
                record.get(CloudServiceCost.COST)))
                .sorted()
                .collect(Collectors.toList());
        assertEquals(expectedServices, actualServices);
    }

    /**
     * String formatting for ease of comparison.
     *
     * @param timestamp Timestamp of record.
     * @param accountOid Business account oid.
     * @param serviceOid Cloud service oid.
     * @param serviceCost Cloud service cost.
     * @return String formatted inputs.
     */
    private static String getFormattedService(long timestamp, long accountOid, long serviceOid,
            double serviceCost) {
        return String.format("%s-%s-%s-%s", timestamp, accountOid, serviceOid, serviceCost);
    }

    /**
     * Util method to set cloud service data in TopDownCostData instance.
     *
     * @param expenseData Input where service settings need to be set.
     * @param timestamp Timestamp of record.
     * @param accountOid Business account oid.
     * @param services Cloud services with oid and cost.
     * @param expectedServices Expected service list for verification.
     */
    private void addServiceData(final TopDownCostData expenseData, long timestamp,
            long accountOid, final List<ServiceExpenseData> services,
            final List<String> expectedServices) {
        final AccountExpensesInfo.Builder builder = AccountExpensesInfo.newBuilder();
        services.forEach(svc -> {
            builder.addServiceExpenses(ServiceExpenses.newBuilder()
                    .setAssociatedServiceId(svc.serviceOid)
                    .setExpenses(CurrencyAmount.newBuilder()
                            .setAmount(svc.serviceCost)
                            .build())
                    .build());
            expectedServices.add(getFormattedService(timestamp, accountOid, svc.serviceOid,
                    svc.serviceCost));
        });
        expenseData.addAccountExpense(AccountExpenses.newBuilder()
                .setExpensesDate(timestamp)
                .setAssociatedAccountId(accountOid)
                .setAccountExpensesInfo(builder)
                .build());
    }

    /**
     * Inner convenience data class to store Cloud Service info.
     */
    private static class ServiceExpenseData {
        /**
         * Cloud Service oid.
         */
        long serviceOid;

        /**
         * Cloud service cost.
         */
        double serviceCost;

        /**
         * Service data instance.
         *
         * @param serviceOid Cloud Service oid.
         * @param serviceCost Cloud service cost.
         */
        ServiceExpenseData(long serviceOid, double serviceCost) {
            this.serviceOid = serviceOid;
            this.serviceCost = serviceCost;
        }
    }

    /**
     * Dummy output of writer for service data records, used for verification.
     */
    private static class CloudServiceSink implements Consumer<Record> {
        /**
         * All records that were written.
         */
        private final List<Record> allRecords;

        /**
         * Create new instance.
         */
        CloudServiceSink() {
            allRecords = new ArrayList<>();
        }

        /**
         * Called to add a new record.
         *
         * @param record DB record to be inserted.
         */
        @Override
        public void accept(Record record) {
            allRecords.add(record);
        }

        /**
         * Gets all records.
         *
         * @return All DB records that were written.
         */
        public List<Record> getAllRecords() {
            return allRecords;
        }
    }
}
