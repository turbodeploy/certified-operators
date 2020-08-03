package com.vmturbo.cost.component.expenses;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.time.Clock;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import com.vmturbo.common.protobuf.cost.Cost.AccountExpenses;
import com.vmturbo.common.protobuf.cost.Cost.AccountExpenses.AccountExpensesInfo;
import com.vmturbo.common.protobuf.cost.Cost.AccountExpenses.AccountExpensesInfo.ServiceExpenses;
import com.vmturbo.commons.TimeFrame;
import com.vmturbo.cost.component.db.Cost;
import com.vmturbo.cost.component.util.AccountExpensesFilter;
import com.vmturbo.cost.component.util.AccountExpensesFilter.AccountExpenseFilterBuilder;
import com.vmturbo.platform.sdk.common.CloudCostDTO.CurrencyAmount;
import com.vmturbo.sql.utils.DbCleanupRule;
import com.vmturbo.sql.utils.DbConfigurationRule;
import com.vmturbo.sql.utils.DbException;

public class SqlAccountExpensesStoreTest {
    /**
     * Rule to create the DB schema and migrate it.
     */
    @ClassRule
    public static DbConfigurationRule dbConfig = new DbConfigurationRule(Cost.COST);

    /**
     * Rule to automatically cleanup DB data before each test.
     */
    @Rule
    public DbCleanupRule dbCleanup = dbConfig.cleanupRule();

    private static final long ACCOUNT_ID_1 = 1111L;
    private static final long ACCOUNT_ID_2 = 2222L;

    private static final long CLOUD_SERVICE_ID_1 = 1L;
    private static final long CLOUD_SERVICE_ID_2 = 2L;

    private static final int ENTITY_TYPE_1 = 43;

    private static final double COST_AMOUNT_1 = 10.0;
    private static final double COST_AMOUNT_2 = 20.0;
    private static final double COST_AMOUNT_3 = 30.0;
    private static final double COST_AMOUNT_4 = 40.0;

    private static final long USAGE_DATE_1 = 1576627200000L; // Wed, 18 Dec 2019 GMT
    private static final long USAGE_DATE_2 = 1576713600000L; // Thu, 19 Dec 2019 GMT

    private static final long RT_TOPO_CONTEXT_ID = 777777L;

    final AccountExpenses.AccountExpensesInfo accountExpensesInfo1 = AccountExpensesInfo.newBuilder()
            .addServiceExpenses(ServiceExpenses
                    .newBuilder()
                    .setAssociatedServiceId(CLOUD_SERVICE_ID_1)
                    .setExpenses(CurrencyAmount.newBuilder()
                            .setCurrency(840)
                            .setAmount(COST_AMOUNT_1).build())
                    .build())
            .build();

    final AccountExpenses.AccountExpensesInfo accountExpensesInfo2 = AccountExpensesInfo.newBuilder()
            .addServiceExpenses(ServiceExpenses
                    .newBuilder()
                    .setAssociatedServiceId(CLOUD_SERVICE_ID_2)
                    .setExpenses(CurrencyAmount.newBuilder()
                            .setCurrency(840)
                            .setAmount(COST_AMOUNT_2).build())
                    .build())
            .build();

    final AccountExpenses.AccountExpensesInfo accountExpensesInfo3 = AccountExpensesInfo.newBuilder()
            .addServiceExpenses(ServiceExpenses
                    .newBuilder()
                    .setAssociatedServiceId(CLOUD_SERVICE_ID_1)
                    .setExpenses(CurrencyAmount.newBuilder()
                            .setCurrency(840)
                            .setAmount(COST_AMOUNT_3).build())
                    .build())
            .build();

    final AccountExpenses.AccountExpensesInfo accountExpensesInfo4 = AccountExpensesInfo.newBuilder()
            .addServiceExpenses(ServiceExpenses
                    .newBuilder()
                    .setAssociatedServiceId(CLOUD_SERVICE_ID_2)
                    .setExpenses(CurrencyAmount.newBuilder()
                            .setCurrency(840)
                            .setAmount(COST_AMOUNT_4).build())
                    .build())
            .build();

    private AccountExpensesStore expensesStore = new SqlAccountExpensesStore(dbConfig.getDslContext(), Clock.systemDefaultZone(), 1);

    /**
     * Get the latest account expenses from {@link SqlAccountExpensesStore}.
     * @throws AccountExpenseNotFoundException if the account expense with associated account id
     * doesn't exist
     * @throws DbException if anything goes wrong in the database
     * @throws InterruptedException if thread has been interrupted
     */
    @Test
    public void testGetLatestExpense() throws AccountExpenseNotFoundException, DbException, InterruptedException {
        expensesStore.persistAccountExpenses(ACCOUNT_ID_1, USAGE_DATE_1, accountExpensesInfo1);
        expensesStore.persistAccountExpenses(ACCOUNT_ID_1, USAGE_DATE_2, accountExpensesInfo2);

        Map<Long, Map<Long, AccountExpenses>> accountExpenses1 = expensesStore
                .getAccountExpenses(AccountExpenseFilterBuilder.newBuilder(TimeFrame.LATEST, RT_TOPO_CONTEXT_ID)
                        .latestTimestampRequested(true)
                        .build()
                );
        assertEquals(1, accountExpenses1.size());
        // the expenses map should have one account ID to one set of account expenses (accountExpensesInfo2)
        Map<Long, AccountExpenses> accountExpensesMap = accountExpenses1.values().stream().findAny().get();
        assertEquals(1, accountExpensesMap.size());
        assertThat(accountExpensesMap.values().stream()
                .map(AccountExpenses::getAccountExpensesInfo)
                .collect(Collectors.toList()), containsInAnyOrder(accountExpensesInfo2));

        // DELETE
        expensesStore.deleteAccountExpensesByAssociatedAccountId(ACCOUNT_ID_1);
        List<AccountExpenses> allAccountExpenses = expensesStore.getAllAccountExpenses();
        assertEquals(0, allAccountExpenses.size());
    }

    /**
     * Test getting current account expenses for specific accounts.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testGetCurrentExpenseByAccount() throws Exception {
        expensesStore.persistAccountExpenses(ACCOUNT_ID_1, USAGE_DATE_1, accountExpensesInfo1);
        expensesStore.persistAccountExpenses(ACCOUNT_ID_2, USAGE_DATE_1, accountExpensesInfo2);
        expensesStore.persistAccountExpenses(ACCOUNT_ID_1, USAGE_DATE_2, accountExpensesInfo3);

        Collection<AccountExpenses> ret = expensesStore.getAccountExpenses(
                AccountExpenseFilterBuilder.newBuilder(TimeFrame.LATEST, RT_TOPO_CONTEXT_ID)
                        .accountIds(Collections.singleton(ACCOUNT_ID_1))
                        .latestTimestampRequested(true)
                        .build()).values()
                .stream()
                .map(Map::values)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());

        assertThat(ret.size(), is(1));
        assertThat(ret.stream()
                .map(AccountExpenses::getAssociatedAccountId)
                .collect(Collectors.toList()), containsInAnyOrder(ACCOUNT_ID_1));
        // make sure we got the newest expenses
        assertThat(ret.stream()
                .map(AccountExpenses::getAccountExpensesInfo)
                .collect(Collectors.toList()), containsInAnyOrder(accountExpensesInfo3));

        // DELETE
        expensesStore.deleteAccountExpensesByAssociatedAccountId(ACCOUNT_ID_1);
        expensesStore.deleteAccountExpensesByAssociatedAccountId(ACCOUNT_ID_2);
        List<AccountExpenses> allAccountExpenses = expensesStore.getAllAccountExpenses();
        assertEquals(0, allAccountExpenses.size());
    }

    /**
     * Test getting current account expenses for all accounts.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testGetCurrentExpensesAll() throws Exception {
        // put expenses in DB
        expensesStore.persistAccountExpenses(ACCOUNT_ID_1, USAGE_DATE_1, accountExpensesInfo1);
        expensesStore.persistAccountExpenses(ACCOUNT_ID_2, USAGE_DATE_1, accountExpensesInfo2);
        // put new expenses in DB
        expensesStore.persistAccountExpenses(ACCOUNT_ID_1, USAGE_DATE_2, accountExpensesInfo3);
        expensesStore.persistAccountExpenses(ACCOUNT_ID_2, USAGE_DATE_2, accountExpensesInfo4);

        // get most recent expenses per account
        Collection<AccountExpenses> ret = expensesStore.getAccountExpenses(
                AccountExpenseFilterBuilder.newBuilder(TimeFrame.LATEST, RT_TOPO_CONTEXT_ID)
                        .latestTimestampRequested(true).build())
                .values()
                .stream()
                .map(Map::values)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());

        // we expect to see one set of expenses per account
        assertThat(ret.size(), is(2));
        assertThat(ret.stream()
                .map(AccountExpenses::getAssociatedAccountId)
                .collect(Collectors.toList()), containsInAnyOrder(ACCOUNT_ID_1, ACCOUNT_ID_2));
        // make sure we got the newest expenses
        assertThat(ret.stream()
                .map(AccountExpenses::getAccountExpensesInfo)
                .collect(Collectors.toList()), containsInAnyOrder(accountExpensesInfo3, accountExpensesInfo4));

        // DELETE
        expensesStore.deleteAccountExpensesByAssociatedAccountId(ACCOUNT_ID_1);
        expensesStore.deleteAccountExpensesByAssociatedAccountId(ACCOUNT_ID_2);
        List<AccountExpenses> allAccountExpenses = expensesStore.getAllAccountExpenses();
        assertEquals(0, allAccountExpenses.size());
    }

    @Test
    public void testGetLatestExpenseWithFilters() throws AccountExpenseNotFoundException, DbException, InterruptedException {
        expensesStore.persistAccountExpenses(ACCOUNT_ID_1, USAGE_DATE_1, accountExpensesInfo1);
        expensesStore.persistAccountExpenses(ACCOUNT_ID_1, USAGE_DATE_2, accountExpensesInfo3);

        Map<Long, Map<Long, AccountExpenses>> accountExpenses = expensesStore
                .getAccountExpenses(
                        AccountExpenseFilterBuilder.newBuilder(TimeFrame.LATEST, RT_TOPO_CONTEXT_ID)
                                .entityIds(Collections.singleton(CLOUD_SERVICE_ID_1))
                                .latestTimestampRequested(true).build());

        assertEquals(1, accountExpenses.size());
        assertEquals(ACCOUNT_ID_1, accountExpenses.values().stream()
                .findFirst()
                .get()
                .get(ACCOUNT_ID_1)
                .getAssociatedAccountId());
        assertEquals(accountExpensesInfo3, accountExpenses.values().stream()
                .findFirst()
                .get()
                .get(ACCOUNT_ID_1)
                .getAccountExpensesInfo());

        // get expenses for entityID = 0, entityType = any
        Map<Long, Map<Long, AccountExpenses>> accountExpenses1 = expensesStore
                .getAccountExpenses(
                        AccountExpenseFilterBuilder.newBuilder(TimeFrame.LATEST, RT_TOPO_CONTEXT_ID)
                                .entityIds(Collections.singleton(0L))
                                .latestTimestampRequested(true).build());

        assertEquals(0, accountExpenses1.size());

        Map<Long, Map<Long, AccountExpenses>> accountExpenses2 = expensesStore
                .getAccountExpenses(
                        AccountExpenseFilterBuilder.newBuilder(TimeFrame.LATEST, RT_TOPO_CONTEXT_ID)
                                .entityTypes(Collections.singleton(ENTITY_TYPE_1))
                                .latestTimestampRequested(true).build());
        assertEquals(1, accountExpenses2.size());

        // get expenses for entityID = 1, entityType = 0
        Map<Long, Map<Long, AccountExpenses>> accountExpenses3 = expensesStore
                .getAccountExpenses(
                        AccountExpenseFilterBuilder.newBuilder(TimeFrame.LATEST, RT_TOPO_CONTEXT_ID)
                                .entityIds(Collections.singleton(CLOUD_SERVICE_ID_1))
                                .entityTypes(Collections.singleton(0))
                                .latestTimestampRequested(true).build());
        assertEquals(0, accountExpenses3.size());

        // DELETE
        expensesStore.deleteAccountExpensesByAssociatedAccountId(ACCOUNT_ID_1);
        List<AccountExpenses> allAccountExpenses = expensesStore.getAllAccountExpenses();
        assertEquals(0, allAccountExpenses.size());
    }

    @Test
    public void tesGetAccountExpenseFilter() throws DbException, AccountExpenseNotFoundException, InterruptedException {

        final AccountExpensesFilter entityCostFilter1 = AccountExpenseFilterBuilder
                .newBuilder(TimeFrame.LATEST, RT_TOPO_CONTEXT_ID)
                .duration(USAGE_DATE_1, USAGE_DATE_2)
                .entityIds(Collections.singleton(CLOUD_SERVICE_ID_1))
                .entityTypes(Collections.singleton(ENTITY_TYPE_1))
                .build();

        final AccountExpensesFilter entityCostFilter2 = AccountExpenseFilterBuilder
            .newBuilder(TimeFrame.LATEST, RT_TOPO_CONTEXT_ID)
            .duration(USAGE_DATE_1, USAGE_DATE_2)
            .entityIds(Collections.singleton(CLOUD_SERVICE_ID_1))
            .build();

        final AccountExpensesFilter entityCostFilter3 = AccountExpenseFilterBuilder
            .newBuilder(TimeFrame.LATEST, RT_TOPO_CONTEXT_ID)
            .duration(USAGE_DATE_1, USAGE_DATE_2)
            .entityTypes(Collections.singleton(ENTITY_TYPE_1))
            .build();

        final AccountExpensesFilter entityCostFilter4 = AccountExpenseFilterBuilder
            .newBuilder(TimeFrame.LATEST, RT_TOPO_CONTEXT_ID)
            .duration(USAGE_DATE_1, USAGE_DATE_2)
            .entityIds(Collections.singleton(Long.MAX_VALUE))
            .entityTypes(Collections.singleton(Integer.MAX_VALUE))
            .build();

        final AccountExpensesFilter entityCostFilter5 = AccountExpenseFilterBuilder
            .newBuilder(TimeFrame.LATEST, RT_TOPO_CONTEXT_ID)
            .duration(USAGE_DATE_1, USAGE_DATE_2)
            .build();

        expensesStore.persistAccountExpenses(ACCOUNT_ID_1, USAGE_DATE_1, accountExpensesInfo1);

        // get latest account expenses for entityID = 1, entityType = 43, timeframe = yesterday until tomorrow
        final Map<Long, Map<Long, AccountExpenses>> accountExpenses1 = expensesStore.getAccountExpenses(entityCostFilter1);
        assertEquals(1, accountExpenses1.size());
        assertEquals(ACCOUNT_ID_1, accountExpenses1.values().stream()
                .findFirst()
                .get()
                .get(ACCOUNT_ID_1)
                .getAssociatedAccountId());

        // get latest account expenses for entityID = 1, entityType = any, timeframe = yesterday until tomorrow
        final Map<Long, Map<Long, AccountExpenses>> accountExpenses2 = expensesStore.getAccountExpenses(entityCostFilter2);
        assertEquals(1, accountExpenses2.size());

        // get latest account expenses for entityID = any, entityType = 43, timeframe = yesterday until tomorrow
        final Map<Long, Map<Long, AccountExpenses>> accountExpenses3 = expensesStore.getAccountExpenses(entityCostFilter3);
        assertEquals(1, accountExpenses3.size());

        // get latest account expenses for entityID = maxValue, entityType = maxValue, timeframe = yesterday until tomorrow
        final Map<Long, Map<Long, AccountExpenses>> accountExpenses4 = expensesStore.getAccountExpenses(entityCostFilter4);
        assertEquals(0, accountExpenses4.size());

        // get latest account expenses for entityID = any, entityType = any, timeframe = yesterday until tomorrow
        final Map<Long, Map<Long, AccountExpenses>> accountExpenses5 = expensesStore.getAccountExpenses(entityCostFilter5);
        assertEquals(1, accountExpenses5.size());

        // DELETE
        expensesStore.deleteAccountExpensesByAssociatedAccountId(ACCOUNT_ID_1);
        List<AccountExpenses> allAccountExpenses = expensesStore.getAllAccountExpenses();
        assertEquals(0, allAccountExpenses.size());
    }

    @Test
    public void testSaveMultipleExpenseForTheSameAccount() throws AccountExpenseNotFoundException, InterruptedException, DbException {
        expensesStore.persistAccountExpenses(ACCOUNT_ID_1, USAGE_DATE_1, accountExpensesInfo1);
        expensesStore.persistAccountExpenses(ACCOUNT_ID_1, USAGE_DATE_2, accountExpensesInfo2);

        // get all expenses for an account, not just the most recent
        List<AccountExpenses> allAccountExpenses = expensesStore.getAllAccountExpenses();
        assertEquals(2, allAccountExpenses.size());

        // clean up
        expensesStore.deleteAccountExpensesByAssociatedAccountId(ACCOUNT_ID_1);
        allAccountExpenses = expensesStore.getAllAccountExpenses();
        assertEquals(0, allAccountExpenses.size());
    }

    @Test(expected = AccountExpenseNotFoundException.class)
    public void testDeleteNotExistedExpenses() throws AccountExpenseNotFoundException, DbException {
        expensesStore.deleteAccountExpensesByAssociatedAccountId(Long.MAX_VALUE);
    }

    /**
     * Test that duplicate records not being stored in account_expense DB. Instead the record being
     * updated with the amount and currency values coming from the most recent record.
     * @throws AccountExpenseNotFoundException if the account expense with associated account id
     * doesn't exist
     * @throws DbException if anything goes wrong in the database
     * @throws InterruptedException if thread has been interrupted
     */
    @Test
    public void testPersistAccountExpensesDuplicateRecordsUpsert() throws InterruptedException,
            DbException, AccountExpenseNotFoundException {

        // accountExpensesInfo1 and accountExpensesInfo3 are related to the same cloud service, have
        // the same expense date, but their cost amounts are different.
        assertNotEquals(COST_AMOUNT_1, COST_AMOUNT_3);
        expensesStore.persistAccountExpenses(ACCOUNT_ID_1, USAGE_DATE_1, accountExpensesInfo1);
        // accountExpensesInfo3 is persisted after accountExpensesInfo1
        expensesStore.persistAccountExpenses(ACCOUNT_ID_1, USAGE_DATE_1, accountExpensesInfo3);

        List<AccountExpenses> allAccountExpenses = expensesStore.getAllAccountExpenses();
        assertEquals(1, allAccountExpenses.size());

        // The expected values are the values of the the record that was persisted later, that is
        // the values of accountExpensesInfo3
        CurrencyAmount cloudServiceExpensesInStore = allAccountExpenses.get(0)
                .getAccountExpensesInfo().getServiceExpenses(0).getExpenses();
        CurrencyAmount expectedCloudServiceExpenses = accountExpensesInfo3
                .getServiceExpenses(0).getExpenses();

        assertEquals(0, Double.compare(expectedCloudServiceExpenses.getAmount(),
                cloudServiceExpensesInStore.getAmount()));
        assertEquals(0, Double.compare(expectedCloudServiceExpenses.getCurrency(),
                cloudServiceExpensesInStore.getCurrency()));

        // cleanup
        expensesStore.deleteAccountExpensesByAssociatedAccountId(ACCOUNT_ID_1);
    }
}
