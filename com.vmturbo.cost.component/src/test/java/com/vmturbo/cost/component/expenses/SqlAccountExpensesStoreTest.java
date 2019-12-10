package com.vmturbo.cost.component.expenses;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static com.vmturbo.cost.component.db.tables.AccountExpenses.ACCOUNT_EXPENSES;

import java.time.Clock;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.flywaydb.core.Flyway;
import org.jooq.Condition;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.google.common.collect.ImmutableSet;

import com.vmturbo.common.protobuf.cost.Cost.AccountExpenses;
import com.vmturbo.common.protobuf.cost.Cost.AccountExpenses.AccountExpensesInfo;
import com.vmturbo.common.protobuf.cost.Cost.AccountExpenses.AccountExpensesInfo.ServiceExpenses;
import com.vmturbo.common.protobuf.cost.Cost.GetCurrentAccountExpensesRequest.AccountExpenseQueryScope;
import com.vmturbo.common.protobuf.cost.Cost.GetCurrentAccountExpensesRequest.AccountExpenseQueryScope.IdList;
import com.vmturbo.components.common.utils.TimeFrameCalculator.TimeFrame;
import com.vmturbo.cost.component.util.AccountExpensesFilter;
import com.vmturbo.platform.sdk.common.CloudCostDTO.CurrencyAmount;
import com.vmturbo.sql.utils.DbException;
import com.vmturbo.sql.utils.TestSQLDatabaseConfig;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(
        classes = {TestSQLDatabaseConfig.class}
)
@TestPropertySource(properties = {"originalSchemaName=cost"})
public class SqlAccountExpensesStoreTest {

    public static final long ACCOUNT_ID_1 = 1111L;
    public static final long ACCOUNT_ID_2 = 2222L;

    public static final long CLOUD_SERVICE_ID_1 = 1L;
    public static final long CLOUD_SERVICE_ID_2 = 2L;

    public static final int ENTITY_TYPE_1 = 43;

    public static final double COST_AMOUNT_1 = 10.0;
    public static final double COST_AMOUNT_2 = 20.0;
    public static final double COST_AMOUNT_3 = 30.0;
    public static final double COST_AMOUNT_4 = 40.0;

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

    @Autowired
    protected TestSQLDatabaseConfig dbConfig;
    private Flyway flyway;
    private AccountExpensesStore expensesStore;
    private Clock clock = Clock.systemDefaultZone();

    @Before
    public void setup() throws Exception {
        flyway = dbConfig.flyway();
        flyway.clean();
        flyway.migrate();
        expensesStore = new SqlAccountExpensesStore(dbConfig.dsl(), clock, 1);
    }

    @After
    public void teardown() {
        flyway.clean();
    }


    @Test
    public void testGetLatestExpense() throws AccountExpenseNotFoundException, DbException, InterruptedException {
        expensesStore.persistAccountExpenses(ACCOUNT_ID_1, accountExpensesInfo1);
        TimeUnit.SECONDS.sleep(2);
        expensesStore.persistAccountExpenses(ACCOUNT_ID_1, accountExpensesInfo2);

        Map<Long, Map<Long, AccountExpenses>> accountExpenses1 = expensesStore
                .getLatestAccountExpensesWithConditions(Collections.emptyList());
        assertEquals(1, accountExpenses1.size());
        assertEquals(1, accountExpenses1.values().size());
        // the expenses map should have one account ID to one set of account expenses (accountExpensesInfo2)
        assertEquals(ACCOUNT_ID_1, accountExpenses1.values().stream()
                .findFirst()
                .get()
                .get(ACCOUNT_ID_1)
                .getAssociatedAccountId());
        assertEquals(1, accountExpenses1.values().stream().findAny().get().values().size());
        assertThat(accountExpenses1.values().stream().findAny().get().values().stream()
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
        expensesStore.persistAccountExpenses(ACCOUNT_ID_1, accountExpensesInfo1);
        expensesStore.persistAccountExpenses(ACCOUNT_ID_2, accountExpensesInfo2);
        TimeUnit.SECONDS.sleep(2);
        expensesStore.persistAccountExpenses(ACCOUNT_ID_1, accountExpensesInfo3);

        Collection<AccountExpenses> ret = expensesStore.getCurrentAccountExpenses(
            AccountExpenseQueryScope.newBuilder()
                .setSpecificAccounts(IdList.newBuilder()
                    .addAccountIds(ACCOUNT_ID_1))
                .build());

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
        expensesStore.persistAccountExpenses(ACCOUNT_ID_1, accountExpensesInfo1);
        expensesStore.persistAccountExpenses(ACCOUNT_ID_2, accountExpensesInfo2);
        TimeUnit.SECONDS.sleep(2);
        // put new expenses in DB
        expensesStore.persistAccountExpenses(ACCOUNT_ID_1, accountExpensesInfo3);
        TimeUnit.SECONDS.sleep(2);
        expensesStore.persistAccountExpenses(ACCOUNT_ID_2, accountExpensesInfo4);

        // get most recent expenses per account
        Collection<AccountExpenses> ret = expensesStore.getCurrentAccountExpenses(
            AccountExpenseQueryScope.newBuilder()
                .setAll(true)
                .build());

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
        expensesStore.persistAccountExpenses(ACCOUNT_ID_1, accountExpensesInfo1);
        TimeUnit.SECONDS.sleep(2);
        expensesStore.persistAccountExpenses(ACCOUNT_ID_1, accountExpensesInfo3);

        // get expenses for entityID = 1, entityType = any
        final List<Condition> conditions = new ArrayList<>();
        conditions.add(ACCOUNT_EXPENSES.field(ACCOUNT_EXPENSES.ASSOCIATED_ENTITY_ID.getName())
                .in(ImmutableSet.of(CLOUD_SERVICE_ID_1)));
        Map<Long, Map<Long, AccountExpenses>> accountExpenses = expensesStore
                .getLatestAccountExpensesWithConditions(conditions);
        conditions.clear();
        assertEquals(1, accountExpenses.size());
        assertEquals(ACCOUNT_ID_1, accountExpenses.values().stream()
                .findFirst()
                .get()
                .get(ACCOUNT_ID_1)
                .getAssociatedAccountId());

        // get expenses for entityID = 0, entityType = any
        conditions.add(ACCOUNT_EXPENSES.field(ACCOUNT_EXPENSES.ASSOCIATED_ENTITY_ID.getName())
                .in(ImmutableSet.of(0L)));
        Map<Long, Map<Long, AccountExpenses>> accountExpenses1 = expensesStore
                .getLatestAccountExpensesWithConditions(conditions);
        conditions.clear();
        assertEquals(0, accountExpenses1.size());

        // get expenses for entityID = any, entityType = 43
        conditions.add(ACCOUNT_EXPENSES.field(ACCOUNT_EXPENSES.ENTITY_TYPE.getName())
                .in(ImmutableSet.of(ENTITY_TYPE_1)));
        Map<Long, Map<Long, AccountExpenses>> accountExpenses2 = expensesStore
                .getLatestAccountExpensesWithConditions(conditions);
        conditions.clear();
        assertEquals(1, accountExpenses2.size());

        // get expenses for entityID = 1, entityType = 0
        conditions.add(ACCOUNT_EXPENSES.field(ACCOUNT_EXPENSES.ASSOCIATED_ENTITY_ID.getName())
                .in(ImmutableSet.of(CLOUD_SERVICE_ID_1)));
        conditions.add(ACCOUNT_EXPENSES.field(ACCOUNT_EXPENSES.ENTITY_TYPE.getName())
                .in(ImmutableSet.of(0L)));
        Map<Long, Map<Long, AccountExpenses>> accountExpenses3 = expensesStore
                .getLatestAccountExpensesWithConditions(conditions);
        conditions.clear();
        assertEquals(0, accountExpenses3.size());

        // DELETE
        expensesStore.deleteAccountExpensesByAssociatedAccountId(ACCOUNT_ID_1);
        List<AccountExpenses> allAccountExpenses = expensesStore.getAllAccountExpenses();
        assertEquals(0, allAccountExpenses.size());
    }

    @Test
    public void tesGetAccountExpenseFilter() throws DbException, AccountExpenseNotFoundException, InterruptedException {
        // get by date
        final LocalDateTime now = LocalDateTime.now(clock);
        long startTime = now.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
        long endTime = now.plusDays(1).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();

        final AccountExpensesFilter entityCostFilter1 = new AccountExpensesFilter(
                ImmutableSet.of(CLOUD_SERVICE_ID_1),
                ImmutableSet.of(ENTITY_TYPE_1),
                startTime,
                endTime,
                TimeFrame.LATEST);

        final AccountExpensesFilter entityCostFilter2 = new AccountExpensesFilter(
                ImmutableSet.of(CLOUD_SERVICE_ID_1),
                Collections.EMPTY_SET, // any entity type
                startTime,
                endTime,
                TimeFrame.LATEST);

        final AccountExpensesFilter entityCostFilter3 = new AccountExpensesFilter(
                Collections.EMPTY_SET, // any entity
                ImmutableSet.of(ENTITY_TYPE_1),
                startTime,
                endTime,
                TimeFrame.LATEST);

        final AccountExpensesFilter entityCostFilter4 = new AccountExpensesFilter(
                ImmutableSet.of(Long.MAX_VALUE), // entity IDs
                ImmutableSet.of(Integer.MAX_VALUE), // entity types
                startTime,
                endTime,
                TimeFrame.LATEST);

        final AccountExpensesFilter entityCostFilter5 = new AccountExpensesFilter(
                Collections.EMPTY_SET, // any entity
                Collections.EMPTY_SET, // any entity type
                startTime,
                endTime,
                TimeFrame.LATEST);

        TimeUnit.SECONDS.sleep(1);
        expensesStore.persistAccountExpenses(ACCOUNT_ID_1, accountExpensesInfo1);

        // get latest account expenses for entityID = 1, entityType = 43, timeframe = now until tomorrow
        final Map<Long, Map<Long, AccountExpenses>> accountExpenses1 = expensesStore.getAccountExpenses(entityCostFilter1);
        assertEquals(1, accountExpenses1.size());
        assertEquals(ACCOUNT_ID_1, accountExpenses1.values().stream()
                .findFirst()
                .get()
                .get(ACCOUNT_ID_1)
                .getAssociatedAccountId());

        // get latest account expenses for entityID = 1, entityType = any, timeframe = now until tomorrow
        final Map<Long, Map<Long, AccountExpenses>> accountExpenses2 = expensesStore.getAccountExpenses(entityCostFilter2);
        assertEquals(1, accountExpenses2.size());

        // get latest account expenses for entityID = any, entityType = 43, timeframe = now until tomorrow
        final Map<Long, Map<Long, AccountExpenses>> accountExpenses3 = expensesStore.getAccountExpenses(entityCostFilter3);
        assertEquals(1, accountExpenses3.size());

        // get latest account expenses for entityID = maxValue, entityType = maxValue, timeframe = now until tomorrow
        final Map<Long, Map<Long, AccountExpenses>> accountExpenses4 = expensesStore.getAccountExpenses(entityCostFilter4);
        assertEquals(0, accountExpenses4.size());

        // get latest account expenses for entityID = any, entityType = any, timeframe = now until tomorrow
        final Map<Long, Map<Long, AccountExpenses>> accountExpenses5 = expensesStore.getAccountExpenses(entityCostFilter5);
        assertEquals(1, accountExpenses5.size());

        // DELETE
        expensesStore.deleteAccountExpensesByAssociatedAccountId(ACCOUNT_ID_1);
        List<AccountExpenses> allAccountExpenses = expensesStore.getAllAccountExpenses();
        assertEquals(0, allAccountExpenses.size());
    }

    @Test
    public void testSaveMultipleExpenseForTheSameAccount() throws AccountExpenseNotFoundException, InterruptedException, DbException {
        expensesStore.persistAccountExpenses(ACCOUNT_ID_1, accountExpensesInfo1);
        TimeUnit.SECONDS.sleep(2);
        expensesStore.persistAccountExpenses(ACCOUNT_ID_1, accountExpensesInfo2);

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


}
