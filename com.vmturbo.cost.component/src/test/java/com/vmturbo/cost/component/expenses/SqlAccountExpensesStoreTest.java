package com.vmturbo.cost.component.expenses;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableSet;

import org.flywaydb.core.Flyway;
import org.jooq.DSLContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.vmturbo.common.protobuf.cost.Cost;
import com.vmturbo.common.protobuf.cost.Cost.AccountExpenses;
import com.vmturbo.common.protobuf.cost.Cost.AccountExpenses.AccountExpensesInfo;
import com.vmturbo.common.protobuf.cost.Cost.AccountExpenses.AccountExpensesInfo.ServiceExpenses;
import com.vmturbo.common.protobuf.cost.Cost.GetCurrentAccountExpensesRequest.AccountExpenseQueryScope;
import com.vmturbo.common.protobuf.cost.Cost.GetCurrentAccountExpensesRequest.AccountExpenseQueryScope.IdList;
import com.vmturbo.components.api.test.MutableFixedClock;
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

    public static final long ASSOCIATED_ACCOUNT_ID = 1111l;
    public static final double ACCOUNT_EXPENSES_PERCENTAGE2 = 20.0;
    public static final double ACCOUNT_EXPENSES_PERCENTAGE1 = 10.0;
    final AccountExpenses.AccountExpensesInfo accountExpensesInfo = AccountExpensesInfo.newBuilder()
            .addServiceExpenses(ServiceExpenses
                    .newBuilder()
                    .setAssociatedServiceId(2l)
                    .setExpenses(CurrencyAmount.newBuilder()
                        .setCurrency(840)
                        .setAmount(ACCOUNT_EXPENSES_PERCENTAGE1).build())
                    .build())
            .build();
    final AccountExpenses.AccountExpensesInfo accountExpensesInfo1 = AccountExpensesInfo.newBuilder()
            .addServiceExpenses(ServiceExpenses
                    .newBuilder()
                    .setAssociatedServiceId(1l)
                    .setExpenses(CurrencyAmount.newBuilder()
                        .setCurrency(840)
                        .setAmount(ACCOUNT_EXPENSES_PERCENTAGE2).build())
                    .build())
            .build();

    /**
     * The clock can't start at too small of a number because TIMESTAMP starts in 1970, but
     * epoch millis starts in 1969.
     */
    private MutableFixedClock clock = new MutableFixedClock(Instant.ofEpochMilli(1_000_000_000), ZoneId.systemDefault());

    @Autowired
    protected TestSQLDatabaseConfig dbConfig;
    private Flyway flyway;
    private AccountExpensesStore expensesStore;
    private DSLContext dsl;

    @Before
    public void setup() throws Exception {
        flyway = dbConfig.flyway();
        dsl = dbConfig.dsl();
        flyway.clean();
        flyway.migrate();
        expensesStore = new SqlAccountExpensesStore(dsl, clock, 1);
    }

    @After
    public void teardown() {
        flyway.clean();
    }


    @Test
    public void testGetLatestExpense() throws AccountExpenseNotFoundException, DbException {

        // INSERT
        expensesStore.persistAccountExpenses(ASSOCIATED_ACCOUNT_ID, accountExpensesInfo1);

        clock.changeInstant(clock.instant().plusMillis(1000));
        expensesStore.persistAccountExpenses(ASSOCIATED_ACCOUNT_ID, accountExpensesInfo);
        Map<Long, Map<Long, AccountExpenses>> accountExpenses1 = expensesStore.getLatestExpenses(Collections.emptySet(), Collections.emptySet());
        assertEquals(1, accountExpenses1.size());
        assertEquals(ASSOCIATED_ACCOUNT_ID, accountExpenses1.values().stream()
                .findFirst()
                .get()
                .get(ASSOCIATED_ACCOUNT_ID)
                .getAssociatedAccountId());

        // DELETE
        expensesStore
                .deleteAccountExpensesByAssociatedAccountId(ASSOCIATED_ACCOUNT_ID);
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
        expensesStore.persistAccountExpenses(ASSOCIATED_ACCOUNT_ID, accountExpensesInfo);
        expensesStore.persistAccountExpenses(ASSOCIATED_ACCOUNT_ID + 1, accountExpensesInfo1);

        final long timestamp = clock.millis();

        clock.changeInstant(clock.instant().plusMillis(1000));

        Collection<AccountExpenses> ret = expensesStore.getCurrentAccountExpenses(
            AccountExpenseQueryScope.newBuilder()
                .setSpecificAccounts(IdList.newBuilder()
                    .addAccountIds(ASSOCIATED_ACCOUNT_ID))
                .build());
        assertThat(ret.size(), is(1));
        assertThat(ret.iterator().next(), is(AccountExpenses.newBuilder()
            .setAssociatedAccountId(ASSOCIATED_ACCOUNT_ID)
            .setAccountExpensesInfo(accountExpensesInfo)
            .setExpenseReceivedTimestamp(timestamp)
            .build()));
    }

    /**
     * Test getting current account expenses for all accounts.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testGetCurrentExpensesAll() throws Exception {
        expensesStore.persistAccountExpenses(ASSOCIATED_ACCOUNT_ID, accountExpensesInfo);
        expensesStore.persistAccountExpenses(ASSOCIATED_ACCOUNT_ID + 1, accountExpensesInfo1);

        final long timestamp = clock.millis();

        clock.changeInstant(clock.instant().plusMillis(1000));

        Collection<AccountExpenses> ret = expensesStore.getCurrentAccountExpenses(
            AccountExpenseQueryScope.newBuilder()
                .setAll(true)
                .build());
        assertThat(ret.size(), is(2));
        assertThat(ret.stream()
            .map(AccountExpenses::getAccountExpensesInfo)
            .collect(Collectors.toList()), containsInAnyOrder(accountExpensesInfo, accountExpensesInfo1));
        assertThat(ret, containsInAnyOrder(
            AccountExpenses.newBuilder()
                .setAssociatedAccountId(ASSOCIATED_ACCOUNT_ID)
                .setAccountExpensesInfo(accountExpensesInfo)
                .setExpenseReceivedTimestamp(timestamp)
                .build(),
            AccountExpenses.newBuilder()
                .setAssociatedAccountId(ASSOCIATED_ACCOUNT_ID + 1)
                .setAccountExpensesInfo(accountExpensesInfo1)
                .setExpenseReceivedTimestamp(timestamp)
                .build()));

        assertThat(ret.iterator().next().getAccountExpensesInfo(), is(accountExpensesInfo));
    }

    @Test
    public void testGetLatestExpenseWithFilters() throws AccountExpenseNotFoundException, DbException {

        // INSERT
        expensesStore.persistAccountExpenses(ASSOCIATED_ACCOUNT_ID, accountExpensesInfo1);

        clock.changeInstant(clock.instant().plusMillis(1000));
        expensesStore.persistAccountExpenses(ASSOCIATED_ACCOUNT_ID, accountExpensesInfo);
        Map<Long, Map<Long, AccountExpenses>> accountExpenses = expensesStore
                .getLatestExpenses(ImmutableSet.of(2l), Collections.emptySet());
        assertEquals(1, accountExpenses.size());
        assertEquals(ASSOCIATED_ACCOUNT_ID, accountExpenses.values().stream()
                .findFirst()
                .get()
                .get(ASSOCIATED_ACCOUNT_ID)
                .getAssociatedAccountId());

        Map<Long, Map<Long, AccountExpenses>> accountExpenses1 = expensesStore
                .getLatestExpenses(ImmutableSet.of(0l), Collections.emptySet());
        assertEquals(0, accountExpenses1.size());

        Map<Long, Map<Long, AccountExpenses>> accountExpenses2 = expensesStore
                .getLatestExpenses(ImmutableSet.of(1l), ImmutableSet.of(43));
        assertEquals(1, accountExpenses2.size());

        Map<Long, Map<Long, AccountExpenses>> accountExpenses3 = expensesStore
                .getLatestExpenses(ImmutableSet.of(1l), ImmutableSet.of(0));
        assertEquals(0, accountExpenses3.size());


        // DELETE
        expensesStore
                .deleteAccountExpensesByAssociatedAccountId(ASSOCIATED_ACCOUNT_ID);
        List<AccountExpenses> allAccountExpenses = expensesStore.getAllAccountExpenses();
        assertEquals(0, allAccountExpenses.size());
    }

    @Test
    public void tesGetAccountExpenseFilter() throws DbException, AccountExpenseNotFoundException {
        // get by date
        final LocalDateTime now = LocalDateTime.now(clock);
        final AccountExpensesFilter entityCostFilter = new AccountExpensesFilter(
                ImmutableSet.of(2l),
                ImmutableSet.of(43),
                now.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli(),
                now.plusDays(1l).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli(),
                TimeFrame.LATEST);

        final AccountExpensesFilter entityCostFilter1 = new AccountExpensesFilter(
                ImmutableSet.of(2l), //Not in the table
                Collections.EMPTY_SET,
                now.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli(),
                now.plusDays(1l).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli(),
                TimeFrame.LATEST);

        final AccountExpensesFilter entityCostFilter2 = new AccountExpensesFilter(
                Collections.EMPTY_SET, //any
                ImmutableSet.of(43),
                now.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli(),
                now.plusDays(1l).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli(),
                TimeFrame.LATEST);

        final AccountExpensesFilter entityCostFilter3 = new AccountExpensesFilter(
                ImmutableSet.of(Long.MAX_VALUE),
                ImmutableSet.of(Integer.MAX_VALUE),
                now.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli(),
                now.plusDays(1l).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli(),
                TimeFrame.LATEST);

        // INSERT
        saveExpense();

        final Map<Long, Map<Long, AccountExpenses>> accountExpenses1 = expensesStore.getAccountExpenses(entityCostFilter);
        assertEquals(1, accountExpenses1.size());
        assertEquals(ASSOCIATED_ACCOUNT_ID, accountExpenses1.values().stream()
                .findFirst()
                .get()
                .get(ASSOCIATED_ACCOUNT_ID)
                .getAssociatedAccountId());

        final Map<Long, Map<Long, AccountExpenses>> accountExpenses2 = expensesStore.getAccountExpenses(entityCostFilter1);
        assertEquals(1, accountExpenses2.size());

        final Map<Long, Map<Long, AccountExpenses>> accountExpenses3 = expensesStore.getAccountExpenses(entityCostFilter2);
        assertEquals(1, accountExpenses3.size());

        final Map<Long, Map<Long, AccountExpenses>> accountExpenses4 = expensesStore.getAccountExpenses(entityCostFilter3);
        assertEquals(0, accountExpenses4.size());

        // DELETE
        expensesStore
                .deleteAccountExpensesByAssociatedAccountId(ASSOCIATED_ACCOUNT_ID);
        List<AccountExpenses> allAccountExpenses = expensesStore.getAllAccountExpenses();
        assertEquals(0, allAccountExpenses.size());
    }

    @Test
    public void testGetCostWithAccountExpenseFilter() throws DbException, AccountExpenseNotFoundException {
        // get by date
        final LocalDateTime now = LocalDateTime.now(clock);
        final AccountExpensesFilter entityCostFilter = new AccountExpensesFilter(
                Collections.EMPTY_SET,
                Collections.EMPTY_SET,
                now.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli(),
                now.plusDays(1l).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli(),
                TimeFrame.LATEST);

        // INSERT
        saveExpense();

        Map<Long, Map<Long, AccountExpenses>> accountExpenses1 = expensesStore.getAccountExpenses(entityCostFilter);
        assertEquals(1, accountExpenses1.size());
        assertEquals(ASSOCIATED_ACCOUNT_ID, accountExpenses1.values().stream()
                .findFirst()
                .get()
                .get(ASSOCIATED_ACCOUNT_ID)
                .getAssociatedAccountId());

        // DELETE
        expensesStore
                .deleteAccountExpensesByAssociatedAccountId(ASSOCIATED_ACCOUNT_ID);
        List<AccountExpenses> allAccountExpenses = expensesStore.getAllAccountExpenses();
        assertEquals(0, allAccountExpenses.size());
    }

    private void saveExpense() throws DbException {
        expensesStore.persistAccountExpenses(ASSOCIATED_ACCOUNT_ID, accountExpensesInfo);
    }

    @Test
    public void testSaveMultipleExpenseForTheSameAccount() throws AccountExpenseNotFoundException, InterruptedException, DbException {
        expensesStore.persistAccountExpenses(ASSOCIATED_ACCOUNT_ID, accountExpensesInfo);
        //assertEquals(ASSOCIATED_ACCOUNT_ID, accountExpenses1.getAssociatedAccountId());
        // sleep for 10 millisecond, so the timestamp will be different
        clock.changeInstant(clock.instant().plusMillis(1000));
        expensesStore.persistAccountExpenses(ASSOCIATED_ACCOUNT_ID, accountExpensesInfo);
        //assertEquals(ASSOCIATED_ACCOUNT_ID, accountExpenses2.getAssociatedAccountId());

        List<AccountExpenses> allAccountExpenses = expensesStore.getAllAccountExpenses();
        assertEquals(2, allAccountExpenses.size());

        // clean up
        expensesStore.deleteAccountExpensesByAssociatedAccountId(ASSOCIATED_ACCOUNT_ID);
        allAccountExpenses = expensesStore.getAllAccountExpenses();
        assertEquals(0, allAccountExpenses.size());
    }

    @Test(expected = AccountExpenseNotFoundException.class)
    public void testDeleteNotExistedExpenses() throws AccountExpenseNotFoundException, DbException {
        expensesStore.deleteAccountExpensesByAssociatedAccountId(Long.MAX_VALUE);
    }


}
