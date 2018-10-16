package com.vmturbo.cost.component.expenses;

import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;

import org.flywaydb.core.Flyway;
import org.jooq.DSLContext;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.vmturbo.common.protobuf.cost.Cost.AccountExpenses;
import com.vmturbo.common.protobuf.cost.Cost.AccountExpenses.AccountExpensesInfo;
import com.vmturbo.common.protobuf.cost.Cost.AccountExpenses.AccountExpensesInfo.ServiceExpenses;
import com.vmturbo.components.api.test.MutableFixedClock;
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
                    .setExpenses(CurrencyAmount.newBuilder().setAmount(ACCOUNT_EXPENSES_PERCENTAGE1).build())
                    .build())
            .build();
    final AccountExpenses.AccountExpensesInfo accountExpensesInfo1 = AccountExpensesInfo.newBuilder()
            .addServiceExpenses(ServiceExpenses
                    .newBuilder()
                    .setExpenses(CurrencyAmount.newBuilder().setAmount(ACCOUNT_EXPENSES_PERCENTAGE2).build())
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

    @Test
    public void testCRD() throws AccountExpenseNotFoundException, DbException {

        // INSERT
        saveExpense();

        LocalDateTime now = LocalDateTime.now(clock);
        // READ by associated account id
        List<AccountExpenses> accountExpenses2 = expensesStore
                .getAccountExpensesByAssociatedAccountId(ASSOCIATED_ACCOUNT_ID);
        assertEquals(ASSOCIATED_ACCOUNT_ID, accountExpenses2.get(0).getAssociatedAccountId());

        accountExpenses2 = expensesStore
                .getAccountExpensesByAssociatedAccountId(accountExpenses2.get(0).getAssociatedAccountId());
        assertEquals(ACCOUNT_EXPENSES_PERCENTAGE1, accountExpenses2.get(0)
                .getAccountExpensesInfo()
                .getServiceExpensesList()
                .get(0).getExpenses().getAmount(), 0.001);


        // DELETE
        expensesStore
                .deleteAccountExpensesByAssociatedAccountId(ASSOCIATED_ACCOUNT_ID);
        List<AccountExpenses> allAccountExpenses = expensesStore.getAllAccountExpenses();
        assertEquals(0, allAccountExpenses.size());
    }


    @Test
    public void testGetLatestExpense() throws AccountExpenseNotFoundException, DbException {

        // INSERT
        expensesStore.persistAccountExpenses(ASSOCIATED_ACCOUNT_ID, accountExpensesInfo1);

        clock.changeInstant(clock.instant().plusMillis(1000));
        expensesStore.persistAccountExpenses(ASSOCIATED_ACCOUNT_ID, accountExpensesInfo);
        Map<Long, Map<Long, AccountExpenses>> accountExpenses1 = expensesStore.getLatestExpenses();
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

    @Test
    public void testGetExpensesWithTwoTimeStamps() throws DbException, AccountExpenseNotFoundException {

        // INSERT
        saveExpense();

        LocalDateTime now = LocalDateTime.now(clock);
        // READ by timestamps
        Map<Long, Map<Long, AccountExpenses>> accountExpenses1 = expensesStore
                .getAccountExpenses(now.minusMinutes(1), now);
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
