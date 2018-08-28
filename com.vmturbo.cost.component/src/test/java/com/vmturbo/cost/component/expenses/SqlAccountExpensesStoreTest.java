package com.vmturbo.cost.component.expenses;

import static org.junit.Assert.assertEquals;

import java.util.List;

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
        expensesStore = new SqlAccountExpensesStore(dsl);
    }

    @Test
    public void testCRUD() throws AccountExpenseNotFoundException, DbException {

        // INSERT
        AccountExpenses accountExpenses1 = saveExpense();

        // READ by associated account id
        List<AccountExpenses> accountExpenses2 = expensesStore
                .getAccountExpensesByAssociatedAccountId(accountExpenses1.getAssociatedAccountId());
        assertEquals(ASSOCIATED_ACCOUNT_ID, accountExpenses2.get(0).getAssociatedAccountId());

        // UPDATE
        expensesStore
                .updateAccountExpenses(ASSOCIATED_ACCOUNT_ID, accountExpensesInfo1);
        accountExpenses2 = expensesStore
                .getAccountExpensesByAssociatedAccountId(accountExpenses2.get(0).getAssociatedAccountId());
        assertEquals(ACCOUNT_EXPENSES_PERCENTAGE2, accountExpenses2.get(0)
                .getAccountExpensesInfo()
                .getServiceExpensesList()
                .get(0).getExpenses().getAmount(), 0.001);


        // DELETE
        expensesStore
                .deleteAccountExpensesByAssociatedAccountId(ASSOCIATED_ACCOUNT_ID);
        List<AccountExpenses> allAccountExpenses = expensesStore.getAllAccountExpenses();
        assertEquals(0, allAccountExpenses.size());
    }

    private AccountExpenses saveExpense() throws DbException {
        AccountExpenses accountExpenses1 = expensesStore
                .persistAccountExpenses(ASSOCIATED_ACCOUNT_ID, accountExpensesInfo);
        assertEquals(ASSOCIATED_ACCOUNT_ID, accountExpenses1.getAssociatedAccountId());
        assertEquals(ACCOUNT_EXPENSES_PERCENTAGE1, accountExpenses1
                .getAccountExpensesInfo()
                .getServiceExpenses(0)
                .getExpenses()
                .getAmount(), 0.001);
        return accountExpenses1;
    }

    @Test
    public void testSaveMultipleExpenseForTheSameAccount() throws AccountExpenseNotFoundException, InterruptedException, DbException {
        AccountExpenses accountExpenses1 = expensesStore
                .persistAccountExpenses(ASSOCIATED_ACCOUNT_ID, accountExpensesInfo);
        assertEquals(ASSOCIATED_ACCOUNT_ID, accountExpenses1.getAssociatedAccountId());
        // sleep for 1 second, so the timestamp will be different
        Thread.sleep(1000);
        AccountExpenses accountExpenses2 = expensesStore
                .persistAccountExpenses(ASSOCIATED_ACCOUNT_ID, accountExpensesInfo);
        assertEquals(ASSOCIATED_ACCOUNT_ID, accountExpenses2.getAssociatedAccountId());

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

    @Test(expected = AccountExpenseNotFoundException.class)
    public void testUpdateNotExistedExpenses() throws AccountExpenseNotFoundException, DbException {
        expensesStore.updateAccountExpenses(Long.MAX_VALUE, accountExpensesInfo);
    }

}
