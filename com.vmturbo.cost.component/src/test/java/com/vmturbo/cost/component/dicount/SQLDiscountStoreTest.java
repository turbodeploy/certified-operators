package com.vmturbo.cost.component.dicount;

import static org.junit.Assert.assertEquals;
import static org.springframework.test.util.AssertionErrors.fail;

import java.sql.SQLException;
import java.util.List;

import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.vmturbo.cloud.common.identity.IdentityProvider.DefaultIdentityProvider;
import com.vmturbo.common.protobuf.cost.Cost.Discount;
import com.vmturbo.common.protobuf.cost.Cost.DiscountInfo;
import com.vmturbo.cost.component.db.Cost;
import com.vmturbo.cost.component.db.TestCostDbEndpointConfig;
import com.vmturbo.cost.component.discount.DiscountNotFoundException;
import com.vmturbo.cost.component.discount.DiscountStore;
import com.vmturbo.cost.component.discount.DuplicateAccountIdException;
import com.vmturbo.cost.component.discount.SQLDiscountStore;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.sql.utils.DbException;
import com.vmturbo.sql.utils.MultiDbTestBase;

@RunWith(Parameterized.class)
public class SQLDiscountStoreTest extends MultiDbTestBase {
    /**
     * Provide test parameters.
     *
     * @return test parameters
     */
    @Parameters
    public static Object[][] parameters() {
        return MultiDbTestBase.POSTGRES_CONVERTED_PARAMS;
    }

    private final DSLContext dsl;

    /**
     * Create a new instance with given parameters.
     *
     * @param configurableDbDialect true to enable POSTGRES_PRIMARY_DB feature flag
     * @param dialect         DB dialect to use
     * @throws SQLException                if a DB operation fails
     * @throws UnsupportedDialectException if dialect is bogus
     * @throws InterruptedException        if we're interrupted
     */
    public SQLDiscountStoreTest(boolean configurableDbDialect, SQLDialect dialect)
            throws SQLException, UnsupportedDialectException, InterruptedException {
        super(Cost.COST, configurableDbDialect, dialect, "cost",
                TestCostDbEndpointConfig::costEndpoint);
        this.dsl = super.getDslContext();
    }

    public static final long ASSOCIATED_ACCOUNT_ID = 1111l;
    public static final double DISCOUNT_PERCENTAGE2 = 20.0;
    public static final double DISCOUNT_PERCENTAGE1 = 10.0;
    public static final long SERVICE_KEY1 = 11111l;
    final DiscountInfo discountInfoAccountLevelOnly1 = DiscountInfo.newBuilder()
            .setAccountLevelDiscount(DiscountInfo
                    .AccountLevelDiscount
                    .newBuilder()
                    .setDiscountPercentage(DISCOUNT_PERCENTAGE1)
                    .build())
            .setServiceLevelDiscount(DiscountInfo
            .ServiceLevelDiscount.newBuilder()
                            .putDiscountPercentageByServiceId(SERVICE_KEY1, DISCOUNT_PERCENTAGE2)
            )
            .build();
    final DiscountInfo discountInfoAccountLevelOnly2 = DiscountInfo.newBuilder()
            .setAccountLevelDiscount(DiscountInfo
                    .AccountLevelDiscount
                    .newBuilder()
                    .setDiscountPercentage(DISCOUNT_PERCENTAGE2)
                    .build())
            .build();
    private DiscountStore discountDao;

    @Before
    public void before() {
        discountDao = new SQLDiscountStore(dsl, new DefaultIdentityProvider(0));
    }

    @Test
    public void testCRUD()
            throws DbException, DiscountNotFoundException, DuplicateAccountIdException {

        // SAVE discount
        Discount discountDto = saveDiscount();

        // READ by discount id
        List<Discount> disCounts = discountDao.getDiscountByDiscountId(discountDto.getId());
        assertEquals(ASSOCIATED_ACCOUNT_ID, disCounts.get(0).getAssociatedAccountId());

        // READ by associated account id
        disCounts = discountDao.getDiscountByAssociatedAccountId(ASSOCIATED_ACCOUNT_ID);
        assertEquals(ASSOCIATED_ACCOUNT_ID, disCounts.get(0).getAssociatedAccountId());

        // UPDATE
        discountDao.updateDiscount(disCounts.get(0).getId(), discountInfoAccountLevelOnly2);
        disCounts = discountDao.getDiscountByDiscountId(disCounts.get(0).getId());
        assertEquals(DISCOUNT_PERCENTAGE2, disCounts.get(0)
                .getDiscountInfo()
                .getAccountLevelDiscount()
                .getDiscountPercentage(), 0.001);

        // DELETE
        discountDao.deleteDiscountByDiscountId(disCounts.get(0).getId());
        assertEquals(0, discountDao.getAllDiscount().size());
    }

    private Discount saveDiscount() throws DuplicateAccountIdException, DbException {
        // INSERT
        Discount discountDto = discountDao.persistDiscount(ASSOCIATED_ACCOUNT_ID, discountInfoAccountLevelOnly1);
        assertEquals(ASSOCIATED_ACCOUNT_ID, discountDto.getAssociatedAccountId());
        assertEquals(DISCOUNT_PERCENTAGE1, discountDto
                .getDiscountInfo()
                .getAccountLevelDiscount()
                .getDiscountPercentage(), 0.001);
        assertEquals(DISCOUNT_PERCENTAGE2, discountDto
                .getDiscountInfo()
                .getServiceLevelDiscount()
                .getDiscountPercentageByServiceIdMap()
                .get(SERVICE_KEY1), 0.001);
        return discountDto;
    }

    // It's not using junit expected exception to ensure cleaning up the db.
    @Test
    public void testCreateDiscountWithExistingAccount()
            throws DuplicateAccountIdException, DbException, DiscountNotFoundException {
        Discount discountDto = saveDiscount();
        try {
            saveDiscount();
            fail("it should throws exception");
        } catch (DuplicateAccountIdException e) {
            // expected exception
        } finally {
            discountDao.deleteDiscountByDiscountId(discountDto.getId());
        }
    }

    @Test(expected = DiscountNotFoundException.class)
    public void testUpdateDiscountWithNotExistedAccount() throws DiscountNotFoundException, DbException {
        discountDao.updateDiscount(Long.MAX_VALUE, discountInfoAccountLevelOnly2);
    }

    @Test
    public void testUpdateAndDeleteByAccountAssociationId() throws DbException, DiscountNotFoundException, DuplicateAccountIdException {

        // SAVE discount
        Discount discountDto = saveDiscount();

        // READ by discount id
        List<Discount> disCounts = discountDao.getDiscountByDiscountId(discountDto.getId());
        assertEquals(ASSOCIATED_ACCOUNT_ID, disCounts.get(0).getAssociatedAccountId());

        // READ by associated account id
        disCounts = discountDao.getDiscountByAssociatedAccountId(ASSOCIATED_ACCOUNT_ID);
        assertEquals(ASSOCIATED_ACCOUNT_ID, disCounts.get(0).getAssociatedAccountId());

        // UPDATE
        discountDao.updateDiscountByAssociatedAccount(ASSOCIATED_ACCOUNT_ID, discountInfoAccountLevelOnly2);
        disCounts = discountDao.getDiscountByAssociatedAccountId(ASSOCIATED_ACCOUNT_ID);
        assertEquals(DISCOUNT_PERCENTAGE2, disCounts.get(0)
                .getDiscountInfo()
                .getAccountLevelDiscount()
                .getDiscountPercentage(), 0.001);

        // DELETE
        discountDao.deleteDiscountByAssociatedAccountId(ASSOCIATED_ACCOUNT_ID);
        assertEquals(0, discountDao.getAllDiscount().size());
    }
}
