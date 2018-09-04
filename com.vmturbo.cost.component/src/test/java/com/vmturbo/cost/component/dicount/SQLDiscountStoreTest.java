package com.vmturbo.cost.component.dicount;

import static org.junit.Assert.assertEquals;
import static org.springframework.test.util.AssertionErrors.fail;

import java.util.List;

import org.flywaydb.core.Flyway;
import org.jooq.DSLContext;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.vmturbo.common.protobuf.cost.Cost.Discount;
import com.vmturbo.common.protobuf.cost.Cost.DiscountInfo;
import com.vmturbo.cost.component.discount.DiscountNotFoundException;
import com.vmturbo.cost.component.discount.DuplicateAccountIdException;
import com.vmturbo.cost.component.discount.SQLDiscountStore;
import com.vmturbo.cost.component.discount.DiscountStore;
import com.vmturbo.cost.component.identity.IdentityProvider;
import com.vmturbo.sql.utils.DbException;
import com.vmturbo.sql.utils.TestSQLDatabaseConfig;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(
        classes = {TestSQLDatabaseConfig.class}
)
@TestPropertySource(properties = {"originalSchemaName=cost"})
public class SQLDiscountStoreTest {

    public static final long ASSOCIATED_ACCOUNT_ID = 1111l;
    public static final double DISCOUNT_PERCENTAGE2 = 20.0;
    public static final double DISCOUNT_PERCENTAGE1 = 10.0;
    final DiscountInfo discountInfoAccountLevelOnly1 = DiscountInfo.newBuilder()
            .setAccountLevelDiscount(DiscountInfo
                    .AccountLevelDiscount
                    .newBuilder()
                    .setDiscountPercentage(DISCOUNT_PERCENTAGE1)
                    .build())
            .build();
    final DiscountInfo discountInfoAccountLevelOnly2 = DiscountInfo.newBuilder()
            .setAccountLevelDiscount(DiscountInfo
                    .AccountLevelDiscount
                    .newBuilder()
                    .setDiscountPercentage(DISCOUNT_PERCENTAGE2)
                    .build())
            .build();
    @Autowired
    protected TestSQLDatabaseConfig dbConfig;
    private Flyway flyway;
    private DiscountStore discountDao;
    private DSLContext dsl;

    @Before
    public void setup() throws Exception {
        flyway = dbConfig.flyway();
        dsl = dbConfig.dsl();
        flyway.clean();
        flyway.migrate();
        discountDao = new SQLDiscountStore(dsl,
                new IdentityProvider(0));
    }

    @Test
    public void testCRUD() throws DbException, DiscountNotFoundException, DuplicateAccountIdException {

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
}
