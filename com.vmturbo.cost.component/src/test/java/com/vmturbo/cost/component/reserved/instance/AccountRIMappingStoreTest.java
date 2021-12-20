package com.vmturbo.cost.component.reserved.instance;

import static com.vmturbo.cost.component.db.Tables.ACCOUNT_TO_RESERVED_INSTANCE_MAPPING;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isOneOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.annotation.DirtiesContext.ClassMode;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.util.CollectionUtils;

import com.vmturbo.common.protobuf.cost.Cost.UploadRIDataRequest.AccountRICoverageUpload;
import com.vmturbo.common.protobuf.cost.Cost.UploadRIDataRequest.EntityRICoverageUpload.Coverage;
import com.vmturbo.components.common.featureflags.FeatureFlags;
import com.vmturbo.cost.component.db.Cost;
import com.vmturbo.cost.component.db.TestCostDbEndpointConfig;
import com.vmturbo.cost.component.db.tables.records.AccountToReservedInstanceMappingRecord;
import com.vmturbo.cost.component.reserved.instance.AccountRIMappingStore.AccountRIMappingItem;
import com.vmturbo.sql.utils.DbCleanupRule;
import com.vmturbo.sql.utils.DbConfigurationRule;
import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.sql.utils.DbEndpointTestRule;
import com.vmturbo.test.utils.FeatureFlagTestRule;

/**
 * Unit tests for {@link AccountRIMappingStore}.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {TestCostDbEndpointConfig.class})
@DirtiesContext(classMode = ClassMode.BEFORE_CLASS)
@TestPropertySource(properties = {"sqlDialect=MARIADB"})
public class AccountRIMappingStoreTest {
    private static final Logger logger = LogManager.getLogger();

    @Autowired(required = false)
    private TestCostDbEndpointConfig dbEndpointConfig;

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

    private static final double DELTA = 0.000001;

    /**
     * Test rule to use {@link DbEndpoint}s in test.
     */
    @Rule
    public DbEndpointTestRule dbEndpointTestRule = new DbEndpointTestRule("cost");

    /**
     * Rule to manage feature flag enablement to make sure FeatureFlagManager store is set up.
     */
    @Rule
    public FeatureFlagTestRule featureFlagTestRule = new FeatureFlagTestRule().testAllCombos(
            FeatureFlags.POSTGRES_PRIMARY_DB);

    private DSLContext dsl;

    private AccountRIMappingStore accountRIMappingStore;

    /**
     * Set up before each test.
     *
     * @throws SQLException if there is db error
     * @throws UnsupportedDialectException if the dialect is not supported
     * @throws InterruptedException if interrupted
     */
    @Before
    public void before() throws SQLException, UnsupportedDialectException, InterruptedException {
        if (FeatureFlags.POSTGRES_PRIMARY_DB.isEnabled()) {
            dbEndpointTestRule.addEndpoints(dbEndpointConfig.costEndpoint());
            dsl = dbEndpointConfig.costEndpoint().dslContext();
        } else {
            dsl = dbConfig.getDslContext();
        }
        accountRIMappingStore = new AccountRIMappingStore(dsl);
    }

    final AccountRICoverageUpload coverageOne = AccountRICoverageUpload.newBuilder()
            .setAccountId(123L)
            .addCoverage(Coverage.newBuilder()
                    .setReservedInstanceId(456L)
                    .setCoveredCoupons(10)
                    .setRiCoverageSource(Coverage.RICoverageSource.BILLING))
            .addCoverage(Coverage.newBuilder()
                    .setReservedInstanceId(457L)
                    .setCoveredCoupons(20)
                    .setRiCoverageSource(Coverage.RICoverageSource.SUPPLEMENTAL_COVERAGE_ALLOCATION))
            .build();

    final AccountRICoverageUpload coverageTwo = AccountRICoverageUpload.newBuilder()
            .setAccountId(124L)
            .addCoverage(Coverage.newBuilder()
                    .setReservedInstanceId(457L)
                    .setCoveredCoupons(30)
                    .setRiCoverageSource(Coverage.RICoverageSource.BILLING))
            .addCoverage(Coverage.newBuilder()
                    .setReservedInstanceId(458L)
                    .setCoveredCoupons(40)
                    .setRiCoverageSource(Coverage.RICoverageSource.BILLING))
            .build();

    final AccountRICoverageUpload coverageThree = AccountRICoverageUpload.newBuilder()
            .setAccountId(125L)
            .addCoverage(Coverage.newBuilder()
                    .setReservedInstanceId(459L)
                    .setCoveredCoupons(10)
                    .setRiCoverageSource(Coverage.RICoverageSource.BILLING))
            .addCoverage(Coverage.newBuilder()
                    .setReservedInstanceId(459L)
                    .setCoveredCoupons(30)
                    .setRiCoverageSource(Coverage.RICoverageSource.SUPPLEMENTAL_COVERAGE_ALLOCATION))
            .build();

    /**
     * testUpdateAndGetRIMappings test update and read RI account mappings.
     */
    @Test
    public void testUpdateAndGetRIMappings() {
        final List<AccountRICoverageUpload> accCoverageLists =
                Arrays.asList(coverageOne, coverageTwo, coverageThree);
        accountRIMappingStore.updateAccountRICoverageMappings(accCoverageLists);

        final Map<Long, List<AccountRIMappingItem>> coverageMap =
                accountRIMappingStore.getAccountRICoverageMappings(accCoverageLists.stream()
                        .map(riCov -> riCov.getAccountId())
                        .collect(toList()));
        final List<AccountRIMappingItem> retCvg1Items = coverageMap.get(coverageOne.getAccountId());
        for (AccountRIMappingItem riItem : retCvg1Items) {
            assertThat(riItem.getBusinessAccountOid(), is(coverageOne.getAccountId()));
            assertThat(riItem.getUsedCoupons(), isOneOf(10.0, 20.0));
        }

        final List<AccountRIMappingItem> retCvg2Items = coverageMap.get(coverageTwo.getAccountId());
        for (AccountRIMappingItem riItem : retCvg2Items) {
            assertThat(riItem.getBusinessAccountOid(), is(coverageTwo.getAccountId()));
            assertThat(riItem.getUsedCoupons(), isOneOf(30.0, 40.0));
        }

        final List<AccountRIMappingItem> retCvg3Items = coverageMap.get(coverageThree.getAccountId());
        for (AccountRIMappingItem riItem : retCvg3Items) {
            assertThat(riItem.getBusinessAccountOid(), is(coverageThree.getAccountId()));
            assertThat(riItem.getUsedCoupons(), isOneOf(10.0, 30.0));
        }
    }

    /**
     * testUpdateAccountRIMappings test update RI account mappings (read directly from the table).
     */
    @Test
    public void testUpdateAccountRIMappings() {
        final List<AccountRICoverageUpload> accCoverageLists =
                Arrays.asList(coverageOne, coverageTwo);
        accountRIMappingStore.updateAccountRICoverageMappings(accCoverageLists);
        List<AccountToReservedInstanceMappingRecord> records =
                dsl.selectFrom(ACCOUNT_TO_RESERVED_INSTANCE_MAPPING).fetch();

        assertEquals(4, records.size());
        assertEquals(10.0, records.stream()
                .filter(record -> record.getBusinessAccountOid().equals(123L)
                        && record.getReservedInstanceId().equals(456L))
                .map(AccountToReservedInstanceMappingRecord::getUsedCoupons)
                .findFirst()
                .orElse(0.0), DELTA);
        assertEquals(20.0, records.stream()
                .filter(record -> record.getBusinessAccountOid().equals(123L)
                        && record.getReservedInstanceId().equals(457L))
                .map(AccountToReservedInstanceMappingRecord::getUsedCoupons)
                .findFirst()
                .orElse(0.0), DELTA);
        assertEquals(30.0, records.stream()
                .filter(record -> record.getBusinessAccountOid().equals(124L)
                        && record.getReservedInstanceId().equals(457L))
                .map(AccountToReservedInstanceMappingRecord::getUsedCoupons)
                .findFirst()
                .orElse(0.0), DELTA);
        assertEquals(40.0, records.stream()
                .filter(record -> record.getBusinessAccountOid().equals(124L)
                        && record.getReservedInstanceId().equals(458L))
                .map(AccountToReservedInstanceMappingRecord::getUsedCoupons)
                .findFirst()
                .orElse(0.0), DELTA);
    }

    /**
     * testDeleteRICoverage4BA test delete RI account mappings.
     */
    @Test
    public void testDeleteRICoverage4BA() {
        // Setup store
        final List<AccountRICoverageUpload> accCoverageLists =
                Arrays.asList(coverageOne, coverageTwo, coverageThree);
        accountRIMappingStore.updateAccountRICoverageMappings(accCoverageLists);
        accountRIMappingStore.deleteAccountRICoverageMappings(Arrays.asList(Long.valueOf(123)));

        // Bring all accounts.
        final Map<Long, List<AccountRIMappingItem>> coverageMap =
                accountRIMappingStore.getAccountRICoverageMappings(new ArrayList<Long>());
        logger.info("testDeleteRICoverage4BA: coverage map after account deleted =\n{}", coverageMap);

        final List<AccountRIMappingItem> retCvg1Items = coverageMap.get(coverageOne.getAccountId());
        // Deleted.
        assertTrue(CollectionUtils.isEmpty(retCvg1Items));
        assertTrue(coverageMap.keySet().size() == 2);
        assertTrue(coverageMap.values().stream().flatMap(List::stream).collect(toList()).size() == 4);
    }

    /**
     * Test for {@link AccountRIMappingStore#getUndiscoveredAccountsCoveredByReservedInstances}.
     */
    @Test
    public void testGetAccountsCoveredByReservedInstances() {
        final List<AccountRICoverageUpload> accountCoverageLists = Arrays.asList(coverageOne,
                coverageTwo, coverageThree);
        accountRIMappingStore.updateAccountRICoverageMappings(accountCoverageLists);
        ImmutableMap.of(Collections.singleton(457L),
                Collections.singletonMap(457L, ImmutableSet.of(123L, 124L)),
                Collections.<Long>emptyList(),
                ImmutableMap.of(456L, Collections.singleton(123L), 457L,
                        ImmutableSet.of(123L, 124L), 458L, Collections.singleton(124L), 459L,
                        Collections.singleton(125L))).forEach(
                (reservedInstances, reservedInstanceToCoveredEntities) -> Assert.assertEquals(
                        reservedInstanceToCoveredEntities,
                        accountRIMappingStore.getUndiscoveredAccountsCoveredByReservedInstances(
                                reservedInstances)));
    }
}
