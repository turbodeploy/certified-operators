package com.vmturbo.cost.component.reserved.instance;

import static org.junit.Assert.assertEquals;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Sets;

import org.jooq.DSLContext;
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

import com.vmturbo.cloud.common.identity.IdentityProvider.DefaultIdentityProvider;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpecInfo;
import com.vmturbo.components.common.featureflags.FeatureFlags;
import com.vmturbo.cost.component.db.Cost;
import com.vmturbo.cost.component.db.Tables;
import com.vmturbo.cost.component.db.TestCostDbEndpointConfig;
import com.vmturbo.cost.component.db.tables.records.ReservedInstanceSpecRecord;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.ReservedInstanceType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.ReservedInstanceType.OfferingClass;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;
import com.vmturbo.platform.sdk.common.CommonCost.PaymentOption;
import com.vmturbo.sql.utils.DbCleanupRule;
import com.vmturbo.sql.utils.DbConfigurationRule;
import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.sql.utils.DbEndpointTestRule;
import com.vmturbo.test.utils.FeatureFlagTestRule;

/**
 * This class tests methods in the ReservedInstanceSpecStore class.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {TestCostDbEndpointConfig.class})
@DirtiesContext(classMode = ClassMode.BEFORE_CLASS)
@TestPropertySource(properties = {"sqlDialect=MARIADB"})
public class ReservedInstanceSpecStoreTest {

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

    private ReservedInstanceSpecStore reservedInstanceSpecStore;

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
        reservedInstanceSpecStore = new ReservedInstanceSpecStore(dsl,
                new DefaultIdentityProvider(0), 10);
    }

    private ReservedInstanceSpec specOne = ReservedInstanceSpec.newBuilder()
            .setId(111)
            .setReservedInstanceSpecInfo(ReservedInstanceSpecInfo.newBuilder()
                    .setOs(OSType.LINUX)
                    .setTenancy(Tenancy.DEDICATED)
                    .setRegionId(99L)
                    .setTierId(123)
                    .setType(ReservedInstanceType.newBuilder()
                            .setPaymentOption(PaymentOption.ALL_UPFRONT)
                            .setOfferingClass(OfferingClass.CONVERTIBLE)
                            .setTermYears(1)))
            .build();

    private ReservedInstanceSpec specTwo = ReservedInstanceSpec.newBuilder()
            .setId(112)
            .setReservedInstanceSpecInfo(ReservedInstanceSpecInfo.newBuilder()
                    .setOs(OSType.WINDOWS)
                    .setTenancy(Tenancy.HOST)
                    .setRegionId(99L)
                    .setTierId(123)
                    .setType(ReservedInstanceType.newBuilder()
                            .setPaymentOption(PaymentOption.ALL_UPFRONT)
                            .setOfferingClass(OfferingClass.CONVERTIBLE)
                            .setTermYears(1)))
            .build();

    private ReservedInstanceSpec specThree = ReservedInstanceSpec.newBuilder()
            .setId(113)
            .setReservedInstanceSpecInfo(ReservedInstanceSpecInfo.newBuilder()
                    .setOs(OSType.SUSE)
                    .setTenancy(Tenancy.DEFAULT)
                    .setRegionId(99L)
                    .setTierId(123)
                    .setType(ReservedInstanceType.newBuilder()
                            .setPaymentOption(PaymentOption.ALL_UPFRONT)
                            .setOfferingClass(OfferingClass.CONVERTIBLE)
                            .setTermYears(1)))
            .build();

    /**
     * Test updateReservedInstanceSpec method.
     */
    @Test
    public void testUpdateReservedInstanceBoughtSpecAddAll() {
        final List<ReservedInstanceSpec> reservedInstanceSpecs = Arrays.asList(specOne, specTwo);
        final Map<Long, Long> localIdToRealIdMap =
                reservedInstanceSpecStore.updateReservedInstanceSpec(dsl, reservedInstanceSpecs);
        final List<ReservedInstanceSpecRecord> records = dsl.selectFrom(Tables.RESERVED_INSTANCE_SPEC).fetch();

        assertEquals(2, records.size());
        assertEquals(1, records.stream()
            .filter(spec -> spec.getOsType().equals(OSType.LINUX_VALUE))
            .count());
        assertEquals(1, records.stream()
                .filter(spec -> spec.getOsType().equals(OSType.WINDOWS_VALUE))
                .count());
        assertEquals(2, localIdToRealIdMap.size());
    }

    /**
     * Test updateReservedInstanceSpec method with a subset of RI specs.
     */
    @Test
    public void testUpdateReservedInstanceBoughtSpecAddSubset() {
        final List<ReservedInstanceSpec> reservedInstanceSpecs = Arrays.asList(specOne, specTwo);
        reservedInstanceSpecStore.updateReservedInstanceSpec(dsl, reservedInstanceSpecs);
        final Map<Long, Long> latestLocalIdToRealIdMap =
                reservedInstanceSpecStore.updateReservedInstanceSpec(dsl,
                        Arrays.asList(specTwo, specThree));
        final List<ReservedInstanceSpecRecord> records = dsl.selectFrom(Tables.RESERVED_INSTANCE_SPEC).fetch();

        assertEquals(3, records.size());
        assertEquals(1, records.stream()
                .filter(spec -> spec.getOsType().equals(OSType.LINUX_VALUE))
                .count());
        assertEquals(1, records.stream()
                .filter(spec -> spec.getOsType().equals(OSType.WINDOWS_VALUE))
                .count());
        assertEquals(1, records.stream()
                .filter(spec -> spec.getOsType().equals(OSType.SUSE_VALUE))
                .count());
        assertEquals(2, latestLocalIdToRealIdMap.size());
    }

    /**
     * Test getAllReservedInstanceSpec method.
     */
    @Test
    public void testGetAllReservedInstanceSpec() {
        final List<ReservedInstanceSpec> reservedInstanceSpecs = Arrays.asList(specOne, specTwo);
        reservedInstanceSpecStore.updateReservedInstanceSpec(dsl, reservedInstanceSpecs);
        final List<ReservedInstanceSpec> allReservedInstanceSpecs =
                reservedInstanceSpecStore.getAllReservedInstanceSpec();
        assertEquals(2, allReservedInstanceSpecs.size());
        assertEquals(1, allReservedInstanceSpecs.stream()
                .filter(spec -> spec.getReservedInstanceSpecInfo().getOs().equals(OSType.LINUX))
                .count());
        assertEquals(1, allReservedInstanceSpecs.stream()
                .filter(spec -> spec.getReservedInstanceSpecInfo().getOs().equals(OSType.WINDOWS))
                .count());
    }

    /**
     * Test getReservedInstanceSpecByIds method.
     */
    @Test
    public void testGetReservedInstanceSpecByIds() {
        final List<ReservedInstanceSpec> reservedInstanceSpecs = Arrays.asList(specOne, specTwo);
        reservedInstanceSpecStore.updateReservedInstanceSpec(dsl, reservedInstanceSpecs);
        final List<ReservedInstanceSpec> allReservedInstanceSpecs =
                reservedInstanceSpecStore.getAllReservedInstanceSpec();
        final Long specId = allReservedInstanceSpecs.get(0).getId();
        final List<ReservedInstanceSpec> results =
                reservedInstanceSpecStore.getReservedInstanceSpecByIds(Sets.newHashSet(specId));
        assertEquals(1L, results.size());
    }
}
