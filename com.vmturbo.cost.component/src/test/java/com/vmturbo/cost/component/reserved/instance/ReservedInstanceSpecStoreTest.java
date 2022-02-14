package com.vmturbo.cost.component.reserved.instance;

import static org.junit.Assert.assertEquals;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Sets;

import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.vmturbo.cloud.common.identity.IdentityProvider.DefaultIdentityProvider;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpecInfo;
import com.vmturbo.cost.component.db.Cost;
import com.vmturbo.cost.component.db.Tables;
import com.vmturbo.cost.component.db.TestCostDbEndpointConfig;
import com.vmturbo.cost.component.db.tables.records.ReservedInstanceSpecRecord;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.ReservedInstanceType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.ReservedInstanceType.OfferingClass;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;
import com.vmturbo.platform.sdk.common.CommonCost.PaymentOption;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.sql.utils.MultiDbTestBase;

/**
 * This class tests methods in the ReservedInstanceSpecStore class.
 */
@RunWith(Parameterized.class)
public class ReservedInstanceSpecStoreTest extends MultiDbTestBase {
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
    public ReservedInstanceSpecStoreTest(boolean configurableDbDialect, SQLDialect dialect)
            throws SQLException, UnsupportedDialectException, InterruptedException {
        super(Cost.COST, configurableDbDialect, dialect, "cost",
                TestCostDbEndpointConfig::costEndpoint);
        this.dsl = super.getDslContext();
    }

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
