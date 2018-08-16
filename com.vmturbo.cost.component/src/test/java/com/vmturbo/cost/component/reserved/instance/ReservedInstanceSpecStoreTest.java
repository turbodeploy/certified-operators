package com.vmturbo.cost.component.reserved.instance;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
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

import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpecInfo;
import com.vmturbo.cost.component.db.Tables;
import com.vmturbo.cost.component.db.tables.records.ReservedInstanceSpecRecord;
import com.vmturbo.cost.component.identity.IdentityProvider;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.ReservedInstanceType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.ReservedInstanceType.OfferingClass;
import com.vmturbo.platform.sdk.common.CloudCostDTO.ReservedInstanceType.PaymentOption;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;
import com.vmturbo.sql.utils.TestSQLDatabaseConfig;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(
        classes = {TestSQLDatabaseConfig.class}
)
@TestPropertySource(properties = {"originalSchemaName=cost"})
public class ReservedInstanceSpecStoreTest {

    @Autowired
    protected TestSQLDatabaseConfig dbConfig;

    private Flyway flyway;

    private DSLContext dsl;

    private ReservedInstanceSpecStore reservedInstanceSpecStore;

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

    @Before
    public void setup() throws Exception {
        flyway = dbConfig.flyway();
        dsl = dbConfig.dsl();
        flyway.clean();
        flyway.migrate();
        reservedInstanceSpecStore = new ReservedInstanceSpecStore(dsl,
                new IdentityProvider(0));
    }

    @Test
    public void testUpdateReservedInstanceBoughtSpecAddAll() {
        final List<ReservedInstanceSpec> reservedInstanceSpecs = Arrays.asList(specOne, specTwo);
        final Map<Long, Long> localIdToRealIdMap =
                reservedInstanceSpecStore.updateReservedInstanceBoughtSpec(dsl, reservedInstanceSpecs);
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

    @Test
    public void testUpdateReservedInstanceBoughtSpecAddSubset() {
        final List<ReservedInstanceSpec> reservedInstanceSpecs = Arrays.asList(specOne, specTwo);
        reservedInstanceSpecStore.updateReservedInstanceBoughtSpec(dsl, reservedInstanceSpecs);
        final Map<Long, Long> latestLocalIdToRealIdMap =
                reservedInstanceSpecStore.updateReservedInstanceBoughtSpec(dsl,
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

    @Test
    public void testGetAllReservedInstanceSpec() {
        final List<ReservedInstanceSpec> reservedInstanceSpecs = Arrays.asList(specOne, specTwo);
        reservedInstanceSpecStore.updateReservedInstanceBoughtSpec(dsl, reservedInstanceSpecs);
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
}
