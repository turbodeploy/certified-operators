package com.vmturbo.cost.component;

import static com.vmturbo.cost.component.db.Tables.BUY_RESERVED_INSTANCE;
import static com.vmturbo.cost.component.db.Tables.RESERVED_INSTANCE_SPEC;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nonnull;

import org.jooq.DSLContext;
import org.jooq.Result;
import org.jooq.SQLDialect;
import org.jooq.impl.TableImpl;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo.ReservedInstanceBoughtCost;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo.ReservedInstanceBoughtCoupons;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo.ReservedInstanceDerivedCost;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpecInfo;
import com.vmturbo.components.common.diagnostics.DiagnosticsAppender;
import com.vmturbo.cost.component.db.Cost;
import com.vmturbo.cost.component.db.TestCostDbEndpointConfig;
import com.vmturbo.cost.component.db.tables.BuyReservedInstance;
import com.vmturbo.cost.component.db.tables.records.BuyReservedInstanceRecord;
import com.vmturbo.cost.component.db.tables.records.ReservedInstanceSpecRecord;
import com.vmturbo.platform.sdk.common.CommonCost.CurrencyAmount;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.sql.utils.MultiDbTestBase;

/**
 * Test {@link TableDiagsRestorable} functionality.
 */
@RunWith(Parameterized.class)
public class TableDiagsRestorableTest extends MultiDbTestBase {

    private static final long RESERVED_INSTANCE_SPEC_TEST_ID = 706803569926466L;

    // private SQLDialect dialect;

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
    public TableDiagsRestorableTest(boolean configurableDbDialect, SQLDialect dialect)
            throws SQLException, UnsupportedDialectException, InterruptedException {
        super(Cost.COST, configurableDbDialect, dialect, "cost",
                TestCostDbEndpointConfig::costEndpoint);
        this.dsl = super.getDslContext();
        // this.dialect = dialect;
    }

    /**
     * Test one particular table export and import.
     */
    @Test
    public void testImportAndExport() {
        final TableDiagsRestorable<Object, BuyReservedInstanceRecord> dumper = getDumper();

        //insert ReservedInstanceSpecRecord to satisfy BUY_RESERVED_INSTANCE FK constraint
        final ReservedInstanceSpecRecord dummySpec = getDummySpecRecord();
        dsl.insertInto(RESERVED_INSTANCE_SPEC).set(dummySpec).execute();

        //insert dummy BuyReservedInstanceRecord records
        final BuyReservedInstanceRecord rec1 = getRecord1();
        final BuyReservedInstanceRecord rec2 = getRecord2();
        dsl.insertInto(dumper.getTable()).set(rec1).newRecord().set(rec2).execute();

        //collect diagnostic strings
        List<String> sb = new ArrayList<>();
        DiagnosticsAppender da = string -> sb.add(string);
        dumper.collectDiags(da);

        //clean table and export diagnostic
        dsl.truncate(dumper.getTable()).execute();

        // Delete the dummy ReservedInstanceSpec record to simulate foreign key violation.
        // BuyReservedInstance: FOREIGN KEY(reserved_instance_spec_id) REFERENCES reserved_instance_spec(id) ON DELETE CASCADE
        // uncomment it when OM-79371 is addressed.
        // dsl.deleteFrom(RESERVED_INSTANCE_SPEC).where(RESERVED_INSTANCE_SPEC.ID.eq(RESERVED_INSTANCE_SPEC_TEST_ID)).execute();

        dumper.restoreDiags(sb.stream(), null);

        Result<BuyReservedInstanceRecord> result = dsl.selectFrom(dumper.getTable()).where(
                BUY_RESERVED_INSTANCE.ID.eq(rec1.getId())).fetch();
        Assert.assertEquals(rec1, result.get(0));
        result = dsl.selectFrom(dumper.getTable())
                .where(BUY_RESERVED_INSTANCE.ID.eq(rec2.getId()))
                .fetch();
        Assert.assertEquals(rec2, result.get(0));
    }

    private TableDiagsRestorable<Object, BuyReservedInstanceRecord> getDumper() {
        return new TableDiagsRestorable<Object, BuyReservedInstanceRecord>() {
            @Nonnull
            @Override
            public String getFileName() {
                return null;
            }

            @Override
            public DSLContext getDSLContext() {
                return dsl;
            }

            @Override
            public TableImpl<BuyReservedInstanceRecord> getTable() {
                return BUY_RESERVED_INSTANCE;
            }
        };
    }

    private ReservedInstanceSpecRecord getDummySpecRecord() {
        final ReservedInstanceSpecRecord dummySpec = new ReservedInstanceSpecRecord();
        dummySpec.setOfferingClass(1);
        dummySpec.setPaymentOption(2);
        dummySpec.setTermYears(3);
        dummySpec.setTenancy(4);
        dummySpec.setOsType(5);
        dummySpec.setTierId(6L);
        dummySpec.setRegionId(7L);
        dummySpec.setReservedInstanceSpecInfo(ReservedInstanceSpecInfo.newBuilder().build());
        dummySpec.setId(RESERVED_INSTANCE_SPEC_TEST_ID);
        return dummySpec;
    }

    private BuyReservedInstanceRecord getRecord1() {
        BuyReservedInstanceRecord rec1 = BuyReservedInstance.BUY_RESERVED_INSTANCE.newRecord();
        rec1.setBusinessAccountId(73484869386730L);
        rec1.setCount(1);
        rec1.setId(2L);
        rec1.setPerInstanceAmortizedCostHourly(1.1);
        rec1.setPerInstanceFixedCost(1.2);
        rec1.setRegionId(12341235L);
        rec1.setReservedInstanceBoughtInfo(ReservedInstanceBoughtInfo.newBuilder()
                .setBusinessAccountId(73484869386730L)
                .setNumBought(1)
                .setReservedInstanceSpec(RESERVED_INSTANCE_SPEC_TEST_ID)

                .setReservedInstanceBoughtCost(ReservedInstanceBoughtCost.newBuilder()
                        .setFixedCost(CurrencyAmount.newBuilder().setAmount(270))
                        .setUsageCostPerHour(CurrencyAmount.newBuilder().setAmount(1))
                        .setRecurringCostPerHour(CurrencyAmount.newBuilder().setAmount(2)))
                .setReservedInstanceBoughtCoupons(ReservedInstanceBoughtCoupons.newBuilder()
                        .setNumberOfCoupons(8.0)
                        .setNumberOfCouponsUsed(2.0))
                .setReservedInstanceDerivedCost(ReservedInstanceDerivedCost.newBuilder()
                        .setAmortizedCostPerHour(
                                CurrencyAmount.newBuilder().setAmount(0.030821917808219176))
                        .setOnDemandRatePerHour(
                                CurrencyAmount.newBuilder().setAmount(0.0528000020980835)))
                .setDisplayName("Some name")
                .setAvailabilityZoneId(73484869386731L)
                .build());
        rec1.setReservedInstanceSpecId(RESERVED_INSTANCE_SPEC_TEST_ID);
        rec1.setTopologyContextId(777777L);
        return rec1;
    }

    private BuyReservedInstanceRecord getRecord2() {
        BuyReservedInstanceRecord rec2 = BuyReservedInstance.BUY_RESERVED_INSTANCE.newRecord();
        rec2.setBusinessAccountId(73484869386720L);
        rec2.setCount(2);
        rec2.setId(4L);
        rec2.setPerInstanceAmortizedCostHourly(2.1);
        rec2.setPerInstanceFixedCost(3.2);
        rec2.setRegionId(78687678L);
        rec2.setReservedInstanceBoughtInfo(ReservedInstanceBoughtInfo.newBuilder()
                .setBusinessAccountId(73484869386720L)
                .setNumBought(1)
                .setReservedInstanceSpec(RESERVED_INSTANCE_SPEC_TEST_ID)

                .setReservedInstanceBoughtCost(ReservedInstanceBoughtCost.newBuilder()
                        .setFixedCost(CurrencyAmount.newBuilder().setAmount(170))
                        .setUsageCostPerHour(CurrencyAmount.newBuilder().setAmount(2))
                        .setRecurringCostPerHour(CurrencyAmount.newBuilder().setAmount(4)))
                .setReservedInstanceBoughtCoupons(ReservedInstanceBoughtCoupons.newBuilder()
                        .setNumberOfCoupons(16.0)
                        .setNumberOfCouponsUsed(0.0))
                .setReservedInstanceDerivedCost(ReservedInstanceDerivedCost.newBuilder()
                        .setAmortizedCostPerHour(
                                CurrencyAmount.newBuilder().setAmount(0.010821917808219176))
                        .setOnDemandRatePerHour(
                                CurrencyAmount.newBuilder().setAmount(0.0328000020980835)))
                .setDisplayName("Some name2")
                .setAvailabilityZoneId(73484869386721L)
                .build());
        rec2.setReservedInstanceSpecId(RESERVED_INSTANCE_SPEC_TEST_ID);
        rec2.setTopologyContextId(777777L);
        return rec2;
    }
}
