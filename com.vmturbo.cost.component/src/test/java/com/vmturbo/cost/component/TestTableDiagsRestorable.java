package com.vmturbo.cost.component;

import static com.vmturbo.cost.component.db.Tables.BUY_RESERVED_INSTANCE;
import static com.vmturbo.cost.component.db.Tables.RESERVED_INSTANCE_SPEC;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nonnull;

import org.jooq.DSLContext;
import org.jooq.Result;
import org.jooq.impl.TableImpl;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo.ReservedInstanceBoughtCost;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo.ReservedInstanceBoughtCoupons;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo.ReservedInstanceDerivedCost;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpecInfo;
import com.vmturbo.components.common.diagnostics.DiagnosticsAppender;
import com.vmturbo.cost.component.db.Cost;
import com.vmturbo.cost.component.db.tables.BuyReservedInstance;
import com.vmturbo.cost.component.db.tables.records.BuyReservedInstanceRecord;
import com.vmturbo.cost.component.db.tables.records.ReservedInstanceSpecRecord;
import com.vmturbo.platform.sdk.common.CloudCostDTO.CurrencyAmount;
import com.vmturbo.sql.utils.DbCleanupRule;
import com.vmturbo.sql.utils.DbConfigurationRule;

/**
 * Test {@link TableDiagsRestorable} functionality.
 */
public class TestTableDiagsRestorable {

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

    private DSLContext dsl = dbConfig.getDslContext();

    /**
     * Test one particular table export and import.
     */
    @Test
    public void testImportAndExport() {
        final TableDiagsRestorable<Void, BuyReservedInstanceRecord> dumper = getDumper();

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
        dumper.restoreDiags(sb, null);

        Result<BuyReservedInstanceRecord> result = dsl.selectFrom(dumper.getTable()).where(
                BUY_RESERVED_INSTANCE.ID.eq(rec1.getId())).fetch();
        Assert.assertEquals(rec1, result.get(0));
        result = dsl.selectFrom(dumper.getTable())
                .where(BUY_RESERVED_INSTANCE.ID.eq(rec2.getId()))
                .fetch();
        Assert.assertEquals(rec2, result.get(0));
    }

    private TableDiagsRestorable<Void, BuyReservedInstanceRecord> getDumper() {
        return new TableDiagsRestorable<Void, BuyReservedInstanceRecord>() {
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
        dummySpec.setId(706803569926466L);
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
                .setReservedInstanceSpec(706803569926466L)

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
        rec1.setReservedInstanceSpecId(706803569926466L);
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
                .setReservedInstanceSpec(706803569926466L)

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
        rec2.setReservedInstanceSpecId(706803569926466L);
        rec2.setTopologyContextId(777777L);
        return rec2;
    }
}
