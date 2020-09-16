package com.vmturbo.cost.component.reserved.instance;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.jooq.DSLContext;

import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceCostStat;
import com.vmturbo.components.common.diagnostics.DiagnosticsAppender;
import com.vmturbo.components.common.diagnostics.DiagnosticsException;
import com.vmturbo.cost.component.reserved.instance.filter.ReservedInstanceBoughtFilter;
import com.vmturbo.cost.component.reserved.instance.filter.ReservedInstanceCostFilter;
import com.vmturbo.platform.sdk.common.PricingDTO.ReservedInstancePrice;

/**
 * An empty implementation of {@link ReservedInstanceBoughtStore}, used to ignore the RI inventory.
 */
public class EmptyReservedInstanceBoughtStore implements ReservedInstanceBoughtStore {

    private static final String DIAG_FILE_NAME = "EmptyReservedInstanceBought_dump";

    @Nonnull
    @Override
    public List<ReservedInstanceBought> getReservedInstanceBoughtByFilter(@Nonnull final ReservedInstanceBoughtFilter filter) {
        return Collections.emptyList();
    }

    @Nonnull
    @Override
    public List<ReservedInstanceBought> getReservedInstanceBoughtByFilterWithContext(
            @Nonnull final DSLContext context,
            @Nonnull final ReservedInstanceBoughtFilter filter) {

        return Collections.emptyList();
    }

    @Nonnull
    @Override
    public Map<Long, Long> getReservedInstanceCountMap(@Nonnull final ReservedInstanceBoughtFilter filter) {
        return Collections.emptyMap();
    }

    @Nonnull
    @Override
    public Map<Long, Long> getReservedInstanceCountByRISpecIdMap(final ReservedInstanceBoughtFilter filter) {
        return Collections.emptyMap();
    }

    @Override
    public void updateReservedInstanceBought(
            @Nonnull final DSLContext context,
            @Nonnull final List<ReservedInstanceBoughtInfo> newReservedInstances) {

        // no-op
    }

    @Override
    public void updateRIBoughtFromRIPriceList(@Nonnull final Map<Long, ReservedInstancePrice> reservedInstanceSpecPrices) {
        // no-op
    }

    @Override
    public void onInventoryChange(@Nonnull final Runnable callback) {
        // no-op
    }

    @Override
    public List<ReservedInstanceBought> getReservedInstanceBoughtForAnalysis(@Nonnull final ReservedInstanceBoughtFilter filter) {
        return Collections.emptyList();
    }

    @Override
    public void restoreDiags(@Nonnull final List<String> collectedDiags, @Nullable Void context) throws DiagnosticsException {
        // no-op
    }

    @Override
    public void collectDiags(@Nonnull final DiagnosticsAppender appender) throws DiagnosticsException {
        // no-op
    }

    @Nonnull
    @Override
    public String getFileName() {
        return DIAG_FILE_NAME;
    }

    @Override
    public ReservedInstanceCostStat getReservedInstanceAggregatedCosts(
            @Nonnull final ReservedInstanceCostFilter filter) {

        return ReservedInstanceCostStat.getDefaultInstance();
    }

    @Nonnull
    @Override
    public List<ReservedInstanceCostStat> queryReservedInstanceBoughtCostStats(
            @Nonnull final ReservedInstanceCostFilter reservedInstanceCostFilter) {
        return Collections.emptyList();
    }

    @Nonnull
    @Override
    public Map<Long, Double> getNumberOfUsedCouponsForReservedInstances(
            @Nonnull final Collection<Long> filterByReservedInstanceIds) {
        return Collections.emptyMap();
    }

    @Nonnull
    @Override
    public Map<Long, Double> getNumberOfUsedCouponsForReservedInstances(
            @Nonnull final DSLContext context,
            @Nonnull final Collection<Long> reservedInstanceIds) {
        return Collections.emptyMap();
    }
}
