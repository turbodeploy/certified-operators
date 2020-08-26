package com.vmturbo.cost.component.reserved.instance;

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

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public List<ReservedInstanceBought> getReservedInstanceBoughtByFilter(@Nonnull final ReservedInstanceBoughtFilter filter) {
        return Collections.emptyList();
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public List<ReservedInstanceBought> getReservedInstanceBoughtByFilterWithContext(
            @Nonnull final DSLContext context,
            @Nonnull final ReservedInstanceBoughtFilter filter) {

        return Collections.emptyList();
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public Map<Long, Long> getReservedInstanceCountMap(@Nonnull final ReservedInstanceBoughtFilter filter) {
        return Collections.emptyMap();
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public Map<Long, Long> getReservedInstanceCountByRISpecIdMap(final ReservedInstanceBoughtFilter filter) {
        return Collections.emptyMap();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateReservedInstanceBought(
            @Nonnull final DSLContext context,
            @Nonnull final List<ReservedInstanceBoughtInfo> newReservedInstances) {

        // no-op
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateRIBoughtFromRIPriceList(@Nonnull final Map<Long, ReservedInstancePrice> reservedInstanceSpecPrices) {
        // no-op
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onInventoryChange(@Nonnull final Runnable callback) {
        // no-op
    }

    @Override
    public List<ReservedInstanceBought> getReservedInstanceBoughtForAnalysis(@Nonnull final ReservedInstanceBoughtFilter filter) {
        return Collections.emptyList();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void restoreDiags(@Nonnull final List<String> collectedDiags, @Nullable Void context) throws DiagnosticsException {
        // no-op
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void collectDiags(@Nonnull final DiagnosticsAppender appender) throws DiagnosticsException {
        // no-op
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public String getFileName() {
        return DIAG_FILE_NAME;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ReservedInstanceCostStat getReservedInstanceAggregatedCosts(
            @Nonnull final ReservedInstanceCostFilter filter) {

        return ReservedInstanceCostStat.getDefaultInstance();
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public List<ReservedInstanceCostStat> queryReservedInstanceBoughtCostStats(
            @Nonnull final ReservedInstanceCostFilter reservedInstanceCostFilter) {

        return Collections.emptyList();
    }


}
