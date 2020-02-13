package com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.demand;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;

import com.vmturbo.cost.component.db.tables.records.ComputeTierTypeHourlyByWeekRecord;
import com.vmturbo.cost.component.reserved.instance.ComputeTierDemandStatsStore;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.ReservedInstanceHistoricalDemandDataType;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.ReservedInstanceZonalContext;

/**
 * Provides a fascade around the historical demand stored in the {@link ComputeTierDemandStatsStore},
 * in order to support buy RI analysis.
 */
public class RIBuyHistoricalDemandProvider {

    private final ComputeTierDemandStatsStore demandStatsStore;

    /**
     * The size of the demand data for a {@link ReservedInstanceZonalContext} from the
     * instance_type_hourly_by_week table. The size is total number of hours per week: 168 = 24 * 7.
     */
    private static final int WEEKLY_DEMAND_DATA_SIZE = 168;

    // Set the default value as -1.
    private static final int NO_DEMAND_DEFAULT_VALUE = -1;

    /**
     * Constructs a new instance of {@link RIBuyHistoricalDemandProvider}.
     *
     * @param demandStatsStore The store containing historical demand used for buy RI analysis.
     */
    public RIBuyHistoricalDemandProvider(@Nonnull ComputeTierDemandStatsStore demandStatsStore) {
        this.demandStatsStore = Objects.requireNonNull(demandStatsStore);
    }

    /**
     * Get demand array from historical demand stats records from the database.
     *
     * @param demandCluster The target demand cluster (account, region, compute tier, etc).
     * @param demandType name of the field that should be used to get demand data.
     * @return Demand array from weekly or monthly records from the database.
     */
    @Nonnull
    public float[] getDemand(RIBuyDemandCluster demandCluster,
                      ReservedInstanceHistoricalDemandDataType demandType) {

        final List<ComputeTierTypeHourlyByWeekRecord> records =
                demandStatsStore.fetchDemandStats(demandCluster);
        // Create a demand data array with the size of the weekly demand data size which is 168 = 24 * 7
        return getDemandFromRecords(records, demandType);
    }

    /**
     * Get demand array from demand stats records from the database.
     *
     * @param records Demand stats records from the database.
     * @param demandDataType Type of the demand stats.
     * @return Demand stats array
     */
    @Nonnull
    @VisibleForTesting
    float[] getDemandFromRecords(@Nonnull List<ComputeTierTypeHourlyByWeekRecord> records,
                                 @Nonnull ReservedInstanceHistoricalDemandDataType demandDataType) {

        final float[] demands = new float[WEEKLY_DEMAND_DATA_SIZE];
        Arrays.fill(demands, NO_DEMAND_DEFAULT_VALUE);
        for (ComputeTierTypeHourlyByWeekRecord record : records) {
            final int index = record.getHour() + (record.getDay() - 1) * 24;
            demands[index] =
                    demandDataType.getDbFieldName().equals(
                            ReservedInstanceHistoricalDemandDataType.ALLOCATION.getDbFieldName()) ?
                            record.getCountFromSourceTopology().floatValue()
                            : record.getCountFromProjectedTopology().floatValue();
        }
        return demands;
    }
}
