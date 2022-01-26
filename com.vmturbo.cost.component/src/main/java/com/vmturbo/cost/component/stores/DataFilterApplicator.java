package com.vmturbo.cost.component.stores;

import javax.annotation.Nullable;

/**
 * An applicator of a filter type to a data type.
 * @param <DataTypeT> The data type.
 * @param <FilterTypeT> The filter type.
 */
public interface DataFilterApplicator<DataTypeT, FilterTypeT> {

    /**
     * Filter {@code sourceData} by {@code filter}.
     * @param sourceData The source data.
     * @param filter The filter.
     * @return The filtered data.
     */
    @Nullable
    DataTypeT filterData(DataTypeT sourceData, FilterTypeT filter);
}
