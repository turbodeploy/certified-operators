package com.vmturbo.cloud.common.persistence;

import java.util.List;

import com.vmturbo.components.common.utils.ThrowingFunction;

/**
 * A data sink, accepting a full batch of data of {@link DataTypeT}, returning data-specific stats.
 * @param <DataTypeT> The data type.
 * @param <DataSpecificStatsT> The data-specific stats type.
 */
public interface DataSink<DataTypeT, DataSpecificStatsT> extends ThrowingFunction<List<DataTypeT>, DataSpecificStatsT, Exception>  {}
