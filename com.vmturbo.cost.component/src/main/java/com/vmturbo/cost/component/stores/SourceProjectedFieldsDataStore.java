package com.vmturbo.cost.component.stores;

import java.util.Optional;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.cloud.CloudCommitmentServices.TopologyType;

/**
 * Source and projected fields data store.
 *
 * @param <T> type of data.
 * @param <FilterTypeT> The filter type.
 */
public interface SourceProjectedFieldsDataStore<T, FilterTypeT> {

    /**
     * Sets the source {@link T}.
     *
     * @param data the {@link T}.
     */
    void setSourceData(@Nonnull T data);

    /**
     * Sets the projected {@link T}.
     *
     * @param data the {@link T}.
     */
    void setProjectedData(@Nonnull T data);

    /**
     * Gets the source {@link T}.
     *
     * @return the {@link T}.
     */
    @Nonnull
    Optional<T> getSourceData();

    /**
     * Gets the projected {@link T}.
     *
     * @return the {@link T}.
     */
    @Nonnull
    Optional<T> getProjectedData();

    /**
     * Filter the data associated with {@code topologyType} by {@code filter}.
     * @param topologyType THe topology type, linking directly to either {@link #getSourceData()} or {@link #getProjectedData()}.
     * @param filter The data filter.
     * @return The filtered data associated with the topology type.
     */
    @Nonnull
    Optional<T> filterData(@Nonnull TopologyType topologyType,
                           @Nonnull FilterTypeT filter);
}
