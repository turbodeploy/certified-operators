package com.vmturbo.cost.component.stores;

import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import org.jetbrains.annotations.NotNull;

import com.vmturbo.common.protobuf.cloud.CloudCommitmentServices.TopologyType;

/**
 * In-memory source and projected fields data store.
 *
 * @param <DataTypeT> type of data.
 * @param <FilterTypeT> The filter type.
 * @param <S> type of store.
 */
@ThreadSafe
public class InMemorySourceProjectedFieldsDataStore<DataTypeT, FilterTypeT, S extends SingleFieldDataStore<DataTypeT, FilterTypeT>>
        implements SourceProjectedFieldsDataStore<DataTypeT, FilterTypeT> {

    private final S sourceSingleFieldDataStore;
    private final S projectedSingleFieldDataStore;

    /**
     * Constructor.
     *
     * @param sourceSingleFieldDataStore the source store.
     * @param projectedSingleFieldDataStore the projected store.
     */
    public InMemorySourceProjectedFieldsDataStore(@Nonnull final S sourceSingleFieldDataStore,
            @Nonnull final S projectedSingleFieldDataStore) {
        this.sourceSingleFieldDataStore = sourceSingleFieldDataStore;
        this.projectedSingleFieldDataStore = projectedSingleFieldDataStore;
    }

    @Nonnull
    @Override
    public Optional<DataTypeT> getProjectedData() {
        return this.projectedSingleFieldDataStore.getData();
    }

    @Override
    public Optional<DataTypeT> filterData(@NotNull TopologyType topologyType, @NotNull FilterTypeT filter) {

        if (topologyType == TopologyType.TOPOLOGY_TYPE_SOURCE) {
            return sourceSingleFieldDataStore.filterData(filter);
        } else if (topologyType == TopologyType.TOPOLOGY_TYPE_PROJECTED) {
            return projectedSingleFieldDataStore.filterData(filter);
        } else {
            return Optional.empty();
        }
    }

    @Override
    public void setProjectedData(@Nullable final DataTypeT data) {
        this.projectedSingleFieldDataStore.setData(data);
    }

    @Nonnull
    @Override
    public Optional<DataTypeT> getSourceData() {
        return this.sourceSingleFieldDataStore.getData();
    }

    @Override
    public void setSourceData(@Nullable final DataTypeT data) {
        this.sourceSingleFieldDataStore.setData(data);
    }
}
