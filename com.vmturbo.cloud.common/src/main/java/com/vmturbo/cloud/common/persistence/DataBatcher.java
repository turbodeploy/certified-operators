package com.vmturbo.cloud.common.persistence;

import java.util.List;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;

import org.jetbrains.annotations.NotNull;

/**
 * Responsible for determining whether data submitted to a data queue represents a complete batch
 * of data to flush to the data sink. Additionally, responsible for splitting data submitted to
 * the queue that is deemed to large too pass to the data sink.
 * @param <DataTypeT> The data type.
 */
public interface DataBatcher<DataTypeT> {

    /**
     * Determines whether the provided {@code dataList} represents a full batch.
     * @param dataList The data list.
     * @return true, if the data list is a full batch. False, otherwise.
     */
    boolean isFullBatch(@Nonnull List<DataTypeT> dataList);

    /**
     * Splits the provided {@code data} into batches.
     * @param data the data.
     * @return A list of batches containing all data from the source.
     */
    @Nonnull
    List<DataTypeT> splitBatches(@Nonnull DataTypeT data);

    /**
     * A {@link DataBatcher} which directly passes on data added to the queue to the
     * configured data sink, without attempting to group the data into batches.
     * @param <DataTypeT> The data type.
     */
    class DirectDataBatcher<DataTypeT> implements DataBatcher<DataTypeT> {

        @Override
        public boolean isFullBatch(@NotNull List<DataTypeT> dataList) {
            return true;
        }

        @NotNull
        @Override
        public List<DataTypeT> splitBatches(@NotNull DataTypeT data) {
            return ImmutableList.of(data);
        }

        static <DataTypeT> DirectDataBatcher<DataTypeT> create() {
            return new DirectDataBatcher<>();
        }
    }
}
