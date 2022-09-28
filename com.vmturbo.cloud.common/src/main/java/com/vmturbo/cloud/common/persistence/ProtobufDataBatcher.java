package com.vmturbo.cloud.common.persistence;

import java.util.List;

import com.google.protobuf.MessageLite;

import org.jetbrains.annotations.NotNull;

/**
 * A {@link DataBatcher} implementation, supporting protobuf messages and determining batch size
 * based on serialized size of the protobuf messages.
 * @param <T> The protobuf message type.
 */
public abstract class ProtobufDataBatcher<T extends MessageLite> implements DataBatcher<T> {


    private final int softMaxSize;

    protected ProtobufDataBatcher(int softMaxSize) {
        this.softMaxSize = softMaxSize;
    }

    @Override
    public boolean isFullBatch(@NotNull List<T> dataList) {

        final int totalSize = dataList.stream()
                .mapToInt(MessageLite::getSerializedSize)
                .sum();
        return totalSize >= softMaxSize;
    }
}
