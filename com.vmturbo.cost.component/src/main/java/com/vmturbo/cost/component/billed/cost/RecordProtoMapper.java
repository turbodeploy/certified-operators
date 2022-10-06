package com.vmturbo.cost.component.billed.cost;

import java.util.function.BiConsumer;
import java.util.function.Function;

import javax.annotation.Nonnull;

import org.immutables.value.Value.Immutable;
import org.jooq.Field;
import org.jooq.Record;

import com.vmturbo.cloud.common.immutable.HiddenImmutableTupleImplementation;

/**
 * A mapper of jOOQ record data to a protobuf message representation. This class is responsible for both verifying the target
 * field is present in the record and verifying the corresponding value is non-null, prior to updating the protobuf builder.
 * Checking for non-null is necessary for primitive data types in the protobuf builder, due to implicit casting of the wrapper
 * value form the jOOQ record, which may lead to an NPE.
 * @param <BuilderT> The protobuf builder type.
 * @param <T> The record and builder data type.
 */
@HiddenImmutableTupleImplementation
@Immutable
interface RecordProtoMapper<BuilderT, T> {

    Function<BilledCostTableAccessor<?>, Field<T>> fieldExtractor();

    BiConsumer<BuilderT, T> costItemSetter();

    default void updateProtoBuilder(@Nonnull Record record,
                                    @Nonnull BilledCostTableAccessor<?> tableAccessor,
                                    @Nonnull BuilderT protoBuilder) {

        final Field<T> field = fieldExtractor().apply(tableAccessor);
        // check that the field was queried before attempting to get the value of the field
        if (record.field(field) != null) {
            final T recordValue = record.get(field);
            if (recordValue != null) {
                costItemSetter().accept(protoBuilder, recordValue);
            }
        }
    }

    static <BuilderT, T> RecordProtoMapper<BuilderT, T> of(@Nonnull Function<BilledCostTableAccessor<?>, Field<T>> fieldExtractor,
                                                           @Nonnull BiConsumer<BuilderT, T> costItemSetter) {

        return RecordProtoMapperTuple.of(fieldExtractor, costItemSetter);
    }
}
