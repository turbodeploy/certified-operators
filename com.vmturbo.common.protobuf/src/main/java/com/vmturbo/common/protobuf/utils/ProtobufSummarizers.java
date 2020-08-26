package com.vmturbo.common.protobuf.utils;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import com.google.protobuf.Message;
import com.google.protobuf.MessageOrBuilder;

import com.vmturbo.common.protobuf.stats.Stats.GetAveragedEntityStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats.GetAveragedEntityStatsRequestOrBuilder;

/**
 * Class that can be used to define {@link ProtobufSummarizer} instances for general use.
 *
 * <p>Note: The intention is for a given summarizer (other than the default) to handle both a
 * given type of message as well as builders of the associated builder type. Therefore you should
 * use your type's `XxxOrBuilder` type to parameterize your summarizer. However, in order to
 * make registration easy, you'll be passing the associated message class when creating your
 * summarizer. Due to limitations in Java Generics, we cannot (or at least have not figured out
 * how to) guarantee that your summarizer's type parameter actually is a `XxxOrBuilder` type, nor
 * that the summmarizer's bound type and the class passed when creating it are related. I.e. if you
 * try you can create a a `ProtobufSummarizer&lt;XxxOrBuilder&gt;` but pass `Yyy.class` to the builder.
 * It probably won't be very useful.</p>
 *
 * <p>Don't forget to register any summarizers you create; just tack `.register()` onto the
 * `.build()` that finishes your construction. It's not automatic because you might create
 * several summarizers for the same type but should only register one of them!</p>
 *
 * <p>On that last point, if multiple summarizers are registered for the same message type, only
 * the last one registered will count.</p>
 */
public class ProtobufSummarizers {

    private ProtobufSummarizers() {
    }

    private static final Map<Class<? extends MessageOrBuilder>, ProtobufSummarizer<?>> registry
            = new HashMap<>();

    /**
     * Summarize a protobuf message, using a summarizer registered for the message type, or using
     * the default summarizer if no specific summarizer is registered.
     *
     * @param msg protobuf message (or builder)
     * @param <T> type of message (or builder)
     * @return message summary
     */
    public static <T extends MessageOrBuilder> String summarize(T msg) {
        return get(msg).summarize(msg);
    }

    /**
     * Get the summarizer that would be used to summarize the given protobuf message
     *
     * <p>If there is no registered summarizer specifically for this type, the default summarizer
     * is returned.</p>
     *
     * @param msg protobuf message (or builder)
     * @param <T> type of message (or builder)
     * @return registered summarizer, or the default summarizer
     */
    public static <T extends MessageOrBuilder> ProtobufSummarizer<T> get(T msg) {
        //noinspection unchecked
        final ProtobufSummarizer<T> summarizer = (ProtobufSummarizer<T>)get(msg.getClass());
        return summarizer;
    }

    /**
     * Get the summarizer that would be used to summarize a message (or builder) of the given
     * type.
     *
     * <p>If there is no registered summarizer specifically for this type, the default summarizer
     * is returned.</p>
     *
     * @param cls type of message (or builder)
     * @param <T> type of message (or builder)
     * @return summarizer
     */
    public static <T extends MessageOrBuilder> ProtobufSummarizer<T> get(Class<T> cls) {
        //noinspection unchecked
        final ProtobufSummarizer<T> orDefault = (ProtobufSummarizer<T>)registry.getOrDefault(
                cls, DEFAULT_SUMMARIZER);
        return (ProtobufSummarizer<T>)orDefault;
    }

    /**
     * A summarizer that can be used with any protobuf message or builder and use default settings.
     *
     * <p>This is not registered; it will be used by this class's {@link
     * #summarize(MessageOrBuilder)} method if no registered summarizer is found.</p>
     */
    public static final ProtobufSummarizer<MessageOrBuilder> DEFAULT_SUMMARIZER =
            ProtobufSummarizer.of(MessageOrBuilder.class, Message.class)
                    .build();

    /**
     * A summarizer for the GetAveragedEntityStats rpc request type.
     *
     * <p>If an entities list is present, its rendering will be limited to at most two rendered
     * values.</p>
     */
    public static final ProtobufSummarizer<GetAveragedEntityStatsRequestOrBuilder> GET_AVERAGED_ENTITY_STATS_REQUEST_SUMMARIZER =
            ProtobufSummarizer.<GetAveragedEntityStatsRequestOrBuilder>of(
                    GetAveragedEntityStatsRequestOrBuilder.class, GetAveragedEntityStatsRequest.class)
                    .limitingArray("entities", 2)
                    .build()
            .register();

    static <T extends MessageOrBuilder> ProtobufSummarizer<T> register(ProtobufSummarizer<T> summarizer) {
        Class<? extends Message> type = summarizer.getType();
        registry.put(type, summarizer);
        getBuilderType(type).map(mtype -> registry.put(mtype, summarizer));
        return summarizer;
    }

    private static Optional<Class<? extends Message.Builder>>
    getBuilderType(Class<? extends Message> type) {
        return getInstance(type).map(msg -> msg.toBuilder().getClass());
    }

    private static <M extends Message> Optional<Message> getInstance(Class<M> msgClass) {
        try {
            return Optional.of((M)msgClass.getDeclaredMethod("getDefaultInstance").invoke(null));
        } catch (IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
            return Optional.empty();
        }
    }
}
