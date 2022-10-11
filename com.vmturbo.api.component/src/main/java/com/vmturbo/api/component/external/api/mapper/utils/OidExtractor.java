package com.vmturbo.api.component.external.api.mapper.utils;

import java.util.Objects;
import java.util.OptionalLong;
import java.util.function.Function;
import java.util.function.Predicate;

import javax.annotation.Nonnull;

/**
 * Extractor of an OID reference from a protobuf message. The extractor is responsible for first checking
 * whether the protobuf message attribute is set.
 * @param <ProtoMessageT> The protobuf message type.
 */
public class OidExtractor<ProtoMessageT> {

    private final Predicate<ProtoMessageT> attributePredicate;

    private final Function<ProtoMessageT, Long> extractorFunction;

    private OidExtractor(@Nonnull Predicate<ProtoMessageT> attributePredicate,
                         @Nonnull Function<ProtoMessageT, Long> extractorFunction) {
        this.attributePredicate = Objects.requireNonNull(attributePredicate);
        this.extractorFunction = Objects.requireNonNull(extractorFunction);
    }

    /**
     * Creates a new {@link OidExtractor} instance.
     * @param attributePredicate The attribute predicate, typically mapping to "has" methods in protobuf messages.
     * @param extractorFunction the extractor function (the "get" method).
     * @param <ProtoMessageT> The protobuf message type.
     * @return The newly created {@link OidExtractor}.
     */
    @Nonnull
    public static <ProtoMessageT> OidExtractor<ProtoMessageT> of(@Nonnull Predicate<ProtoMessageT> attributePredicate,
                                                                 @Nonnull Function<ProtoMessageT, Long> extractorFunction) {
        return new OidExtractor<>(attributePredicate, extractorFunction);
    }

    /**
     * Resolves the OID from the provided message.
     * @param protoMessage The protobuf message.
     * @return The OID extracted from the protobuf message or an empty {@link OptionalLong}, if the attribute
     * predicate indicates the attribute is not set.
     */
    @Nonnull
    public OptionalLong resolveEntityOid(@Nonnull ProtoMessageT protoMessage) {

        if (attributePredicate.test(protoMessage)) {
            return OptionalLong.of(extractorFunction.apply(protoMessage));
        } else {
            return OptionalLong.empty();
        }
    }
}
