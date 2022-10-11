package com.vmturbo.api.component.external.api.mapper.stat;

import java.util.Objects;
import java.util.function.Function;
import java.util.function.Predicate;

import javax.annotation.Nonnull;

import com.google.protobuf.ProtocolMessageEnum;

import com.vmturbo.api.dto.statistic.StatApiDTO;

/**
 * {@link StatAttributeMapper} for mapping a protobuf enum to a {@link com.vmturbo.api.dto.statistic.StatFilterApiDTO}
 * instance on the {@link StatApiDTO}.
 * @param <InternalTypeT> The internal stat type.
 */
public class EnumFilterMapper<InternalTypeT> implements StatAttributeMapper<InternalTypeT> {

    private final Predicate<InternalTypeT> attributePredicate;

    private final Function<InternalTypeT, ProtocolMessageEnum> enumValueExtractor;

    private EnumFilterMapper(@Nonnull Predicate<InternalTypeT> attributePredicate,
                             @Nonnull Function<InternalTypeT, ProtocolMessageEnum> enumValueExtractor) {

        this.attributePredicate = Objects.requireNonNull(attributePredicate);
        this.enumValueExtractor = Objects.requireNonNull(enumValueExtractor);
    }

    /**
     * Creates a new {@link EnumFilterMapper} instance.
     * @param attributePredicate A predicate for checking whether the attribute is set on {@link InternalTypeT}.
     * @param enumValueExtractor The enum value extractor from {@link InternalTypeT}.
     * @param <InternalTypeT> The internal stat representation.
     * @return The new {@link EnumFilterMapper} instance.
     */
    @Nonnull
    public static <InternalTypeT> EnumFilterMapper<InternalTypeT> of(@Nonnull Predicate<InternalTypeT> attributePredicate,
                                                                     @Nonnull Function<InternalTypeT, ProtocolMessageEnum> enumValueExtractor) {
        return new EnumFilterMapper<>(attributePredicate, enumValueExtractor);
    }

    @Override
    public void updateStatDto(@Nonnull InternalTypeT internalStat, @Nonnull StatApiDTO statDto) {

        if (attributePredicate.test(internalStat)) {
            final ProtocolMessageEnum enumValue = enumValueExtractor.apply(internalStat);

            statDto.addFilter(
                    enumValue.getDescriptorForType().getName(),
                    enumValue.getValueDescriptor().getName());
        }
    }
}
