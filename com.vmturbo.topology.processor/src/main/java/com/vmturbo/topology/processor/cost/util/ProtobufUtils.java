package com.vmturbo.topology.processor.cost.util;

import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.Type;

/**
 * Utility class for Protobuf messages.
 */
public final class ProtobufUtils {

    /**
     * Binary size of the largest VARINT value, an up-to-128-bit number with a termination bit.
     */
    public static final int MAX_VARINT_SIZE = 10 /* bytes */;

    /**
     * While it would take a very large proto definition, the field tag can be as large as the
     * largest VARINT.
     */
    public static final int TAG_SIZE = MAX_VARINT_SIZE;

    private ProtobufUtils() {}

    /**
     * Recursively determines the worst case size of the Protobuf message and its fields, assuming
     * there are a very large number of fields defined, and assuming that all fields are very large
     * 10-byte values. This ignores repeated, map, string, and bytes fields, because their size is
     * unbounded.
     *
     * <p>See https://developers.google.com/protocol-buffers/docs/encoding#varints
     *
     * @param messageDescriptor Protobuf message definition
     * @return the worst case number of bytes needed to serialize the message
     */
    public static int getMaximumMessageSizeExcludingRepeatedFields(
            @Nonnull final Descriptor messageDescriptor) {

        final Predicate<FieldDescriptor> isRepeatedField =
                field -> field.isRepeated() || field.isMapField() || field.getType() == Type.STRING
                        || field.getType() == Type.BYTES;

        final List<FieldDescriptor> nonRepeatedFields = messageDescriptor.getFields()
                .stream()
                .filter(isRepeatedField.negate())
                .collect(Collectors.toList());

        // Assume all fields are (tag : varint). Tags can be as large as varints, potentially.
        // This is an overestimation because varints are, well, variable in size, and also because
        // varint is the largest non-repeated type. For example, a boolean will really be 1 byte.
        final int nonRepeatedFieldsSize = nonRepeatedFields.size() * (TAG_SIZE + MAX_VARINT_SIZE);

        final int messageSubfieldsSize = nonRepeatedFields.stream()
                .filter(f -> f.getType() == Type.MESSAGE)
                .map(FieldDescriptor::getMessageType)
                .mapToInt(ProtobufUtils::getMaximumMessageSizeExcludingRepeatedFields)
                .sum();

        return nonRepeatedFieldsSize + messageSubfieldsSize;
    }
}
