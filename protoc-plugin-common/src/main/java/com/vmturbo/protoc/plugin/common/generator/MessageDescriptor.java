package com.vmturbo.protoc.plugin.common.generator;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;

/**
 * A wrapper around {@link DescriptorProto} with additional information and utility methods.
 */
@Immutable
public class MessageDescriptor extends AbstractDescriptor {
    /**
     * The comment for this message.
     */
    private final String comment;

    /**
     * The proto describing the message.
     */
    private final DescriptorProto descriptorProto;

    /**
     * The messages and enums nested in this message. Cases like:
     * message TestMsg {
     *     message NestedMsg { }
     *     enum NestedEnum { }
     * }
     */
    private final ImmutableList<AbstractDescriptor> nestedMessages;

    /**
     * The descriptors for the fields of this message. For instance:
     * message TestMsg {
     *     optional int64 one_field = 1;
     *     optional SomeType two_field = 2;
     * }
     */
    private final ImmutableList<FieldDescriptor> fieldDescriptors;

    public MessageDescriptor(@Nonnull final FileDescriptorProcessingContext context,
                             @Nonnull final DescriptorProto descriptorProto,
                             @Nonnull final ImmutableList<AbstractDescriptor> nestedMessages) {
        super(context, descriptorProto.getName());
        this.descriptorProto = descriptorProto;
        this.nestedMessages = nestedMessages;
        this.comment = context.getCommentAtPath();
        context.startFieldList();

        // Pre-parse to build up the list of field names that will map to the same Java
        // name, so that individual FieldDescriptors can take measures to avoid duplication.
        final Map<String, Boolean> duplicateNameMap = new HashMap<>();
        for (FieldDescriptorProto field : descriptorProto.getFieldList()) {
            final String formattedName = FieldDescriptor.formatFieldName(field.getName());
            // Put whether or not the map already contains an entry for this name.
            duplicateNameMap.put(formattedName, duplicateNameMap.containsKey(formattedName));
        }

        final ImmutableList.Builder<FieldDescriptor> fieldDescriptorBuilder = ImmutableList.builder();
        for (int i = 0; i < descriptorProto.getFieldCount(); ++i) {
            context.startListElement(i);

            final FieldDescriptor fieldDescriptor = new FieldDescriptor(context, descriptorProto,
                    descriptorProto.getField(i), duplicateNameMap);
            fieldDescriptorBuilder.add(fieldDescriptor);

            context.endListElement();
        }
        context.endFieldList();
        fieldDescriptors = fieldDescriptorBuilder.build();
    }

    @Nonnull
    public String getComment() {
        return comment;
    }

    @Nonnull
    public List<AbstractDescriptor> getNestedMessages() {
        return nestedMessages;
    }

    @Nonnull
    public List<FieldDescriptor> getFieldDescriptors() {
        return fieldDescriptors;
    }

    // START - MapEntry related methods.
    // The protobuf compiler generates MapEntry DescriptorProtos when maps
    // are present. For example:
    // message TestMsg {
    //    map<string, string> test_map = 1;
    //
    // Will actually appear as the equivalent of:
    // message TestMsg {
    //    message <MapEntry> {...}
    //    repeated <MapEntry> test_map = 1;
    // }
    // From the point of view of the plugin.

    /**
     * This should return true if this descriptor describes a MapEntry object.
     */
    public boolean isMapEntry() {
        return descriptorProto.hasOptions() && descriptorProto.getOptions().getMapEntry();
    }

    /**
     * Only called for MapEntry objects. Return the descriptor of the key.
     *
     * @return The descriptor of the key.
     */
    @Nonnull
    private FieldDescriptor getMapKey() {
        assert(isMapEntry());
        return fieldDescriptors.get(0);
    }

    /**
     * Only called for MapEntry objects. Return the descriptor of the value.
     *
     * @return The descriptor of the value.
     */
    @Nonnull
    public FieldDescriptor getMapValue() {
        assert (isMapEntry());
        return fieldDescriptors.get(1);
    }

    public DescriptorProto getDescriptorProto() {
        return descriptorProto;
    }

    /**
     * Only called for MapEntry objects. Return the typename that will describe
     * the map for code generation.
     *
     * @return The type name (e.g. "Map<String,String>")
     */
    @Nonnull
    String getMapTypeName() {
        // Doesn't support map of maps.
        assert (isMapEntry());
        FieldDescriptor key = getMapKey();
        FieldDescriptor value = getMapValue();
        return "Map<" + key.getTypeName() + "," + value.getTypeName() + ">";
    }
    // END - MapEntry related methods.
}
