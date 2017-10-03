package com.vmturbo.protoc.spring.rest.generator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import org.apache.commons.lang3.StringUtils;
import org.stringtemplate.v4.ST;

import com.google.common.base.CaseFormat;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.EnumDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Label;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type;
import com.google.protobuf.DescriptorProtos.MethodDescriptorProto;
import com.google.protobuf.DescriptorProtos.ServiceDescriptorProto;

import com.vmturbo.protoc.spring.rest.generator.FileDescriptorProcessingContext.OuterClass;

/**
 * This class contains the various descriptor definitions.
 * <p>
 * The descriptor classes wrap around the descriptor protobuf messages
 * provided by the protobuf compiler to provide additional information
 * for code generation.
 */
public class Descriptors {

    @Immutable
    public abstract static class AbstractDescriptor {

        /**
         * The non-qualified name of this descriptor.
         */
        protected final String name;

        /**
         * The Java package this descriptor's code will be in.
         */
        protected final String javaPkgName;

        /**
         * The protobuf package that defined the descriptor.
         */
        protected final String protoPkgName;

        /**
         * The outer class the descriptor's code will be in.
         */
        protected final OuterClass outerClass;

        protected final Registry registry;

        /**
         * The names of the outer messages for this descriptor.
         * Only non-empty for nested {@link MessageDescriptor} and {@link EnumDescriptor}
         * definitions.
         */
        private final List<String> outerMessages;

        public AbstractDescriptor(@Nonnull final FileDescriptorProcessingContext context,
                                  @Nonnull final String name) {
            this.name = name;
            this.javaPkgName = context.getJavaPackage();
            this.protoPkgName = context.getProtobufPackage();
            this.outerClass = context.getOuterClass();

            // Report the name of the newly instantiated descriptor
            // to the outer class. This is to track name collisions
            // between the outer class and the descriptors within it.
            outerClass.onNewDescriptor(name);

            this.registry = context.getRegistry();
            // Make a copy, because the outers in the context change
            // during processing.
            this.outerMessages = new ArrayList<>(context.getOuters());
        }

        /**
         * Get the unqualified name of this message. For example:
         * message Msg { <-- Msg
         *     message Msg2 { <-- Msg2
         *         message Msg3 {} <-- Msg3
         *     }
         * }
         * @return The unqualified name of the message.
         */
        @Nonnull
        String getName() {
            return name;
        }

        /**
         * Gets the name of this message within the overarching outer class
         * that's generated for every .proto.
         * <p>
         * If the message is NOT nested, this name is equivalent to
         * {@link AbstractDescriptor#getName}.
         * <p>
         * Otherwise, it's the path to the nested message. For example:
         * message Msg { <-- Msg
         *     message Msg2 { <-- Msg.Msg2
         *         message Msg3 {} <-- Msg.Msg2.Msg3
         *     }
         * }
         * @return The name of this message qualified within the overarching
         *         outer class.
         */
        @Nonnull
        String getNameWithinOuterClass() {
            if (outerMessages == null || outerMessages.isEmpty()) {
                return name;
            } else {
                return StringUtils.join(outerMessages, ".") + "." + name;
            }
        }

        /**
         * Get the fully qualified name of the class this descriptor
         * will generate.
         *
         * @return The name.
         */
        @Nonnull
        String getQualifiedName() {
            return javaPkgName + "." + outerClass.getPluginJavaClass() + "." + getNameWithinOuterClass();
        }

        /**
         * Get the name of the original Java class generated by the
         * protobuf compiler for the message this descriptor relates
         * to.
         *
         * @return The name.
         */
        @Nonnull
        String getQualifiedOriginalName() {
            return javaPkgName + "." + outerClass.getProtoJavaClass() + "." + getNameWithinOuterClass();
        }

        /**
         * Get the fully qualified name of the protobuf message
         * this descriptor relates to.
         *
         * @return The name.
         */
        @Nonnull
        String getQualifiedProtoName() {
            return protoPkgName + "." + getNameWithinOuterClass();
        }

        @Nonnull
        abstract String generateCode();
    }

    /**
     * Descriptor for a service defined in a protobuf. For example:
     * service TestService { ... }
     */
    @Immutable
    public static class ServiceDescriptor extends AbstractDescriptor {
        /**
         * Method name -> Description of that method.
         */
        private final Map<String, String> methodDescriptions = new HashMap<>();

        private final ServiceDescriptorProto serviceDescriptor;

        public ServiceDescriptor(@Nonnull final FileDescriptorProcessingContext context,
                                 @Nonnull final ServiceDescriptorProto serviceDescriptor) {
            super(context, serviceDescriptor.getName());
            this.serviceDescriptor = serviceDescriptor;

            context.startServiceMethodList();
            for (int methodIdx = 0; methodIdx < serviceDescriptor.getMethodCount(); ++methodIdx) {
                context.startListElement(methodIdx);
                final MethodDescriptorProto methodDescriptor =
                        serviceDescriptor.getMethod(methodIdx);
                methodDescriptions.put(methodDescriptor.getName(), context.getCommentAtPath());
                context.endListElement();
            }
            context.endServiceMethodList();

        }

        /**
         * A helper enum to differentiate the types of methods
         * based on the use of client or server side streaming.
         */
        private enum MethodType {
            SIMPLE,
            SERVER_STREAM,
            CLIENT_STREAM,
            BI_STREAM;

            static MethodType fromDescriptor(@Nonnull final MethodDescriptorProto methodDescriptor) {
                if (methodDescriptor.getServerStreaming() && !methodDescriptor.getClientStreaming()) {
                    return MethodType.SERVER_STREAM;
                } else if (methodDescriptor.getClientStreaming() && !methodDescriptor.getServerStreaming()) {
                    return MethodType.CLIENT_STREAM;
                } else if (methodDescriptor.getServerStreaming() && methodDescriptor.getClientStreaming()) {
                    return MethodType.BI_STREAM;
                } else {
                    return MethodType.SIMPLE;
                }
            }
        }

        @Nonnull
        private String generateMethodCode(@Nonnull final MethodDescriptorProto methodDescriptor,
                                          @Nonnull final String responseWrapper) {

            // These casts are totally safe, because the input type is supposed to
            // be a message.
            final MessageDescriptor inputDescriptor =
                    (MessageDescriptor) registry.getMessageDescriptor(methodDescriptor.getInputType());
            final MessageDescriptor outputDescriptor =
                    (MessageDescriptor) registry.getMessageDescriptor(methodDescriptor.getOutputType());

            final MethodType type = MethodType.fromDescriptor(methodDescriptor);
            String requestBodyType = inputDescriptor.getQualifiedName();
            if (type == MethodType.CLIENT_STREAM || type == MethodType.BI_STREAM) {
                requestBodyType = "List<" + requestBodyType + ">";
            }

            String responseBodyType = outputDescriptor.getQualifiedName();
            if (type == MethodType.SERVER_STREAM || type == MethodType.BI_STREAM) {
                responseBodyType = "List<" + responseBodyType + ">";
            }
            responseBodyType = responseWrapper + "<" + responseBodyType + ">";

            return Templates.serviceMethod()
                    .add("resultProto", outputDescriptor.getQualifiedOriginalName())
                    .add("resultType", outputDescriptor.getQualifiedName())
                    .add("requestProto", inputDescriptor.getQualifiedOriginalName())
                    .add("requestType", inputDescriptor.getQualifiedName())
                    .add("responseBodyType", responseBodyType)
                    .add("requestBodyType", requestBodyType)
                    .add("responseWrapper", responseWrapper)
                    .add("methodName", StringUtils.uncapitalize(methodDescriptor.getName()))
                    .add("comments", methodDescriptions.getOrDefault(methodDescriptor.getName(), ""))
                    .add("isClientStream", type == MethodType.BI_STREAM || type == MethodType.CLIENT_STREAM)
                    .add("isSingleResponse", type == MethodType.SIMPLE || type == MethodType.CLIENT_STREAM)
                    .render();
        }

        @Nonnull
        @Override
        String generateCode() {
            final String responseWrapper = serviceDescriptor.getName() + "Response";

            return Templates.service()
                    .add("serviceName", serviceDescriptor.getName())
                    .add("responseWrapper", responseWrapper)
                    .add("package", javaPkgName)
                    .add("methodDefinitions", serviceDescriptor.getMethodList().stream()
                            .map(methodDescriptor -> generateMethodCode(methodDescriptor, responseWrapper))
                            .collect(Collectors.toList()))
                    .render();
        }
    }

    /**
     * Descriptor for a protobuf message. For example:
     * message Test { ... }
     */
    @Immutable
    public static class MessageDescriptor extends AbstractDescriptor {
        private String comment;

        private DescriptorProto descriptorProto;

        /**
         * The messages and enums nested in this message. Cases like:
         * message TestMsg {
         *     message NestedMsg { }
         *     enum NestedEnum { }
         * }
         */
        private ImmutableList<AbstractDescriptor> nestedMessages;

        /**
         * The descriptors for the fields of this message. For instance:
         * message TestMsg {
         *     optional int64 one_field = 1;
         *     optional SomeType two_field = 2;
         * }
         */
        private List<FieldDescriptor> fieldDescriptors = new ArrayList<>();

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
                final String formattedName = FieldDescriptor.formatName(field.getName());
                // Put whether or not the map already contains an entry for this name.
                duplicateNameMap.put(formattedName, duplicateNameMap.containsKey(formattedName));
            }

            for (int i = 0; i < descriptorProto.getFieldCount(); ++i) {
                context.startListElement(i);

                final FieldDescriptor fieldDescriptor = new FieldDescriptor(context, descriptorProto.getField(i), duplicateNameMap);
                fieldDescriptors.add(fieldDescriptor);

                context.endListElement();
            }
            context.endFieldList();
        }

        @Nonnull
        @Override
        String generateCode() {
            final Map<Integer, String> oneofNameMap = new HashMap<>();
            for (int i = 0; i < descriptorProto.getOneofDeclCount(); ++i) {
                oneofNameMap.put(i, descriptorProto.getOneofDecl(i).getName());
            }

            return Templates.message()
                    .add("comment", comment)
                    .add("className", getName())
                    .add("originalProtoType", getQualifiedOriginalName())
                    .add("nestedDefinitions", nestedMessages.stream()
                            .filter(nestedDescriptor -> !(nestedDescriptor instanceof MessageDescriptor &&
                                    ((MessageDescriptor) nestedDescriptor).isMapEntry()))
                            .map(AbstractDescriptor::generateCode)
                            .collect(Collectors.toList()))
                    .add("fieldDeclarations", fieldDescriptors.stream()
                            .map(descriptor -> descriptor.generateDeclaration(oneofNameMap))
                            .collect(Collectors.toList()))
                    .add("setBuilderFields", fieldDescriptors.stream()
                            .map(FieldDescriptor::addToProtoBuilder)
                            .collect(Collectors.toList()))
                    .add("setMsgFields", fieldDescriptors.stream()
                            .map(descriptor -> descriptor.addSetFromProto("newMsg"))
                            .collect(Collectors.toList()))
                    .render();
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
        boolean isMapEntry() {
            return descriptorProto.hasOptions() && descriptorProto.getOptions().getMapEntry();
        }

        /**
         * Only called for MapEntry objects. Return the descriptor of the key.
         *
         * @return The descriptor of the key.
         */
        @Nonnull
        FieldDescriptor getMapKey() {
            assert(isMapEntry());
            return fieldDescriptors.get(0);
        }

        /**
         * Only called for MapEntry objects. Return the descriptor of the value.
         *
         * @return The descriptor of the value.
         */
        @Nonnull
        FieldDescriptor getMapValue() {
            assert (isMapEntry());
            return fieldDescriptors.get(1);
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

    /**
     * Descriptor for an enum declaration.
     * For example:
     * enum TestEnum {
     *     TEST = 1;
     * }
     */
    @Immutable
    public static class EnumDescriptor extends AbstractDescriptor {

        /**
         * Comment for the entire enum.
         */
        private final String comment;

        /**
         * The map of (value name) -> (value index) for the values of this enum.
         * For example, in:
         * enum TestEnum {
         *     TEST = 1;
         * }
         * This should be:
         * ("TEST" -> 1)
         */
        private final Map<String, Integer> values = new HashMap<>();

        /**
         * Comments for the individual values, indexed by value name.
          */
        private final Map<String, String> valueComments = new HashMap<>();

        public EnumDescriptor(@Nonnull final FileDescriptorProcessingContext context,
                              @Nonnull final EnumDescriptorProto enumDescriptor) {
            super(context, enumDescriptor.getName());

            enumDescriptor.getValueList().forEach(valueDescriptor -> {
                values.put(valueDescriptor.getName(), valueDescriptor.getNumber());
            });

            // Check for comments on the enum.
            comment = context.getCommentAtPath();

            // Check for comments on the enum values.
            context.startEnumValueList();
            for (int i = 0; i < enumDescriptor.getValueList().size(); ++i) {
                // Add the index of the value to the path
                context.startListElement(i);

                final String valueName = enumDescriptor.getValue(i).getName();
                valueComments.put(valueName, context.getCommentAtPath());

                // Remove the last element added in the beginning of the loop.
                context.endListElement();
            }
            context.endEnumValueList();
        }

        @Nonnull
        @Override
        String generateCode() {
            return Templates.enumerator()
                    .add("enumName", getName())
                    .add("comment", comment)
                    .add("originalProtoType", getQualifiedOriginalName())
                    .add("values", values.entrySet().stream()
                            .map(valueEntry -> {
                                String valueTemplate =
                                        "@ApiModelProperty(value=<comment>)" +
                                                "<name>(<value>)";
                                ST valueMsg = new ST(valueTemplate);
                                valueMsg.add("comment", valueComments.get(valueEntry.getKey()));
                                valueMsg.add("name", valueEntry.getKey());
                                valueMsg.add("value", valueEntry.getValue());
                                return valueMsg.render();
                            })
                            .collect(Collectors.toList()))
                    .render();
        }
    }

    /**
     * Descriptor for a field in a protobuf message. For example:
     * message TestMsg {
     *     optional int64 field; <-- this is a field.
     * }
     */
    @Immutable
    public static class FieldDescriptor {

        private static final String JAVA_SUFFIX = "_";

        private String comment;

        private Registry registry;

        private FieldDescriptorProto fieldDescriptorProto;

        private final boolean appendFieldNumber;

        /**
         * @param duplicateNameMap A map containing the names of fields that end up being duplicates
         *                         in the message the field is defined in. Names end up as duplicates
         *                         when the only thing that distinguishes them is the underscore.
         */
        public FieldDescriptor(@Nonnull final FileDescriptorProcessingContext context,
                               @Nonnull final FieldDescriptorProto fieldDescriptorProto,
                               @Nonnull final Map<String, Boolean> duplicateNameMap) {
            this.comment = context.getCommentAtPath();
            this.registry = context.getRegistry();
            this.fieldDescriptorProto = fieldDescriptorProto;
            appendFieldNumber =
                duplicateNameMap.getOrDefault(formatName(fieldDescriptorProto.getName()),
                        false);
        }

        /**
         * Format the name of the
         * @param name
         * @return
         */
        @Nonnull
        static String formatName(@Nonnull final String name) {
            return name.contains("_") ? CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, name) : name;
        }

        @Nonnull
        String getJavaName() {
            return getName() + JAVA_SUFFIX;
        }

        @Nonnull
        String getName() {
            String formattedName = formatName(fieldDescriptorProto.getName());
            if (appendFieldNumber) {
                formattedName += fieldDescriptorProto.getNumber();
            }
            return formattedName;
        }

        @Nonnull
        private String getTypeName() {
            return getContentMessage()
                    .map(descriptor -> descriptor.getQualifiedName())
                    .orElseGet(() -> getBaseFieldType(fieldDescriptorProto.getType()));
        }

        private boolean isMapField() {
            return isList() && getContentMessage()
                    .map(descriptor -> descriptor instanceof MessageDescriptor &&
                            ((MessageDescriptor) descriptor).isMapEntry())
                    .orElse(false);
        }

        @Nonnull
        private String getType() {
            String type;
            if (isMapField()) {
                type = getContentMessage()
                        .map(descriptor -> ((MessageDescriptor) descriptor).getMapTypeName())
                        .orElseThrow(() -> new IllegalStateException("Content message not present in map field."));
            } else {
                type = getTypeName();
                if (isList()) {
                    type = "List<" + type + ">";
                }
            }
            return type;
        }

        @Nonnull
        public String generateDeclaration(@Nonnull final Map<Integer, String> oneofNameMap) {
            final ST template = Templates.fieldDeclaration()
                    .add("type", getType())
                    .add("displayName", getName())
                    .add("name", getJavaName())
                    .add("isRequired", fieldDescriptorProto.getLabel() == Label.LABEL_REQUIRED)
                    .add("comment", comment);
            if (fieldDescriptorProto.hasOneofIndex()) {
                template.add("hasOneOf", true);
                template.add("oneof", formatName(oneofNameMap.get(fieldDescriptorProto.getOneofIndex())));
            } else {
                template.add("hasOneOf", false);
            }
            return template.render();
        }

        /**
         * Generate code to add this field to the builder that creates
         * a protobuf object from the generated Java object.
         *
         * @return The generated code string.
         */
        @Nonnull
        public String addToProtoBuilder() {
            final ST template = Templates.addFieldToProtoBuilder()
                    .add("name", getJavaName())
                    .add("capProtoName", StringUtils.capitalize(getName()))
                    .add("isList", isList())
                    .add("isMsg", getContentMessage().isPresent())
                    .add("isMap", isMapField());
            if (isMapField()) {
                FieldDescriptor value = getContentMessage()
                        .map(descriptor -> ((MessageDescriptor) descriptor).getMapValue())
                        .orElseThrow(() -> new IllegalStateException("Content message not present in map field."));
                template.add("isMapMsg", value.getContentMessage().isPresent());
            }
            return template.render();
        }

        /**
         * Generate code to set this field in the generated Java
         * object from a protobuf object.
         *
         * @param msgName The variable name of the protobuf object.
         * @return The generated code string.
         */
        @Nonnull
        public String addSetFromProto(@Nonnull final String msgName) {
            final ST template = Templates.setFieldFromProto()
                    .add("isMsg", getContentMessage().isPresent())
                    .add("msgName", msgName)
                    .add("fieldName", getJavaName())
                    .add("capProtoName", StringUtils.capitalize(getName()))
                    .add("msgType", getTypeName())
                    .add("isList", isList())
                    .add("isMap", isMapField());

            if (isMapField()) {
                FieldDescriptor value = getContentMessage()
                        .map(descriptor -> ((MessageDescriptor) descriptor).getMapValue())
                        .orElseThrow(() -> new IllegalStateException("Content message not present in map field."));
                template.add("isMapMsg", value.getContentMessage().isPresent());
                template.add("mapValType", value.getTypeName());
            }

            return template.render();
        }

        private boolean isList() {
            return fieldDescriptorProto.getLabel() == Label.LABEL_REPEATED;
        }

        /**
         * Returns the descriptor for the non-base message type of this field.
         *
         * @return An optional containing this descriptor, or an empty optional
         *         if the field is a base type.
         */
        @Nonnull
        private Optional<AbstractDescriptor> getContentMessage() {
            if (fieldDescriptorProto.getType() == Type.TYPE_MESSAGE || fieldDescriptorProto.getType() == Type.TYPE_ENUM) {
                return Optional.of(registry.getMessageDescriptor(fieldDescriptorProto.getTypeName()));
            }
            return Optional.empty();
        }
    }

    @Nonnull
    private static String getBaseFieldType(@Nonnull final FieldDescriptorProto.Type type) {
        switch (type) {
            case TYPE_DOUBLE:
                return "Double";
            case TYPE_FLOAT:
                return "Float";
            case TYPE_INT64:
                return "Long";
            case TYPE_UINT64:
                return "Long";
            case TYPE_INT32:
                return "Integer";
            case TYPE_FIXED64:
                return "Long";
            case TYPE_FIXED32:
                return "Integer";
            case TYPE_BOOL:
                return "Boolean";
            case TYPE_STRING:
                return "String";
            case TYPE_BYTES:
                return "ByteString";
            case TYPE_UINT32:
                return "Integer";
            case TYPE_SFIXED32:
                return "Integer";
            case TYPE_SFIXED64:
                return "Long";
            case TYPE_SINT32:
                return "Integer";
            case TYPE_SINT64:
                return "Long";
            case TYPE_GROUP:
            case TYPE_MESSAGE:
            case TYPE_ENUM:
            default:
                throw new IllegalArgumentException("Unexpected non-base type: " + type);
        }
    }
}
