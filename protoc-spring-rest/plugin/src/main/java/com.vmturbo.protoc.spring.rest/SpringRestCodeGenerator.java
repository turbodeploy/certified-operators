package com.vmturbo.protoc.spring.rest;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.commons.lang3.StringUtils;
import org.stringtemplate.v4.ST;

import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Label;

import com.vmturbo.protoc.plugin.common.generator.ProtocPluginCodeGenerator;
import com.vmturbo.protoc.plugin.common.generator.EnumDescriptor;
import com.vmturbo.protoc.plugin.common.generator.FieldDescriptor;
import com.vmturbo.protoc.plugin.common.generator.MessageDescriptor;
import com.vmturbo.protoc.plugin.common.generator.ServiceMethodDescriptor;
import com.vmturbo.protoc.plugin.common.generator.ServiceMethodDescriptor.MethodType;
import com.vmturbo.protoc.plugin.common.generator.ServiceDescriptor;

/**
 * An implementation of {@link ProtocPluginCodeGenerator} that generates Spring Framework-compatible
 * REST controllers and swagger-annotated POJOs for gRPC services.
 */
class SpringRestCodeGenerator extends ProtocPluginCodeGenerator {
    /**
     * {@inheritDoc}
     */
    @Override
    @Nonnull
    protected String getPluginName() {
        return "protoc-spring-rest";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Nonnull
    protected String generatePluginJavaClass(@Nonnull final String protoJavaClass) {
        return protoJavaClass + "REST";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Nonnull
    protected String generateImports() {
        return SpringRestTemplates.imports();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Nonnull
    protected Optional<String> generateEnumCode(@Nonnull final EnumDescriptor enumDescriptor) {
        return Optional.of(SpringRestTemplates.enumerator()
                .add("enumName", enumDescriptor.getName())
                .add("comment", enumDescriptor.getComment())
                .add("originalProtoType", enumDescriptor.getQualifiedOriginalName())
                .add("values", enumDescriptor.getValues().entrySet().stream()
                        .map(valueEntry -> {
                            String valueTemplate =
                                    "@ApiModelProperty(value=<comment>)" +
                                            "<name>(<value>)";
                            ST valueMsg = new ST(valueTemplate);
                            valueMsg.add("comment", enumDescriptor.getValueComment(valueEntry.getKey()));
                            valueMsg.add("name", valueEntry.getKey());
                            valueMsg.add("value", valueEntry.getValue());
                            return valueMsg.render();
                        })
                        .collect(Collectors.toList()))
                .render());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Nonnull
    protected Optional<String> generateServiceCode(@Nonnull final ServiceDescriptor serviceDescriptor) {
        final String responseWrapper = serviceDescriptor.getName() + "Response";

        return Optional.of(SpringRestTemplates.service()
                .add("serviceName", serviceDescriptor.getName())
                .add("responseWrapper", responseWrapper)
                .add("package", serviceDescriptor.getJavaPkgName())
                .add("methodDefinitions", serviceDescriptor.getMethodDescriptors().stream()
                        .map(serviceMethodDescriptor -> generateMethodCode(serviceMethodDescriptor, responseWrapper))
                        .collect(Collectors.toList()))
                .render());
    }

    @Nonnull
    private String generateMethodCode(@Nonnull final ServiceMethodDescriptor serviceMethodDescriptor,
                                      @Nonnull final String responseWrapper) {
        final MessageDescriptor inputDescriptor = serviceMethodDescriptor.getInputMessage();
        final MessageDescriptor outputDescriptor = serviceMethodDescriptor.getOutputMessage();

        final MethodType type = serviceMethodDescriptor.getType();
        String requestBodyType = inputDescriptor.getQualifiedName();
        if (type == MethodType.CLIENT_STREAM || type == MethodType.BI_STREAM) {
            requestBodyType = "List<" + requestBodyType + ">";
        }

        String responseBodyType = outputDescriptor.getQualifiedName();
        if (type == MethodType.SERVER_STREAM || type == MethodType.BI_STREAM) {
            responseBodyType = "List<" + responseBodyType + ">";
        }
        responseBodyType = responseWrapper + "<" + responseBodyType + ">";

        return SpringRestTemplates.serviceMethod()
                .add("resultProto", outputDescriptor.getQualifiedOriginalName())
                .add("resultType", outputDescriptor.getQualifiedName())
                .add("requestProto", inputDescriptor.getQualifiedOriginalName())
                .add("requestType", inputDescriptor.getQualifiedName())
                .add("responseBodyType", responseBodyType)
                .add("requestBodyType", requestBodyType)
                .add("responseWrapper", responseWrapper)
                .add("methodName", StringUtils.uncapitalize(serviceMethodDescriptor.getName()))
                .add("comments", serviceMethodDescriptor.getComment())
                .add("isClientStream", type == MethodType.BI_STREAM || type == MethodType.CLIENT_STREAM)
                .add("isSingleResponse", type == MethodType.SIMPLE || type == MethodType.CLIENT_STREAM)
                .render();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Nonnull
    protected Optional<String> generateMessageCode(@Nonnull final MessageDescriptor messageDescriptor) {
        final Map<Integer, String> oneofNameMap = new HashMap<>();
        final DescriptorProto descriptorProto = messageDescriptor.getDescriptorProto();
        for (int i = 0; i < descriptorProto.getOneofDeclCount(); ++i) {
            oneofNameMap.put(i, descriptorProto.getOneofDecl(i).getName());
        }

        return Optional.of(SpringRestTemplates.message()
                .add("comment", messageDescriptor.getComment())
                .add("className", messageDescriptor.getName())
                .add("originalProtoType", messageDescriptor.getQualifiedOriginalName())
                .add("nestedDefinitions", messageDescriptor.getNestedMessages().stream()
                        .filter(nestedDescriptor -> !(nestedDescriptor instanceof MessageDescriptor &&
                                ((MessageDescriptor) nestedDescriptor).isMapEntry()))
                        .map(this::generateCode)
                        .filter(Optional::isPresent)
                        .map(Optional::get)
                        .collect(Collectors.toList()))
                .add("fieldDeclarations", messageDescriptor.getFieldDescriptors().stream()
                        .map(descriptor -> generateFieldDeclaration(descriptor, oneofNameMap))
                        .collect(Collectors.toList()))
                .add("setBuilderFields", messageDescriptor.getFieldDescriptors().stream()
                        .map(this::addFieldToProtoBuilder)
                        .collect(Collectors.toList()))
                .add("setMsgFields", messageDescriptor.getFieldDescriptors().stream()
                        .map(descriptor -> addFieldSetFromProto(descriptor, "newMsg"))
                        .collect(Collectors.toList()))
                .render());
    }

    @Nonnull
    private String generateFieldDeclaration(@Nonnull final FieldDescriptor fieldDescriptor,
                                            @Nonnull final Map<Integer, String> oneofNameMap) {
        final ST template = SpringRestTemplates.fieldDeclaration()
                .add("type", fieldDescriptor.getType())
                .add("displayName", fieldDescriptor.getName())
                .add("name", fieldDescriptor.getSuffixedName())
                .add("isRequired", fieldDescriptor.getProto().getLabel() == Label.LABEL_REQUIRED)
                .add("comment", fieldDescriptor.getComment());
        if (fieldDescriptor.getProto().hasOneofIndex()) {
            template.add("hasOneOf", true);
            template.add("oneof", FieldDescriptor.formatFieldName(
                    oneofNameMap.get(fieldDescriptor.getProto().getOneofIndex())));
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
    private String addFieldToProtoBuilder(@Nonnull final FieldDescriptor fieldDescriptor) {
        final ST template = SpringRestTemplates.addFieldToProtoBuilder()
                .add("name", fieldDescriptor.getSuffixedName())
                .add("capProtoName", StringUtils.capitalize(fieldDescriptor.getName()))
                .add("isList", fieldDescriptor.isList())
                .add("isMsg", fieldDescriptor.getContentMessage().isPresent())
                .add("isMap", fieldDescriptor.isMapField());
        if (fieldDescriptor.isMapField()) {
            FieldDescriptor value = fieldDescriptor.getContentMessage()
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
    private String addFieldSetFromProto(@Nonnull final FieldDescriptor fieldDescriptor,
                                        @Nonnull final String msgName) {
        final ST template = SpringRestTemplates.setFieldFromProto()
                .add("isMsg", fieldDescriptor.getContentMessage().isPresent())
                .add("isProto3", fieldDescriptor.isProto3Syntax())
                .add("msgName", msgName)
                .add("fieldName", fieldDescriptor.getSuffixedName())
                .add("fieldNumber", fieldDescriptor.getProto().getNumber())
                .add("capProtoName", StringUtils.capitalize(fieldDescriptor.getName()))
                .add("msgType", fieldDescriptor.getTypeName())
                .add("isList", fieldDescriptor.isList())
                .add("isMap", fieldDescriptor.isMapField());

        fieldDescriptor.getOneofName().ifPresent(oneOfName -> {
            template.add("isOneOf", true);
            template.add("oneOfName", StringUtils.capitalize(oneOfName));
        });

        if (fieldDescriptor.isMapField()) {
            FieldDescriptor value = fieldDescriptor.getContentMessage()
                    .map(descriptor -> ((MessageDescriptor) descriptor).getMapValue())
                    .orElseThrow(() -> new IllegalStateException("Content message not present in map field."));
            template.add("isMapMsg", value.getContentMessage().isPresent());
            template.add("mapValType", value.getTypeName());
        }

        return template.render();
    }
}
