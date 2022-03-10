package com.vmturbo.protoc.pojo.gen;

import static com.vmturbo.protoc.pojo.gen.PojoCodeGenerator.GET_DEFAULT_INSTANCE;
import static com.vmturbo.protoc.pojo.gen.PojoCodeGenerator.IMPLEMENTATION_SUFFIX;
import static com.vmturbo.protoc.pojo.gen.PojoCodeGenerator.JAVADOC_WARNING;
import static com.vmturbo.protoc.pojo.gen.fields.UnaryPojoField.DEFAULT_VALUE_STRING;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.lang.model.element.Modifier;

import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;

import com.vmturbo.protoc.plugin.common.generator.FieldDescriptor;
import com.vmturbo.protoc.plugin.common.generator.MessageDescriptor;
import com.vmturbo.protoc.plugin.common.generator.TypeNameFormatter;
import com.vmturbo.protoc.pojo.gen.TypeNameUtilities.ParameterizedTypeName;
import com.vmturbo.protoc.pojo.gen.fields.IPojoField;
import com.vmturbo.protoc.pojo.gen.fields.IPojoField.GenerationMode;

/**
 * Generator for a "view" interface for a POJO. This interface provides read-only access
 * to the underlying POJO and its field. Methods from the concrete implementation that
 * are exposed by this interface:
 * 1. Getters
 * 2. Hazzers (ie for a field foo, hasFoo tells you whether or not foo was set)
 * 3. Getting field defaults
 * 4. Conversion to proto
 * <p/>
 * Functionality that is not exposed:
 * 1. Setters
 * 2. Conversion from proto
 * 3. Copy method and copy constructor
 */
public class PojoViewGenerator {

    @Nonnull
    private final MessageDescriptor messageDescriptor;

    @Nonnull
    private final String formattedTypeName;

    @Nonnull
    private final TypeName typeName;

    @Nonnull
    private final TypeName implementationTypeName;

    @Nonnull
    private final TypeName protoTypeName;

    @Nonnull
    private final TypeName protoBuilderTypeName;

    /**
     * To indicate that these interfaces are read-only, we name them as "views".
     */
    public static final String INTERFACE_SUFFIX = "View";

    // getters for each field
    // default instance
    // hazzers
    // toProto, toProtoBuilder
    // toByteArray
    // Mark methods as "@Override" when they override these interface methods.
    /**
     * Create a new {@link PojoViewGenerator} which generates an interface that
     * provides read-only ("view") access to the underlying POJO.
     *
     * @param msgDescriptor Descriptor for the proto message for which we are generating the POJO
     * @param formattedImplementationTypeName formatted name for the concrete type that implements this interface.
     * @param implementationTypeName The {@link TypeName} for the concrete implementation for this interace.
     * @param protoTypeName The {@link TypeName} for the related protobuf message.
     * @param protoBuilderTypeName The {@link TypeName} for the related protobuf message builder.
     * @param nameFormatter A formatter for type names.
     */
    public PojoViewGenerator(@Nonnull final MessageDescriptor msgDescriptor,
                             @Nonnull final String formattedImplementationTypeName,
                             @Nonnull final TypeName implementationTypeName,
                             @Nonnull final TypeName protoTypeName,
                             @Nonnull final TypeName protoBuilderTypeName,
                             @Nonnull final TypeNameFormatter nameFormatter) {
        this.messageDescriptor = Objects.requireNonNull(msgDescriptor);
        this.formattedTypeName = formatInterfaceTypeName(formattedImplementationTypeName);
        this.implementationTypeName = Objects.requireNonNull(implementationTypeName);
        this.protoTypeName = Objects.requireNonNull(protoTypeName);
        this.protoBuilderTypeName = Objects.requireNonNull(protoBuilderTypeName);

        this.typeName = TypeNameUtilities.generateParameterizedTypeName(
            formatInterfaceTypeName(nameFormatter.formatTypeName(
                    msgDescriptor.getQualifiedName(nameFormatter)))).getTypeName();
    }

    /**
     * Format the name for the type of the POJO interface given the name of the implementation for
     * that interface.
     *
     * @param implementationTypeName The name of the implementation for the interface.
     * @return The formatted name for the interface type.
     */
    public static String formatInterfaceTypeName(@Nonnull final String implementationTypeName) {
        if (implementationTypeName.endsWith(IMPLEMENTATION_SUFFIX)) {
            return implementationTypeName.substring(0, implementationTypeName.length() - IMPLEMENTATION_SUFFIX.length())
                + INTERFACE_SUFFIX;
        } else {
            return implementationTypeName + INTERFACE_SUFFIX;
        }
    }

    /**
     * Create the typeName for the read-only interface associated with the field.
     *
     * @param fieldDescriptor The field whose interface type name should be generated.
     * @return the typeName for the read-only interface associated with the field. Empty for non-message fields.
     */
    public static Optional<ParameterizedTypeName> interfaceTypeNameFor(@Nonnull final FieldDescriptor fieldDescriptor) {
        if (fieldDescriptor.getProto().getType() == Type.TYPE_MESSAGE) {
            return interfaceTypeNameFor(fieldDescriptor.getType());
        } else {
            return Optional.empty();
        }
    }

    /**
     * Create the typeName for the read-only interface associated with the field.
     *
     * @param implType The name of the impl type for the interface.
     * @return the typeName for the read-only interface associated with the field.
     */
    public static Optional<ParameterizedTypeName> interfaceTypeNameFor(@Nonnull final String implType) {
        return Optional.of(TypeNameUtilities.generateParameterizedTypeName(
            PojoViewGenerator.formatInterfaceTypeName(implType)));
    }

    /**
     * Get the {@link TypeName} for this interface.
     *
     * @return the {@link TypeName} for this interface.
     */
    public TypeName getViewTypeName() {
        return typeName;
    }

    /**
     * Generate the read-only "view" interface for the protobuf message. This will be implemented
     * by a mutable concrete implementation.
     *
     * @param pojoFields The fields defined on the protobuf.
     * @param intoProto The interface generator for the {@code IntoProto} interface that this interface will extend
     * @return The builder for the POJO read-only POJO interface for the proto message. Return
     *         {@link Optional#empty()} if you do not wish to generate a POJO interface for the message.
     */
    @Nonnull
    Optional<TypeSpec.Builder> generateInterfaceForMessage(@Nonnull final List<IPojoField> pojoFields,
                                                           @Nonnull final IntoProtoInterfaceGenerator intoProto) {
        final TypeSpec.Builder typeSpec = TypeSpec.interfaceBuilder(formattedTypeName)
            .addModifiers(Modifier.PUBLIC)
            .addSuperinterface(intoProto.getIntoProtoTypeName(protoTypeName, protoBuilderTypeName));

        // We generate interfaces for methods here.
        final GenerationMode mode = GenerationMode.INTERFACE;

        final Optional<String> comment = PojoCodeGenerator.multiLineComment(messageDescriptor.getComment());
        comment.ifPresent(typeSpec::addJavadoc);
        comment.ifPresent(unused -> typeSpec.addJavadoc("\n\n"));
        typeSpec.addJavadoc("This is a read-only \"view\" for this POJO. It provides accessesors for"
            + "\nthe POJOs fields but no methods for mutation. A concrete, mutable implementation of this"
            + "\ninterface is defined in $T", implementationTypeName);
        typeSpec.addJavadoc("\n\n" + JAVADOC_WARNING);

        // Default value field.
        typeSpec.addField(
            FieldSpec.builder(getViewTypeName(), defaultValueName(),
                    Modifier.PUBLIC, Modifier.STATIC, Modifier.FINAL)
                // Be sure to mark the parent as set on the default instance!
                .initializer(CodeBlock.builder().add("$T.fromProto($T.$L()).$L()",
                    implementationTypeName, protoTypeName, GET_DEFAULT_INSTANCE,
                        PojoCodeGenerator.ON_PARENT_SETTING_METHOD)
                    .build())
                .build());

        // Generate message default
        generateDefaultInstanceMethod()
            .ifPresent(method -> typeSpec.addMethod(method.build()));

        // Getters
        pojoFields.forEach(pj -> pj.generateGetterMethods(mode)
            .forEach(getter -> typeSpec.addMethod(getter.build())));

        // Hazzers
        pojoFields.forEach(pj -> pj.generateHazzerMethods(mode)
            .forEach(hazzer -> typeSpec.addMethod(hazzer.build())));

        // toProto
        generateToProtos(protoTypeName, protoBuilderTypeName, formattedTypeName)
            .forEach(method -> typeSpec.addMethod(method.build()));

        // Create a convenience method for copying a pojo
        typeSpec.addMethod(MethodSpec.methodBuilder("copy")
            .addAnnotation(Nonnull.class)
            .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
            .addJavadoc("Create a copy of this {@link $T} as a new {@link $T}."
                    + "\n\n@return a copy of this {@link $T}",
                typeName, implementationTypeName, typeName)
            .returns(implementationTypeName)
            .build());

        // Create a convenience method for converting to a byte array
        typeSpec.addMethod(MethodSpec.methodBuilder("toByteArray")
            .addAnnotation(Nonnull.class)
            .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
            .addJavadoc("Convert this pojo to an equivalent protobuf, and then convert that proto to a byte array."
                    + "\n\n@return a byte array for the proto version of this $L",
                formattedTypeName)
            .returns(byte[].class)
            .build());

        return Optional.of(typeSpec);
    }

    /**
     * Get the name for the associated default value.
     *
     * @return the name for the associated default value.
     */
    String defaultValueName() {
        return IPojoField.lowerCamelToUpperUnderscore(formattedTypeName) + DEFAULT_VALUE_STRING;
    }

    private Optional<MethodSpec.Builder> generateDefaultInstanceMethod() {
        // A public static nonnull method for getting the default instance.
        // We should always get a new default instance every time we call the method.
        // Since protos are immutable it's fine to return the same instance, but since
        // these pojos are mutable we don't want someone modifying the default instance
        // that everyone else is using and messing up the others.
        final MethodSpec.Builder method = MethodSpec.methodBuilder(GET_DEFAULT_INSTANCE)
            .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
            .returns(typeName)
            .addJavadoc("Get the default value for the $T pojo message type."
                + "\nA new instance is generated each time this method is called.", typeName)
            .addAnnotation(Nonnull.class)
            .addCode(CodeBlock.builder()
                .addStatement("return $L", defaultValueName()).build());
        return Optional.of(method);
    }

    /**
     * Generate the toProto and toProtoBuilder methods.
     *
     * @param protoTypeName The type name of the proto message being processed.
     * @param protoBuilderTypeName The type name of the proto message builder.
     * @param simpleName The simple (unqualified) name of the proto.
     * @return the toProto method
     */
    private List<MethodSpec.Builder> generateToProtos(@Nonnull final TypeName protoTypeName,
                                                      @Nonnull final TypeName protoBuilderTypeName,
                                                      @Nonnull final String simpleName) {
        final MethodSpec.Builder toBuilder = MethodSpec.methodBuilder("toProtoBuilder")
            .addJavadoc("Convert this {@code $L} to an equivalent $L protobuf builder.", simpleName, protoTypeName)
            .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
            .addAnnotation(Nonnull.class)
            .returns(protoBuilderTypeName);

        final MethodSpec.Builder toProto = MethodSpec.methodBuilder("toProto")
            .addJavadoc("Convert this {@code $L} to an equivalent $L built protobuf.", simpleName, protoTypeName)
            .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
            .addAnnotation(Nonnull.class)
            .returns(protoTypeName);

        return Arrays.asList(toBuilder, toProto);
    }
}
