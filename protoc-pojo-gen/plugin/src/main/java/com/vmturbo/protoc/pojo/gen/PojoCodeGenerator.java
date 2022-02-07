package com.vmturbo.protoc.pojo.gen;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.lang.model.element.Modifier;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterSpec;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;

import com.vmturbo.protoc.plugin.common.generator.EnumDescriptor;
import com.vmturbo.protoc.plugin.common.generator.FileDescriptorProcessingContext;
import com.vmturbo.protoc.plugin.common.generator.MessageDescriptor;
import com.vmturbo.protoc.plugin.common.generator.OneOfDescriptor;
import com.vmturbo.protoc.plugin.common.generator.ProtocPluginCodeGenerator;
import com.vmturbo.protoc.plugin.common.generator.Registry;
import com.vmturbo.protoc.plugin.common.generator.ServiceDescriptor;
import com.vmturbo.protoc.plugin.common.generator.TypeNameFormatter;
import com.vmturbo.protoc.pojo.gen.fields.IPojoField;
import com.vmturbo.protoc.pojo.gen.fields.OneOfPojoField;
import com.vmturbo.protoc.pojo.gen.fields.OneOfVariantPojoField;
import com.vmturbo.protoc.pojo.gen.fields.UnaryPojoField;

/**
 * An implementation of {@link ProtocPluginCodeGenerator} that generates POJOs
 * that can be converted to and from protos. The POJOs are useful when you need
 * to make many mutations to a proto over time because although you can do this
 * with proto builders, the builders use lots of memory and the POJOs use less.
 *
 * <p/>Functionality implemented:
 * 1. Getters and Setters
 * 2. Hazzers (ie for a field foo, hasFoo tells you whether or not foo was set)
 * 3. Support for field defaults
 * 4. Conversion to proto
 * 5. Conversion from proto
 * 6. HashCode && Equals
 *
 * <p/>Note that we reuse enum definitions directly from the protos rather than
 * re-defining them.
 */
public class PojoCodeGenerator extends ProtocPluginCodeGenerator {

    public static final String POJO_SUFFIX = "POJO";

    public static final String POJO_PACKAGE_SUFFIX = "POJOPkg";

    public static final String JAVA_LANG_PACKAGE = "java.lang";

    public static final String GOOGLE_PROTOBUF_PACKAGE = "com.google.protobuf";

    public static final String LIST_CLASS_NAME = "List";

    public static final String MAP_CLASS_NAME = "Map";

    public static final String GET_DEFAULT_INSTANCE = "getDefaultInstance";

    /**
     * Name of the protobuf to be translated to a POJO in the {@code #fromProto} method.
     */
    public static final String FROM_PROTO_FIELD_NAME = "proto";

    private final TypeNameFormatter nameFormatter = new PojoTypeNameFormatter();

    public static final Map<String, TypeName> PRIMITIVES_MAP = ImmutableMap.<String, TypeName>builder()
        .put("Boolean", TypeName.BOOLEAN)
        .put("Byte", TypeName.BYTE)
        .put("Short", TypeName.SHORT)
        .put("Integer", TypeName.INT)
        .put("Long", TypeName.LONG)
        .put("Character", TypeName.CHAR)
        .put("Float", TypeName.FLOAT)
        .put("Double", TypeName.DOUBLE)
        .build();

    /**
     * {@inheritDoc}
     */
    @Override
    @Nonnull
    public String getPluginName() {
        return "protoc-pojo-gen";
    }

    @Nonnull
    public static Optional<String> multiLineComment(@Nonnull final String originalComment) {
        if (originalComment.length() > 2) {
            // Proto comments are always surrounded by double quotes. Strip those off.
            // ie "this is a comment" --> this is a comment
            // Multiline comments have a " character and a + character followed by another "
            // as in:
            // This is a bar!\n" + "And it has more than one line.
            return Optional.of(originalComment.substring(1, originalComment.length() - 1)
                .replaceAll("\\\\n\" \\+ \"", "\n")
                .replaceAll("\\$", "\\$\\$")
            );
        } else {
            return Optional.empty();
        }
    }

    /**
     * {@inheritDoc}
     *
     * <p/>TopologyEntityDTO --> TopologyEntityPJ
     * AnalysisSettings --> AnalysisSettingsPJ
     */
    @Override
    @Nonnull
    protected String generatePluginJavaClass(@Nonnull final String protoJavaClass) {
        return protoJavaClass.endsWith("DTO")
            ? protoJavaClass.substring(0, protoJavaClass.length() - 3) + POJO_PACKAGE_SUFFIX
            : protoJavaClass + POJO_PACKAGE_SUFFIX;
    }

    @Override
    @Nonnull
    protected FileDescriptorProcessingContext createFileDescriptorProcessingContext(
        @Nonnull final Registry registry, @Nonnull final FileDescriptorProto fileDescriptorProto) {
        return new PojoFileDescriptorProcessingContext(this, registry, fileDescriptorProto);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Nonnull
    protected String generateImports() {
        return "";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Nonnull
    protected Optional<String> generateEnumCode(@Nonnull final EnumDescriptor enumDescriptor) {
        // We reuse the actual proto enums.
        return Optional.empty();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Nonnull
    protected Optional<String> generateServiceCode(@Nonnull final ServiceDescriptor serviceDescriptor) {
        // No need to generate code for services.
        return Optional.empty();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Nonnull
    protected Optional<String> generateMessageCode(@Nonnull final MessageDescriptor messageDescriptor) {
        // We do generate code for messages but we go through the generateTypeForMessage route.
        return Optional.empty();
    }

    /**
     * Generate the type for the method.
     *
     * @param msgDescriptor The descriptor for the proto message.
     * @return The builder for the POJO class that represents the proto message. Return
     *         {@link Optional#empty()} if you do not wish to generate a POJO class for the message.
     */
    @Nonnull
    Optional<TypeSpec.Builder> generateTypeForMessage(@Nonnull final MessageDescriptor msgDescriptor) {
        final String formattedTypeName = nameFormatter.formatTypeName(msgDescriptor.getName());
        final TypeSpec.Builder typeSpec = TypeSpec.classBuilder(formattedTypeName)
            .addModifiers(Modifier.PUBLIC, Modifier.STATIC);
        final TypeName typeName = TypeNameUtilities
            .generateParameterizedTypeName(
                nameFormatter.formatTypeName(msgDescriptor.getQualifiedName(nameFormatter))).getTypeName();
        final TypeName protoTypeName = TypeNameUtilities
            .generateParameterizedTypeName(msgDescriptor.getQualifiedOriginalName()).getTypeName();
        final TypeName protoBuilderTypeName = TypeNameUtilities
            .generateParameterizedTypeName(
                msgDescriptor.getQualifiedOriginalName() + ".Builder").getTypeName();
        final TypeName protoOrBuilderTypeName = TypeNameUtilities.generateParameterizedTypeName(
            msgDescriptor.getQualifiedOriginalName() + "OrBuilder").getTypeName();
        final PrimitiveFieldBits primitiveFieldBits = new PrimitiveFieldBits();

        PojoCodeGenerator.multiLineComment(msgDescriptor.getComment())
            .ifPresent(typeSpec::addJavadoc);
        typeSpec.addJavadoc("Do not use this POJO in a Map or Set except by identity (ie IdentityHashMap)"
            + "\nbecause updating a field on the POJO will change its hashCode, resulting in no longer"
            + "\nbeing able to find it in the map or set after the change even though it is there. Also"
            + "\nnote that {@link #hashCode} and {@link #equals} for POJOs with many fields can be"
            + "\n very expensive.");

        // Create public constructor.
        typeSpec.addMethod(MethodSpec.constructorBuilder()
            .addModifiers(Modifier.PUBLIC)
            .addJavadoc("Create a new $L", formattedTypeName)
            .build());

        // Create pojo fields
        final List<IPojoField> pojoFields = new ArrayList<>(msgDescriptor.getFieldDescriptors().size());
        final Map<Integer, List<OneOfVariantPojoField>> oneOfVariants = new HashMap<>();
        msgDescriptor.getFieldDescriptors().stream()
            .map(fieldDescriptor -> UnaryPojoField.create(fieldDescriptor, primitiveFieldBits, typeName, oneOfVariants))
            .forEach(pojoFields::add);
        msgDescriptor.getNestedMessages().stream()
            .filter(d -> d instanceof OneOfDescriptor)
            .map(d -> (OneOfDescriptor)d)
            .map(d -> new OneOfPojoField(d, oneOfVariants
                .getOrDefault(d.getOneOfIndex(), Collections.emptyList())))
            .forEach(pojoFields::add);

        // Generate message default
        generateDefaultInstanceMethod(typeName, protoTypeName)
            .ifPresent(method -> typeSpec.addMethod(method.build()));

        // Generate field specs.
        pojoFields.forEach(pj -> pj.generateFieldSpecs()
            .forEach(typeSpec::addField));

        // Getters
        pojoFields.forEach(pj -> pj.generateGetterMethods()
            .forEach(getter -> typeSpec.addMethod(getter.build())));

        // Setters
        pojoFields.forEach(pj -> pj.generateSetterMethods()
            .forEach(setter -> typeSpec.addMethod(setter.build())));

        // Hazzers
        pojoFields.forEach(pj -> pj.generateHazzerMethods()
            .forEach(hazzer -> typeSpec.addMethod(hazzer.build())));

        // Clearing fields
        pojoFields.forEach(pj -> pj.generateClearMethods()
            .forEach(clear -> typeSpec.addMethod(clear.build())));

        // toProto
        generateToProtos(protoTypeName, protoBuilderTypeName, formattedTypeName, pojoFields)
            .forEach(method -> typeSpec.addMethod(method.build()));

        // fromProto
        generateFromProto(typeName, protoOrBuilderTypeName, pojoFields)
            .ifPresent(method -> typeSpec.addMethod(method.build()));

        // Create copy constructor
        generateCopyConstructor(typeName, primitiveFieldBits, pojoFields)
            .ifPresent(method -> typeSpec.addMethod(method.build()));

        // Create a convenience method for copying a pojo
        typeSpec.addMethod(MethodSpec.methodBuilder("copy")
            .addAnnotation(Nonnull.class)
            .addModifiers(Modifier.PUBLIC)
            .addJavadoc("Create a copy of this $L.\n\n@return a copy of this $L",
                formattedTypeName, formattedTypeName)
            .returns(typeName)
            .addCode(CodeBlock.builder()
                .addStatement("return new $T(this)", typeName)
                .build())
            .build());

        // Create a convenience method for converting to a byte array
        typeSpec.addMethod(MethodSpec.methodBuilder("toByteArray")
            .addAnnotation(Nonnull.class)
            .addModifiers(Modifier.PUBLIC)
            .addJavadoc("Convert this pojo to an equivalent protobuf, and then convert that proto to a byte array."
                    + "\n\n@return a byte array for the proto version of this $L",
                formattedTypeName)
            .returns(byte[].class)
            .addCode(CodeBlock.builder()
                .addStatement("return this.toProto().toByteArray()")
                .build())
            .build());

        // equals
        generateEquals(typeName, pojoFields)
            .ifPresent(method -> typeSpec.addMethod(method.build()));

        // hashCode
        generateHashCode(typeName, pojoFields)
            .ifPresent(method -> typeSpec.addMethod(method.build()));

        // Nested types
        msgDescriptor.getNestedMessages().stream()
            .filter(d -> d instanceof MessageDescriptor)
            .map(d -> (MessageDescriptor)d)
            .filter(d -> !d.isMapEntry())
            .forEach(nestedDef -> generateTypeForMessage(nestedDef)
                .ifPresent(type -> typeSpec.addType(type.build())));

        return Optional.of(typeSpec);
    }

    private Optional<MethodSpec.Builder> generateDefaultInstanceMethod(@Nonnull final TypeName typeName,
                                                                       @Nonnull final TypeName protoTypeName) {
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
                .addStatement("return $T.fromProto($T.$L())", typeName, protoTypeName, GET_DEFAULT_INSTANCE).build());
        return Optional.of(method);
    }

    /**
     * Generate the toProto and toProtoBuilder methods.
     *
     * @param protoTypeName The type name of the proto message being processed.
     * @param protoBuilderTypeName The type name of the proto message builder.
     * @param simpleName The simple (unqualified) name of the proto.
     * @param pojoFields The fields on this message.
     * @return the toProto method
     */
    private List<MethodSpec.Builder> generateToProtos(@Nonnull final TypeName protoTypeName,
                                                      @Nonnull final TypeName protoBuilderTypeName,
                                                      @Nonnull final String simpleName,
                                                      @Nonnull final  List<IPojoField> pojoFields) {
        final MethodSpec.Builder toBuilder = MethodSpec.methodBuilder("toProtoBuilder")
            .addJavadoc("Convert this {@code $L} to an equivalent $L protobuf builder.", simpleName, protoTypeName)
            .addModifiers(Modifier.PUBLIC)
            .addAnnotation(Nonnull.class)
            .returns(protoBuilderTypeName);

        final CodeBlock.Builder codeBlock = CodeBlock.builder();
        codeBlock.addStatement("final $T.Builder builder = $T.newBuilder()", protoTypeName, protoTypeName);
        pojoFields.forEach(field -> field.addToProtoForField(codeBlock, protoTypeName));
        codeBlock.addStatement("return builder");
        toBuilder.addCode(codeBlock.build());

        final MethodSpec.Builder toProto = MethodSpec.methodBuilder("toProto")
            .addJavadoc("Convert this {@code $L} to an equivalent $L built protobuf.", simpleName, protoTypeName)
            .addModifiers(Modifier.PUBLIC)
            .addAnnotation(Nonnull.class)
            .returns(protoTypeName);

        toProto.addCode(CodeBlock.builder()
            .addStatement("return this.toProtoBuilder().build()").build());

        return Arrays.asList(toBuilder, toProto);
    }

    /**
     * Generate the fromProto method.
     *
     * @param typeName The type to be created from the proto.
     * @param protoOrBuilderTypeName The proto to be converted to this message type.
     * @param pojoFields The fields in this pojo that represents the proto message.
     * @return the fromProto method
     */
    private Optional<MethodSpec.Builder> generateFromProto(@Nonnull final TypeName typeName,
                                                           @Nonnull final TypeName protoOrBuilderTypeName,
                                                           @Nonnull final  List<IPojoField> pojoFields) {
        final ParameterSpec.Builder param = ParameterSpec.builder(protoOrBuilderTypeName, FROM_PROTO_FIELD_NAME)
            .addModifiers(Modifier.FINAL)
            .addAnnotation(Nonnull.class)
            .addJavadoc("The proto to translate to an equivalent $T.", typeName);

        final MethodSpec.Builder method = MethodSpec.methodBuilder("fromProto")
            .addJavadoc("Create a new {@link $T} equivalent to a $T protobuf", typeName, protoOrBuilderTypeName)
            .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
            .addParameter(param.build())
            .addAnnotation(Nonnull.class)
            .returns(typeName);

        final CodeBlock.Builder codeBlock = CodeBlock.builder();
        codeBlock.addStatement("final $T pojo = new $T()", typeName, typeName);
        pojoFields.forEach(field -> field.addFromProtoForField(codeBlock, protoOrBuilderTypeName, FROM_PROTO_FIELD_NAME));
        codeBlock.addStatement("return pojo");
        method.addCode(codeBlock.build());

        return Optional.of(method);
    }

    /**
     * Generate the copy constructor.
     *
     * @param typeName The type to be created from the proto.
     * @param primitiveFieldBits The bits for storing primitive fields that are set.
     * @param pojoFields The fields in this pojo that represents the proto message.
     * @return the copy constructor method
     */
    private Optional<MethodSpec.Builder> generateCopyConstructor(@Nonnull final TypeName typeName,
                                                                 @Nonnull final PrimitiveFieldBits primitiveFieldBits,
                                                                 @Nonnull final List<IPojoField> pojoFields) {
        final ParameterSpec.Builder param = ParameterSpec.builder(typeName, "other")
            .addModifiers(Modifier.FINAL)
            .addAnnotation(Nonnull.class)
            .addJavadoc("The pojo to copy.");

        final MethodSpec.Builder method = MethodSpec.constructorBuilder()
            .addJavadoc("Create a new {@link $T} equivalent to another", typeName)
            .addModifiers(Modifier.PUBLIC)
            .addParameter(param.build())
            .addAnnotation(Nonnull.class);

        final CodeBlock.Builder codeBlock = CodeBlock.builder();

        primitiveFieldBits.getAllBitFieldNames()
                .forEach(bitFieldName -> method.addStatement("this.$L = other.$L", bitFieldName, bitFieldName));
        pojoFields.forEach(field -> field.addCopyForField(codeBlock));
        method.addCode(codeBlock.build());

        return Optional.of(method);
    }

    /**
     * Generate the equals method
     *
     * @param typeName The of this pojo message.
     * @param pojoFields The fields in this pojo that represents the proto message.
     * @return the equals method
     */
    private Optional<MethodSpec.Builder> generateEquals(@Nonnull final TypeName typeName,
                                                        @Nonnull final  List<IPojoField> pojoFields) {
        final ParameterSpec.Builder param = ParameterSpec.builder(Object.class, "obj")
            .addModifiers(Modifier.FINAL);

        final MethodSpec.Builder method = MethodSpec.methodBuilder("equals")
            .addModifiers(Modifier.PUBLIC)
            .addAnnotation(Override.class)
            .addParameter(param.build())
            .returns(TypeName.BOOLEAN);

        final CodeBlock.Builder codeBlock = CodeBlock.builder();
        codeBlock.beginControlFlow("if (obj == this)")
            .addStatement("return true")
            .endControlFlow();
        codeBlock.beginControlFlow("if (!(obj instanceof $T))", typeName)
            .addStatement("return false")
            .endControlFlow();
        codeBlock.addStatement("final $T other = ($T)obj", typeName, typeName);
        pojoFields.forEach(field -> field.addEqualsForField(codeBlock));
        codeBlock.addStatement("return true");
        method.addCode(codeBlock.build());

        return Optional.of(method);
    }

    /**
     * Generate the hashCode method. Note that we cannot have pojo's memoize their hashCodes
     * because the pojos can change, leading to a different hashCode.
     *
     * @return the hashCode method
     */
    private Optional<MethodSpec.Builder> generateHashCode(@Nonnull final TypeName typeName,
                                                          @Nonnull final List<IPojoField> pojoFields) {

        final MethodSpec.Builder method = MethodSpec.methodBuilder("hashCode")
            .addModifiers(Modifier.PUBLIC)
            .addAnnotation(Override.class)
            .returns(TypeName.INT);

        final CodeBlock.Builder codeBlock = CodeBlock.builder();
        codeBlock.addStatement("int hash = 41");
        pojoFields.forEach(field -> field.addHashCodeForField(codeBlock));
        codeBlock.addStatement("return hash");
        method.addCode(codeBlock.build());

        return Optional.of(method);
    }
}
