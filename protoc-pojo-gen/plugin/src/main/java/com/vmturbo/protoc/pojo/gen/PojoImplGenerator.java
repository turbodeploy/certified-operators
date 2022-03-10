package com.vmturbo.protoc.pojo.gen;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.lang.model.element.Modifier;

import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterSpec;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;
import com.squareup.javapoet.TypeSpec.Builder;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.protoc.plugin.common.generator.MessageDescriptor;
import com.vmturbo.protoc.plugin.common.generator.OneOfDescriptor;
import com.vmturbo.protoc.plugin.common.generator.TypeNameFormatter;
import com.vmturbo.protoc.pojo.gen.fields.IPojoField;
import com.vmturbo.protoc.pojo.gen.fields.IPojoField.GenerationMode;
import com.vmturbo.protoc.pojo.gen.fields.OneOfPojoField;
import com.vmturbo.protoc.pojo.gen.fields.OneOfVariantPojoField;
import com.vmturbo.protoc.pojo.gen.fields.UnaryPojoField;

/**
 * Generator for the implementation class for POJOs. Methods from the concrete implementation that
 *  * are exposed by this interface:
 *  * 1. Getters
 *  * 2. Hazzers (ie for a field foo, hasFoo tells you whether or not foo was set)
 *  * 3. Getting field defaults
 *  * 4. Conversion to proto
 *  * 5. Setters
 *  * 6. Conversion from proto
 *  * 7. Copy method and copy constructor
 */
public class PojoImplGenerator {
    private static final Logger logger = LogManager.getLogger();

    private final IntoProtoInterfaceGenerator intoProtoGenerator;

    private final TypeNameFormatter nameFormatter = new PojoTypeNameFormatter();

    /**
     * Create a new {@link PojoCodeGenerator}.
     *
     * @param intoProtoGenerator The generator for the {@code IntoProto} interface.
     */
    public PojoImplGenerator(@Nonnull final IntoProtoInterfaceGenerator intoProtoGenerator) {
        this.intoProtoGenerator = Objects.requireNonNull(intoProtoGenerator);
    }

    /**
     * Generate the concrete POJO implementation for the protobuf message along with a read-only interface
     * definition.
     *
     * @param msgDescriptor The descriptor for the proto message.
     * @return The builder for the POJO class and its read-only interface that represents the proto message.
     *         Return an empty list if you do not wish to generate a POJO class or interface for the message.
     */
    @Nonnull
    List<Builder> generateImplForMessage(@Nonnull final MessageDescriptor msgDescriptor) {
        final PrimitiveFieldBits primitiveFieldBits = new PrimitiveFieldBits();
        final ParentBitField parentBit = new ParentBitField(primitiveFieldBits);

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

        // The read-only "view" interface for this POJO.
        final PojoViewGenerator viewGenerator = new PojoViewGenerator(msgDescriptor,
            formattedTypeName, typeName, protoTypeName, protoBuilderTypeName, nameFormatter);
        typeSpec.addSuperinterface(viewGenerator.getViewTypeName());
        implementSerializable(typeSpec);

        // We generate implementations for methods here.
        final GenerationMode mode = GenerationMode.IMPLEMENTATION;

        final Optional<String> comment = PojoCodeGenerator.multiLineComment(msgDescriptor.getComment());
        comment.ifPresent(typeSpec::addJavadoc);
        comment.ifPresent(unused -> typeSpec.addJavadoc("\n\n"));
        typeSpec.addJavadoc(PojoCodeGenerator.JAVADOC_WARNING);

        // Create public constructor.
        typeSpec.addMethod(MethodSpec.constructorBuilder()
            .addModifiers(Modifier.PUBLIC)
            .addJavadoc("Create a new $L", formattedTypeName)
            .build());

        // Create pojo fields
        final List<IPojoField> pojoFields = new ArrayList<>(msgDescriptor.getFieldDescriptors().size());
        final Map<Integer, List<OneOfVariantPojoField>> oneOfVariants = new HashMap<>();
        msgDescriptor.getFieldDescriptors().stream()
            .filter(p -> p.getTypeName().endsWith("Entry"))
            .forEach(p -> logger.info("Found field {} {}", p.getName(), p.getTypeName()));

        msgDescriptor.getFieldDescriptors().stream()
            .map(fieldDescriptor -> UnaryPojoField.create(fieldDescriptor, primitiveFieldBits, typeName, oneOfVariants))
            .forEach(pojoFields::add);
        msgDescriptor.getNestedMessages().stream()
            .filter(d -> d instanceof OneOfDescriptor)
            .map(d -> (OneOfDescriptor)d)
            .map(d -> new OneOfPojoField(d, oneOfVariants
                .getOrDefault(d.getOneOfIndex(), Collections.emptyList())))
            .forEach(pojoFields::add);

        // Generate bit field for parent tracking
        typeSpec.addField(generateBitFieldForParentTracking(parentBit).build());

        // Generate field specs.
        pojoFields.forEach(pj -> pj.generateFieldSpecs()
            .forEach(typeSpec::addField));

        // Getters
        pojoFields.forEach(pj -> pj.generateGetterMethods(mode)
            .forEach(getter -> typeSpec.addMethod(getter.build())));

        // Setters
        pojoFields.forEach(pj -> pj.generateSetterMethods()
            .forEach(setter -> typeSpec.addMethod(setter.build())));

        // Hazzers
        pojoFields.forEach(pj -> pj.generateHazzerMethods(mode)
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
        generateCopyConstructor(typeName, viewGenerator.getViewTypeName(), pojoFields)
            .ifPresent(method -> typeSpec.addMethod(method.build()));

        // Create a convenience method for copying a pojo
        typeSpec.addMethod(MethodSpec.methodBuilder("copy")
            .addAnnotation(Nonnull.class)
            .addAnnotation(Override.class)
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
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .addJavadoc("Convert this pojo to an equivalent protobuf, and then convert that proto to a byte array."
                    + "\n\n@return a byte array for the proto version of this $L",
                formattedTypeName)
            .returns(byte[].class)
            .addCode(CodeBlock.builder()
                .addStatement("return this.toProto().toByteArray()")
                .build())
            .build());

        // Create parenting methods.
        generateParentingMethods(typeName, parentBit)
            .forEach(method -> typeSpec.addMethod(method.build()));

        // equals
        generateEquals(typeName, pojoFields)
            .ifPresent(method -> typeSpec.addMethod(method.build()));

        // hashCode
        generateHashCode(pojoFields)
            .ifPresent(method -> typeSpec.addMethod(method.build()));

        // clear
        generateClear(typeName, pojoFields, primitiveFieldBits, parentBit)
            .ifPresent(method -> typeSpec.addMethod(method.build()));

        // Nested types
        msgDescriptor.getNestedMessages().stream()
            .filter(d -> d instanceof MessageDescriptor)
            .map(d -> (MessageDescriptor)d)
            .filter(d -> !d.isMapEntry())
            .forEach(nestedDef -> generateImplForMessage(nestedDef)
                .forEach(type -> typeSpec.addType(type.build())));

        final Optional<TypeSpec.Builder> interfaceType =
            viewGenerator.generateInterfaceForMessage(pojoFields, intoProtoGenerator);
        return interfaceType
            .map(builder -> Arrays.asList(builder, typeSpec))
            .orElseGet(() -> Collections.singletonList(typeSpec));
    }

    private FieldSpec.Builder generateBitFieldForParentTracking(@Nonnull final ParentBitField parentBitField) {
        return FieldSpec.builder(TypeName.INT, parentBitField.parentBitFieldName, Modifier.PRIVATE)
            .initializer(CodeBlock.builder().add("0").build());
    }

    private void implementSerializable(TypeSpec.Builder typeSpec) {
        typeSpec.addField(FieldSpec
            .builder(TypeName.LONG, "serialVersionUID", Modifier.PUBLIC, Modifier.STATIC, Modifier.FINAL)
            .initializer(CodeBlock.builder().add("1").build())
            .build());
    }

    /**
     * Generate methods for marking parent as set and checking if a message has a parent.
     *
     * @param typeName The {@link TypeName} for the message.
     * @param parentBit The bit used for parent tracking of a POJO.
     * @return methods for marking parent as set and checking if a message has a parent.
     */
    private List<MethodSpec.Builder> generateParentingMethods(@Nonnull final TypeName typeName,
                                                              @Nonnull final ParentBitField parentBit) {
        // Mark parent is private and
        final MethodSpec.Builder onParentSet = MethodSpec.methodBuilder(PojoCodeGenerator.ON_PARENT_SETTING_METHOD)
            .addJavadoc("Call when setting the parent of a POJO. This method is for internal"
                + "\nuse by the generated POJO library code. Each POJO can have its parent"
                + "\nset only once. POJOs cannot be re-parented, instead further calls to"
                + "\nthis method may generate a copy of the POJO. This prevents multiple POJOs"
                + "\nfrom retaining references to the same POJO. If we allowed this, it would let"
                + "\nchanging the internals of one POJO to change the internals of a different"
                + "\nPOJO which could lead to extremely hard to debug problems where POJO internals"
                + "\nseem to magically change."
                + "\n\n@return reference to this POJO, or if this POJO already has a parent set,"
                + "\nreturns a copy of this POJO.")
            .addModifiers(Modifier.PUBLIC)
            .addAnnotation(Nonnull.class)
            .returns(typeName);

        onParentSet.addCode(
            CodeBlock.builder()
                .addStatement("final $T pojo", typeName)
                .add("\n")
                .beginControlFlow("if (hasParent())")
                .add("// This pojo already has a parent. Create a copy of this POJO to be retained\n"
                    + "// by the new parent.\n")
                .addStatement("pojo = copy()")
                .nextControlFlow("else")
                .add("// This pojo has no existing parent, it is fine to let another POJO retain a reference\n"
                    + "// to this one without a copy.\n")
                .addStatement("pojo = this")
                .endControlFlow()
                .addStatement("pojo.$L |= $L", parentBit.parentBitFieldName, parentBit.parentBitFieldMask)
                .addStatement("return pojo")
                .build());

        final MethodSpec.Builder hasParent = MethodSpec.methodBuilder("hasParent")
            .addJavadoc("Check if this POJO has ever had its parent set."
                + "\n\n@return Check if this POJO has ever had its parent set")
            .addAnnotation(Nonnull.class)
            .returns(TypeName.BOOLEAN);

        hasParent.addCode(
            CodeBlock.builder()
                .addStatement("return (this.$L & $L) != 0", parentBit.parentBitFieldName,
                    parentBit.parentBitFieldMask)
                .build());

        return Arrays.asList(onParentSet, hasParent);
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
            .addAnnotation(Override.class)
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
            .addAnnotation(Override.class)
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
                                                           @Nonnull final List<IPojoField> pojoFields) {
        final ParameterSpec.Builder param = ParameterSpec.builder(protoOrBuilderTypeName,
                PojoCodeGenerator.FROM_PROTO_FIELD_NAME)
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
        pojoFields.forEach(field -> field.addFromProtoForField(codeBlock, protoOrBuilderTypeName,
            PojoCodeGenerator.FROM_PROTO_FIELD_NAME));
        codeBlock.addStatement("return pojo");
        method.addCode(codeBlock.build());

        return Optional.of(method);
    }

    /**
     * Generate the copy constructor.
     *
     * @param typeName The type to be created from the proto.
     * @param interfaceTypeName The type name for the read-only interface for the POJO.
     * @param pojoFields The fields in this pojo that represents the proto message.
     * @return the copy constructor method
     */
    private Optional<MethodSpec.Builder> generateCopyConstructor(@Nonnull final TypeName typeName,
                                                                 @Nonnull final TypeName interfaceTypeName,
                                                                 @Nonnull final List<IPojoField> pojoFields) {
        final ParameterSpec.Builder param = ParameterSpec.builder(interfaceTypeName, "other")
            .addModifiers(Modifier.FINAL)
            .addAnnotation(Nonnull.class)
            .addJavadoc("The pojo to copy.");

        final MethodSpec.Builder method = MethodSpec.constructorBuilder()
            .addJavadoc("Create a new {@link $T} equivalent to another", typeName)
            .addModifiers(Modifier.PUBLIC)
            .addParameter(param.build())
            .addAnnotation(Nonnull.class);

        final CodeBlock.Builder codeBlock = CodeBlock.builder();

        pojoFields.forEach(field -> field.addCopyForField(codeBlock));
        method.addCode(codeBlock.build());

        return Optional.of(method);
    }

    /**
     * Generate the equals method.
     *
     * @param typeName The of this pojo message.
     * @param pojoFields The fields in this pojo that represents the proto message.
     * @return the equals method
     */
    private Optional<MethodSpec.Builder> generateEquals(@Nonnull final TypeName typeName,
                                                        @Nonnull final List<IPojoField> pojoFields) {
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
     * @param pojoFields List of all pojo fields we are generating for this POJO.
     * @return the hashCode method
     */
    private Optional<MethodSpec.Builder> generateHashCode(@Nonnull final List<IPojoField> pojoFields) {

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

    /**
     * Generate the clear method. The clear method clears all the fields on the POJO.
     *
     * @param typeName the type name for the message
     * @param pojoFields all fields on the message
     * @param primitiveFieldBits The bits for storing primitive fields that are set.
     * @param parentBit The bit used for parent-tracking.
     * @return the clear method
     */
    private Optional<MethodSpec.Builder> generateClear(@Nonnull final TypeName typeName,
                                                       @Nonnull final List<IPojoField> pojoFields,
                                                       @Nonnull final PrimitiveFieldBits primitiveFieldBits,
                                                       @Nonnull final ParentBitField parentBit) {
        final MethodSpec.Builder method = MethodSpec.methodBuilder("clear")
            .addModifiers(Modifier.PUBLIC)
            .addJavadoc("Clear all fields of this POJO (restores them to their default values).")
            .returns(typeName);

        // Clear all the bitfields.
        primitiveFieldBits.getAllBitFieldNames()
            .forEach(bitFieldName -> {
                if (bitFieldName.equals(parentBit.parentBitFieldName)) {
                    // Do not clear the parent bit. If the parent is set, clear should not
                    // unset it.
                    method.addStatement("this.$L &= $L", bitFieldName, parentBit.parentBitFieldMask);
                } else {
                    method.addStatement("this.$L = 0", bitFieldName);
                }
            });

        final CodeBlock.Builder codeBlock = CodeBlock.builder();
        pojoFields.forEach(field -> field.addClearForField(codeBlock));
        codeBlock.addStatement("return this");
        method.addCode(codeBlock.build());

        return Optional.of(method);
    }

    /**
     * The {@link ParentBitField} assigns a bit in a bit field for tracking whether a parent
     * has ever been set on a POJO. This helps prevent sharing references to the same POJO
     * by different parents, because if we allowed this, changing the internals of one POJO
     * could change the internals of a completely different POJO, leading to bugs.
     */
    private static class ParentBitField {
        private final String parentBitFieldName;
        private final String parentBitFieldMask;

        private ParentBitField(@Nonnull final PrimitiveFieldBits bits) {
            parentBitFieldName = bits.getCurrentBitfieldName();
            parentBitFieldMask = bits.getCurrentBitmask();
            bits.increment();
        }
    }
}
