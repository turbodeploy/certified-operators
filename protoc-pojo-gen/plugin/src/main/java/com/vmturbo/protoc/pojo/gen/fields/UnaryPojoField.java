package com.vmturbo.protoc.pojo.gen.fields;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.lang.model.element.Modifier;

import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.TypeName;

import org.apache.commons.lang3.StringUtils;

import com.vmturbo.protoc.plugin.common.generator.FieldDescriptor;
import com.vmturbo.protoc.pojo.gen.PojoCodeGenerator;
import com.vmturbo.protoc.pojo.gen.PrimitiveFieldBits;
import com.vmturbo.protoc.pojo.gen.TypeNameUtilities;
import com.vmturbo.protoc.pojo.gen.TypeNameUtilities.ParameterizedTypeName;

/**
 * Contains code for generating field definition and getters/setters/hazzers
 * for protobuf fields.
 * <p/>
 * A unary pojo field is an explicit member on the proto
 * (required, optional, repeated, map, and oneofVariant). Non-unary pojo field
 * examples are oneofs.
 */
public abstract class UnaryPojoField implements IPojoField {

    private static final String DEFAULT_VALUE_STRING = "_DEFAULT_VALUE";

    /**
     * The {@link TypeName} for the {@link String} class.
     */
    public static final TypeName STRING_TYPE_NAME = TypeName.get(String.class);

    /**
     * The {@link TypeName} for the {@link ByteString} class.
     */
    public static final TypeName BYTE_STRING_TYPE_NAME = TypeName.get(ByteString.class);

    protected final FieldDescriptor fieldDescriptor;

    /**
     * The {@link ParameterizedTypeName} corresponding to the Java class for the field.
     * Includes any generics associated with the type.
     */
    private final ParameterizedTypeName parameterizedTypeName;

    /**
     * The {@link TypeName} for the parent class. Useful as the return type on many utility
     * methods for a field.
     */
    private final TypeName parentTypeName;

    /**
     * Create a new {@link UnaryPojoField}.
     *
     * @param fieldDescriptor The descriptor for the field.
     * @param parentTypeName The {@link TypeName} for the field's parent class.
     */
    public UnaryPojoField(@Nonnull final FieldDescriptor fieldDescriptor,
                          @Nonnull final TypeName parentTypeName) {
        this.fieldDescriptor = Objects.requireNonNull(fieldDescriptor);
        this.parameterizedTypeName = TypeNameUtilities.generateParameterizedTypeName(fieldDescriptor);
        this.parentTypeName = Objects.requireNonNull(parentTypeName);
    }

    /**
     * Create a new {@link UnaryPojoField}.
     *
     * @param fieldDescriptor The descriptor for the field.
     * @param typeName The {@link TypeName} for the field.
     * @param parentTypeName The {@link TypeName} for the parent message of the field.
     */
    public UnaryPojoField(@Nonnull final FieldDescriptor fieldDescriptor,
                          @Nonnull final ParameterizedTypeName typeName,
                          @Nonnull final TypeName parentTypeName) {
        this.fieldDescriptor = Objects.requireNonNull(fieldDescriptor);
        // We reuse enum types from the original protobuf-compiler generation to avoid having to
        // have our own versions that require conversions back and forth.
        this.parameterizedTypeName = Objects.requireNonNull(typeName);
        this.parentTypeName = Objects.requireNonNull(parentTypeName);
    }

    /**
     * Get the name for the field.
     *
     * @return the name for the field.
     */
    public String getFieldName() {
        return fieldDescriptor.getName();
    }

    /**
     * The {@link TypeName} for the field.
     *
     * @return the {@link TypeName} for the field.
     */
    public TypeName getTypeName() {
        return parameterizedTypeName.getTypeName();
    }

    public List<ParameterizedTypeName> getGenericTypeParameters() {
        return parameterizedTypeName.getTypeParameters();
    }

    /**
     * The {@link TypeName} for the field's parent.
     *
     * @return the {@link TypeName} for the field's parent.
     */
    public TypeName getParentTypeName() {
        return parentTypeName;
    }

    /**
     * Get whether this field is also a proto message.
     *
     * @return whether this field is also a proto message.
     */
    public boolean isProtoMessage() {
        return fieldDescriptor.getProto().getType() == Type.TYPE_MESSAGE;
    }

    /**
     * The capitalized name for the field.
     *
     * @return The capitalized name for the field.
     */
    public String capitalizedFieldName() {
        return StringUtils.capitalize(fieldDescriptor.getName());
    }

    @Override
    public void addToProtoForField(@Nonnull final CodeBlock.Builder codeBlock,
                                   @Nonnull final TypeName protoTypeName) {
        codeBlock.add("")
            .beginControlFlow("if (has$L())", capitalizedFieldName())
            .addStatement("builder.set$L($L)", capitalizedFieldName(), fieldDescriptor.getSuffixedName())
            .endControlFlow();
    }

    @Override
    public void addFromProtoForField(@Nonnull final CodeBlock.Builder codeBlock,
                                     @Nonnull final TypeName protoOrBuilderTypeName,
                                     @Nonnull final String protoFieldName) {
        codeBlock.add("")
            .beginControlFlow("if ($L.has$L())", protoFieldName, capitalizedFieldName())
            .addStatement("pojo.set$L($L.get$L())", capitalizedFieldName(), protoFieldName, capitalizedFieldName())
            .endControlFlow();
    }

    /**
     * Factory method for creating a new {@link UnaryPojoField} variant based on the
     * field descriptor.
     *
     * @param fieldDescriptor The descriptor for the field to be created.
     * @param primitiveFieldBits Tracker for bitfields so that we can setup hazzers to tell whether
     *                           primitive fields are set or unset.
     * @param parentTypeName The {@link TypeName} for the field's parent.
     * @param oneOfVariants The variants for each oneOf on the parent message. The key is the oneOfIndex and
     *                      the values are the corresponding fields for the oneOf variants. When creating a new
     *                      oneOfVariant, the variant should be added to this map.
     * @return A new {@link UnaryPojoField} to wrap around the descriptor for code generation.
     */
    public static UnaryPojoField create(@Nonnull final FieldDescriptor fieldDescriptor,
                                        @Nonnull final PrimitiveFieldBits primitiveFieldBits,
                                        @Nonnull final TypeName parentTypeName,
                                        @Nonnull final  Map<Integer, List<OneOfVariantPojoField>> oneOfVariants) {
        if (fieldDescriptor.isMapField()) {
            return new MapPojoField(fieldDescriptor, parentTypeName);
        } else if (fieldDescriptor.isList()) {
            return new RepeatedPojoField(fieldDescriptor, parentTypeName);
        } else if (fieldDescriptor.getProto().hasOneofIndex()) {
            final OneOfVariantPojoField variant = new OneOfVariantPojoField(fieldDescriptor, parentTypeName);
            oneOfVariants.computeIfAbsent(variant.getOneOfIndex(), ndx -> new ArrayList<>())
                .add(variant);
            return variant;
        } else {
            return SinglePojoField.create(fieldDescriptor, primitiveFieldBits, parentTypeName);
        }
    }

    /**
     * Generate the {@link FieldSpec} for adding a field to the overall message
     * {@link com.squareup.javapoet.TypeSpec}.
     *
     * @return The {@link FieldSpec} for adding the associated field.
     */
    @Nonnull
    public List<FieldSpec> generateFieldSpecs() {
        final FieldSpec.Builder field = FieldSpec.builder(getTypeName(), fieldDescriptor.getSuffixedName());
        field.addModifiers(Modifier.PRIVATE);
        getInitializer().ifPresent(field::initializer);

        PojoCodeGenerator.multiLineComment(fieldDescriptor.getComment())
            .ifPresent(comment -> {
                field.addJavadoc(comment);
                if (fieldDescriptor.isRequired()) {
                    field.addJavadoc("\n\n<p/>");
                }
            });
        if (fieldDescriptor.isRequired()) {
            field.addJavadoc("This field is required.");
        }

        return Lists.newArrayList(field.build());
    }

    /**
     * Get the initializer for the field.
     *
     * @return the initializer for the field.
     */
    protected Optional<CodeBlock> getInitializer() {
        return Optional.of(CodeBlock.builder().add("null").build());
    }

    protected boolean hasDefaultValue() {
        return fieldDescriptor.getProto().hasDefaultValue();
    }

    protected String defaultValueName() {
        return IPojoField.lowerCamelToUpperUnderscore(fieldDescriptor.getName()) + DEFAULT_VALUE_STRING;
    }

    protected String initialValueString() {
        return "null";
    }
}

