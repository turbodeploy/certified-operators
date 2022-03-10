package com.vmturbo.protoc.pojo.gen.fields;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.lang.model.element.Modifier;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type;
import com.google.protobuf.Internal;
import com.squareup.javapoet.AnnotationSpec;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterSpec;
import com.squareup.javapoet.TypeName;

import com.vmturbo.protoc.plugin.common.generator.FieldDescriptor;
import com.vmturbo.protoc.pojo.gen.PojoCodeGenerator;
import com.vmturbo.protoc.pojo.gen.PojoViewGenerator;
import com.vmturbo.protoc.pojo.gen.PrimitiveFieldBits;
import com.vmturbo.protoc.pojo.gen.TypeNameUtilities;
import com.vmturbo.protoc.pojo.gen.TypeNameUtilities.ParameterizedTypeName;

/**
 * A field representing a single field (optional or required, not repeated) on a protobuf being
 * compiled to a POJO.
 */
public abstract class SinglePojoField extends UnaryPojoField {

    /**
     * Create a new {@link SinglePojoField}.
     *
     * @param fieldDescriptor The descriptor for the field.
     * @param typeName The typeName for the field.
     * @param parentTypeName The {@link TypeName} for the field's parent.
     */
    private SinglePojoField(@Nonnull FieldDescriptor fieldDescriptor,
                            @Nonnull final ParameterizedTypeName typeName,
                            @Nonnull final TypeName parentTypeName) {
        super(fieldDescriptor, typeName, parentTypeName);
    }

    /**
     * Factory method for creating a new {@link SinglePojoField} variant based on the
     * field descriptor.
     *
     * @param fieldDescriptor The descriptor for the field to be created.
     * @param primitiveFieldBits Tracker for bitfields so that we can setup hazzers to tell whether
     *                           primitive fields are set or unset.
     * @param parentTypeName The {@link TypeName} for the field's parent.
     * @return A new {@link UnaryPojoField} to wrap around the descriptor for code generation.
     */
    public static SinglePojoField create(@Nonnull FieldDescriptor fieldDescriptor,
                                         @Nonnull final PrimitiveFieldBits primitiveFieldBits,
                                         @Nonnull final TypeName parentTypeName) {
        final ParameterizedTypeName implementationTypeName =
            TypeNameUtilities.generateParameterizedTypeName(fieldDescriptor);
        if (implementationTypeName.getTypeName().isPrimitive()) {
            return new PrimitiveSinglePojoField(fieldDescriptor,
                implementationTypeName, primitiveFieldBits, parentTypeName);
        } else {
            return new ObjectSinglePojoField(fieldDescriptor, implementationTypeName, parentTypeName);
        }
    }

    @Nonnull
    @Override
    public List<MethodSpec.Builder> generateGetterMethods(@Nonnull final GenerationMode mode) {
        String javadoc = getGetterJavadoc();

        final MethodSpec.Builder getter = MethodSpec.methodBuilder("get" + capitalizedFieldName())
            .addModifiers(Modifier.PUBLIC)
            .returns(getViewOrImplTypeName())
            .addJavadoc(javadoc + "\n\n@return the $L if it has been set, or a default if not.",
                fieldDescriptor.getName(), fieldDescriptor.getName());

        if (mode == GenerationMode.IMPLEMENTATION) {
            getter.addAnnotation(Override.class);
            getter.addCode(getGetterCodeBlock());
        } else {
            getter.addModifiers(Modifier.ABSTRACT);
        }

        final List<MethodSpec.Builder> getters = new ArrayList<>(Collections.singletonList(getter));
        if (mode == GenerationMode.IMPLEMENTATION && isProtoMessage()) {
            javadoc = "Get the $L. If not set, creates a new " + capitalizedFieldName()
                + " equal to the default and assigns it to this POJO.";
            if (fieldDescriptor.isRequired()) {
                javadoc += "\nNote that this field is required by its protobuf equivalent.";
            }

            // Proto messages also need a getOrCreate method.
            final MethodSpec.Builder getOrCreate = MethodSpec.methodBuilder("getOrCreate" + capitalizedFieldName())
                .addModifiers(Modifier.PUBLIC)
                .returns(getTypeName())
                .addJavadoc(javadoc + "\n\n@return the $L if it has been set, or create a new one if not.",
                    fieldDescriptor.getName(), fieldDescriptor.getName())
                .addCode(getGetOrCreateCodeBlock());

            getters.add(getOrCreate);
        }

        return getters;
    }

    @Nonnull
    @Override
    public List<MethodSpec.Builder> generateSetterMethods() {
        final String parameterName = fieldDescriptor.getName() + "Value";
        final ParameterSpec.Builder param = ParameterSpec.builder(getViewOrImplTypeName(), parameterName)
            .addModifiers(Modifier.FINAL);
        String paramJavadoc = String.format("The %s.", fieldDescriptor.getName());
        if (isObject()) {
            paramJavadoc += " Cannot be null. To clear the field, use the {@link #clear"
                + capitalizedFieldName() + "()} method.";
            param.addAnnotation(AnnotationSpec.builder(Nonnull.class).build());
        }

        String javadoc = "Set the $L.";
        if (fieldDescriptor.isRequired()) {
            javadoc += "\nNote that this field is required by its protobuf equivalent.";
        }

        final MethodSpec.Builder setter = MethodSpec.methodBuilder("set" + capitalizedFieldName())
            .addModifiers(Modifier.PUBLIC)
            .addParameter(param.addJavadoc(paramJavadoc).build())
            .returns(getParentTypeName())
            .addJavadoc(javadoc, fieldDescriptor.getName());

        final CodeBlock.Builder codeBlock = CodeBlock.builder();

        if (isObject()) {
            codeBlock.addStatement("$T.requireNonNull($L)", Objects.class, parameterName);
        } else {
            addBitFieldMaskForSetter(codeBlock);
        }

        setter.addCode(codeBlock
            .add(setterCodeBlock(parameterName))
            .addStatement("return this")
            .build());

        return Collections.singletonList(setter);
    }

    @Nonnull
    @Override
    public List<MethodSpec.Builder> generateHazzerMethods(@Nonnull final GenerationMode mode) {
        final MethodSpec.Builder hasMethod = MethodSpec.methodBuilder("has" + capitalizedFieldName())
            .addModifiers(Modifier.PUBLIC)
            .returns(TypeName.BOOLEAN)
            .addJavadoc("Check whether the {@code $L} field has been set.\n\n"
                + "@return whether the {@code $L} field has been set.",
                fieldDescriptor.getName(), fieldDescriptor.getName());

        if (mode == GenerationMode.INTERFACE) {
            hasMethod.addModifiers(Modifier.ABSTRACT);
        } else {
            hasMethod.addAnnotation(Override.class);
            hasMethod.addCode(getHazzerCodeBlock());
        }

        return Collections.singletonList(hasMethod);
    }

    @Nonnull
    @Override
    public List<MethodSpec.Builder> generateClearMethods() {
        final MethodSpec.Builder clearMethod = MethodSpec.methodBuilder("clear" + capitalizedFieldName())
            .addModifiers(Modifier.PUBLIC)
            .returns(getParentTypeName())
            .addJavadoc("Clear the $L.", fieldDescriptor.getName());

        final CodeBlock.Builder codeBlock = CodeBlock.builder()
            .addStatement("this.$L = $L", fieldDescriptor.getSuffixedName(), initialValueString());
        addBitFieldMaskForClear(codeBlock);

        clearMethod.addCode(codeBlock
            .addStatement("return this")
            .build());

        return Collections.singletonList(clearMethod);
    }

    @Override
    public void addCopyForField(@Nonnull CodeBlock.Builder codeBlock) {
        codeBlock.add("\n");
        codeBlock.beginControlFlow("if (other.has$L())", capitalizedFieldName());
        if (isProtoMessage()) {
            codeBlock.addStatement("set$L(new $T(other.get$L()))", capitalizedFieldName(),
                getTypeName(), capitalizedFieldName());
        } else {
            codeBlock.addStatement("set$L(other.get$L())", capitalizedFieldName(), capitalizedFieldName());
        }
        codeBlock.endControlFlow();
    }

    @Override
    public void addHashCodeForField(@Nonnull CodeBlock.Builder codeBlock) {
        codeBlock.add("\n");
        codeBlock.beginControlFlow("if (has$L())", capitalizedFieldName());
        codeBlock.addStatement("hash = (37 * hash) + $L", fieldDescriptor.getProto().getNumber());
        codeBlock.add("hash = (53 * hash) + ");
        codeBlock.addStatement(getHashCodeCodeBlockForType(
            fieldDescriptor.getProto().getType(), fieldDescriptor.getSuffixedName()).build());
        codeBlock.endControlFlow();
    }

    @Override
    public void addClearForField(@Nonnull CodeBlock.Builder codeBlock) {
        // Clearing the associated bit-field is done elsewhere.
        codeBlock.add("\n")
            .addStatement("this.$L = $L", fieldDescriptor.getSuffixedName(), initialValueString());
    }

    /**
     * Get the {@link CodeBlock} for the hashCode for particular pojo field.
     *
     * @param fieldDescriptorType The descriptor for the type of the field whose hashCode is being generated.
     * @param suffixedFieldName The name for the field (ie for a field foo, we name that member variable
     *                          with a suffixed field name "foo_".
     * @return the builder for {@link CodeBlock} for the hashCode for particular pojo field.
     */
    public static CodeBlock.Builder getHashCodeCodeBlockForType(
        @Nonnull final FieldDescriptorProto.Type fieldDescriptorType,
        @Nonnull final String suffixedFieldName) {
        final CodeBlock.Builder codeBlock = CodeBlock.builder();

        switch (fieldDescriptorType) {
            case TYPE_BOOL:
                return codeBlock
                    .add("$T.hashBoolean($L)", Internal.class, suffixedFieldName);
            case TYPE_INT64:
            case TYPE_UINT64:
            case TYPE_FIXED64:
            case TYPE_SFIXED64:
            case TYPE_SINT64:
                return codeBlock
                    .add("$T.hashLong($L)", Internal.class, suffixedFieldName);
            case TYPE_INT32:
            case TYPE_UINT32:
            case TYPE_FIXED32:
            case TYPE_SFIXED32:
            case TYPE_SINT32:
                return codeBlock
                    .add("$L", suffixedFieldName);
            case TYPE_FLOAT:
                return codeBlock
                    .add("$T.floatToIntBits($L)", Float.class, suffixedFieldName);
            case TYPE_DOUBLE:
                return codeBlock
                    .add("$T.hashLong($T.doubleToLongBits($L))", Internal.class,
                        Double.class, suffixedFieldName);
            case TYPE_ENUM:
                return codeBlock.add("$L.getNumber()", suffixedFieldName);
            default:
                // String, other message, etc.
                return codeBlock.add("$L.hashCode()", suffixedFieldName);
        }
    }

    protected CodeBlock getGetterCodeBlock() {
        return CodeBlock.builder().addStatement("return $L", fieldDescriptor.getSuffixedName()).build();
    }

    protected CodeBlock getGetOrCreateCodeBlock() {
        Preconditions.checkState(isProtoMessage(), "Should only call this method for proto message fields.");
        final CodeBlock.Builder codeBlock = CodeBlock.builder();
        codeBlock.beginControlFlow("if ($L == null)", fieldDescriptor.getSuffixedName())
            .addStatement("set$L(new $T())", capitalizedFieldName(), getTypeName())
            .endControlFlow()
            .addStatement("return $L", fieldDescriptor.getSuffixedName());

        return codeBlock.build();
    }

    protected CodeBlock setterCodeBlock(@Nonnull final String parameterName) {
        return CodeBlock.builder()
            .addStatement("this.$L = $L", fieldDescriptor.getSuffixedName(), parameterName)
            .build();
    }

    protected String getGetterJavadoc() {
        String javadoc = "Get the $L. If not set, returns the default value for this field.";
        if (fieldDescriptor.isRequired()) {
            javadoc += "\nNote that this field is required by its protobuf equivalent.";
            if (isProtoMessage()) {
                javadoc += "\nThis method will not create a new {@link " + capitalizedFieldName()
                    + "} if one does not exist.";
            }
        }

        return javadoc;
    }

    protected TypeName getViewOrImplTypeName() {
        return getTypeName();
    }

    protected abstract boolean isObject();

    protected abstract void addBitFieldMaskForSetter(@Nonnull CodeBlock.Builder codeBlock);

    protected abstract void addBitFieldMaskForClear(@Nonnull CodeBlock.Builder codeBlock);

    protected abstract CodeBlock getHazzerCodeBlock();

    /**
     * Get the default value for a field. If no default value is explicitly specified, we return
     * the default value defined by the protobuf standard for the field type.
     *
     * @param fieldDescriptor The descriptor for the field whose default value should be generated.
     * @param typeName The type for the field whose default value should be generated.
     * @return the default value for the field.
     */
    public static CodeBlock getDefaultValueForSingleField(@Nonnull final FieldDescriptor fieldDescriptor,
                                                          @Nonnull final TypeName typeName) {
        final String defaultValue = fieldDescriptor.getProto().getDefaultValue();
        final boolean isPrimitive = typeName.isPrimitive();

        if (Strings.isNullOrEmpty(defaultValue)) {
            if (isPrimitive) {
                if (typeName == TypeName.BOOLEAN) {
                    return CodeBlock.builder().add("false").build();
                } else {
                    return CodeBlock.builder().add("0").build();
                }
            } else {
                if (fieldDescriptor.getProto().getType() == Type.TYPE_STRING) {
                    return CodeBlock.builder().add("\"\"").build();
                } else if (fieldDescriptor.getProto().getType() == Type.TYPE_ENUM) {
                    // The default value for a proto enum is always the first defined variant regardless of number.
                    // ie for enum { foo = 100, bar = 1 } the default is actually foo even though it has a higher number.
                    return CodeBlock.builder().add("$T.values()[0]", typeName).build();
                } else if (fieldDescriptor.getProto().getType() == Type.TYPE_BYTES) {
                    // The default value for proto bytes is empty.
                    return CodeBlock.builder().add("$T.EMPTY", ByteString.class).build();
                } else {
                    return CodeBlock.builder().add("$T.$L()",
                        typeName, PojoCodeGenerator.GET_DEFAULT_INSTANCE).build();
                }
            }
        } else {
            if (isPrimitive) {
                return CodeBlock.builder().add("$L", defaultValue).build();
            } else if (fieldDescriptor.getProto().getType() == Type.TYPE_STRING) {
                return CodeBlock.builder().add("\"$L\"", defaultValue).build();
            } else if (fieldDescriptor.getProto().getType() == Type.TYPE_ENUM) {
                return CodeBlock.builder().add("$T.$L", typeName, defaultValue).build();
            } else if (fieldDescriptor.getProto().getType() == Type.TYPE_BYTES) {
                return CodeBlock.builder().add("$T.bytesDefaultValue(\"$L\")", Internal.class, defaultValue).build();
            } else {
                return CodeBlock.builder().add("null").build();
            }
        }
    }

    /**
     * Represents an optional or required primitive field.
     */
    public static class PrimitiveSinglePojoField extends SinglePojoField {

        private final boolean createNewBitfield;

        private final int bitFieldIndex;

        private final String bitFieldName;

        private final String bitFieldMask;

        /**
         * Create a new {@link PrimitiveSinglePojoField}.
         *
         * @param fieldDescriptor The descriptor for the field.
         * @param typeName the {@link TypeName} for the field.
         * @param fieldBits Tracker for the bitField to be used for tracking whether the primitive
         *                  field is set or unset.
         * @param parentTypeName The {@link TypeName} for the field's parent.
         */
        public PrimitiveSinglePojoField(@Nonnull FieldDescriptor fieldDescriptor,
                                        @Nonnull final ParameterizedTypeName typeName,
                                        @Nonnull final PrimitiveFieldBits fieldBits,
                                        @Nonnull final TypeName parentTypeName) {
            super(fieldDescriptor, typeName, parentTypeName);

            this.createNewBitfield = fieldBits.nextIncrementRequiresNewBitField();
            this.bitFieldName = fieldBits.getCurrentBitfieldName();
            this.bitFieldMask = fieldBits.getCurrentBitmask();
            this.bitFieldIndex = fieldBits.increment();
        }

        @Override
        protected boolean isObject() {
            return false;
        }

        @Override
        protected Optional<CodeBlock> getInitializer() {
            if (hasDefaultValue()) {
                return Optional.of(CodeBlock.builder()
                    .add("$L", defaultValueName())
                    .build());
            }
            return Optional.empty();
        }

        @Override
        @Nonnull
        public List<FieldSpec> generateFieldSpecs() {
            final List<FieldSpec> fields = super.generateFieldSpecs();
            if (createNewBitfield) {
                // create new bitfield
                fields.add(FieldSpec.builder(TypeName.INT, bitFieldName, Modifier.PRIVATE)
                    .initializer(CodeBlock.builder().add("0").build())
                    .build());
            }

            // Create static field for default value (if there is one)
            if (hasDefaultValue()) {
                final FieldSpec defaultValue = FieldSpec.builder(getTypeName(), defaultValueName(),
                        Modifier.PUBLIC, Modifier.STATIC, Modifier.FINAL)
                    .initializer(CodeBlock.builder().add(fieldDescriptor.getProto().getDefaultValue()).build())
                    .build();
                fields.add(defaultValue);
            }

            return fields;
        }

        @Override
        public void addEqualsForField(@Nonnull CodeBlock.Builder codeBlock) {
            codeBlock.add("\n");
            codeBlock.beginControlFlow("if (has$L() != other.has$L())",
                capitalizedFieldName(), capitalizedFieldName());
            codeBlock.addStatement("return false");
            codeBlock.endControlFlow();

            codeBlock.beginControlFlow("if (has$L() && $L != other.$L)",
                capitalizedFieldName(), fieldDescriptor.getSuffixedName(), fieldDescriptor.getSuffixedName());
            codeBlock.addStatement("return false");
            codeBlock.endControlFlow();
        }

        @Override
        protected void addBitFieldMaskForSetter(@Nonnull final CodeBlock.Builder codeBlock) {
            codeBlock.addStatement("$L |= $L", bitFieldName, bitFieldMask);
        }

        @Override
        protected void addBitFieldMaskForClear(@Nonnull CodeBlock.Builder codeBlock) {
            codeBlock.addStatement("$L &= ~$L", bitFieldName, bitFieldMask);
        }

        @Override
        protected CodeBlock getHazzerCodeBlock() {
            return CodeBlock.builder()
                .addStatement("return (this.$L & $L) != 0", bitFieldName, bitFieldMask)
                .build();
        }

        @Override
        protected String initialValueString() {
            final String valueWithoutDefault = getTypeName().equals(TypeName.BOOLEAN) ? "false" : "0";
            return hasDefaultValue() ? defaultValueName() : valueWithoutDefault;
        }
    }

    /**
     * Represents an optional or required field that is not a primitive.
     */
    public static class ObjectSinglePojoField extends SinglePojoField {

        private final Optional<TypeName> interfaceTypename;

        /**
         * Create a new {@link SinglePojoField}.
         *
         * @param fieldDescriptor The descriptor for the field.
         * @param parameterizedTypeName the {@link ParameterizedTypeName} for the field.
         * @param parentTypeName The {@link TypeName} for the field's parent.
         */
        public ObjectSinglePojoField(@Nonnull FieldDescriptor fieldDescriptor,
                                     @Nonnull final ParameterizedTypeName parameterizedTypeName,
                                     @Nonnull final TypeName parentTypeName) {
            super(fieldDescriptor, parameterizedTypeName, parentTypeName);
            interfaceTypename = PojoViewGenerator.interfaceTypeNameFor(fieldDescriptor)
                .map(ParameterizedTypeName::getTypeName);
        }

        @Override
        public void addToProtoForField(@Nonnull final CodeBlock.Builder codeBlock,
                                       @Nonnull final TypeName protoTypeName) {
            codeBlock.add("").beginControlFlow("if (has$L())", capitalizedFieldName());
            if (isProtoMessage()) {
                codeBlock.addStatement("builder.set$L($L.toProto())",
                    capitalizedFieldName(), fieldDescriptor.getSuffixedName());
            } else {
                codeBlock.addStatement("builder.set$L($L)",
                    capitalizedFieldName(), fieldDescriptor.getSuffixedName());
            }
            codeBlock.endControlFlow();
        }

        @Override
        public void addFromProtoForField(@Nonnull final CodeBlock.Builder codeBlock,
                                         @Nonnull final TypeName protoOrBuilderTypeName,
                                         @Nonnull final String protoFieldName) {
            codeBlock.add("")
                .beginControlFlow("if ($L.has$L())", protoFieldName, capitalizedFieldName());
            if (isProtoMessage()) {
                codeBlock.addStatement("pojo.set$L($T.fromProto($L.get$L()))",
                    capitalizedFieldName(), getTypeName(), protoFieldName, capitalizedFieldName());
            } else {
                codeBlock.addStatement("pojo.set$L($L.get$L())",
                    capitalizedFieldName(), protoFieldName, capitalizedFieldName());
            }
            codeBlock.endControlFlow();
        }

        @Override
        public void addEqualsForField(@Nonnull CodeBlock.Builder codeBlock) {
            codeBlock.add("\n");
            codeBlock.beginControlFlow("if (has$L() != other.has$L())",
                capitalizedFieldName(), capitalizedFieldName());
            codeBlock.addStatement("return false");
            codeBlock.endControlFlow();

            codeBlock.beginControlFlow("if (has$L() && !$L.equals(other.$L))",
                capitalizedFieldName(), fieldDescriptor.getSuffixedName(), fieldDescriptor.getSuffixedName());
            codeBlock.addStatement("return false");
            codeBlock.endControlFlow();
        }

        @Override
        protected boolean isObject() {
            return true;
        }

        @Override
        protected void addBitFieldMaskForSetter(@Nonnull CodeBlock.Builder codeBlock) {
            // Nothing to do
        }

        @Override
        protected void addBitFieldMaskForClear(@Nonnull CodeBlock.Builder codeBlock) {
            // Nothing to do. We use a null value to indicate a field has not been set.
        }

        @Override
        protected CodeBlock getGetterCodeBlock() {
            final CodeBlock.Builder codeBlock = CodeBlock.builder();
            codeBlock.beginControlFlow("if ($L == null)", fieldDescriptor.getSuffixedName())
                .addStatement("return $L", getDefaultValueForSingleField(fieldDescriptor, getViewOrImplTypeName()))
                .nextControlFlow("else")
                .addStatement("return $L", fieldDescriptor.getSuffixedName())
                .endControlFlow();

            return codeBlock.build();
        }

        @Override
        protected CodeBlock getHazzerCodeBlock() {
            return CodeBlock.builder()
                .addStatement("return this.$L != null", fieldDescriptor.getSuffixedName())
                .build();
        }

        @Override
        protected CodeBlock setterCodeBlock(@Nonnull final String parameterName) {
            if (isProtoMessage()) {
                return CodeBlock.builder()
                    .addStatement("this.$L = (($T)$L).$L()", fieldDescriptor.getSuffixedName(),
                        getTypeName(),
                        parameterName,
                        PojoCodeGenerator.ON_PARENT_SETTING_METHOD)
                    .build();
            } else {
                return super.setterCodeBlock(parameterName);
            }
        }

        private boolean requiresInterface() {
            return interfaceTypename.isPresent();
        }

        @Override
        protected String getGetterJavadoc() {
            if (requiresInterface()) {
                String javadoc = "Get the $L. If not set, returns the default value for this field.";
                if (fieldDescriptor.isRequired()) {
                    javadoc += "\nNote that this field is required by its protobuf equivalent.";
                    if (isProtoMessage()) {
                        javadoc += "\nThis method will not create a new {@link " + capitalizedFieldName()
                            + "} if one does not exist. Returns an unmodifiable view of the backing "
                            + capitalizedFieldName() + ". To retrieve a mutable instance, call "
                            + "{@link #getOrCreate" + capitalizedFieldName() + "()}.";
                    }
                }
                return javadoc;
            } else {
                return super.getGetterJavadoc();
            }
        }

        protected TypeName getViewOrImplTypeName() {
            return interfaceTypename.orElseGet(this::getTypeName);
        }
    }
}
