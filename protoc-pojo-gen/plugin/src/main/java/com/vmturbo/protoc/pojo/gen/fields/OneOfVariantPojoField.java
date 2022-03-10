package com.vmturbo.protoc.pojo.gen.fields;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.lang.model.element.Modifier;

import com.squareup.javapoet.AnnotationSpec;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.MethodSpec.Builder;
import com.squareup.javapoet.ParameterSpec;
import com.squareup.javapoet.TypeName;

import com.vmturbo.protoc.plugin.common.generator.FieldDescriptor;
import com.vmturbo.protoc.pojo.gen.PojoCodeGenerator;
import com.vmturbo.protoc.pojo.gen.PojoViewGenerator;
import com.vmturbo.protoc.pojo.gen.TypeNameUtilities.ParameterizedTypeName;

/**
 * A {@link UnaryPojoField} for a protobuf oneOf member. ie in the code
 *
 * <code>oneof foo {
 *     int32 bar = 1;
 *     int32 baz = 2;
 * }</code>
 * <p/>
 * foo is a oneOf while bar and baz are oneOf variants.
 */
public class OneOfVariantPojoField extends UnaryPojoField {

    private final String oneOfFieldName;

    private final Optional<TypeName> interfaceTypename;

    /**
     * Create a new {@link OneOfVariantPojoField}.
     *
     * @param fieldDescriptor The descriptor for the field.
     * @param parentTypeName The {@link TypeName} for the field's parent.
     */
    public OneOfVariantPojoField(@Nonnull FieldDescriptor fieldDescriptor,
                                 @Nonnull final TypeName parentTypeName) {
        super(fieldDescriptor, parentTypeName);
        oneOfFieldName = OneOfPojoField.formatOneOfName(fieldDescriptor.getOneofName().get());
        interfaceTypename = PojoViewGenerator.interfaceTypeNameFor(fieldDescriptor)
            .map(ParameterizedTypeName::getTypeName);
    }

    /**
     * Get the index for the oneOf of which this field is a variant.
     *
     * @return the index for the oneOf of which this field is a variant.
     */
    public int getOneOfIndex() {
        return fieldDescriptor.getProto().getOneofIndex();
    }

    /**
     * Get the number for the field within the proto.
     *
     * @return the number for the field within the proto.
     */
    public int getFieldNumber() {
        return fieldDescriptor.getProto().getNumber();
    }

    @Nonnull
    @Override
    public List<FieldSpec> generateFieldSpecs() {
        // The field is defined in the oneOf and not by the specific variants.
        return Collections.emptyList();
    }

    @Nonnull
    @Override
    public List<Builder> generateGetterMethods(@Nonnull final GenerationMode mode) {
        String javadoc = getGetterJavadoc();

        final MethodSpec.Builder getter = MethodSpec.methodBuilder("get" + capitalizedFieldName())
            .addModifiers(Modifier.PUBLIC)
            .returns(getViewOrImplTypeName())
            .addJavadoc(javadoc, fieldDescriptor.getName(), fieldDescriptor.getName());

        if (mode == GenerationMode.IMPLEMENTATION) {
            getter.addAnnotation(Override.class);
            final CodeBlock.Builder builder = CodeBlock.builder()
                .beginControlFlow("if ($L == $L)", getSuffixedCaseName(), getFieldNumber())
                .addStatement("return ($T)$L", getViewOrImplTypeName(), getSuffixedOneOfName())
                .nextControlFlow("else")
                .add("return ")
                .addStatement(SinglePojoField.getDefaultValueForSingleField(fieldDescriptor, getViewOrImplTypeName()))
                .endControlFlow();
            getter.addCode(builder.build());
        } else {
            getter.addModifiers(Modifier.ABSTRACT);
        }

        final List<MethodSpec.Builder> getters = new ArrayList<>(Collections.singletonList(getter));
        if (mode == GenerationMode.IMPLEMENTATION && isProtoMessage()) {
            javadoc = "Get the $L. If not set, creates a new {@link " + capitalizedFieldName()
                + "} equal to the default and assigns it to this POJO,\n"
                + "overriding any other case already set for this oneOf field.";

            // Proto messages also need a getOrCreate method.
            final MethodSpec.Builder getOrCreate = MethodSpec.methodBuilder("getOrCreate" + capitalizedFieldName())
                .addModifiers(Modifier.PUBLIC)
                .returns(getTypeName())
                .addJavadoc(javadoc + "\n\n@return the $L if it has been set, or create a new one if not.",
                    fieldDescriptor.getName(), fieldDescriptor.getName());

            final CodeBlock.Builder getOrCreateBuilder = CodeBlock.builder()
                .beginControlFlow("if ($L != $L)", getSuffixedCaseName(), getFieldNumber())
                .addStatement("set$L(new $T())", capitalizedFieldName(), getTypeName())
                .endControlFlow()
                .addStatement("return ($T)$L", getTypeName(), getSuffixedOneOfName());
            getOrCreate.addCode(getOrCreateBuilder.build());

            getters.add(getOrCreate);
        }

        return getters;
    }

    @Nonnull
    @Override
    public List<Builder> generateSetterMethods() {
        final boolean isPrimitive = getTypeName().isPrimitive();
        final ParameterSpec.Builder param = ParameterSpec.builder(getViewOrImplTypeName(), fieldDescriptor.getName())
            .addModifiers(Modifier.FINAL);
        String paramJavadoc = String.format("The %s.", fieldDescriptor.getName());
        if (!isPrimitive) {
            paramJavadoc += " Cannot be null. To clear the field, use the {@link #clear"
                + capitalizedFieldName() + "()} method.";
            param.addAnnotation(AnnotationSpec.builder(Nonnull.class).build());
        }

        final MethodSpec.Builder setter = MethodSpec.methodBuilder("set" + capitalizedFieldName())
            .addModifiers(Modifier.PUBLIC)
            .addParameter(param.addJavadoc(paramJavadoc).build())
            .returns(getParentTypeName())
            .addJavadoc("Set the $L.", fieldDescriptor.getName());

        final CodeBlock.Builder codeBlock = CodeBlock.builder();

        if (!isPrimitive) {
            codeBlock.addStatement("$T.requireNonNull($L)", Objects.class, fieldDescriptor.getName());
        }

        setter.addCode(codeBlock
            .addStatement("this.$L = $L", getSuffixedCaseName(), getFieldNumber())
            .add("this.$L = ", getSuffixedOneOfName())
            .addStatement(codeBlockForSetter())
            .addStatement("return this")
            .build());

        return Collections.singletonList(setter);
    }

    private CodeBlock codeBlockForSetter() {
        if (isProtoMessage()) {
            return CodeBlock.builder()
                .add("(($T)$L).$L()", getTypeName(), fieldDescriptor.getName(),
                    PojoCodeGenerator.ON_PARENT_SETTING_METHOD)
                .build();
        } else {
            return CodeBlock.builder()
                .add("$L", fieldDescriptor.getName())
                .build();
        }
    }

    @Nonnull
    @Override
    public List<Builder> generateHazzerMethods(@Nonnull final GenerationMode mode) {
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
            hasMethod.addCode(CodeBlock.builder()
                .addStatement("return $L == $L", getSuffixedCaseName(), getFieldNumber())
                .build());
        }

        return Collections.singletonList(hasMethod);
    }

    @Nonnull
    @Override
    public List<Builder> generateClearMethods() {
        final MethodSpec.Builder clearMethod = MethodSpec.methodBuilder("clear" + capitalizedFieldName())
            .addModifiers(Modifier.PUBLIC)
            .returns(getParentTypeName())
            .addJavadoc("Clear the $L.", fieldDescriptor.getName());

        final CodeBlock.Builder codeBlock = CodeBlock.builder()
            .beginControlFlow("if ($L == $L)", getSuffixedCaseName(), getFieldNumber())
            .addStatement("$L = 0", getSuffixedCaseName())
            .addStatement("$L = null", getSuffixedOneOfName())
            .endControlFlow();

        clearMethod.addCode(codeBlock
            .addStatement("return this")
            .build());

        return Collections.singletonList(clearMethod);
    }

    @Override
    public void addClearForField(@Nonnull CodeBlock.Builder codeBlock) {
        // Nothing to do. The parent oneOf will handle this.
    }

    @Override
    public void addToProtoForField(@Nonnull final CodeBlock.Builder codeBlock,
                                   @Nonnull final TypeName protoTypeName) {
        // Nothing to do. The parent oneOf will handle this.
    }

    @Override
    public void addFromProtoForField(@Nonnull final CodeBlock.Builder codeBlock,
                                     @Nonnull final TypeName protoOrBuilderTypeName,
                                     @Nonnull final String protoFieldName) {
        // Nothing to do. The parent oneOf will handle this.
    }

    @Override
    public void addEqualsForField(@Nonnull CodeBlock.Builder codeBlock) {
        // Nothing to do. The parent oneOf will handle this.
    }

    @Override
    public void addHashCodeForField(@Nonnull CodeBlock.Builder codeBlock) {
        // Nothing to do. The parent oneOf will handle this.
    }

    @Override
    public void addCopyForField(@Nonnull CodeBlock.Builder codeBlock) {
        // Nothing to do. The parent oneOf will handle this.
    }

    private String getSuffixedOneOfName() {
        return oneOfFieldName + "_";
    }

    private String getSuffixedCaseName() {
        return oneOfFieldName + "Case_";
    }

    private String getGetterJavadoc() {
        String javadoc = "Get the $L. If not set, returns the default value for this field."
            + "\nThis method does NOT override the existing oneOf case to " + oneOfFieldName
            + "Case if it is not already the oneOf case.";
        if (isProtoMessage()) {
            javadoc += "\nThis method will not create a new {@link " + capitalizedFieldName()
                + "} if one does not exist. Returns an unmodifiable view of the backing "
                + capitalizedFieldName() + ". To retrieve a mutable instance, call "
                + "{@link #getOrCreate" + capitalizedFieldName() + "()}.";
        }
        javadoc += "\n\n@return the $L if it has been set, or a default if not.";

        return javadoc;
    }

    private TypeName getViewOrImplTypeName() {
        return interfaceTypename.orElseGet(this::getTypeName);
    }
}
