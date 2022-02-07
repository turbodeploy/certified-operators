package com.vmturbo.protoc.pojo.gen.fields;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

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
    public List<Builder> generateGetterMethods() {
        final MethodSpec.Builder getter = MethodSpec.methodBuilder("get" + capitalizedFieldName())
            .addModifiers(Modifier.PUBLIC)
            .returns(getTypeName())
            .addJavadoc("Get the $L. If not set, returns the default value for this field."
                    + "\n\n@return the $L",
                fieldDescriptor.getName(), fieldDescriptor.getName());

        final CodeBlock.Builder builder = CodeBlock.builder()
            .beginControlFlow("if ($L == $L)", getSuffixedCaseName(), getFieldNumber())
            .addStatement("return ($T)$L", getTypeName(), getSuffixedOneOfName())
            .nextControlFlow("else")
            .add("return ")
            .addStatement(SinglePojoField.getDefaultValueForSingleField(fieldDescriptor, getTypeName()))
            .endControlFlow();
        getter.addCode(builder.build());

        return Collections.singletonList(getter);
    }

    @Nonnull
    @Override
    public List<Builder> generateSetterMethods() {
        final boolean isPrimitive = getTypeName().isPrimitive();
        final ParameterSpec.Builder param = ParameterSpec.builder(getTypeName(), fieldDescriptor.getName())
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
            .addStatement("this.$L = $L", getSuffixedOneOfName(), fieldDescriptor.getName())
            .addStatement("return this")
            .build());

        return Collections.singletonList(setter);
    }

    @Nonnull
    @Override
    public List<Builder> generateHazzerMethods() {
        final MethodSpec.Builder hasMethod = MethodSpec.methodBuilder("has" + capitalizedFieldName())
            .addModifiers(Modifier.PUBLIC)
            .returns(TypeName.BOOLEAN)
            .addJavadoc("Check whether the {@code $L} field has been set.\n\n"
                    + "@return whether the {@code $L} field has been set.",
                fieldDescriptor.getName(), fieldDescriptor.getName());

        hasMethod.addCode(CodeBlock.builder()
            .addStatement("return $L == $L", getSuffixedCaseName(), getFieldNumber())
            .build());

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
}
