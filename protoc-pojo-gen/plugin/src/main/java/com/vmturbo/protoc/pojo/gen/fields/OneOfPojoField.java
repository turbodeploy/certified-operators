package com.vmturbo.protoc.pojo.gen.fields;

import static com.vmturbo.protoc.plugin.common.generator.FieldDescriptor.formatFieldName;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;
import javax.lang.model.element.Modifier;

import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.MethodSpec.Builder;
import com.squareup.javapoet.TypeName;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.protoc.plugin.common.generator.OneOfDescriptor;
import com.vmturbo.protoc.pojo.gen.PojoCodeGenerator;
import com.vmturbo.protoc.pojo.gen.TypeNameUtilities;

/**
 * A {@link UnaryPojoField} for a protobuf oneOf. ie in the code
 *
 * <code>oneof foo {
 *     int32 bar = 1;
 *     int32 baz = 2;
 * }</code>
 * <p/>
 * foo is a oneOf while bar and baz are oneOf variants.
 */
public class OneOfPojoField implements IPojoField {

    protected static final Logger logger = LogManager.getLogger();

    private final OneOfDescriptor oneOf;

    private final String fieldName;

    private static final String CASE = "Case";

    private final TypeName oneOfCaseType;

    private final String unsetValueName;

    private final List<OneOfVariantPojoField> variants;

    /**
     * Create a new {@link OneOfPojoField}.
     *
     * @param oneOf Descriptor for the oneOf.
     * @param variants The list of individual variants associated with this oneOf.
     */
    public OneOfPojoField(@Nonnull final OneOfDescriptor oneOf,
                          @Nonnull final List<OneOfVariantPojoField> variants) {
        this.oneOf = Objects.requireNonNull(oneOf);
        this.fieldName = formatOneOfName(oneOf.getName());
        this.variants = Objects.requireNonNull(variants);

        final String protoOneOfCaseName = oneOf.getQualifiedProtoName() + CASE;
        // OneOf variant can't have generic parameters so it's fine to just keep the base typeName.
        this.oneOfCaseType = TypeNameUtilities.generateParameterizedTypeName(protoOneOfCaseName)
            .getTypeName();
        this.unsetValueName = formatFieldName(protoOneOfCaseName)
            .toUpperCase() + "_NOT_SET";
    }

    @Nonnull
    @Override
    public List<FieldSpec> generateFieldSpecs() {
        // We generate a single member variable on the typeSpec to house all variants for this oneOf.
        // We then cast as needed from object to the specific variant type. Note that this
        // means that variants that could be modeled as primitives wind up getting boxed into
        // their object versions.
        final FieldSpec.Builder field = FieldSpec.builder(Object.class, getSuffixedName());
        field.initializer(CodeBlock.builder().add("null").build());
        field.addModifiers(Modifier.PRIVATE);

        PojoCodeGenerator.multiLineComment(oneOf.getComment())
            .ifPresent(field::addJavadoc);

        final FieldSpec.Builder oneOfCase = FieldSpec.builder(TypeName.INT, getSuffixedCaseName())
            .initializer("0")
            .addModifiers(Modifier.PRIVATE);

        return Arrays.asList(field.build(), oneOfCase.build());
    }

    @Nonnull
    @Override
    public List<Builder> generateGetterMethods() {
        // We have a special getter method for getting the oneof case.
        // We reuse the proto-generated cases to avoid having to redefine an identical enum
        // and then generate translation code for them.
        final MethodSpec.Builder caseGetter = MethodSpec.methodBuilder(getCaseMethodName())
            .addModifiers(Modifier.PUBLIC)
            .returns(oneOfCaseType)
            .addJavadoc("Get the oneOf case, if set. If the oneof has not been set, returns {@code $T.$L}."
                    + "\n\n@return the oneOf case.",
                oneOfCaseType, unsetValueName)
            .addCode(CodeBlock.builder().addStatement("return $T.forNumber($L)",
                oneOfCaseType, getSuffixedCaseName()).build());

        return Collections.singletonList(caseGetter);
    }

    @Nonnull
    @Override
    public List<Builder> generateSetterMethods() {
        return Collections.emptyList();
    }

    @Nonnull
    @Override
    public List<Builder> generateHazzerMethods() {
        return Collections.emptyList();
    }

    @Nonnull
    @Override
    public List<Builder> generateClearMethods() {
        return Collections.emptyList();
    }

    @Override
    public void addToProtoForField(@Nonnull CodeBlock.Builder codeBlock,
                                   @Nonnull TypeName protoTypeName) {
        codeBlock.add("")
            .beginControlFlow("switch ($L)", getSuffixedCaseName());

        variants.forEach(variant -> {
            codeBlock.add("case $L:\n", variant.getFieldNumber());
            codeBlock.add("$>builder.set$L(", StringUtils.capitalize(variant.getFieldName()))
                .add(getVariantToProtoCode(variant))
                .addStatement(")");
            codeBlock.addStatement("break$<");
        });

        codeBlock.add("default:\n");
        codeBlock.add("$>// Nothing to do\n");
        codeBlock.addStatement("break$<");
        codeBlock.endControlFlow();
    }

    @Override
    public void addFromProtoForField(@Nonnull final CodeBlock.Builder codeBlock,
                                     @Nonnull final TypeName protoOrBuilderTypeName,
                                     @Nonnull final String protoFieldName) {
        codeBlock.add("\n")
            .beginControlFlow("switch ($L.get$L().getNumber())",
                protoFieldName, capitalizedCaseName());

        variants.forEach(variant -> {
            codeBlock.add("case $L:\n", variant.getFieldNumber());
            codeBlock.add("$>pojo.set$L(", StringUtils.capitalize(variant.getFieldName()))
                .add(getVariantFromProtoCode(variant, protoFieldName))
                .addStatement(")");
            codeBlock.addStatement("break$<");
        });

        codeBlock.add("default:\n");
        codeBlock.add("$>// Nothing to do\n");
        codeBlock.addStatement("break$<");
        codeBlock.endControlFlow();
    }

    @Override
    public void addEqualsForField(@Nonnull CodeBlock.Builder codeBlock) {
        codeBlock.add("\n");
        codeBlock.beginControlFlow("if ($L != other.$L)",
            getSuffixedCaseName(), getSuffixedCaseName());
        codeBlock.addStatement("return false");
        codeBlock.endControlFlow();

        codeBlock.add("\n")
            .beginControlFlow("if ($L != null && !($L.equals(other.$L)))",
                getSuffixedName(), getSuffixedName(), getSuffixedName())
            .addStatement("return false")
            .endControlFlow();
    }

    @Override
    public void addHashCodeForField(@Nonnull CodeBlock.Builder codeBlock) {
        codeBlock.add("")
            .beginControlFlow("switch ($L)", getSuffixedCaseName());

        variants.forEach(variant -> {
            codeBlock.add("case $L:\n", variant.getFieldNumber());
            codeBlock.addStatement("$>hash = (37 * hash) + $L", getSuffixedCaseName());
            codeBlock.add("hash = (53 * hash) + ");
            codeBlock.addStatement(SinglePojoField.getHashCodeCodeBlockForType(
                variant.fieldDescriptor.getProto().getType(),
                "get" + variant.capitalizedFieldName() + "()").build());
            codeBlock.addStatement("break$<");
        });

        codeBlock.add("default:\n");
        codeBlock.add("$>// Nothing to do\n");
        codeBlock.addStatement("break$<");
        codeBlock.endControlFlow();
    }

    @Override
    public void addCopyForField(@Nonnull CodeBlock.Builder codeBlock) {
        codeBlock.add("\n");
        codeBlock.addStatement("this.$L = other.$L", getSuffixedCaseName(), getSuffixedCaseName());
        codeBlock.addStatement("this.$L = other.$L", getSuffixedName(), getSuffixedName());
    }

    /**
     * Format the name for the oneOf.
     *
     * @param oneOfName The name for the oneOf.
     * @return The formatted name for the oneOf.
     */
    public static String formatOneOfName(@Nonnull final String oneOfName) {
        return StringUtils.uncapitalize(formatFieldName(oneOfName));
    }

    private String getSuffixedName() {
        return fieldName + "_";
    }

    private String getSuffixedCaseName() {
        return fieldName + "Case_";
    }

    private String capitalizedName() {
        return StringUtils.capitalize(fieldName);
    }

    private String capitalizedCaseName() {
        return capitalizedName() + "Case";
    }

    private CodeBlock getVariantToProtoCode(@Nonnull final OneOfVariantPojoField variant) {
        if (variant.isProtoMessage()) {
            return CodeBlock.builder()
                .add("(($T)$L).toProto()", variant.getTypeName(), getSuffixedName()).build();
        } else {
            return CodeBlock.builder()
                .add("($T)$L", variant.getTypeName(), getSuffixedName()).build();
        }
    }

    private CodeBlock getVariantFromProtoCode(@Nonnull final OneOfVariantPojoField variant,
                                              @Nonnull final String protoFieldName) {
        if (variant.isProtoMessage()) {
            return CodeBlock.builder()
                .add("$T.fromProto($L.get$L())", variant.getTypeName(),
                    protoFieldName, variant.capitalizedFieldName()).build();
        } else {
            return CodeBlock.builder()
                .add("$L.get$L()", protoFieldName, variant.capitalizedFieldName()).build();
        }
    }

    @Nonnull
    private String getCaseMethodName() {
        return "get" + capitalizedName() + "Case";
    }
}
