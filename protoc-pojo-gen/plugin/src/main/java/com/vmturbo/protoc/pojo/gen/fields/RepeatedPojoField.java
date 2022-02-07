package com.vmturbo.protoc.pojo.gen.fields;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.lang.model.element.Modifier;

import com.google.common.base.Preconditions;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.WildcardTypeName;

import com.vmturbo.protoc.plugin.common.generator.FieldDescriptor;

/**
 * A field representing a repeated (list) field on a protobuf being compiled to a POJO.
 */
public class RepeatedPojoField extends UnaryPojoField {

    private final TypeName listTypeParameter;

    /**
     * Create a new {@link RepeatedPojoField}.
     *
     * @param fieldDescriptor The descriptor for the field.
     * @param parentTypeName The {@link TypeName} for the field's parent.
     */
    public RepeatedPojoField(@Nonnull FieldDescriptor fieldDescriptor,
                             @Nonnull TypeName parentTypeName) {
        super(fieldDescriptor, parentTypeName);

        Preconditions.checkArgument(getGenericTypeParameters().size() == 1,
            "RepeatedPojoField %s (%s) (%s) with %s generic type parameters when 1 required.",
            fieldDescriptor.getSuffixedName(), fieldDescriptor.getProto().getTypeName(),
            fieldDescriptor.getTypeName(),
            Integer.toString(getGenericTypeParameters().size()));
        listTypeParameter = getGenericTypeParameters().get(0).getTypeName();
    }

    @Nonnull
    @Override
    public List<MethodSpec.Builder> generateGetterMethods() {
        return Arrays.asList(generateListGetter(), generateCount(), generateGetterAtIndex());
    }

    @Nonnull
    @Override
    public List<MethodSpec.Builder> generateSetterMethods() {
        return Arrays.asList(generateAdd(), generateAddAll(), generateSetterAtIndex());
    }

    @Nonnull
    @Override
    public List<MethodSpec.Builder> generateHazzerMethods() {
        // No hazzer methods for repeated fields.
        return Collections.emptyList();
    }

    @Nonnull
    @Override
    public List<MethodSpec.Builder> generateClearMethods() {
        final MethodSpec.Builder method = MethodSpec.methodBuilder("clear" + capitalizedFieldName())
            .addModifiers(Modifier.PUBLIC)
            .addAnnotation(Nonnull.class)
            .returns(getParentTypeName())
            .addJavadoc("Clear the $L.\n", listFieldName());

        final CodeBlock codeBlock = CodeBlock.builder()
            .addStatement("this.$L = null", fieldDescriptor.getSuffixedName())
            .addStatement("return this")
            .build();

        return Collections.singletonList(method.addCode(codeBlock));
    }

    @Override
    public void addToProtoForField(@Nonnull final CodeBlock.Builder codeBlock,
                                   @Nonnull final TypeName protoTypeName) {
        codeBlock.add("")
            .beginControlFlow("if ($L != null)", fieldDescriptor.getSuffixedName())
            .add("builder.addAll$L(",
                capitalizedFieldName())
            .add(codeBlockForToProto())
            .addStatement(")")
            .endControlFlow();
    }

    @Override
    public void addFromProtoForField(@Nonnull final CodeBlock.Builder codeBlock,
                                     @Nonnull final TypeName protoOrBuilderTypeName,
                                     @Nonnull final  String protoFieldName) {
        codeBlock.add("")
            .beginControlFlow("if ($L.get$LCount() > 0)", protoFieldName, capitalizedFieldName())
            .add("pojo.addAll$L(", capitalizedFieldName())
            .add(codeBlockForFromProto())
            .addStatement(")")
            .endControlFlow();
    }

    @Override
    public void addEqualsForField(@Nonnull CodeBlock.Builder codeBlock) {
        codeBlock.add("\n");
        codeBlock.beginControlFlow("if (!get$L().equals(other.get$L()))",
            listFieldName(), listFieldName());
        codeBlock.addStatement("return false");
        codeBlock.endControlFlow();
    }

    @Override
    public void addHashCodeForField(@Nonnull CodeBlock.Builder codeBlock) {
        codeBlock.add("\n");
        codeBlock.beginControlFlow("if ($L != null)", fieldDescriptor.getSuffixedName());
        codeBlock.addStatement("hash = (37 * hash) + $L", fieldDescriptor.getProto().getNumber());
        codeBlock.addStatement("hash = (53 * hash) + $L.hashCode()", fieldDescriptor.getSuffixedName());
        codeBlock.endControlFlow();
    }

    @Override
    public void addCopyForField(@Nonnull CodeBlock.Builder codeBlock) {
        codeBlock.add("\n");
        codeBlock.beginControlFlow("if (other.$L != null)", fieldDescriptor.getSuffixedName())
            .add("this.$L = ", fieldDescriptor.getSuffixedName())
            .addStatement(codeBlockForCopy())
            .endControlFlow();
    }

    private MethodSpec.Builder generateListGetter() {
        final MethodSpec.Builder method = MethodSpec.methodBuilder("get" + capitalizedFieldName() + "List")
            .addModifiers(Modifier.PUBLIC)
            .addAnnotation(Nonnull.class)
            .returns(getTypeName())
            .addJavadoc("Get the $L list. The returned list cannot be modified.\n"
                + "To modify use the appropriate add,remove, and clear methods.", getFieldName());

        final CodeBlock codeBlock = CodeBlock.builder()
            .beginControlFlow("if ($L == null)", fieldDescriptor.getSuffixedName())
            .addStatement("return $T.emptyList()", Collections.class)
            .endControlFlow()
            .addStatement("return $T.unmodifiableList($L)",
                Collections.class, fieldDescriptor.getSuffixedName())
            .build();
        return method.addCode(codeBlock);
    }

    private MethodSpec.Builder generateCount() {
        final MethodSpec.Builder method = MethodSpec.methodBuilder("get" + capitalizedFieldName() + "Count")
            .addModifiers(Modifier.PUBLIC)
            .returns(TypeName.INT)
            .addJavadoc("Get the count of $L.", getFieldName());

        final CodeBlock codeBlock = CodeBlock.builder()
            .beginControlFlow("if ($L == null)", fieldDescriptor.getSuffixedName())
            .addStatement("return 0")
            .endControlFlow()
            .addStatement("return $L.size()", fieldDescriptor.getSuffixedName())
            .build();
        return method.addCode(codeBlock);
    }

    private MethodSpec.Builder generateGetterAtIndex() {
        final MethodSpec.Builder method = MethodSpec.methodBuilder("get" + capitalizedFieldName())
            .addParameter(TypeName.INT, "index", Modifier.FINAL)
            .addModifiers(Modifier.PUBLIC)
            .addAnnotation(Nonnull.class)
            .returns(listTypeParameter)
            .addJavadoc("Get the $L at the given index."
                + "\n\n@param index The index of the element to retrieve."
                + "\n@return The element at the corresponding index.", getFieldName());


        final CodeBlock codeBlock = CodeBlock.builder()
            .beginControlFlow("if ($L == null)", fieldDescriptor.getSuffixedName())
            .addStatement("throw new $T(\"Index: \" + index + \", Size: 0\")", IndexOutOfBoundsException.class)
            .endControlFlow()
            .addStatement("return $L.get(index)", fieldDescriptor.getSuffixedName())
            .build();

        return method.addCode(codeBlock);
    }

    private MethodSpec.Builder generateAdd() {
        final ParameterSpec.Builder param = ParameterSpec.builder(listTypeParameter, "value");
        param.addJavadoc("The $T to add.", listTypeParameter);
        if (!listTypeParameter.isPrimitive()) {
            param.addAnnotation(Nonnull.class);
            param.addJavadoc(" Cannot be null.");
        }

        final MethodSpec.Builder method = MethodSpec.methodBuilder("add" + capitalizedFieldName())
            .addModifiers(Modifier.PUBLIC)
            .addAnnotation(Nonnull.class)
            .addParameter(param.build())
            .returns(getParentTypeName())
            .addJavadoc("Add a $L to the $L.\n", listTypeParameter, listFieldName());

        final CodeBlock codeBlock = CodeBlock.builder()
            .addStatement("$T.requireNonNull(value)", Objects.class)
            .beginControlFlow("if ($L == null)", fieldDescriptor.getSuffixedName())
            .addStatement("this.$L = new $T<>()", fieldDescriptor.getSuffixedName(), ArrayList.class)
            .endControlFlow()
            .addStatement("this.$L.add(value)", fieldDescriptor.getSuffixedName())
            .addStatement("return this")
            .build();
        return method.addCode(codeBlock);
    }

    private MethodSpec.Builder generateAddAll() {
        final WildcardTypeName wildcardType = WildcardTypeName.subtypeOf(listTypeParameter);
        final ParameterizedTypeName collectionType = ParameterizedTypeName.get(ClassName.get(Collection.class),
            wildcardType);

        final ParameterSpec.Builder param = ParameterSpec.builder(collectionType, "values");
        param.addJavadoc("The $T values to add. Cannot be null.\n"
            + "Elements in the collection cannot be null either.", collectionType);
        param.addAnnotation(Nonnull.class);

        final MethodSpec.Builder method = MethodSpec.methodBuilder("addAll" + capitalizedFieldName())
            .addModifiers(Modifier.PUBLIC)
            .addAnnotation(Nonnull.class)
            .addParameter(param.build())
            .returns(getParentTypeName())
            .addJavadoc("Add all elements of the $L to the $L.\n", collectionType, listFieldName());

        final CodeBlock.Builder codeBlock = CodeBlock.builder();

        // First check that there are no elements in the values collection that are null.
        // We have to do this because proto builders do this and it's important to preserve
        // the existing proto builder behavior.
        codeBlock
            .addStatement("final $T<$T> vals = values.iterator()", Iterator.class, wildcardType)
            .beginControlFlow("for (int i = 0; vals.hasNext(); i++)")
            .beginControlFlow("if (vals.next() == null)")
            .addStatement("throw new $T(\"Element at index \" + i + \" is null.\")",
                NullPointerException.class)
            .endControlFlow()
            .endControlFlow();

        // Then go ahead and add the collection.
        codeBlock
            .beginControlFlow("if ($L == null)", fieldDescriptor.getSuffixedName())
            .addStatement("this.$L = new $T<>(values)", fieldDescriptor.getSuffixedName(), ArrayList.class)
            .nextControlFlow("else")
            .addStatement("this.$L.addAll(values)", fieldDescriptor.getSuffixedName())
            .endControlFlow()
            .addStatement("return this");
        return method.addCode(codeBlock.build());
    }

    private MethodSpec.Builder generateSetterAtIndex() {
        final ParameterSpec.Builder valueParam = ParameterSpec.builder(listTypeParameter, "value");
        valueParam.addJavadoc("The $T to add.", listTypeParameter);
        if (!listTypeParameter.isPrimitive()) {
            valueParam.addAnnotation(Nonnull.class);
            valueParam.addJavadoc(" Cannot be null.");
        }

        final MethodSpec.Builder method = MethodSpec.methodBuilder("set" + capitalizedFieldName())
            .addParameter(TypeName.INT, "index", Modifier.FINAL)
            .addParameter(valueParam.build())
            .addModifiers(Modifier.PUBLIC)
            .addAnnotation(Nonnull.class)
            .returns(getParentTypeName())
            .addJavadoc("Add a $L to the $L at the given index.\n", listTypeParameter, listFieldName());

        final CodeBlock codeBlock = CodeBlock.builder()
            .beginControlFlow("if ($L == null)", fieldDescriptor.getSuffixedName())
            .addStatement("$L = new $T<>()", fieldDescriptor.getSuffixedName(), ArrayList.class)
            .endControlFlow()
            .addStatement("$L.set(index, value)", fieldDescriptor.getSuffixedName())
            .addStatement("return this")
            .build();

        return method.addCode(codeBlock);
    }

    private CodeBlock codeBlockForToProto() {
        if (isMessageList()) {
            // We need to convert List of POJO messages to an iterable of proto messages.
            return CodeBlock.builder()
                .add("() -> $L.stream()\n", fieldDescriptor.getSuffixedName())
                .add("$>.map(pojo -> pojo.toProto())\n")
                .add(".iterator()$<")
                .build();
        } else {
            return CodeBlock.builder()
                .add("$L", fieldDescriptor.getSuffixedName())
                .build();
        }
    }

    private CodeBlock codeBlockForFromProto() {
        if (isMessageList()) {
            // We need to convert List of POJO messages to an iterable of proto messages.
            return CodeBlock.builder()
                .add("proto.get$L().stream()\n", listFieldName())
                .add("$>.map($T::fromProto)\n", listTypeParameter)
                .add(".collect($T.toList())$<", Collectors.class)
                .build();
        } else {
            return CodeBlock.builder()
                .add("proto.get$L()", listFieldName())
                .build();
        }
    }

    private CodeBlock codeBlockForCopy() {
        if (isMessageList()) {
            // We need to copy the individual proto messages into a new list.
            return CodeBlock.builder()
                .add("other.$L.stream()\n", fieldDescriptor.getSuffixedName())
                .add("$>.map($T::copy)\n", listTypeParameter)
                .add(".collect($T.toList())$<", Collectors.class)
                .build();
        } else {
            return CodeBlock.builder()
                .add("new $T<>(other.$L)", ArrayList.class, fieldDescriptor.getSuffixedName())
                .build();
        }
    }

    private boolean isMessageList() {
        return !(listTypeParameter.isPrimitive()
            || listTypeParameter.isBoxedPrimitive()
            || listTypeParameter.equals(STRING_TYPE_NAME)
            || listTypeParameter.equals(BYTE_STRING_TYPE_NAME)
            || fieldDescriptor.isEnum());
    }

    private String listFieldName() {
        return capitalizedFieldName() + "List";
    }
}
