package com.vmturbo.protoc.pojo.gen.fields;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;

import javax.annotation.Nonnull;
import javax.lang.model.element.Modifier;

import com.google.common.base.Preconditions;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.MethodSpec.Builder;
import com.squareup.javapoet.ParameterSpec;
import com.squareup.javapoet.TypeName;

import com.vmturbo.protoc.plugin.common.generator.FieldDescriptor;
import com.vmturbo.protoc.plugin.common.generator.MessageDescriptor;

/**
 * A field representing a map field on a protobuf being compiled to a POJO.
 *
 * <p/>TODO: It would be nice to allow an annotation in the proto definition to specify what
 * type of map implementation to back the POJO with (ie Hashmap, LinkedHashMap, TreeMap, etc.)
 */
public class MapPojoField extends UnaryPojoField {

    private final TypeName keyTypeParameter;
    private final TypeName valueTypeParameter;

    private final FieldDescriptor mapKeyFieldDescriptor;
    private final FieldDescriptor mapValueFieldDescriptor;

    private static final Class<?> MAP_CLASS = LinkedHashMap.class;

    /**
     * Create a new {@link UnaryPojoField}.
     *
     * @param fieldDescriptor The descriptor for the field.
     * @param parentTypeName The {@link TypeName} for the field's parent.
     */
    public MapPojoField(@Nonnull FieldDescriptor fieldDescriptor,
                        @Nonnull final TypeName parentTypeName) {
        super(fieldDescriptor, parentTypeName);

        Preconditions.checkArgument(getGenericTypeParameters().size() == 2,
            "MapPojoField %s (%s) (%s) with %s generic type parameters when 2 required.",
            fieldDescriptor.getSuffixedName(), fieldDescriptor.getProto().getTypeName(),
            fieldDescriptor.getTypeName(),
            Integer.toString(getGenericTypeParameters().size()));
        keyTypeParameter = getGenericTypeParameters().get(0).getTypeName();
        valueTypeParameter = getGenericTypeParameters().get(1).getTypeName();

        final MessageDescriptor msgDescriptor = fieldDescriptor.getContentMessage()
            .map(descriptor -> ((MessageDescriptor)descriptor))
            .orElseThrow(() -> new IllegalStateException("Content message not present in map field."));

        if (!msgDescriptor.isMapEntry()) {
            throw new IllegalStateException("Message descriptor for map type is not a map entry.");
        }
        mapKeyFieldDescriptor = msgDescriptor.getMapKey();
        mapValueFieldDescriptor = msgDescriptor.getMapValue();
    }

    @Nonnull
    @Override
    public List<Builder> generateGetterMethods() {
        return Arrays.asList(generateMapGetter(), generateGetOrDefault(), generateGetOrThrow(), generateCount());
    }

    @Nonnull
    @Override
    public List<MethodSpec.Builder> generateSetterMethods() {
        return Arrays.asList(generatePut(), generatePutAll(), generateRemove());
    }

    @Nonnull
    @Override
    public List<MethodSpec.Builder> generateHazzerMethods() {
        final ParameterSpec.Builder param = ParameterSpec.builder(keyTypeParameter, "key", Modifier.FINAL);
        param.addJavadoc("The key to check for membership in the map.");
        if (!keyTypeParameter.isPrimitive()) {
            param.addAnnotation(Nonnull.class);
            param.addJavadoc(" Cannot be null.");
        }

        final MethodSpec.Builder method = MethodSpec.methodBuilder("contains" + capitalizedFieldName())
            .addParameter(param.build())
            .addModifiers(Modifier.PUBLIC)
            .addAnnotation(Nonnull.class)
            .returns(TypeName.BOOLEAN)
            .addJavadoc("Check whether the key is in the map.");

        final CodeBlock codeBlock = CodeBlock.builder()
            .beginControlFlow("if (key == null)")
            .addStatement("throw new $T()", NullPointerException.class)
            .endControlFlow()
            .beginControlFlow("if ($L == null)", fieldDescriptor.getSuffixedName())
            .addStatement("return false")
            .endControlFlow()
            .addStatement("return $L.containsKey(key)", fieldDescriptor.getSuffixedName())
            .build();

        return Collections.singletonList(method.addCode(codeBlock));
    }

    @Nonnull
    @Override
    public List<MethodSpec.Builder> generateClearMethods() {
        final MethodSpec.Builder method = MethodSpec.methodBuilder("clear" + capitalizedFieldName())
            .addModifiers(Modifier.PUBLIC)
            .addAnnotation(Nonnull.class)
            .returns(getParentTypeName())
            .addJavadoc("Clear the $L.\n", mapFieldName());

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
            .add(codeBlockForToProto())
            .endControlFlow();
    }

    @Override
    public void addFromProtoForField(@Nonnull final CodeBlock.Builder codeBlock,
                                     @Nonnull final TypeName protoOrBuilderTypeName,
                                     @Nonnull final String protoFieldName) {
        codeBlock.add("")
            .beginControlFlow("if ($L.get$LCount() > 0)", protoFieldName, capitalizedFieldName())
            .add(codeBlockForFromProto(protoFieldName))
            .endControlFlow();
    }

    @Override
    public void addEqualsForField(@Nonnull CodeBlock.Builder codeBlock) {
        codeBlock.add("\n");
        codeBlock.beginControlFlow("if (!get$L().equals(other.get$L()))",
            mapFieldName(), mapFieldName());
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
            .add(codeBlockForCopy())
            .endControlFlow();
    }

    private MethodSpec.Builder generateMapGetter() {
        final MethodSpec.Builder method = MethodSpec.methodBuilder("get" + mapFieldName())
            .addModifiers(Modifier.PUBLIC)
            .addAnnotation(Nonnull.class)
            .returns(getTypeName())
            .addJavadoc("Get the $L. The returned map cannot be modified.\n", mapFieldName());

        final CodeBlock codeBlock = CodeBlock.builder()
            .addStatement("return ($L == null) ? $T.emptyMap() : $T.unmodifiableMap($L)",
                fieldDescriptor.getSuffixedName(), Collections.class,
                Collections.class, fieldDescriptor.getSuffixedName())
            .build();

        return method.addCode(codeBlock);
    }

    private MethodSpec.Builder generateGetOrDefault() {
        final ParameterSpec.Builder keyParam = ParameterSpec.builder(keyTypeParameter, "key", Modifier.FINAL);
        keyParam.addJavadoc("The key for the entry to get.");
        if (!keyTypeParameter.isPrimitive()) {
            keyParam.addAnnotation(Nonnull.class);
            keyParam.addJavadoc(" Cannot be null.");
        }

        final ParameterSpec.Builder defaultParam = ParameterSpec.builder(valueTypeParameter, "defaultValue", Modifier.FINAL);
        defaultParam.addJavadoc("The default value to return if the key is not contained.");

        final MethodSpec.Builder method = MethodSpec.methodBuilder("get" + capitalizedFieldName() + "OrDefault")
            .addModifiers(Modifier.PUBLIC)
            .addParameter(keyParam.build())
            .addParameter(defaultParam.build())
            .returns(valueTypeParameter)
            .addJavadoc("Get the $L entry corresponding to the key or a default value if not contained.\n", mapFieldName());

        final CodeBlock codeBlock = CodeBlock.builder()
            .addStatement("$T.requireNonNull(key)", Objects.class)
            .beginControlFlow("if ($L != null)", fieldDescriptor.getSuffixedName())
            .addStatement("this.$L.getOrDefault(key, defaultValue)", fieldDescriptor.getSuffixedName())
            .endControlFlow()
            .addStatement("return defaultValue")
            .build();

        return method.addCode(codeBlock);
    }

    private MethodSpec.Builder generateGetOrThrow() {
        final ParameterSpec.Builder keyParam = ParameterSpec.builder(keyTypeParameter, "key", Modifier.FINAL);
        keyParam.addJavadoc("The key for the entry to get.");
        if (!keyTypeParameter.isPrimitive()) {
            keyParam.addAnnotation(Nonnull.class);
            keyParam.addJavadoc(" Cannot be null.");
        }

        final MethodSpec.Builder method = MethodSpec.methodBuilder("get" + capitalizedFieldName() + "OrThrow")
            .addModifiers(Modifier.PUBLIC)
            .addParameter(keyParam.build())
            .returns(valueTypeParameter)
            .addJavadoc("Get the $L entry corresponding to the key or throw an $T if not contained.\n",
                mapFieldName(), IllegalArgumentException.class);

        final CodeBlock codeBlock = CodeBlock.builder()
            .addStatement("$T.requireNonNull(key)", Objects.class)
            .beginControlFlow("if ($L != null)", fieldDescriptor.getSuffixedName())
            .add("// This is safe because we do not allow null values in the map.\n")
            .addStatement("final $T value = $L.get(key)", valueTypeParameter, fieldDescriptor.getSuffixedName())
            .beginControlFlow("if (value != null)")
            .addStatement("return value")
            .endControlFlow()
            .endControlFlow()
            .addStatement("throw new $T()", IllegalArgumentException.class)
            .build();

        return method.addCode(codeBlock);
    }

    private MethodSpec.Builder generateCount() {
        final MethodSpec.Builder method = MethodSpec.methodBuilder("get" + capitalizedFieldName() + "Count")
            .addModifiers(Modifier.PUBLIC)
            .returns(TypeName.INT)
            .addJavadoc("Get the number of entries in the $L.", mapFieldName());

        final CodeBlock codeBlock = CodeBlock.builder()
            .beginControlFlow("if ($L == null)", fieldDescriptor.getSuffixedName())
            .addStatement("return 0")
            .endControlFlow()
            .addStatement("return $L.size()", fieldDescriptor.getSuffixedName())
            .build();
        return method.addCode(codeBlock);
    }

    private MethodSpec.Builder generatePut() {
        final ParameterSpec.Builder keyParam = ParameterSpec.builder(keyTypeParameter, "key", Modifier.FINAL);
        keyParam.addJavadoc("The key for the entry to add.");
        if (!keyTypeParameter.isPrimitive()) {
            keyParam.addAnnotation(Nonnull.class);
            keyParam.addJavadoc(" Cannot be null.");
        }

        final ParameterSpec.Builder valueParam = ParameterSpec.builder(valueTypeParameter, "value", Modifier.FINAL);
        valueParam.addJavadoc("The value to add for the given key.");
        if (!valueTypeParameter.isPrimitive()) {
            valueParam.addAnnotation(Nonnull.class);
            valueParam.addJavadoc(" Cannot be null.");
        }

        final MethodSpec.Builder method = MethodSpec.methodBuilder("put" + capitalizedFieldName())
            .addModifiers(Modifier.PUBLIC)
            .addAnnotation(Nonnull.class)
            .addParameter(keyParam.build())
            .addParameter(valueParam.build())
            .returns(getParentTypeName())
            .addJavadoc("Put the value in the map at the given key");

        final CodeBlock codeBlock = CodeBlock.builder()
            .beginControlFlow("if (key == null)")
            .addStatement("throw new $T(\"Key cannot be null\")", NullPointerException.class)
            .endControlFlow()
            .beginControlFlow("if (value == null)")
            .addStatement("throw new $T(\"Value cannot be null\")", NullPointerException.class)
            .endControlFlow()
            .beginControlFlow("\nif ($L == null)", fieldDescriptor.getSuffixedName())
            .addStatement("this.$L = new $T<>()", fieldDescriptor.getSuffixedName(), MAP_CLASS)
            .endControlFlow()
            .addStatement("this.$L.put(key, value)", fieldDescriptor.getSuffixedName())
            .addStatement("return this")
            .build();
        return method.addCode(codeBlock);
    }

    private MethodSpec.Builder generatePutAll() {
        final ParameterSpec.Builder param = ParameterSpec.builder(getTypeName(), "map", Modifier.FINAL);
        param.addJavadoc("The map whose entries should be added.");
        param.addAnnotation(Nonnull.class);
        param.addJavadoc(" Cannot be null. Cannot have any null keys or values.");

        final MethodSpec.Builder method = MethodSpec.methodBuilder("putAll" + capitalizedFieldName())
            .addModifiers(Modifier.PUBLIC)
            .addAnnotation(Nonnull.class)
            .addParameter(param.build())
            .returns(getParentTypeName())
            .addJavadoc("Add all the entries of the map to the $L.\n", mapFieldName());

        final CodeBlock codeBlock = CodeBlock.builder()
            .addStatement("$T.requireNonNull(map)", Objects.class)
            .add("// Ensure that all keys and values are non-null. This is required to ensure proto compatibility.\n")
            .beginControlFlow("for ($T<$T, $T> entry : map.entrySet())",
                Entry.class, keyTypeParameter, valueTypeParameter)
            .beginControlFlow("if (entry.getKey() == null || entry.getValue() == null)")
            .addStatement("throw new $T()", NullPointerException.class)
            .endControlFlow()
            .endControlFlow()
            .beginControlFlow("\nif ($L == null)", fieldDescriptor.getSuffixedName())
            .addStatement("this.$L = new $T<>(map)", fieldDescriptor.getSuffixedName(), MAP_CLASS)
            .nextControlFlow("else")
            .addStatement("this.$L.putAll(map)", fieldDescriptor.getSuffixedName())
            .endControlFlow()
            .addStatement("return this")
            .build();

        return method.addCode(codeBlock);
    }

    private MethodSpec.Builder generateRemove() {
        final ParameterSpec.Builder keyParam = ParameterSpec.builder(keyTypeParameter, "key", Modifier.FINAL);
        keyParam.addJavadoc("The key for the entry to remove.");
        if (!keyTypeParameter.isPrimitive()) {
            keyParam.addAnnotation(Nonnull.class);
            keyParam.addJavadoc(" Cannot be null.");
        }

        final MethodSpec.Builder method = MethodSpec.methodBuilder("remove" + capitalizedFieldName())
            .addModifiers(Modifier.PUBLIC)
            .addAnnotation(Nonnull.class)
            .addParameter(keyParam.build())
            .returns(getParentTypeName())
            .addJavadoc("Remove the $L entry corresponding to the key.\n", mapFieldName());

        final CodeBlock codeBlock = CodeBlock.builder()
            .addStatement("$T.requireNonNull(key)", Objects.class)
            .beginControlFlow("if ($L != null)", fieldDescriptor.getSuffixedName())
            .addStatement("this.$L.remove(key)", fieldDescriptor.getSuffixedName())
            .endControlFlow()
            .addStatement("return this")
            .build();

        return method.addCode(codeBlock);
    }

    private CodeBlock codeBlockForToProto() {
        final boolean hasMessageKey = hasMessageKey();
        final boolean hasMessageValue = hasMessageValue();

        if (hasMessageKey || hasMessageValue) {
            // We need to copy the individual pojo messages into the proto map.
            final CodeBlock.Builder codeBlock = CodeBlock.builder()
                .beginControlFlow("for ($T<$T, $T> entry : $L.entrySet())",
                    Entry.class, keyTypeParameter, valueTypeParameter, fieldDescriptor.getSuffixedName())
                .add("builder.put$L(", capitalizedFieldName());
            if (hasMessageKey) {
                codeBlock.add("entry.getKey().toProto()");
            } else {
                codeBlock.add("entry.getKey()");
            }

            codeBlock.add(", ");
            if (hasMessageValue) {
                codeBlock.add("entry.getValue().toProto()");
            } else {
                codeBlock.add("entry.getValue()");
            }
            codeBlock.addStatement(")")
                .endControlFlow();
            return codeBlock.build();
        } else {
            return CodeBlock.builder()
                .addStatement("builder.putAll$L($L)", capitalizedFieldName(), fieldDescriptor.getSuffixedName())
                .build();
        }
    }

    private CodeBlock codeBlockForFromProto(@Nonnull final String protoFieldName) {
        final boolean hasMessageKey = hasMessageKey();
        final boolean hasMessageValue = hasMessageValue();

        if (hasMessageKey || hasMessageValue) {
            // We need to copy the individual proto messages into a new map.
            final CodeBlock.Builder codeBlock = CodeBlock.builder()
                .addStatement("pojo.$L = new $T<>($L.get$L().size())",
                    fieldDescriptor.getSuffixedName(), MAP_CLASS, protoFieldName, mapFieldName())
                .add("$L.get$L().entrySet().forEach(entry ->\n$>pojo.$L.put(",
                    protoFieldName, mapFieldName(), fieldDescriptor.getSuffixedName());
            if (hasMessageKey) {
                codeBlock.add("$T.fromProto(entry.getKey())", keyTypeParameter);
            } else {
                codeBlock.add("entry.getKey()");
            }

            codeBlock.add(", ");
            if (hasMessageValue) {
                codeBlock.add("$T.fromProto(entry.getValue())", valueTypeParameter);
            } else {
                codeBlock.add("entry.getValue()");
            }
            codeBlock.addStatement("))$<");
            return codeBlock.build();
        } else {
            return CodeBlock.builder()
                .addStatement("pojo.putAll$L($L.get$L())", capitalizedFieldName(),
                    protoFieldName, mapFieldName())
                .build();
        }
    }

    private CodeBlock codeBlockForCopy() {
        final boolean hasMessageKey = hasMessageKey();
        final boolean hasMessageValue = hasMessageValue();

        if (hasMessageKey || hasMessageValue) {
            // We need to copy the individual proto messages into a new map.
            final CodeBlock.Builder codeBlock = CodeBlock.builder()
                .addStatement("this.$L = new $T<>(other.$L.size())", fieldDescriptor.getSuffixedName(),
                    MAP_CLASS, fieldDescriptor.getSuffixedName())
                .beginControlFlow("for ($T<$T, $T> entry : other.$L.entrySet())",
                    Entry.class, keyTypeParameter, valueTypeParameter, fieldDescriptor.getSuffixedName())
                .addStatement("this.$L.put(entry.getKey(), entry.getValue())", fieldDescriptor.getSuffixedName())
                .endControlFlow();
            return codeBlock.build();
        } else {
            return CodeBlock.builder()
                .addStatement("this.$L = new $T<>(other.$L)", fieldDescriptor.getSuffixedName(),
                    MAP_CLASS, fieldDescriptor.getSuffixedName())
                .build();
        }
    }

    private boolean hasMessageKey() {
        // Note that technically protobuf doesn't allow messages as map keys but there's
        // no reason our code can't support it in case it becomes a proto feature at some point.
        return !(keyTypeParameter.isPrimitive()
            || keyTypeParameter.isBoxedPrimitive()
            || keyTypeParameter.equals(STRING_TYPE_NAME)
            || keyTypeParameter.equals(BYTE_STRING_TYPE_NAME)
            || mapKeyFieldDescriptor.isEnum());
    }

    private boolean hasMessageValue() {
        return !(valueTypeParameter.isPrimitive()
            || valueTypeParameter.isBoxedPrimitive()
            || valueTypeParameter.equals(STRING_TYPE_NAME)
            || valueTypeParameter.equals(BYTE_STRING_TYPE_NAME)
            || mapValueFieldDescriptor.isEnum());
    }

    private String mapFieldName() {
        return capitalizedFieldName() + "Map";
    }
}
