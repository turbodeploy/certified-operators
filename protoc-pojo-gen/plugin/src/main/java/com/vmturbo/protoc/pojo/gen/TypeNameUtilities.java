package com.vmturbo.protoc.pojo.gen;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.MirroredTypeException;

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.TypeName;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.protoc.plugin.common.generator.FieldDescriptor;
import com.vmturbo.protoc.plugin.common.generator.MessageDescriptor;

/**
 * Utilities for generating {@link TypeName}s from qualified java package/class names.
 */
public class TypeNameUtilities {
    private static final Logger logger = LogManager.getLogger();

    /**
     * Private constructor for utility class.
     */
    private TypeNameUtilities() {

    }

    /**
     * Get the {@link TypeName} for a field.
     *
     * @param fieldDescriptor The field whose {@link TypeName} should be generated.
     * @return The {@link TypeName} for the field.
     */
    public static ParameterizedTypeName generateParameterizedTypeName(@Nonnull final FieldDescriptor fieldDescriptor) {
        final String fieldType;
        if (fieldDescriptor.isEnum()) {
            // We reuse enum types from the original protobuf-compiler generation to avoid having to
            // have our own versions that require conversions back and forth.
            if (fieldDescriptor.isList()) {
                fieldType = PojoCodeGenerator.LIST_CLASS_NAME
                    + "<" + fieldDescriptor.getProtoTypeName() + ">";
            } else {
                fieldType = fieldDescriptor.getProtoTypeName();
            }
        } else if (fieldDescriptor.isMapField()) {
            // We also need to check for enums in maps.
            final MessageDescriptor msgDescriptor = fieldDescriptor.getContentMessage()
                .map(descriptor -> ((MessageDescriptor)descriptor))
                .orElseThrow(() -> new IllegalStateException("Content message not present in map field."));

            if (!msgDescriptor.isMapEntry()) {
                throw new IllegalStateException("Message descriptor for map type is not a map entry.");
            }

            final FieldDescriptor mapKey = msgDescriptor.getMapKey();
            final FieldDescriptor mapValue = msgDescriptor.getMapValue();
            fieldType = PojoCodeGenerator.MAP_CLASS_NAME
                + "<" + (mapKey.isEnum() ? mapKey.getProtoTypeName() : mapKey.getType())
                + "," + (mapValue.isEnum() ? mapValue.getProtoTypeName() : mapValue.getType()) + ">";
        } else {
            fieldType = fieldDescriptor.getType();
        }

        return generateParameterizedTypeName(fieldType);
    }

    /**
     * Generate a {@link TypeName} for a qualified Java name. If the type is a primitive, will
     * return that type. Handles List and Map types.
     *
     * @param qualifiedNameForType The name for the type. Examples:
     *                             {@code List<my.package.of.Foo>} or {@code Integer}
     * @return The corresponding {@link TypeName}.
     */
    @Nonnull
    public static ParameterizedTypeName generateParameterizedTypeName(@Nonnull final String qualifiedNameForType) {
        // For some reason, protos don't qualify the typename for lists and maps.
        // So we need to qualify them ourselves. We also have to handle generics properly.
        // First we check for a primitive
        TypeName typeName = PojoCodeGenerator.PRIMITIVES_MAP.get(qualifiedNameForType);
        if (typeName == null) {
            // First check if we have a generic type (map or list).
            if (qualifiedNameForType.startsWith(PojoCodeGenerator.LIST_CLASS_NAME)) {
                return generateCollectionFieldParameterizedTypeName(qualifiedNameForType, List.class);
            } else if (qualifiedNameForType.startsWith(PojoCodeGenerator.MAP_CLASS_NAME)) {
                return generateCollectionFieldParameterizedTypeName(qualifiedNameForType, Map.class);
            }

            // If not, we have another qualified type.
            return new ParameterizedTypeName(generateTypeNameForQualifiedType(qualifiedNameForType));
        } else {
            // We have a primitive type, return the FieldSpec for it.
            return new ParameterizedTypeName(typeName);
        }
    }

    @Nonnull
    private static ParameterizedTypeName generateCollectionFieldParameterizedTypeName(
        @Nonnull final String nameForType, @Nonnull final Class<?> klass) {
        final String[] genericParams = getGenericParameters(nameForType);
        final List<ParameterizedTypeName> genericTypes = new ArrayList<>();
        final TypeName[] generics;
        if (genericParams != null) {
            generics = new TypeName[genericParams.length];
            for (int i = 0; i < genericParams.length; i++) {
                generics[i] = generateTypeNameForQualifiedType(genericParams[i]);
                genericTypes.add(new ParameterizedTypeName(generics[i]));
            }
        } else {
            generics = new TypeName[0];
        }

        return new ParameterizedTypeName(
            com.squareup.javapoet.ParameterizedTypeName.get(ClassName.get(klass), generics),
            genericTypes);
    }

    @Nonnull
    private static TypeName generateTypeNameForQualifiedType(@Nonnull final String nameForType) {
        // We need to find the package and classname for the type.
        // ie com.vmturbo.protoc.pojo.gen.testJavaOpt2.BarClassPJ.Bar.OtherMessage should be split into
        // "com.vmturbo.protoc.pojo.gen.testJavaOpt2.BarClassPJ.Bar" and "OtherMessage".
        final int lastIndex = nameForType.lastIndexOf('.');

        if (lastIndex < 0) {
            // No qualifying package. We'll go with 'java.lang'
            if (nameForType.equals("ByteString")) {
                logger.trace("No qualifying package in type {}. Using package {}.",
                    nameForType, PojoCodeGenerator.GOOGLE_PROTOBUF_PACKAGE);
                return ClassName.get(PojoCodeGenerator.GOOGLE_PROTOBUF_PACKAGE, nameForType);
            } else {
                logger.trace("No qualifying package in type {}. Using package {}.",
                    nameForType, PojoCodeGenerator.JAVA_LANG_PACKAGE);
                return ClassName.get(PojoCodeGenerator.JAVA_LANG_PACKAGE, nameForType);
            }
        } else {
            final String packageName = nameForType.substring(0, lastIndex);
            final String stringClassName = nameForType.substring(lastIndex + 1);

            try {
                return ClassName.get(packageName, stringClassName);
            } catch (MirroredTypeException mte) {
                logger.error("Exception: ", mte);
                DeclaredType classTypeMirror = (DeclaredType)mte.getTypeMirror();
                logger.info("Generating type for classTypeMirror {}", classTypeMirror);
                return TypeName.get(classTypeMirror);
            }
        }
    }

    // Note this is coded in a way we don't support generics that have their own generics.
    // ie we support List<Foo> and Map<String, Foo>, but not List<Foo<Bar>> or Map<String, Foo<Bar>>.
    @Nullable
    private static String[] getGenericParameters(@Nonnull final String nameForType) {
        final int genericStart = nameForType.indexOf("<");
        final int genericEnd = nameForType.lastIndexOf(">");
        if (genericStart < 0 || genericEnd < 0) {
            logger.warn("Unable to identify generic parameters in nameForType: {}", nameForType);
            return null;
        }

        final String generics = nameForType.substring(genericStart + 1, genericEnd);
        return generics.split(",");
    }

    /**
     * Captures a {@link ParameterizedTypeName} and, if it has a generics, enables retrieval
     * of those generic parameters.
     */
    public static class ParameterizedTypeName {
        private final TypeName typeName;
        private final List<ParameterizedTypeName> typeParameters = new ArrayList<>();

        private ParameterizedTypeName(@Nonnull final TypeName typeName) {
            this.typeName = Objects.requireNonNull(typeName);
        }

        private ParameterizedTypeName(@Nonnull final TypeName typeName,
                                     @Nonnull final List<ParameterizedTypeName> typeParameters) {
            this.typeName = Objects.requireNonNull(typeName);
            this.typeParameters.addAll(typeParameters);
        }

        /**
         * Get the outer type for the parameterized type.
         * For a type like Map< String, Object\>, the typeName corresponds to the Map parameter.
         *
         * @return the outer type for the parameterized type.
         */
        public TypeName getTypeName() {
            return typeName;
        }

        /**
         * Get the list of type parameters.
         * For a type like Map< String, Object\>, the typeParameters correspond to [String, Object] parameters.
         *
         * @return the type parameters for the parameterized type.
         */
        public List<ParameterizedTypeName> getTypeParameters() {
            return typeParameters;
        }
    }
}
