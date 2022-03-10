package com.vmturbo.protoc.pojo.gen;

import java.io.Serializable;

import javax.annotation.Nonnull;
import javax.lang.model.element.Modifier;

import com.google.protobuf.AbstractMessage;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;
import com.squareup.javapoet.TypeVariableName;

/**
 * Generate the interface for "IntoProto" which is basically an interface
 * for objects on which you can call "toProto()" and "toProtoBuilder()".
 *
 * <p/>For proper proto equivalence, this interface extends {@link Serializable}
 * because protos are also serializable.
 */
public class IntoProtoInterfaceGenerator {
    /**
     * The name for the IntoProto interface.
     */
    public static final String INTERFACE_NAME = "IntoProto";

    /**
     * The javadoc for the interface.
     */
    public static final String JAVADOC = "An interface for converting a POJO to an equivalent proto or its builder.";

    /**
     * Javadoc for toProto method.
     */
    public static final String TO_PROTO_JAVADOC = "Create a built protobuf equivalent to this POJO.";

    /**
     * Javadoc for toProtoBuilder method.
     */
    public static final String TO_BUILDER_JAVADOC = "Create a protobuf builder equivalent to this POJO.";

    private final ClassName className;
    private final String javaPackage;

    /**
     * Create a new {@link IntoProtoInterfaceGenerator}.
     *
     * @param javaPackage The java package where this interface should reside.
     */
    public IntoProtoInterfaceGenerator(@Nonnull final String javaPackage) {
        className = ClassName.get(javaPackage, INTERFACE_NAME);
        this.javaPackage = javaPackage;
    }

    /**
     * Get the class path for the interface.
     *
     * @return the class path for the interface.
     */
    public String getClassPath() {
        return javaPackage + "." + INTERFACE_NAME;
    }

    /**
     * Get the java package.
     *
     * @return the java package.
     */
    public String getJavaPackage() {
        return javaPackage;
    }

    /**
     * Generate the interface.
     *
     * @return A builder for the interface definition.
     */
    public TypeSpec.Builder generateInterface() {
        // public interface IntoProto<T extends AbstractMessage, B extends AbstractMessage.Builder<B>> {
        //     T toProto();
        //     B toProtoBuilder();
        // }
        final TypeVariableName protoVariable = TypeVariableName.get("T", AbstractMessage.class);
        final TypeVariableName builderVariable = TypeVariableName.get("B",
            ParameterizedTypeName.get(ClassName.get(AbstractMessage.Builder.class), TypeVariableName.get("B")));

        return TypeSpec.interfaceBuilder(INTERFACE_NAME)
            .addModifiers(Modifier.PUBLIC)
            .addTypeVariable(protoVariable)
            .addTypeVariable(builderVariable)
            .addSuperinterface(ClassName.get(Serializable.class))
            .addJavadoc(JAVADOC)
            .addMethod(buildMethodSpec("toProto", TO_PROTO_JAVADOC, protoVariable))
            .addMethod(buildMethodSpec("toProtoBuilder", TO_BUILDER_JAVADOC, builderVariable));
    }

    /**
     * Clause for interfaces that extend this one.
     *
     * @param protoTypeName The typename for the protobuf.
     * @param builderTypeName The typename for the protobuf builder.
     * @return The typeName.
     */
    public ParameterizedTypeName getIntoProtoTypeName(@Nonnull final TypeName protoTypeName,
                                                      @Nonnull final TypeName builderTypeName) {
        return buildParameterizedTypeName(protoTypeName, builderTypeName);
    }

    private MethodSpec buildMethodSpec(@Nonnull final String name,
                                       @Nonnull final String javadoc,
                                       @Nonnull final TypeName returnType) {
        return MethodSpec.methodBuilder(name)
            .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
            .addAnnotation(Nonnull.class)
            .addJavadoc(javadoc)
            .returns(returnType)
            .build();
    }

    private ParameterizedTypeName buildParameterizedTypeName(@Nonnull final TypeName protoTypeName,
                                                             @Nonnull final TypeName builderTypeName) {
        return ParameterizedTypeName.get(className, protoTypeName, builderTypeName);
    }
}
