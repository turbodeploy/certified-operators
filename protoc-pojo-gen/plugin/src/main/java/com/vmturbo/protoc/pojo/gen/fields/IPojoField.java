package com.vmturbo.protoc.pojo.gen.fields;

import java.util.List;

import javax.annotation.Nonnull;

import com.google.common.base.CaseFormat;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.CodeBlock.Builder;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeName;

/**
 * Interface for fields (member variables) on POJO (plain old java object) representations
 * for protobuf objects.
 */
public interface IPojoField {

    /**
     * Generate the {@link FieldSpec} for adding a field to the overall message
     * {@link com.squareup.javapoet.TypeSpec}.
     *
     * @return The {@link FieldSpec}s for adding the associated field(s).
     *         Return an empty list to add no fields.
     */
    @Nonnull
    List<FieldSpec> generateFieldSpecs();

    /**
     * Generate getter methods for the field. Repeated and map fields
     * may have multiple getter methods.
     *
     * @return The list of getter methods for the field.
     */
    @Nonnull
    List<MethodSpec.Builder> generateGetterMethods();

    /**
     * Generate setter methods for the field. Repeated and map fields
     * may have multiple setter methods.
     *
     * @return The list of setter methods for the field.
     */
    @Nonnull
    List<MethodSpec.Builder> generateSetterMethods();

    /**
     * Generate "has" methods for the field. This reports whether
     * the field has been set. Also generate contains methods
     * for map fields.
     *
     * @return Generate "has" methods for the fields.
     */
    @Nonnull
    List<MethodSpec.Builder> generateHazzerMethods();

    /**
     * Generate methods for clearing the field. This also includes
     * "remove" methods for repeated and map fields.
     *
     * <p/>Clearing a protobuf field restores the default value for the field.
     *
     * @return List of methods for clearing the field.
     */
    @Nonnull
    List<MethodSpec.Builder> generateClearMethods();

    /**
     * Ask the field to add the codeblock for setting its equivalent protobuf
     * builder field.
     *
     * @param codeBlock The codeBlock the field should add its toProto information to.
     * @param protoTypeName The type of the proto whose builder will be modified.
     */
    void addToProtoForField(@Nonnull CodeBlock.Builder codeBlock,
                            @Nonnull TypeName protoTypeName);

    /**
     * Ask the field to add the codeblock for setting itself from a proto.
     *
     * @param codeBlock The codeBlock the field should add its fromProto information to.
     * @param protoOrBuilderTypeName The type of the proto whose builder will be modified.
     * @param protoFieldName The name of the protoField in the proto method signature.
     */
    void addFromProtoForField(@Nonnull Builder codeBlock,
                              @Nonnull TypeName protoOrBuilderTypeName,
                              @Nonnull String protoFieldName);

    /**
     * Add code for the {@link #equals(Object)} method for this field.
     *
     * @param codeBlock The codeBlock the field should add its equals information to.
     */
    void addEqualsForField(@Nonnull CodeBlock.Builder codeBlock);

    /**
     * Add code for the {@link #equals(Object)} method for this field.
     *
     * @param codeBlock The codeBlock the field should add its equals information to.
     */
    void addHashCodeForField(@Nonnull CodeBlock.Builder codeBlock);

    /**
     * Add code for the copy constructor for this field.
     *
     * @param codeBlock The codeBlock the field should add its copy constructor information to.
     */
    void addCopyForField(@Nonnull CodeBlock.Builder codeBlock);

    /**
     * Get the capitalized name for the field. Transforms a name from lowerCamel to upperUnderscore.
     *
     * @param name The name that should be capitalized.
     * @return the name transformed from lowerCamel to upperUnderscore.
     */
    @Nonnull
    static String lowerCamelToUpperUnderscore(@Nonnull final String name) {
        return CaseFormat.LOWER_CAMEL.to(CaseFormat.UPPER_UNDERSCORE, name);
    }
}
