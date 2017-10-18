package com.vmturbo.protoc.grpc.moles;

import java.util.LinkedList;
import java.util.Optional;
import java.util.Queue;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.commons.lang3.StringUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.CaseFormat;
import com.google.googlejavaformat.java.Formatter;
import com.google.googlejavaformat.java.FormatterException;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.EnumDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.MethodDescriptorProto;
import com.google.protobuf.DescriptorProtos.ServiceDescriptorProto;
import com.google.protobuf.compiler.PluginProtos.CodeGeneratorRequest;
import com.google.protobuf.compiler.PluginProtos.CodeGeneratorResponse;
import com.google.protobuf.compiler.PluginProtos.CodeGeneratorResponse.File;

/**
 * Generates spyable gRPC services.
 */
public class ServiceMoleGenerator {

    /**
     * Generate spyable services for a {@link CodeGeneratorRequest}.
     *
     * @param req The {@link CodeGeneratorRequest}.
     * @return The {@link CodeGeneratorResponse} containing the service definitions.
     */
    @Nonnull
    CodeGeneratorResponse generate(@Nonnull final CodeGeneratorRequest req) {
        return CodeGeneratorResponse.newBuilder()
                .addAllFile(req.getProtoFileList().stream()
                        .map(this::generateGrpcMockFile)
                        .filter(Optional::isPresent)
                        .map(Optional::get)
                        .collect(Collectors.toList()))
                .build();
    }

    /**
     * Generate a {@link File} proto describing the gRPC mock services for a particular
     * {@link FileDescriptorProto}.
     *
     * @param fileDescriptorProto The input file. This is {@link FileDescriptorProto} describing
     *                            a single .proto file.
     * @return An {@link Optional} containing the {@link File} proto describing the gRPC mock
     *         services file to generate. An empty optional if the input did not have any services.
     */
    private Optional<File> generateGrpcMockFile(@Nonnull final FileDescriptorProto fileDescriptorProto) {
        // Check for services first.
        if (fileDescriptorProto.getServiceCount() == 0) {
            return Optional.empty();
        }

        final String javaPkg = getJavaPackage(fileDescriptorProto);
        final String mockOuterClass = formatMockOuterClassName(fileDescriptorProto);
        final String originalOuterClassPath = javaPkg + "." +
                getOriginalOuterClass(fileDescriptorProto);
        final String generatedFile = MoleTemplateFactory.file()
                .add("protoSourceName", fileDescriptorProto.getName())
                .add("pkgName", javaPkg)
                .add("outerClassName", mockOuterClass)
                .add("serviceCode", fileDescriptorProto.getServiceList().stream()
                        .map(svc -> generateServiceCode(javaPkg, originalOuterClassPath, svc))
                        .collect(Collectors.toList()))
                .render();

        // Format the code, and put it in a File proto.
        try {
            final String formattedContent = new Formatter().formatSource(generatedFile);
            return Optional.of(File.newBuilder()
                    .setName(javaPkg.replace('.','/') + "/" + mockOuterClass + ".java")
                    .setContent(formattedContent)
                    .build());
        } catch (FormatterException e) {
            throw new RuntimeException("Got error " + e.getMessage() + " when formatting content:\n" + generatedFile, e);
        }
    }

    /**
     * Generate the mockable code associated with a service method described by
     * {@link MethodDescriptorProto}.
     *
     * @param originalOuterClassPath The path to the original outer class (i.e. the outer class
     *                               of the classes generated by the regular protobuf compiler)
     * @param methodDescriptorProto The descriptor of the service method.
     * @return A string of Java code for the mockable implementation of the method.
     */
    @Nonnull
    private String generateServiceMethodCode(@Nonnull final String originalOuterClassPath,
                                             @Nonnull final MethodDescriptorProto methodDescriptorProto) {
        return MoleTemplateFactory.serviceMethod(methodDescriptorProto.getClientStreaming())
                .add("isServerStream", methodDescriptorProto.getServerStreaming())
                .add("outputType",
                        formatOriginalType(originalOuterClassPath, methodDescriptorProto.getOutputType()))
                .add("methodName", StringUtils.uncapitalize(methodDescriptorProto.getName()))
                .add("inputType",
                        formatOriginalType(originalOuterClassPath, methodDescriptorProto.getInputType()))
                .render();
    }

    /**
     * Generate the code for a spyable service (i.e. an implementation of the *ImplBase class
     * generated for a service).
     *
     * @param javaPkg The Java package of the generated service. The spyable service will be in
     *                the same package.
     * @param originalOuterClassPath The path to the original outer class (i.e. the outer class
     *                               of the classes generated by the regular protobuf compiler).
     * @param serviceDescriptor The descriptor of the gRPC service.
     * @return A srting of Java code for the spyable implementation of the service.
     */
    @Nonnull
    private String generateServiceCode(@Nonnull final String javaPkg,
                                       @Nonnull final String originalOuterClassPath,
                                       @Nonnull final ServiceDescriptorProto serviceDescriptor) {
        return MoleTemplateFactory.service()
                .add("serviceName", serviceDescriptor.getName())
                .add("package", javaPkg)
                .add("methodDefinitions", serviceDescriptor.getMethodList().stream()
                        .map(method -> generateServiceMethodCode(originalOuterClassPath, method))
                        .collect(Collectors.toList()))
                .render();
    }

    /**
     * Get the outer class for the original protobuf classes generated for
     * {@link FileDescriptorProto}. Most of the time this is just the name of the .proto file
     * (e.g. TopologyDTO.proto -> TopologyDTO). However, there are a couple of special cases
     * (see code).
     *
     * @param fileDescriptorProto The {@link FileDescriptorProto} for which we want the outer class.
     * @return The same outer class that the protobuf compiler generates for messages in the input
     *         file.
     */
    @Nonnull
    @VisibleForTesting
    String getOriginalOuterClass(@Nonnull final FileDescriptorProto fileDescriptorProto) {
        // If the Java outer class name is explicitly stated
        // (via the java_outer_classname option), use that.
        if (fileDescriptorProto.getOptions().hasJavaOuterClassname()) {
            return fileDescriptorProto.getOptions().getJavaOuterClassname();
        }

        // Need to trim the folders.
        final String fileName = fileDescriptorProto.getName()
                .substring(fileDescriptorProto.getName().lastIndexOf("/") + 1);

        // The protobuf Java compiler has some rules about how it converts
        // file names to outer class names. We try to replicate those rules
        // here, because we need to know the original class name to create
        // generated code that compiles.
        String originalClassName = fileName.replace(".proto", "");
        originalClassName = StringUtils.capitalize(originalClassName);
        if (originalClassName.contains("_")) {
            originalClassName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL,
                    StringUtils.lowerCase(originalClassName));
        }

        // Check if the file has a message equal to the file name (e.g. if
        // TopologyDTO.proto has a "TopologyDTO" enum, message, or service.
        if (fileHasType(originalClassName, fileDescriptorProto)) {
            // This is what the protobuf compiler does, so it should match.
            originalClassName += "OuterClass";
        }
        return originalClassName;
    }

    @Nonnull
    @VisibleForTesting
    String formatMockOuterClassName(@Nonnull final FileDescriptorProto fileDescriptorProto) {
        return getOriginalOuterClass(fileDescriptorProto) + "Moles";
    }

    @Nonnull
    @VisibleForTesting
    String getJavaPackage(@Nonnull final FileDescriptorProto fileDescriptorProto) {
        return fileDescriptorProto.getOptions().hasJavaPackage() ?
            fileDescriptorProto.getOptions().getJavaPackage() : fileDescriptorProto.getPackage();
    }

    /**
     * Get the fully qualified Java name of a type referenced in a {@link MethodDescriptorProto}.
     * This name can be used to refer to the
     *
     * @param originalOuterClassPath The path to the original outer class (i.e. the outer class
     *                               of the classes generated by the regular protobuf compiler).
     * @param protoFullTypeName The full type name in the protobuf file.
     * @return The fully qualified name of the type.
     */
    @Nonnull
    @VisibleForTesting
    String formatOriginalType(@Nonnull final String originalOuterClassPath,
                              @Nonnull final String protoFullTypeName) {
        // Strip away the protobuf-specific path - the outer class path is the right
        // one to use for Java.
        final int lastPeriod = protoFullTypeName.lastIndexOf(".");
        return originalOuterClassPath + protoFullTypeName.substring(lastPeriod);
    }

    /**
     * Check if the file described by a  {@link FileDescriptorProto} has a type
     * with a given name. Enums and messages (however nested) as well as services
     * count as types.
     *
     * @param typeName The type name to look for.
     * @param fileDescriptorProto The {@link FileDescriptorProto} to look in.
     * @return True if the file has the type name. False otherwise.
     */
    @VisibleForTesting
    boolean fileHasType(@Nonnull final String typeName,
                        @Nonnull final FileDescriptorProto fileDescriptorProto) {
        final Queue<EnumDescriptorProto> enumDescriptors = new LinkedList<>();
        final Queue<DescriptorProto> fieldDescriptors = new LinkedList<>();
        final Queue<ServiceDescriptorProto> serviceDescriptors = new LinkedList<>();

        enumDescriptors.addAll(fileDescriptorProto.getEnumTypeList());
        fieldDescriptors.addAll(fileDescriptorProto.getMessageTypeList());
        serviceDescriptors.addAll(fileDescriptorProto.getServiceList());

        // Process descriptors first, because they can have nested enums.
        while (!fieldDescriptors.isEmpty()) {
            final DescriptorProto fieldDescriptor = fieldDescriptors.remove();
            if (typeName.equals(fieldDescriptor.getName())) {
                return true;
            }
            enumDescriptors.addAll(fieldDescriptor.getEnumTypeList());
            fieldDescriptors.addAll(fieldDescriptor.getNestedTypeList());
        }

        while (!enumDescriptors.isEmpty()) {
            if (typeName.equals(enumDescriptors.remove().getName())) {
                return true;
            }
        }

        while (!serviceDescriptors.isEmpty()) {
            if (typeName.equals(serviceDescriptors.remove().getName())) {
                return true;
            }
        }

        return false;
    }
}
