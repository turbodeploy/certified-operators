package com.vmturbo.protoc.spring.rest.generator;

import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.EnumDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.ServiceDescriptorProto;
import com.google.protobuf.compiler.PluginProtos.CodeGeneratorRequest;
import com.google.protobuf.compiler.PluginProtos.CodeGeneratorResponse;
import com.google.protobuf.compiler.PluginProtos.CodeGeneratorResponse.File;

/**
 * The generator is the class responsible for the actual
 * code generation.
 */
public class Generator {
    private static final Logger logger = LogManager.getLogger();

    /**
     * Stores information about processed messages.
     */
    private Registry registry = new Registry();

    @Nonnull
    private File generateRestFile(@Nonnull final FileDescriptorProto fileDescriptorProto) {
        logger.debug("Registering messages in file: {} in package: {}",
                fileDescriptorProto.getName(),
                fileDescriptorProto.getPackage());

        final FileDescriptorProcessingContext context =
               new FileDescriptorProcessingContext(registry, fileDescriptorProto);
        context.startEnumList();
        for (int enumIdx = 0; enumIdx < fileDescriptorProto.getEnumTypeCount(); ++enumIdx) {
            context.startListElement(enumIdx);
            final EnumDescriptorProto enumDescriptor = fileDescriptorProto.getEnumType(enumIdx);
            registry.registerEnum(context, enumDescriptor);
            context.endListElement();
        }
        context.endEnumList();

        context.startMessageList();
        for (int msgIdx = 0; msgIdx < fileDescriptorProto.getMessageTypeCount(); ++msgIdx) {
            context.startListElement(msgIdx);
            final DescriptorProto msgDescriptor = fileDescriptorProto.getMessageType(msgIdx);
            registry.registerMessage(context, msgDescriptor);
            context.endListElement();
        }
        context.endMessageList();

        context.startServiceList();
        for (int svcIdx = 0; svcIdx < fileDescriptorProto.getServiceCount(); ++svcIdx) {
            context.startListElement(svcIdx);
            final ServiceDescriptorProto svcDescriptor = fileDescriptorProto.getService(svcIdx);
            registry.registerService(context, svcDescriptor);
            context.endListElement();
        }
        context.endServiceList();

        logger.debug("Generating messages in file: {} in package: {}",
                fileDescriptorProto.getName(),
                fileDescriptorProto.getPackage());
        return context.generateFile();
    }


    @Nonnull
    public CodeGeneratorResponse generate(@Nonnull final CodeGeneratorRequest request) {
        // The request presents the proto file descriptors in topological order
        // w.r.t. dependencies - i.e. the dependencies appear before the dependents.
        // This means we can process one file at a time without a separate linking step,
        // as long as we record the processed messages in the registry.
        return CodeGeneratorResponse.newBuilder()
            .addAllFile(request.getProtoFileList().stream()
                .map(this::generateRestFile)
                .collect(Collectors.toList()))
            .build();
    }
}
