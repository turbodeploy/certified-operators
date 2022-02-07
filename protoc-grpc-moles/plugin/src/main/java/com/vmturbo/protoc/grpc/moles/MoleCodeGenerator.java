package com.vmturbo.protoc.grpc.moles;

import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.commons.lang3.StringUtils;

import com.google.protobuf.DescriptorProtos.FileDescriptorProto;

import com.vmturbo.protoc.plugin.common.generator.ProtocPluginCodeGenerator;
import com.vmturbo.protoc.plugin.common.generator.ServiceMethodDescriptor;
import com.vmturbo.protoc.plugin.common.generator.ServiceDescriptor;

/**
 * An implementation of {@link ProtocPluginCodeGenerator} that generates gRPC service
 * implementations intended for use in unit tests via Mockito.spy().
 */
public class MoleCodeGenerator extends ProtocPluginCodeGenerator {

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean skipFile(@Nonnull final FileDescriptorProto file) {
        return file.getServiceList().isEmpty();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Nonnull
    public String getPluginName() {
        return "protoc-grpc-moles";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Nonnull
    protected String generatePluginJavaClass(@Nonnull final String protoJavaClass) {
        return protoJavaClass + "Moles";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Nonnull
    protected String generateImports() {
        return "import java.util.Collections;" +
            "import java.util.List;" +
            "import java.util.LinkedList;" +
            "import java.util.Optional;" +
            "import io.grpc.stub.StreamObserver;";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Nonnull
    protected Optional<String> generateServiceCode(@Nonnull final ServiceDescriptor serviceDescriptor) {
        return Optional.of(MoleTemplateFactory.service()
                .add("serviceName", serviceDescriptor.getName())
                .add("package", serviceDescriptor.getJavaPkgName())
                .add("methodDefinitions", serviceDescriptor.getMethodDescriptors().stream()
                        .map(this::generateServiceMethodCode)
                        .collect(Collectors.toList()))
                .render());
    }

    @Nonnull
    private String generateServiceMethodCode(@Nonnull final ServiceMethodDescriptor serviceMethodDescriptorProto) {
        return MoleTemplateFactory.serviceMethod(serviceMethodDescriptorProto.getProto().getClientStreaming())
            .add("isServerStream", serviceMethodDescriptorProto.getProto().getServerStreaming())
            .add("outputType", serviceMethodDescriptorProto.getOutputMessage().getQualifiedOriginalName())
            .add("methodName", StringUtils.uncapitalize(serviceMethodDescriptorProto.getName()))
            .add("inputType", serviceMethodDescriptorProto.getInputMessage().getQualifiedOriginalName())
            .render();
    }
}
