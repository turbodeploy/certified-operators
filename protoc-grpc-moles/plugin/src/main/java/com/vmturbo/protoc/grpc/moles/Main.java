package com.vmturbo.protoc.grpc.moles;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;

import com.google.protobuf.compiler.PluginProtos.CodeGeneratorRequest;
import com.google.protobuf.compiler.PluginProtos.CodeGeneratorResponse;

public class Main {
    /**
     * This is the main entry point.
     *
     * This reads the {@link CodeGeneratorRequest} from stdin, and writes a
     * {@link CodeGeneratorResponse} to stdout.
     *
     * @param args Command-line args (ignored).
     * @throws IOException If there is a problem writing the response.
     */
    public static void main(String[] args) throws IOException {
        final CodeGeneratorRequest req = CodeGeneratorRequest.parseFrom(new BufferedInputStream(System.in));
        final ServiceMoleGenerator spyableServiceGenerator = new ServiceMoleGenerator();
        final CodeGeneratorResponse response = spyableServiceGenerator.generate(req);

        final BufferedOutputStream outputStream = new BufferedOutputStream(System.out);
        response.writeTo(outputStream);
        outputStream.flush();
    }
}
