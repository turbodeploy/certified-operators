package com.vmturbo.protoc.grpc.moles;

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
        new MoleCodeGenerator().generate();
    }
}
