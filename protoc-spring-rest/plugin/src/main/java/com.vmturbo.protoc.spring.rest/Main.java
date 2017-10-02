package com.vmturbo.protoc.spring.rest;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;

import com.google.protobuf.compiler.PluginProtos.CodeGeneratorRequest;
import com.google.protobuf.compiler.PluginProtos.CodeGeneratorResponse;

import com.vmturbo.protoc.spring.rest.generator.Generator;

/**
 * This is the main entrypoint for the plugin. This class
 * handles interaction with the protobuf compiler.
 * <p>
 * The protobuf compiler calls the plugin, passing a
 * {@link CodeGeneratorRequest} message via stdin, and
 * expects the {@link CodeGeneratorResponse} message via
 * stdout.
 */
public class Main {

    public static void main(String[] args) throws IOException {
        final CodeGeneratorRequest req = CodeGeneratorRequest.parseFrom(new BufferedInputStream(System.in));
        final Generator generator = new Generator();
        final CodeGeneratorResponse response = generator.generate(req);

        final BufferedOutputStream outputStream = new BufferedOutputStream(System.out);
        response.writeTo(outputStream);
        outputStream.flush();
    }

}
