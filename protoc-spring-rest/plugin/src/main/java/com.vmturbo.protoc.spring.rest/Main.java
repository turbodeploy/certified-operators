package com.vmturbo.protoc.spring.rest;

import java.io.IOException;

import com.google.protobuf.compiler.PluginProtos.CodeGeneratorRequest;
import com.google.protobuf.compiler.PluginProtos.CodeGeneratorResponse;

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
        new SpringRestCodeGenerator().generate();
    }

}
