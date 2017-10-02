package com.vmturbo.protoc.spring.rest;

import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

import com.google.protobuf.compiler.PluginProtos.CodeGeneratorRequest;
import com.google.protobuf.compiler.PluginProtos.CodeGeneratorResponse;
import com.google.protobuf.util.JsonFormat;

import com.vmturbo.protoc.spring.rest.generator.Generator;

public class GeneratorTest {

    private static final Logger logger = LogManager.getLogger();

    @Test
    public void testImport() throws Exception {
        runTest("testImport.json");
    }

    @Test
    public void testEnum() throws Exception {
        runTest("testEnum.json");
    }

    @Test
    public void testNested() throws Exception {
        runTest("testNested.json");
    }

    @Test
    public void testGenerateSwagger() throws Exception {
        runTest("testGenerateSwagger.json");
    }

    @Test
    public void testJavaOpt() throws Exception {
        runTest("testJavaOpt.json");
    }

    @Test
    public void testNestedEnum() throws Exception {
        runTest("testNestedEnum.json");
    }

    @Test
    public void testCommentsToSwagger() throws Exception {
        runTest("testCommentsToSwagger.json");
    }

    private void runTest(String file) throws Exception {
        InputStream inputStream = Thread.currentThread().getContextClassLoader()
                .getResourceAsStream(file);
        CodeGeneratorRequest.Builder builder = CodeGeneratorRequest.newBuilder();
        JsonFormat.parser().merge(new InputStreamReader(inputStream), builder);
        CodeGeneratorRequest req = builder.build();
        Generator generator = new Generator();
        CodeGeneratorResponse response = generator.generate(req);
    }
}
