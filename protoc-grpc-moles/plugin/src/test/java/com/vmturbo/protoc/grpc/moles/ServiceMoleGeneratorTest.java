package com.vmturbo.protoc.grpc.moles;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.EnumDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileOptions;
import com.google.protobuf.DescriptorProtos.ServiceDescriptorProto;

/**
 * Unit tests for some of the utility methods in {@link ServiceMoleGenerator}.
 * <p>
 * The majority of the "functional" testing is done via the "protoc-grpc-moles-test" module,
 * which actually uses the generator on some proto files, makes sure the results compile well, and
 * runs some tests using the generated classes.
 */
public class ServiceMoleGeneratorTest {

    private ServiceMoleGenerator moleGenerator = new ServiceMoleGenerator();

    @Test
    public void testFileHasTypeEnum() {
        final String name = "NAME";
        final FileDescriptorProto file = FileDescriptorProto.newBuilder()
                .addEnumType(EnumDescriptorProto.newBuilder()
                        .setName(name))
                .build();
        assertTrue(moleGenerator.fileHasType(name, file));
    }

    @Test
    public void testFileHasTypeMessage() {
        final String name = "NAME";
        final FileDescriptorProto file = FileDescriptorProto.newBuilder()
                .addMessageType(DescriptorProto.newBuilder()
                        .setName(name))
                .build();
        assertTrue(moleGenerator.fileHasType(name, file));
    }

    @Test
    public void testFileHasTypeNestedMessage() {
        final String name = "NAME";
        final FileDescriptorProto file = FileDescriptorProto.newBuilder()
                .addMessageType(DescriptorProto.newBuilder()
                        .setName("BLAH")
                        .addNestedType(DescriptorProto.newBuilder()
                            .setName(name)))
                .build();
        assertTrue(moleGenerator.fileHasType(name, file));
    }

    @Test
    public void testFileHasTypeNestedEnum() {
        final String name = "NAME";
        final FileDescriptorProto file = FileDescriptorProto.newBuilder()
                .addMessageType(DescriptorProto.newBuilder()
                        .setName("BLAH")
                        .addEnumType(EnumDescriptorProto.newBuilder()
                                .setName(name)))
                .build();
        assertTrue(moleGenerator.fileHasType(name, file));
    }

    @Test
    public void testFileHasTypeService() {
        final String name = "NAME";
        final FileDescriptorProto file = FileDescriptorProto.newBuilder()
                .addService(ServiceDescriptorProto.newBuilder()
                        .setName(name))
                .build();
        assertTrue(moleGenerator.fileHasType(name, file));
    }

    @Test
    public void testFileNotHasType() {
        final String name = "NAME";
        final FileDescriptorProto file = FileDescriptorProto.newBuilder()
                .addMessageType(DescriptorProto.newBuilder()
                        .setName("BLAH"))
                .build();
        assertFalse(moleGenerator.fileHasType(name, file));
    }

    @Test
    public void testFormatOriginalType() {
        // Suppose a file TestDTO.proto like this:
        //
        // package protoPkg;
        //
        // message TestMessage { ... }
        final String originalOuterClassPath = "com.vmturbo.moles.TestDTO";
        final String protoFullTypeName = "protoPkg.TestMessage";
        assertEquals("com.vmturbo.moles.TestDTO.TestMessage",
                moleGenerator.formatOriginalType(originalOuterClassPath, protoFullTypeName));
    }

    @Test
    public void testGetJavaPackage() {
        final String pkg = "com.vmturbo.moles";
        assertEquals(pkg, moleGenerator.getJavaPackage(FileDescriptorProto.newBuilder()
                .setOptions(FileOptions.newBuilder()
                        .setJavaPackage(pkg))
                .build()));
    }

    @Test
    public void testGetJavaPackageNotSpecified() {
        final String pkg = "protoPkg";
        assertEquals(pkg, moleGenerator.getJavaPackage(FileDescriptorProto.newBuilder()
                .setPackage(pkg)
                .build()));
    }

    @Test
    public void testGetJavaPackageOverridesProtoPkg() {
        final String pkg = "com.vmturbo.moles";
        assertEquals(pkg, moleGenerator.getJavaPackage(FileDescriptorProto.newBuilder()
                .setPackage("protoPkg")
                .setOptions(FileOptions.newBuilder()
                        .setJavaPackage(pkg))
                .build()));
    }

    @Test
    public void testFormatMockOuterClassName() {
        final FileDescriptorProto file = FileDescriptorProto.newBuilder()
                .setOptions(FileOptions.newBuilder()
                        .setJavaOuterClassname("JavaOuterClass"))
                .build();
        assertEquals("JavaOuterClassMoles", moleGenerator.formatMockOuterClassName(file));
    }

    @Test
    public void testOriginalOuterClassOuterClassName() {
        final FileDescriptorProto file = FileDescriptorProto.newBuilder()
                .setOptions(FileOptions.newBuilder()
                        .setJavaOuterClassname("JavaOuterClass"))
                .build();
        assertEquals("JavaOuterClass", moleGenerator.getOriginalOuterClass(file));
    }

    @Test
    public void testOriginalOuterClassName() {
        final FileDescriptorProto file = FileDescriptorProto.newBuilder()
                .setName("path/to/the/file/MyDTO.proto")
                .build();
        assertEquals("MyDTO", moleGenerator.getOriginalOuterClass(file));
    }

    @Test
    public void testOriginalOuterClassNameUnderscore() {
        final FileDescriptorProto file = FileDescriptorProto.newBuilder()
                .setName("path/to/the/file/my_messages.proto")
                .build();
        assertEquals("MyMessages", moleGenerator.getOriginalOuterClass(file));
    }

    @Test
    public void testOriginalOuterClassNameWithTypeCollision() {
        final FileDescriptorProto file = FileDescriptorProto.newBuilder()
                .setName("path/to/the/file/my_messages.proto")
                .addEnumType(EnumDescriptorProto.newBuilder()
                        .setName("MyMessages"))
                .build();
        assertEquals("MyMessagesOuterClass", moleGenerator.getOriginalOuterClass(file));
    }

}
