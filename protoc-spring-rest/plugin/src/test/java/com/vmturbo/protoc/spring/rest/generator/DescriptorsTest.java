package com.vmturbo.protoc.spring.rest.generator;

import java.util.Collections;

import javax.annotation.Nonnull;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableList;

import com.vmturbo.protoc.spring.rest.generator.Descriptors.AbstractDescriptor;
import com.vmturbo.protoc.spring.rest.generator.FileDescriptorProcessingContext.OuterClass;

public class DescriptorsTest {

    /**
     * Test that the various name getters in {@link AbstractDescriptor} work
     * as expected with non-nested messages.
     */
    @Test
    public void testNames() {
        // The protobuf structure in this test looks like:
        //
        // TestDTO.proto:
        //
        // package test;
        //
        // option java_package = "com.vmturbo.test";
        //
        // message TestMessage {}
        OuterClass outerClass = Mockito.mock(OuterClass.class);
        Mockito.when(outerClass.getPluginJavaClass()).thenReturn("TestDTOREST");
        Mockito.when(outerClass.getProtoJavaClass()).thenReturn("TestDTO");

        final FileDescriptorProcessingContext context =
                Mockito.mock(FileDescriptorProcessingContext.class);
        Mockito.when(context.getJavaPackage()).thenReturn("com.vmturbo.test");
        Mockito.when(context.getProtobufPackage()).thenReturn("test");
        Mockito.when(context.getRegistry()).thenReturn(Mockito.mock(Registry.class));
        Mockito.when(context.getOuters()).thenReturn(Collections.emptyList());
        Mockito.when(context.getOuterClass()).thenReturn(outerClass);

        final TestDescriptor descriptor = new TestDescriptor(context, "TestName");
        Assert.assertEquals("TestName", descriptor.getName());
        Assert.assertEquals("TestName", descriptor.getNameWithinOuterClass());
        Assert.assertEquals("com.vmturbo.test.TestDTO.TestName",
                descriptor.getQualifiedOriginalName());
        Assert.assertEquals("com.vmturbo.test.TestDTOREST.TestName",
                descriptor.getQualifiedName());
        Assert.assertEquals("test.TestName",
                descriptor.getQualifiedProtoName());
    }

    /**
     * Test that the various name getters in {@link AbstractDescriptor} work
     * as expected with nested messages.
     */
    @Test
    public void testNestedTestNames() {
        // The protobuf structure in this test looks like:
        //
        // TestDTO.proto:
        //
        // package test;
        //
        // option java_package = "com.vmturbo.test";
        //
        // message MainMessage {
        //    message NestedMessage {
        //        message TestMessage {}
        //    }
        // }
        OuterClass outerClass = Mockito.mock(OuterClass.class);
        Mockito.when(outerClass.getPluginJavaClass()).thenReturn("TestDTOREST");
        Mockito.when(outerClass.getProtoJavaClass()).thenReturn("TestDTO");

        final FileDescriptorProcessingContext context =
                Mockito.mock(FileDescriptorProcessingContext.class);
        Mockito.when(context.getJavaPackage()).thenReturn("com.vmturbo.test");
        Mockito.when(context.getProtobufPackage()).thenReturn("test");
        Mockito.when(context.getRegistry()).thenReturn(Mockito.mock(Registry.class));
        Mockito.when(context.getOuters()).thenReturn(ImmutableList.of("MainMessage", "NestedMessage"));
        Mockito.when(context.getOuterClass()).thenReturn(outerClass);

        final TestDescriptor descriptor = new TestDescriptor(context, "TestName");
        Assert.assertEquals("TestName", descriptor.getName());
        Assert.assertEquals("MainMessage.NestedMessage.TestName", descriptor.getNameWithinOuterClass());
        Assert.assertEquals("com.vmturbo.test.TestDTO.MainMessage.NestedMessage.TestName",
                descriptor.getQualifiedOriginalName());
        Assert.assertEquals("com.vmturbo.test.TestDTOREST.MainMessage.NestedMessage.TestName",
                descriptor.getQualifiedName());
        Assert.assertEquals("test.MainMessage.NestedMessage.TestName",
                descriptor.getQualifiedProtoName());
    }

    /**
     * A descriptor implementation to test {@link AbstractDescriptor} functionality.
     */
    public class TestDescriptor extends AbstractDescriptor {

        public TestDescriptor(@Nonnull final FileDescriptorProcessingContext context,
                              @Nonnull final String name) {
            super(context, name);
        }

        @Nonnull
        @Override
        String generateCode() {
            return "";
        }
    }
}
