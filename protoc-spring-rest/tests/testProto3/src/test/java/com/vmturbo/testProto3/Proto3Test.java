package com.vmturbo.testProto3;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.is;

import org.junit.Test;

import com.vmturbo.protoc.spring.rest.testProto3.Proto3Test.EchoRequest;
import com.vmturbo.protoc.spring.rest.testProto3.Proto3Test.EchoRequest.SubMessage;
import com.vmturbo.protoc.spring.rest.testProto3.Proto3TestREST;

public class Proto3Test {

    @Test
    public void testConversionEcho() {
        final EchoRequest echoRequest = EchoRequest.newBuilder()
                .setEcho("foo")
                .build();
        assertThat(Proto3TestREST.EchoRequest.fromProto(echoRequest).toProto(), is(echoRequest));
    }

    @Test
    public void testConversionSubmessage() {
        final EchoRequest echoRequest = EchoRequest.newBuilder()
                .setEchoSubmessage(SubMessage.newBuilder()
                        .setEcho("foo"))
                .build();
        assertThat(Proto3TestREST.EchoRequest.fromProto(echoRequest).toProto(), is(echoRequest));
    }

    @Test
    public void testConversionUnset() {
        final EchoRequest echoRequest = EchoRequest.newBuilder()
                .build();
        assertThat(Proto3TestREST.EchoRequest.fromProto(echoRequest).toProto(), is(echoRequest));
    }
}
