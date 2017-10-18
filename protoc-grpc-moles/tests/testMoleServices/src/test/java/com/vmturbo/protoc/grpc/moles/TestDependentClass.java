package com.vmturbo.protoc.grpc.moles;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nonnull;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;

import com.vmturbo.protoc.grpc.moles.testMoleServices.MoleServicesTest.TestRequest;
import com.vmturbo.protoc.grpc.moles.testMoleServices.MoleServicesTest.TestResponse;
import com.vmturbo.protoc.grpc.moles.testMoleServices.TestMoleServicesTestServices.TheServiceMole;
import com.vmturbo.protoc.grpc.moles.testMoleServices.TheServiceGrpc;
import com.vmturbo.protoc.grpc.moles.testMoleServices.TheServiceGrpc.TheServiceBlockingStub;

/**
 * This is half-test and half-sample, to illustrate what it's like to actually
 * use the generated implementations in a unit testing context.
 */
public class TestDependentClass {
    private static AtomicLong INSTANCE_COUNTER = new AtomicLong(0);

    private TheServiceMole mole = Mockito.spy(new TheServiceMole());

    private Server server;

    private ManagedChannel serverChannel;

    private UserOfTheService serverUser;

    @Before
    public void setup() throws IOException {
        final String name = "theTest" + INSTANCE_COUNTER.incrementAndGet();
        this.server = InProcessServerBuilder.forName(name)
                .addService(mole)
                .build();
        this.server.start();

        this.serverChannel = InProcessChannelBuilder
                .forName(name)
                .build();

        serverUser = new UserOfTheService(this.serverChannel);
    }

    @After
    public void teardown() {
        serverChannel.shutdownNow();
        server.shutdownNow();
    }
    @Test
    public void testDoSomething() {
        final long theNum = 7;
        when(mole.noStreamMethod(eq(TestRequest.newBuilder()
                .setId(theNum)
                .build())))
            .thenReturn(TestResponse.newBuilder()
                .setId(theNum)
                .build());

        assertEquals(theNum, serverUser.doSomething(theNum));
    }

    @Test
    public void testDoSomethingError() {
        final long theNum = 7;
        when(mole.noStreamMethodError(eq(TestRequest.newBuilder()
                .setId(theNum)
                .build())))
            .thenReturn(Optional.of(Status.INTERNAL.asException()));

        try {
            serverUser.doSomething(theNum);
            fail("Expected exception!");
        } catch (StatusRuntimeException e) {
            assertEquals(Status.INTERNAL, e.getStatus());
        }
    }

    /**
     * A sample class that depends on "The Service".
     */
    public static class UserOfTheService {
        private TheServiceBlockingStub theServiceStub;

        public UserOfTheService(@Nonnull final Channel theServiceChannel) {
            theServiceStub = TheServiceGrpc.newBlockingStub(theServiceChannel);
        }

        public long doSomething(long input) {
            return theServiceStub.noStreamMethod(TestRequest.newBuilder()
                    .setId(input)
                    .build())
                .getId();
        }
    }
}
