package com.vmturbo.protoc.grpc.moles;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;

import com.vmturbo.protoc.grpc.moles.testMoleServices.MoleServicesTest.TestRequest;
import com.vmturbo.protoc.grpc.moles.testMoleServices.MoleServicesTest.TestResponse;
import com.vmturbo.protoc.grpc.moles.testMoleServices.MoleServicesTestMoles.TheServiceMole;


public class TestMoleServices {

    private TheServiceMole service = Mockito.spy(new TheServiceMole());

    final TestRequest request = TestRequest.newBuilder()
            .setId(7L)
            .build();
    final TestRequest secondRequest = TestRequest.newBuilder()
            .setId(77L)
            .build();
    final TestResponse response = TestResponse.newBuilder()
            .setId(7L)
            .build();
    final TestResponse secondResponse = TestResponse.newBuilder()
            .setId(77L)
            .build();

    @Mock
    StreamObserver<TestResponse> mockObserver;

    @Captor
    ArgumentCaptor<TestResponse> responseCaptor;

    @Captor
    ArgumentCaptor<Throwable> errorCaptor;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testNoStream() {
        when(service.noStreamMethod(eq(request))).thenReturn(response);

        service.noStreamMethod(request, mockObserver);

        verify(mockObserver).onNext(eq(response));
        verify(mockObserver).onCompleted();
    }

    @Test
    public void testNoStreamError() {
        when(service.noStreamMethodError(eq(request)))
            .thenReturn(Optional.of(Status.INVALID_ARGUMENT.asException()));

        service.noStreamMethod(request, mockObserver);

        verify(mockObserver).onError(errorCaptor.capture());

        assertTrue(errorCaptor.getValue() instanceof StatusException);
        final StatusException exception = (StatusException)errorCaptor.getValue();
        assertEquals(Status.INVALID_ARGUMENT, exception.getStatus());
    }

    @Test
    public void testServerStreamMethod() {
        final List<TestResponse> responses = Arrays.asList(response, secondResponse);
        when(service.serverStreamMethod(request)).thenReturn(responses);

        service.serverStreamMethod(request, mockObserver);

        verify(mockObserver, times(2)).onNext(responseCaptor.capture());
        verify(mockObserver).onCompleted();

        // Order is important.
        assertEquals(responses, responseCaptor.getAllValues());
    }

    @Test
    public void testServerStreamError() {
        when(service.serverStreamMethodError(request))
            .thenReturn(Optional.of(Status.ALREADY_EXISTS.asException()));

        service.serverStreamMethod(request, mockObserver);

        verify(mockObserver).onError(errorCaptor.capture());

        assertTrue(errorCaptor.getValue() instanceof StatusException);
        final StatusException exception = (StatusException)errorCaptor.getValue();
        assertEquals(Status.ALREADY_EXISTS, exception.getStatus());
    }

    @Test
    public void testClientStreamMethod() {
        final List<TestRequest> requests = Arrays.asList(request, secondRequest);
        when(service.clientStreamMethod(eq(requests))).thenReturn(response);

        final StreamObserver<TestRequest> requestStream = service.clientStreamMethod(mockObserver);
        requests.forEach(requestStream::onNext);
        requestStream.onCompleted();

        verify(mockObserver).onNext(eq(response));
        verify(mockObserver).onCompleted();
    }

    @Test
    public void testClientStreamClientError() {
        final StreamObserver<TestRequest> requestStream = service.clientStreamMethod(mockObserver);
        requestStream.onError(Status.INTERNAL.asException());

        verify(mockObserver).onError(errorCaptor.capture());
        final StatusException exception = (StatusException)errorCaptor.getValue();
        assertEquals(Status.INTERNAL, exception.getStatus());
    }

    @Test
    public void testClientStreamError() {
        final List<TestRequest> requests = Arrays.asList(request, secondRequest);
        when(service.clientStreamMethodError(eq(requests)))
            .thenReturn(Optional.of(Status.DATA_LOSS.asException()));

        final StreamObserver<TestRequest> requestStream = service.clientStreamMethod(mockObserver);
        requests.forEach(requestStream::onNext);
        requestStream.onCompleted();

        verify(mockObserver).onError(errorCaptor.capture());
        final StatusException exception = (StatusException)errorCaptor.getValue();
        assertEquals(Status.DATA_LOSS, exception.getStatus());
    }

    @Test
    public void testTwoStreamMethod() {
        final List<TestResponse> responses = Arrays.asList(response, secondResponse);
        final List<TestRequest> requests = Arrays.asList(request, secondRequest);
        when(service.twoStreamMethod(eq(requests))).thenReturn(responses);

        final StreamObserver<TestRequest> requestStream = service.twoStreamMethod(mockObserver);
        requests.forEach(requestStream::onNext);
        requestStream.onCompleted();

        verify(mockObserver, times(2)).onNext(responseCaptor.capture());
        verify(mockObserver).onCompleted();

        // Order is important
        assertEquals(responses, responseCaptor.getAllValues());
    }

    @Test
    public void testTwoStreamError() {
        final List<TestRequest> requests = Arrays.asList(request, secondRequest);
        when(service.twoStreamMethodError(eq(requests)))
                .thenReturn(Optional.of(Status.CANCELLED.asException()));

        final StreamObserver<TestRequest> requestStream = service.twoStreamMethod(mockObserver);
        requests.forEach(requestStream::onNext);
        requestStream.onCompleted();

        verify(mockObserver).onError(errorCaptor.capture());
        final StatusException exception = (StatusException)errorCaptor.getValue();
        assertEquals(Status.CANCELLED, exception.getStatus());
    }
}
