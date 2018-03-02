package com.vmturbo.api.component.external.api.service;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Optional;

import com.google.common.base.Charsets;
import com.google.protobuf.ByteString;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.eclipse.jetty.server.NetworkConnector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.mockito.Mockito;
import org.springframework.util.StreamUtils;

import com.vmturbo.api.enums.ReportOutputFormat;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.reporting.api.protobuf.Reporting.ReportData;
import com.vmturbo.reporting.api.protobuf.Reporting.ReportInstanceId;
import com.vmturbo.reporting.api.protobuf.ReportingMoles.ReportingServiceMole;
import com.vmturbo.reporting.api.protobuf.ReportingServiceGrpc;

/**
 * Unit test for {@link ReportCgiServlet}.
 */
public class ReportCgiServletTest {

    @Rule
    public TestName testName = new TestName();
    private GrpcTestServer grpcServer;
    private ReportingServiceMole reportingService;
    private Server jettyServer;
    private String serverPrefix;
    private CloseableHttpClient httpClient;

    @Before
    public void init() throws Exception {
        reportingService = Mockito.spy(new ReportingServiceMole());
        grpcServer = GrpcTestServer.newServer(reportingService);
        grpcServer.start();

        final ReportCgiServlet servlet =
                new ReportCgiServlet(ReportingServiceGrpc.newBlockingStub(grpcServer.getChannel()));
        jettyServer = new Server();
        final ServletContextHandler context =
                new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");
        jettyServer.setHandler(context);
        final ServletHolder holder = new ServletHolder();
        holder.setServlet(servlet);
        context.addServlet(holder, ServiceConfig.REPORT_CGI_PATH);
        final NetworkConnector connector = new ServerConnector(jettyServer);
        jettyServer.addConnector(connector);
        jettyServer.start();
        serverPrefix = "http://localhost:" + connector.getLocalPort();
        httpClient = HttpClientBuilder.create().build();
    }

    @After
    public void shutdown() throws Exception {
        httpClient.close();
        jettyServer.stop();
        grpcServer.close();
        jettyServer.join();
    }

    private URI getReportURI(long reportId) throws URISyntaxException {
        return getReportURI(Long.toString(reportId));
    }

    private URI getReportURI(String reportId) throws URISyntaxException {
        return new URI(serverPrefix + ServiceConfig.REPORT_CGI_PATH +
                "?userName=user&callType=DOWN&actionType=REPORT&title=VMTurbo+Report&format=" +
                reportId + "&output=" + reportId);
    }

    /**
     * Tests how the existing report is retrieved from the reporting comonent.
     *
     * @throws Exception if exceptions occur
     */
    @Test
    public void testExistingReport() throws Exception {
        final long reportId = 12345678L;
        Mockito.when(reportingService.getReportData(
                ReportInstanceId.newBuilder().setId(reportId).build()))
                .thenReturn(ReportData.newBuilder()
                        .setReportName("report-name")
                        .setFormat(ReportOutputFormat.PDF.getLiteral())
                        .setData(ByteString.copyFromUtf8("report-bin-data"))
                        .build());
        final HttpResponse response = httpClient.execute(new HttpGet(getReportURI(reportId)));
        Assert.assertEquals(200, response.getStatusLine().getStatusCode());
        Assert.assertEquals("report-bin-data",
                StreamUtils.copyToString(response.getEntity().getContent(), Charsets.UTF_8));
    }

    /**
     * Tests what happens when report requested is absent in reporting component. It is expected to
     * receive 404 error message.
     *
     * @throws Exception if exceptions occur
     */
    @Test
    public void testAbsentReport() throws Exception {
        final long reportId = 12345678L;
        Mockito.when(reportingService.getReportDataError(
                ReportInstanceId.newBuilder().setId(reportId).build()))
                .thenReturn(Optional.of(new StatusRuntimeException(
                        Status.NOT_FOUND.withDescription("not-found-error"))));
        final HttpResponse response = httpClient.execute(new HttpGet(getReportURI(reportId)));
        Assert.assertEquals(404, response.getStatusLine().getStatusCode());
        Assert.assertThat(
                StreamUtils.copyToString(response.getEntity().getContent(), Charsets.UTF_8),
                CoreMatchers.containsString("not-found-error"));
    }

    /**
     * Tests when reporting component reports an internal error. 501 message is expected.
     *
     * @throws Exception if exceptions occur
     */
    @Test
    public void testInternalErrorFromReporting() throws Exception {
        final long reportId = 12345678L;
        Mockito.when(reportingService.getReportDataError(
                ReportInstanceId.newBuilder().setId(reportId).build()))
                .thenReturn(Optional.of(new StatusRuntimeException(
                        Status.INTERNAL.withDescription("internal-error"))));
        final HttpResponse response = httpClient.execute(new HttpGet(getReportURI(reportId)));
        Assert.assertEquals(501, response.getStatusLine().getStatusCode());
        Assert.assertThat(
                StreamUtils.copyToString(response.getEntity().getContent(), Charsets.UTF_8),
                CoreMatchers.containsString("internal-error"));
    }

    /**
     * Tests wrong (non-numeric) report id specified as a request parameter. 400 message is expected
     * as all the report ids must be long.
     *
     * @throws Exception if exceptions occur
     */
    @Test
    public void testNotNumericReportId() throws Exception {
        final HttpResponse response = httpClient.execute(new HttpGet(getReportURI("some-words")));
        Assert.assertEquals(400, response.getStatusLine().getStatusCode());
        Assert.assertThat(
                StreamUtils.copyToString(response.getEntity().getContent(), Charsets.UTF_8),
                CoreMatchers.containsString("not a numeric value"));
    }
}
