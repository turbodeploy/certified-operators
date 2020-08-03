package com.vmturbo.api.component.external.api.service;

import java.io.IOException;

import com.google.gson.Gson;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.MediaType;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.annotation.DirtiesContext.ClassMode;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.AnnotationConfigWebContextLoader;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;

import com.vmturbo.api.dto.report.ReportScheduleApiInputDTO;
import com.vmturbo.api.enums.DayOfWeek;
import com.vmturbo.api.enums.Period;
import com.vmturbo.api.enums.ReportOutputFormat;
import com.vmturbo.api.serviceinterfaces.IGroupsService;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.reporting.api.ReportingConstants;
import com.vmturbo.reporting.api.protobuf.Reporting.GenerateReportRequest;
import com.vmturbo.reporting.api.protobuf.Reporting.ScheduleDTO;
import com.vmturbo.reporting.api.protobuf.Reporting.ScheduleInfo;
import com.vmturbo.reporting.api.protobuf.ReportingMoles.ReportingServiceMole;
import com.vmturbo.reporting.api.protobuf.ReportingServiceGrpc;
import com.vmturbo.reporting.api.protobuf.ReportingServiceGrpc.ReportingServiceBlockingStub;

/**
 * Unit test for {@link ReportsService}.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@WebAppConfiguration
@ContextConfiguration(loader = AnnotationConfigWebContextLoader.class)
@DirtiesContext(classMode = ClassMode.BEFORE_EACH_TEST_METHOD)
@Ignore
public class ReportsServiceTest {
    @Autowired
    private TestConfig testConfig;
    @Autowired
    private WebApplicationContext wac;

    private MockMvc mockMvc;
    private static final Gson GSON = new Gson();

    @Before
    public void startup() {
        mockMvc = MockMvcBuilders.webAppContextSetup(wac).build();
        Mockito.when(testConfig.reportingServiceMole().addSchedule(Mockito.any()))
                .thenAnswer(new Answer<ScheduleDTO>() {

                    @Override
                    public ScheduleDTO answer(InvocationOnMock invocation) throws Throwable {
                        final ScheduleInfo info = invocation.getArgumentAt(0, ScheduleInfo.class);
                        return ScheduleDTO.newBuilder().setId(123L).setScheduleInfo(info).build();
                    }
                });
    }

    /**
     * Tests, that day of month is set for the requests with monthly period.
     *
     * @throws Exception if exceptions occur
     */
    @Test
    public void testScheduleMonthlyReport() throws Exception {
        final ReportScheduleApiInputDTO inputDto = new ReportScheduleApiInputDTO();
        inputDto.setPeriod(Period.Monthly);
        inputDto.setFormat(ReportOutputFormat.PDF);
        final String reportId = "2_1234";
        final Gson gson = new Gson();
        final String reqStr = gson.toJson(inputDto);

        final MvcResult result = mockMvc.perform(
                MockMvcRequestBuilders.post("/reports/templates/" + reportId + "/schedules")
                        .contentType(MediaType.APPLICATION_JSON_UTF8)
                        .content(reqStr)
                        .accept(MediaType.APPLICATION_JSON_UTF8_VALUE))
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andReturn();
        final ArgumentCaptor<ScheduleInfo> captor = ArgumentCaptor.forClass(ScheduleInfo.class);
        Mockito.verify(testConfig.reportingServiceMole()).addSchedule(captor.capture());
        Assert.assertTrue(captor.getValue().hasDayOfMonth());
    }

    /**
     * Tests, that day of week is set for the requests with weekly period.
     *
     * @throws Exception if exceptions occur
     */
    @Test
    public void testScheduleWeeklyReport() throws Exception {
        final ReportScheduleApiInputDTO inputDto = new ReportScheduleApiInputDTO();
        inputDto.setPeriod(Period.Weekly);
        inputDto.setFormat(ReportOutputFormat.PDF);
        final String reportId = "2_1234";
        final Gson gson = new Gson();
        final String reqStr = gson.toJson(inputDto);

        final MvcResult result = mockMvc.perform(
                MockMvcRequestBuilders.post("/reports/templates/" + reportId + "/schedules")
                        .contentType(MediaType.APPLICATION_JSON_UTF8)
                        .content(reqStr)
                        .accept(MediaType.APPLICATION_JSON_UTF8_VALUE))
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andReturn();
        final ArgumentCaptor<ScheduleInfo> captor = ArgumentCaptor.forClass(ScheduleInfo.class);
        Mockito.verify(testConfig.reportingServiceMole()).addSchedule(captor.capture());
        Assert.assertTrue(captor.getValue().hasDayOfWeek());
        Assert.assertNotNull(DayOfWeek.get(captor.getValue().getDayOfWeek()));
    }

    /**
     * Tests, that day of week is set for the requests with weekly period.
     *
     * @throws Exception if exceptions occur
     */
    @Test
    public void testScheduleWithParameters() throws Exception {
        final ReportScheduleApiInputDTO inputDto = new ReportScheduleApiInputDTO();
        inputDto.setPeriod(Period.Weekly);
        inputDto.setFormat(ReportOutputFormat.PDF);
        final String oid = "dfdsfs444444";
        final boolean showCharts = true;
        inputDto.setScope(oid);
        inputDto.setShowCharts(showCharts);
        final String reportId = "2_1234";
        final Gson gson = new Gson();
        final String reqStr = gson.toJson(inputDto);

        final MvcResult result = mockMvc.perform(
                MockMvcRequestBuilders.post("/reports/templates/" + reportId + "/schedules")
                        .contentType(MediaType.APPLICATION_JSON_UTF8)
                        .content(reqStr)
                        .accept(MediaType.APPLICATION_JSON_UTF8_VALUE))
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andReturn();
        final ArgumentCaptor<ScheduleInfo> captor = ArgumentCaptor.forClass(ScheduleInfo.class);
        Mockito.verify(testConfig.reportingServiceMole()).addSchedule(captor.capture());
        final GenerateReportRequest request = captor.getValue().getReportRequest();
        Assert.assertEquals(oid,
                request.getParametersMap().get(ReportingConstants.ITEM_UUID_PROPERTY));
        Assert.assertEquals(showCharts, Boolean.valueOf(
                request.getParametersMap().get(ReportingConstants.SHOW_CHARTS_PROPERTY)));
    }

    @Configuration
    @EnableWebMvc
    public static class TestConfig extends WebMvcConfigurerAdapter {

        @Bean
        public ReportsService reportsService() {
            return new ReportsService(reportingRpcService(), groupsService());
        }

        @Bean
        public IGroupsService groupsService() {
            return Mockito.mock(IGroupsService.class);
        }

        @Bean
        public ReportingServiceMole reportingServiceMole() {
            return Mockito.spy(new ReportingServiceMole());
        }

        @Bean
        public ReportingServiceBlockingStub reportingRpcService() {
            return ReportingServiceGrpc.newBlockingStub(grpcTestServer().getChannel());
        }

        @Bean
        public GrpcTestServer grpcTestServer() {
            try {
                final GrpcTestServer testServer = GrpcTestServer.newServer(reportingServiceMole());
                testServer.start();
                return testServer;
            } catch (IOException e) {
                throw new BeanCreationException("Failed to create test channel", e);
            }
        }
    }
}
