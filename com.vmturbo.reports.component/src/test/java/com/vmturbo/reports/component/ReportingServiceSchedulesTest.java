package com.vmturbo.reports.component;

import static com.vmturbo.reports.component.db.Tables.SCHEDULE;
import static com.vmturbo.reports.component.db.Tables.SCHEDULE_SUBSCRIBERS;

import java.io.IOException;
import java.util.List;

import javax.annotation.Nonnull;

import org.jooq.DSLContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.AnnotationConfigContextLoader;

import com.google.common.collect.ImmutableList;

import com.vmturbo.reporting.api.protobuf.Reporting;
import com.vmturbo.reporting.api.protobuf.ReportingServiceGrpc;

/**
 * Tests for CRUD operations with Reports schedules in db.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(loader = AnnotationConfigContextLoader.class,
                classes = {ReportingTestConfig.class})
@DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_EACH_TEST_METHOD)
public class ReportingServiceSchedulesTest {

    @Autowired
    private ReportingTestConfig reportingConfig;

    private ReportingServiceGrpc.ReportingServiceBlockingStub reportingService;

    private DSLContext dslContext;

    private static final Reporting.ScheduleInfo TEST_SCHEDULE_INFO = buildScheduleInfo(
                    "123", "Sunday", "PDF", "Weekly", 1,
                    19, true, ImmutableList.of("a@a.com", "b@b.com"));

    @Before
    public void init() throws IOException {
        reportingConfig.init();
        reportingService = ReportingServiceGrpc.newBlockingStub(reportingConfig.planGrpcServer().getChannel());
        dslContext = reportingConfig.dslContext();
        clearAllTables();
    }

    /**
     * Tests adding of schedule to db.
     */
    @Test
    public void testAddSchedule() {
        final long id = reportingService.addSchedule(TEST_SCHEDULE_INFO).getId();
        Reporting.ScheduleDTO schedule = reportingService.getSchedule(
                        Reporting.ScheduleId.newBuilder().setId(id).build());
        Assert.assertEquals(TEST_SCHEDULE_INFO, schedule.getScheduleInfo());
    }

    /**
     * Tests removing of schedule from db.
     */
    @Test
    public void testDeleteSchedule() {
        final Reporting.ScheduleDTO addedSchedule = reportingService.addSchedule(TEST_SCHEDULE_INFO);
        final int countBefore = ImmutableList.copyOf(reportingService.getAllSchedules(
                        Reporting.Empty.newBuilder().build())).size();
        Assert.assertEquals(1, countBefore);
        reportingService.deleteSchedule(Reporting.ScheduleId.newBuilder().setId(addedSchedule.getId()).build());
        final ImmutableList<Reporting.ScheduleDTO> actualSchedules = ImmutableList.copyOf(
                        reportingService.getAllSchedules(Reporting.Empty.newBuilder().build()));
        final int countAfter = actualSchedules.size();
        Assert.assertEquals(0, countAfter);
        Assert.assertFalse(actualSchedules.contains(addedSchedule));
    }

    /**
     * Tests editing of schedule.
     */
    @Test
    public void testEditSchedule() {
        final Reporting.ScheduleDTO beforeEdit = reportingService.addSchedule(TEST_SCHEDULE_INFO);
        final Reporting.ScheduleInfo newInfo = buildScheduleInfo("345", "Monday",
                        "XLSX", "Monthly", 2, 22, false,
                        ImmutableList.of("c@c.com"));
        reportingService.editSchedule(beforeEdit.toBuilder().setScheduleInfo(newInfo).build());
        Reporting.ScheduleDTO afterEdit = reportingService.getSchedule(
                        Reporting.ScheduleId.newBuilder().setId(beforeEdit.getId()).build());
        Assert.assertEquals(newInfo, afterEdit.getScheduleInfo());
    }

    /**
     * Tests searching of schedules by report type and template id.
     */
    @Test
    public void testGetScheduleBy() {
        reportingService.addSchedule(TEST_SCHEDULE_INFO).getId();
        final Reporting.ScheduleDTO dto = reportingService.getSchedulesBy(Reporting.GetSchedulesByRequest
                        .newBuilder()
                        .setTemplateId(TEST_SCHEDULE_INFO.getTemplateId())
                        .setReportType(TEST_SCHEDULE_INFO.getReportType())
                        .build()).next();
        Assert.assertEquals(TEST_SCHEDULE_INFO, dto.getScheduleInfo());
    }

    /**
     * Tests getting of schedule by id.
     */
    @Test
    public void testGetSchedule() {
        final Reporting.ScheduleDTO expected = reportingService.addSchedule(TEST_SCHEDULE_INFO);
        final Reporting.ScheduleDTO actual = reportingService.getSchedule(
                        Reporting.ScheduleId.newBuilder().setId(expected.getId()).build());
        Assert.assertEquals(expected, actual);
    }

    private static Reporting.ScheduleInfo buildScheduleInfo(@Nonnull String scopeOid, @Nonnull String day,
                    @Nonnull String format, @Nonnull String period, int reportType, int templateId,
                    boolean showCharts, List<String> emails) {
        return Reporting.ScheduleInfo.newBuilder()
                        .setScopeOid(scopeOid)
                        .setDayOfWeek(day)
                        .setFormat(format)
                        .setPeriod(period)
                        .setReportType(reportType)
                        .setTemplateId(templateId)
                        .setShowCharts(showCharts)
                        .addAllSubscribersEmails(emails)
                        .build();
    }

    private void clearAllTables() {
        dslContext.deleteFrom(SCHEDULE).execute();
        dslContext.deleteFrom(SCHEDULE_SUBSCRIBERS).execute();
    }
}
