package com.vmturbo.reports.component.schedules;

import java.util.Set;

import javax.annotation.Nonnull;

import com.vmturbo.reporting.api.protobuf.Reporting;
import com.vmturbo.sql.utils.DbException;

/**
 * Provides methods for data acess to schedules stored in storage(like add new schedule to db, edit
 * existing schedule, etc).
 */
public interface ScheduleDAO {

    /**
     * Store new schedule to storage.
     *
     * @throws DbException if there is an error with db
     * @param scheduleInfo params of schedule
     * @return stored schedule object
     */
    @Nonnull
    Reporting.ScheduleDTO addSchedule(@Nonnull Reporting.ScheduleInfo scheduleInfo)
                    throws DbException;

    /**
     * Lists all stored schedules.
     *
     * @throws DbException if there is an error with db
     * @return all schedules stored in storage.
     */
    @Nonnull
    Set<Reporting.ScheduleDTO> getAllSchedules() throws DbException;

    /**
     * Delete schedule from storage by id.
     *
     * @throws DbException if there is an error with db
     * @param id of schedule to delete
     */
    void deleteSchedule(long id) throws DbException;

    /**
     * Edit stored schedule.
     *
     * @throws DbException if there is an error with db
     * @param scheduleDTO with schedule id to edit and new params.
     */
    @Nonnull
    void editSchedule(@Nonnull Reporting.ScheduleDTO scheduleDTO) throws DbException;

    /**
     * Returns stored schedule by id from storage.
     *
     * @throws DbException if there is an error with db
     * @param id of schedule to return
     * @return schedule with specified id
     */
    @Nonnull
    Reporting.ScheduleDTO getSchedule(long id) throws DbException;

    /**
     * Returns schedules with specified report type and template id.
     *
     * @throws DbException if there is an error with db
     * @param reportType specified
     * @param templateId specified
     * @return schedule with specified reportType and templateId
     */
    @Nonnull
    Set<Reporting.ScheduleDTO> getSchedulesBy(int reportType, int templateId) throws DbException;
}
