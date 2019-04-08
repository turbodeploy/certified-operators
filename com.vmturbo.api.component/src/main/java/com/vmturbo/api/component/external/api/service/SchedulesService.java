package com.vmturbo.api.component.external.api.service;

import java.util.List;

import com.vmturbo.api.component.external.api.util.ApiUtils;
import com.vmturbo.api.dto.action.ActionApiDTO;
import com.vmturbo.api.dto.settingspolicy.ScheduleApiDTO;
import com.vmturbo.api.dto.settingspolicy.SettingsPolicyApiDTO;
import com.vmturbo.api.serviceinterfaces.ISchedulesService;

/**
 * This class provides services for managing schedules. A schedule represents a number periods in time.
 * The operations include adding, editing, retrieving, deleting, and getting associated actions and policies.
 */
public class SchedulesService implements ISchedulesService {

    @Override
    public List<ScheduleApiDTO> getSchedules() throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public ScheduleApiDTO getSchedule(String uuid) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public ScheduleApiDTO addSchedule(ScheduleApiDTO schedule) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public ScheduleApiDTO editSchedule(String uuid, ScheduleApiDTO schedule) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public void deleteSchedule(String uuid) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    /**
     * Returns the list of actions that will be executed in the next occurrence of this schedule
     */
    @Override
    public List<ActionApiDTO> getActionsToBeExecutedInSchedule(String uuid) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    /**
     * Returns the list of policies which are scheduled and are using this schedule as their
     * schedule.
     */
    @Override
    public List<SettingsPolicyApiDTO> getPoliciesUsingTheSchedule(String uuid) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

}
