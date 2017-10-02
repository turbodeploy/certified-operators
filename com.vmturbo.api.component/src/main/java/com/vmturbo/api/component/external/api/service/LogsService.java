package com.vmturbo.api.component.external.api.service;

import java.util.List;

import com.vmturbo.api.component.external.api.util.ApiUtils;
import com.vmturbo.api.dto.LogEntryApiDTO;
import com.vmturbo.api.serviceinterfaces.ILogsService;

/**
 * Service implementation of Logs
 **/
public class LogsService implements ILogsService {
    @Override
    public List<LogEntryApiDTO> getLogs(String starttime, String endtime, List<String> categories, List<String> lNames) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public LogEntryApiDTO getLogByUuid(Integer uuid) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }
}
