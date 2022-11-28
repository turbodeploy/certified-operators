package com.vmturbo.topology.processor.listeners;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.history.component.api.ApplicationServiceHistoryListener;
import com.vmturbo.history.component.api.HistoryComponentNotifications.ApplicationServiceHistoryNotification;
import com.vmturbo.history.component.api.HistoryComponentNotifications.ApplicationServiceHistoryNotification.DaysEmptyInfo;
import com.vmturbo.history.component.api.HistoryComponentNotifications.ApplicationServiceHistoryNotification.Type;

/**
 * The topology processor's listener for app service history messages.
 */
public class TpAppSvcHistoryListener implements ApplicationServiceHistoryListener {

    private static final Logger logger = LogManager.getLogger();

    private Map<Long, DaysEmptyInfo> daysEmptyByAppSvc;

    /**
     * Gets the app service history days empty info.
     *
     * @return a map of days empty infos by app service
     */
    public Map<Long, DaysEmptyInfo> getDaysEmptyInfosByAppSvc() {
        if (daysEmptyByAppSvc == null) {
            return Collections.emptyMap();
        }
        return new HashMap<>(daysEmptyByAppSvc);
    }

    @Override
    public void onApplicationServiceHistoryNotification(ApplicationServiceHistoryNotification msg) {
        logger.info("TP received ApplicationServiceHistoryNotification message, "
                        + "type={}, daysInfoListSize={}, topologyId={}",
                msg.getType(), msg.getDaysEmptyInfoList().size(), msg.getTopologyId());
        if (msg.getType() != Type.DAYS_EMPTY) {
            return;
        }
        daysEmptyByAppSvc = msg
                .getDaysEmptyInfoList()
                .stream()
                .collect(Collectors.toMap(DaysEmptyInfo::getAppSvcOid, Function.identity()));
    }
}
