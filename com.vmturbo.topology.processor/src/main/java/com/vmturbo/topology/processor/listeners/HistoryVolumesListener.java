package com.vmturbo.topology.processor.listeners;

import java.util.HashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import com.vmturbo.common.protobuf.history.VolAttachmentHistoryOuterClass;
import com.vmturbo.history.component.api.HistoryNotificationListener;

/**
 * A listener class to listen to kafka topic from history component.
 */
public class HistoryVolumesListener implements HistoryNotificationListener {

    private HashMap<Long, Long> volIdToLastAttachmentTime =   new HashMap<>();
    private static final Logger logger = LogManager.getLogger();

    public synchronized HashMap<Long, Long> getVolIdToLastAttachmentTime() {
        return (HashMap<Long, Long>)volIdToLastAttachmentTime.clone();
    }

    @Override
    public synchronized void  onHistoryNotificationReceived(@NotNull VolAttachmentHistoryOuterClass.VolAttachmentHistory historyNotification) {
            volIdToLastAttachmentTime.clear();
            volIdToLastAttachmentTime.putAll(historyNotification.getVolIdToLastAttachedDateMap());
            logger.info("Received history notification with days unattached for " + volIdToLastAttachmentTime.size() + " volumes and topology id = " + historyNotification.getTopologyId());
    }
}
