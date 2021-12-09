package com.vmturbo.history.stats.readers;

import java.sql.Date;
import java.util.Collections;
import java.util.List;
import java.util.PriorityQueue;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.Record3;
import org.jooq.Result;
import org.jooq.exception.DataAccessException;

import com.vmturbo.history.ingesters.live.writers.VolumeAttachmentHistoryWriter;
import com.vmturbo.history.schema.abstraction.tables.VolumeAttachmentHistory;

/**
 * Class that queries Volume Attachment History table.
 */
public class VolumeAttachmentHistoryReader {

    private static final Logger logger = LogManager.getLogger();
    private static final VolumeAttachmentHistory table =
            VolumeAttachmentHistory.VOLUME_ATTACHMENT_HISTORY;
    private final DSLContext dsl;

    /**
     * Creates an instance of VolumeAttachmentHistoryReader.
     *
     * @param dsl instance to execute queries.
     */
    public VolumeAttachmentHistoryReader(@Nonnull final DSLContext dsl) {
        this.dsl = dsl;
    }

    /**
     * Retrieves the most recent Volume Attachment History record for the given volume oids.
     *
     * @param volumeOids volumes for which attachment history is being retrieved.
     * @return list of records for the volumeOids provided.
     */
    @Nonnull
    public List<Record3<Long, Long, Date>> getVolumeAttachmentHistory(
        @Nonnull final List<Long> volumeOids) {
        if (volumeOids.isEmpty()) {
            return Collections.emptyList();
        }
        try {
            @SuppressWarnings("unchecked") final Result<Record3<Long, Long, Date>> result =
                    dsl.select(table.VOLUME_OID, table.VM_OID, table.LAST_ATTACHED_DATE)
                            .from(table)
                            .where(table.VOLUME_OID.in(volumeOids)).fetch();
            if (result != null) {
                return result.stream()
                        // group records related to a volume oid
                        .collect(Collectors.groupingBy(Record3::component1, Collectors.toList()))
                        .values().stream()
                        // get the most recent record for each volume
                        .map(this::getMostRecentRecord)
                        .collect(Collectors.toList());
            }
        } catch (DataAccessException e) {
            logger.error(String.format("Error retrieving volume attachment history for volumes %s ",
                volumeOids), e);
        }
        return Collections.emptyList();
    }

    @Nullable
    private Record3<Long, Long, Date> getMostRecentRecord(
        @Nonnull final List<Record3<Long, Long, Date>> records) {
        final PriorityQueue<Record3<Long, Long, Date>> recordsPriorityQueue =
            new PriorityQueue<>((r1, r2) -> r2.component3().compareTo(r1.component3()));
        recordsPriorityQueue.addAll(records);
        final Record3<Long, Long, Date> latestRecord = recordsPriorityQueue.poll();
        final boolean isLatestRecordUnattached = latestRecord != null
            && latestRecord.component2() == VolumeAttachmentHistoryWriter
            .VM_OID_VALUE_FOR_UNATTACHED_VOLS;
        // if latest record is unattached (i.e. with placeholder VM oid) and there are more records
        // in queue, then return the next one in queue as it would be associated with a real VM oid
        if (isLatestRecordUnattached && !recordsPriorityQueue.isEmpty()) {
            return recordsPriorityQueue.poll();
        } else {
            return latestRecord;
        }
    }
}