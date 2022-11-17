package com.vmturbo.sql.utils.partition;

import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.exception.DataAccessException;

import com.vmturbo.sql.utils.InformationSchema;
import com.vmturbo.sql.utils.InformationSchemaConstants;

/**
 * PartitionAdapter for use with MariaDB and MySQL databases.
 */
public class MariaDBPartitionAdapter implements IPartitionAdapter {
    private static final Logger logger = LogManager.getLogger();
    // our partitions used MariaDB's to_seconds function, which is described as returning the
    // number of seconds from the start of year zero to the given time or date. It turns out that
    // the following computation based on that description exactly matches the result of the
    // query `SELECT to_seconds('1970-01-01 00:00:00')`.
    private static final long SECS_FROM_YEAR_ZERO_TO_EPOCH = -Instant.parse("0000-01-01T00:00:00Z")
            .toEpochMilli() / 1000;
    private static final SimpleDateFormat partitionNameFormat = new SimpleDateFormat(
            "'before'yyyyMMddHHmmss");

    static {
        partitionNameFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    /** partition spec to (re)create a future partition. */
    public static final String FUTURE_PARTITION_SPEC =
            "PARTITION `future` VALUES LESS THAN MAXVALUE";

    private final DSLContext dsl;

    /**
     * Create a new instance.
     *
     * @param dsl {@link DSLContext} for DB access
     */
    public MariaDBPartitionAdapter(DSLContext dsl) {
        this.dsl = dsl;
    }

    @Override
    public Map<String, List<Partition<Instant>>> getSchemaPartitions(String schemaName)
            throws PartitionProcessingException {
        Map<String, List<Record>> tableParts;
        try {
            tableParts = InformationSchema.getPartitions(schemaName, dsl);
        } catch (DataAccessException | org.springframework.dao.DataAccessException e) {
            throw new PartitionProcessingException(
                    String.format("Failed to retrieve partition information for schema %s",
                            schemaName), e);
        }
        Map<String, List<Partition<Instant>>> result = new HashMap<>();
        for (Entry<String, List<Record>> entry : tableParts.entrySet()) {
            String key = entry.getKey();
            List<Record> value = entry.getValue();
            result.put(key, convertToPartitions(value));
        }
        logger.info("Loaded partitioning data for {} tables from schema {}",
                result.size(), schemaName);
        return result;
    }

    @Override
    public Partition<Instant> createPartition(
            String schemaName, String tableName, Instant fromInclusive, Instant toExclusive)
            throws PartitionProcessingException {
        String partitionName = partitionNameFormat.format(toExclusive.toEpochMilli());
        long bound = instantToSecs(toExclusive);
        String partSpec = String.format("PARTITION `%s` VALUES LESS THAN (%s)",
                partitionName, bound);
        String sql =
                String.format("ALTER TABLE `%s`.`%s` REORGANIZE PARTITION `future` INTO (%s, %s)",
                        schemaName, tableName, partSpec, FUTURE_PARTITION_SPEC);
        try {
            dsl.execute(sql);
            logger.info("Created new partition with upper bound {} for table {}.{}",
                    toExclusive, schemaName, tableName);
            return new Partition<>(
                    schemaName, tableName, partitionName, fromInclusive, toExclusive);
        } catch (DataAccessException | org.springframework.dao.DataAccessException e) {
            throw new PartitionProcessingException(
                    String.format("Failed to create partition %s in table %s.%s",
                            partitionName, schemaName, tableName), e);
        }
    }

    @Override
    public void dropPartition(Partition<Instant> partition) throws PartitionProcessingException {
        String schemaName = partition.getSchemaName();
        String tableName = partition.getTableName();
        String partitionName = partition.getPartitionName();
        try {
            dsl.execute(String.format("ALTER TABLE `%s`.`%s` DROP PARTITION `%s`",
                    schemaName, tableName, partitionName));
            logger.info("Dropped partition {} from table {}.{}", partitionName,
                    schemaName, tableName);
        } catch (DataAccessException | org.springframework.dao.DataAccessException e) {
            throw new PartitionProcessingException(
                    String.format("Failed to drop partition %s from table %s.%s",
                            partitionName, schemaName, tableName), e);
        }
    }

    private List<Partition<Instant>> convertToPartitions(List<Record> records) {
        List<Partition<Instant>> result = new ArrayList<>();
        Instant priorBound = Instant.ofEpochSecond(-SECS_FROM_YEAR_ZERO_TO_EPOCH);
        for (Record r : records) {
            String schemaName = r.get(InformationSchemaConstants.TABLE_SCHEMA);
            String tableName = r.get(InformationSchemaConstants.TABLE_NAME);
            String partitionName = r.get(InformationSchemaConstants.PARTITION_NAME);
            Instant bound = secsToInstant(Long.parseLong(r.get(
                    InformationSchemaConstants.PARTITION_DESCRIPTION)));
            Partition<Instant> partition = new Partition<>(
                    schemaName, tableName, partitionName, priorBound, bound);
            result.add(partition);
            priorBound = bound;
        }
        return result;
    }

    private static Instant secsToInstant(long secs) {
        return Instant.ofEpochMilli(
                TimeUnit.SECONDS.toMillis(secs - SECS_FROM_YEAR_ZERO_TO_EPOCH));
    }

    private static long instantToSecs(Instant t) {
        return SECS_FROM_YEAR_ZERO_TO_EPOCH + t.toEpochMilli() / 1000;
    }
}
