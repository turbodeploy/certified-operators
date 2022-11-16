package com.vmturbo.sql.utils.partition;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.exception.DataAccessException;

import com.vmturbo.sql.utils.InformationSchemaConstants;

/**
 * PartitionAdapter for use with Postgres databases.
 */
public class PostgresPartitionAdapter implements IPartitionAdapter {
    private static final Logger logger = LogManager.getLogger();
    private final DSLContext dsl;

    /**
     * Create a new instance.
     *
     * @param dsl {@link DSLContext} for DB access
     */
    public PostgresPartitionAdapter(DSLContext dsl) {
        this.dsl = dsl;
    }

    /**
     * Retrieve details of existing partitions for tables in the given schema.
     *
     * <p>"Bookend" partitions (e.g. the typical MariaDB "start" and "future" partitions with
     * boundaries set to 0 and MAXINT, respectively) are not retrieved. These are partitions in
     * which no records are ever intended to appear.</p>
     *
     * <p>Tables that are declared as partitioned but for which no partitions (except bookends)
     * exist will not be represented in the results.</p>
     *
     * @param schemaName schema whose partitioning info is desired
     * @return map of table names to lists of table partitions
     * @throws PartitionProcessingException if there's a problem retrieving partition info
     */
    @Override
    public Map<String, List<Partition<Instant>>> getSchemaPartitions(String schemaName)
            throws PartitionProcessingException {
        String sqlFormat =
                // note use of argument indexes in format placeholders. 1$ = schema name colname,
                // 2$ = table name colname, 3$ = partition name colname, 4$ = bounds spec colname,
                // 5$ = schema name (value, not colname)
                "WITH partitions AS ("
                        + "SELECT n.nspname AS %1$s, "
                        + "  (parse_ident(i.inhparent::regclass::text))[1] AS %2$s, "
                        + "  c.relname AS %3$s, "
                        + "  pg_get_expr(c.relpartbound, c.oid) AS %4$s "
                        + "FROM pg_class c, pg_namespace n, pg_inherits i  "
                        + "JOIN pg_partitioned_table p ON i.inhparent=p.partrelid "
                        + "WHERE c.relispartition "
                        + "  AND c.relnamespace = n.oid "
                        + "  AND n.nspname='%5$s' "
                        + "  AND i.inhrelid = c.oid "
                        + "  AND p.partstrat='r' "
                        + "  AND c.relkind='r') "
                        + "SELECT * FROM partitions WHERE %4$s != 'DEFAULT' ORDER BY %4$s";
        String sql = String.format(sqlFormat,
                // we use compatible and aptly-named fields defined for information_schema for
                // convenience, even though we're not actually using information_schema
                InformationSchemaConstants.TABLE_SCHEMA.getName(),
                InformationSchemaConstants.TABLE_NAME.getName(),
                InformationSchemaConstants.PARTITION_NAME.getName(),
                InformationSchemaConstants.PARTITION_DESCRIPTION.getName(),
                schemaName);
        Map<String, List<Record>> tableParts = dsl.fetch(sql).stream()
                .collect(Collectors.groupingBy(
                        r -> r.getValue(InformationSchemaConstants.TABLE_NAME)));
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

    private List<Partition<Instant>> convertToPartitions(List<Record> records)
            throws PartitionProcessingException {
        List<Partition<Instant>> partitions = new ArrayList<>();
        for (Record record : records) {
            Partition<Instant> instantPartition = convertToPartition(record);
            partitions.add(instantPartition);
        }
        return partitions;
    }

    private Partition<Instant> convertToPartition(Record record)
            throws PartitionProcessingException {
        String schemaName = record.getValue(InformationSchemaConstants.TABLE_SCHEMA);
        String tableName = record.getValue(InformationSchemaConstants.TABLE_NAME);
        String partitionName = record.getValue(InformationSchemaConstants.PARTITION_NAME);
        Pair<Instant, Instant> bounds = parsePartitionBounds(
                record.getValue(InformationSchemaConstants.PARTITION_DESCRIPTION));
        return new Partition<>(schemaName, tableName, partitionName,
                bounds.getLeft(), bounds.getRight());
    }

    private static final Pattern boundsPattern = Pattern.compile(
            "FOR VALUES FROM \\('(\\d{4}-\\d{2}-\\d{2}) (\\d{2}:\\d{2}:\\d{2})'\\) TO "
                    + "\\('(\\d{4}-\\d{2}-\\d{2}) (\\d{2}:\\d{2}:\\d{2})'\\)");

    /**
     * Parse the bounds specification provided by Postgres for a partition, which takes the form
     * "FROM 'yyyy-mm-dd hh:mm:ss' TO 'yyyy-mm-dd hh:mm:ss'". The time/date values are UTF8.
     *
     * @param boundsSpec bounds specification provided by postgres
     * @return parsed start and end instants for partition
     * @throws PartitionProcessingException if there's a problem with the operation
     */
    private Pair<Instant, Instant> parsePartitionBounds(String boundsSpec)
            throws PartitionProcessingException {
        Matcher matcher = boundsPattern.matcher(boundsSpec);
        String dateTimeText;
        if (matcher.matches()) {
            try {
                dateTimeText = matcher.group(1) + "T" + matcher.group(2) + "Z";
                Instant start = Instant.parse(dateTimeText);
                dateTimeText = matcher.group(3) + "T" + matcher.group(4) + "Z";
                Instant end = Instant.parse(dateTimeText);
                return Pair.of(start, end);
            } catch (DateTimeParseException e) {
                throw badBoundsSpec(boundsSpec, e);
            }
        } else {
            throw badBoundsSpec(boundsSpec, null);
        }
    }

    @NotNull
    private PartitionProcessingException badBoundsSpec(
            String boundsSpec, DateTimeParseException e) {
        return new PartitionProcessingException(
                String.format("Unparseable bounds spec \"%s\"", boundsSpec), e);
    }

    /**
     * Create a new partition.
     *
     * @param schemaName    name of scheme containing table
     * @param tableName     name of table
     * @param fromInclusive lower bound of partition
     * @param toExclusive   upper bound of partition
     * @return a {@link Partition} instance representing the new partition
     * @throws PartitionProcessingException if there's a problem creating the partition
     */
    @Override
    public Partition<Instant> createPartition(String schemaName, String tableName,
            Instant fromInclusive, Instant toExclusive) throws PartitionProcessingException {
        String partitionName = getPartitionName(tableName, fromInclusive, toExclusive);
        String sql = String.format(
                "CREATE TABLE \"%s\" PARTITION OF \"%s\" FOR VALUES FROM ('%s') TO ('%s')",
                partitionName, tableName, fromInclusive, toExclusive);
        try {
            dsl.execute(sql);
            logger.info("Created new partition {} covering range [{}, {}) for table {}.{}",
                    partitionName, fromInclusive, toExclusive, schemaName, tableName);
            return new Partition<>(
                    schemaName, tableName, partitionName, fromInclusive, toExclusive);
        } catch (DataAccessException e) {
            throw new PartitionProcessingException(
                    String.format("Failed to create partition %s in table %s.%s",
                            partitionName, schemaName, tableName), e);
        }
    }

    private static final SimpleDateFormat partDateFormat = new SimpleDateFormat("yyyyMMddHHmmss");

    private String getPartitionName(
            String tableName, Instant fromInclusive, Instant toExclusive) {
        return tableName + "_from_" + partDateFormat.format(Timestamp.from(fromInclusive))
                + "_to_" + partDateFormat.format(Timestamp.from(toExclusive));
    }

    /**
     * Drop an existing partition.
     *
     * @param partition partition to drop
     * @throws PartitionProcessingException if there's a problem dropping the partition
     */
    @Override
    public void dropPartition(Partition<Instant> partition) throws PartitionProcessingException {
        String schemaName = partition.getSchemaName();
        String tableName = partition.getTableName();
        String partitionName = partition.getPartitionName();
        try {
            dsl.execute(String.format("DROP TABLE \"%s\".\"%s\"", schemaName, partitionName));
            logger.info("Dropped partition {} from table {}.{}", partition, schemaName, tableName);
        } catch (DataAccessException e) {
            throw new PartitionProcessingException(
                    String.format("Failed to drop partition %s from table %s.%s",
                            partitionName, schemaName, tableName), e);
        }
    }
}
