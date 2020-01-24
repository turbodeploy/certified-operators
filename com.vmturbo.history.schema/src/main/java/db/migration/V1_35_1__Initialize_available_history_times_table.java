package db.migration;

import static com.vmturbo.history.schema.HistoryVariety.ENTITY_STATS;
import static com.vmturbo.history.schema.HistoryVariety.PRICE_DATA;
import static com.vmturbo.history.schema.RetentionUtil.DAILY_STATS_RETENTION_POLICY_NAME;
import static com.vmturbo.history.schema.RetentionUtil.HOURLY_STATS_RETENTION_POLICY_NAME;
import static com.vmturbo.history.schema.RetentionUtil.LATEST_STATS_RETENTION_POLICY_NAME;
import static com.vmturbo.history.schema.RetentionUtil.MONTHLY_STATS_RETENTION_POLICY_NAME;
import static com.vmturbo.history.schema.TimeFrame.DAY;
import static com.vmturbo.history.schema.TimeFrame.HOUR;
import static com.vmturbo.history.schema.TimeFrame.LATEST;
import static com.vmturbo.history.schema.TimeFrame.MONTH;
import static com.vmturbo.history.schema.abstraction.tables.VmStatsByMonth.VM_STATS_BY_MONTH;

import java.lang.reflect.Method;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.flywaydb.core.api.migration.MigrationChecksumProvider;
import org.flywaydb.core.api.migration.jdbc.BaseJdbcMigration;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Record1;
import org.jooq.Record2;
import org.jooq.Result;
import org.jooq.Table;
import org.jooq.impl.DSL;

import com.vmturbo.history.schema.HistoryVariety;
import com.vmturbo.history.schema.RetentionUtil;
import com.vmturbo.history.schema.TimeFrame;

/**
 * Migration to populate newly-created (by migration V1.27) available_timestamps table, with existing timestamps
 * found in the corresponding stats tables.
 *
 * <p>This will will permit much more efficient execution of some frequently used queries.</p>
 *
 * <p>Note that this migration is in the <code>com.vmturbo.history</code> module, not the
 * <code>com.vmturbo.history.schema</code> module that houses the SQL migrations for history component.
 * This means we can use not only jOOq (including the classes generated for our new table), but also other classes and
 * resources of the <code>com.vmturbo.history</code> component, which are not available to code in the schema
 * component. (And making those resources available would be difficult and perhaps impossible, because
 * it would require a circular dependency between the two modules.)</p>
 */
public class V1_35_1__Initialize_available_history_times_table extends BaseJdbcMigration
        implements MigrationChecksumProvider {

    // Tables we'll be using, without reference to generated jOOQ code.
    // This is important because future changes to these tables could cause generated code to be
    // incompatible with the database schema upon which this migration will always operate.
    // This way we can use jOOQ API but without being vulnerable to future schema evolution.
    private static final Table<Record> T_VM_STATS_LATEST = DSL.table("vm_stats_latest");
    private static final Table<Record> T_VM_STATS_BY_HOUR = DSL.table("vm_stats_by_hour");
    private static final Table<Record> T_VM_STATS_BY_DAY = DSL.table("vm_stats_by_day");
    private static final Table<Record> T_VM_STATS_BY_MONTH = DSL.table("vm_stats_by_month");
    private static final Table<Record> T_RETENTION_POLICIES = DSL.table("retention_policies");
    private static final Table<Record> T_AVAILABLE_TIMESTAMPS = DSL.table("available_timestamps");

    // fields needed from above tables
    private static final Field<Timestamp> F_SNAPSHOT_TIME = DSL.field("snapshot_time", Timestamp.class);
    private static final Field<String> F_POLICY_NAME = DSL.field("policy_name", String.class);
    private static final Field<String> F_UNIT = DSL.field("unit", String.class);
    private static final Field<Integer> F_RETENTION_PERIOD = DSL.field("retention_period", Integer.class);
    private static final Field<Timestamp> F_TIME_STAMP = DSL.field("time_stamp", Timestamp.class);
    private static final Field<String> F_TIME_FRAME = DSL.field("time_frame", String.class);
    private static final Field<String> F_HISTORY_VARIETY = DSL.field("history_variety", String.class);
    private static final Field<Timestamp> F_EXPIRES_AT = DSL.field("expires_at", Timestamp.class);


    @Override
    public void migrate(Connection connection) {
        DSLContext context = DSL.using(connection);
        context.settings().setRenderSchema(false);

        // clear the table first, for idempotency
        context.truncate(T_AVAILABLE_TIMESTAMPS).execute();

        // For ENTITY_STATS we assume the topology has VMs, and load snapshot times from the VM stats tables.
        loadTimes(context,
                T_VM_STATS_LATEST,
                F_SNAPSHOT_TIME,
                LATEST,
                ENTITY_STATS,
                LATEST_STATS_RETENTION_POLICY_NAME);
        loadTimes(context,
                T_VM_STATS_BY_HOUR,
                F_SNAPSHOT_TIME,
                HOUR,
                ENTITY_STATS,
                HOURLY_STATS_RETENTION_POLICY_NAME);
        loadTimes(context,
                T_VM_STATS_BY_DAY,
                F_SNAPSHOT_TIME,
                DAY,
                ENTITY_STATS,
                DAILY_STATS_RETENTION_POLICY_NAME);
        loadTimes(context,
                T_VM_STATS_BY_MONTH,
                VM_STATS_BY_MONTH.SNAPSHOT_TIME,
                MONTH,
                ENTITY_STATS,
                MONTHLY_STATS_RETENTION_POLICY_NAME);
        // We'll use the same timestamps for price data since the data to exract more accurate data from existing
        // tables would make the upgrade process prohibitively expensive.
        loadTimes(context,
                T_VM_STATS_LATEST,
                F_SNAPSHOT_TIME,
                LATEST,
                PRICE_DATA,
                LATEST_STATS_RETENTION_POLICY_NAME);
        loadTimes(context,
                T_VM_STATS_BY_HOUR,
                F_SNAPSHOT_TIME,
                HOUR,
                PRICE_DATA,
                HOURLY_STATS_RETENTION_POLICY_NAME);
        loadTimes(context,
                T_VM_STATS_BY_DAY,
                F_SNAPSHOT_TIME,
                DAY,
                PRICE_DATA,
                DAILY_STATS_RETENTION_POLICY_NAME);
        loadTimes(context,
                T_VM_STATS_BY_MONTH,
                F_SNAPSHOT_TIME,
                MONTH,
                PRICE_DATA,
                MONTHLY_STATS_RETENTION_POLICY_NAME);
    }

    /**
     * Retrieve timestamps from the given table and copy them into new available_timestamps records.
     *
     * @param ctx            jOOQ DSL context for DB operations
     * @param table          table from which to retrieve timestamps
     * @param timestampField field in that table containing the timestamps we want
     * @param timeFrame      timeframe to use in new available_timestamps records
     * @param historyVariety history variety to use in new available_timestamps records
     * @param policyName     name of a policy in the retention table
     */
    private void loadTimes(DSLContext ctx, Table<?> table, Field<Timestamp> timestampField,
            TimeFrame timeFrame, HistoryVariety historyVariety, String policyName) {
        Result<Record1<Timestamp>> timestamps = ctx.selectDistinct(timestampField).from(table).fetch();
        Record2<String, Integer> retentionRecord =
                ctx.select(F_UNIT, F_RETENTION_PERIOD)
                        .from(T_RETENTION_POLICIES)
                        .where(F_POLICY_NAME.eq(policyName))
                        .fetchOne();
        ChronoUnit unit = ChronoUnit.valueOf(retentionRecord.getValue(F_UNIT));
        int period = retentionRecord.getValue(F_RETENTION_PERIOD);
        timestamps.forEach(record -> {
            Timestamp timestamp = record.value1();
            Instant expiration = RetentionUtil.getExpiration(Instant.ofEpochMilli(
                    timestamp.getTime()), unit, period);
            ctx.insertInto(T_AVAILABLE_TIMESTAMPS)
                    .set(F_TIME_STAMP, timestamp)
                    .set(F_TIME_FRAME, timeFrame.name())
                    .set(F_HISTORY_VARIETY, historyVariety.name())
                    .set(F_EXPIRES_AT, Timestamp.from(expiration))
                    .execute();
        });
    }


    /**
     * By default, flyway JDBC migrations do not provide chekcpoints, but we do so here.
     *
     * <p>The goal is to prevent any change to this migration from ever being made after it goes into release.
     * We do that by gathering some information that would, if it were to change, signal that this class definition
     * has been changed, and then computing a checksum value from that information. It's not as fool-proof as a
     * checksum on the source code, but there's no way to reliably obtain the exact source code at runtime.</p>
     *
     * @return checksum for this migration
     */
    @Override
    public Integer getChecksum() {
        try {
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            // include this class's fully qualified name
            md5.update(getClass().getName().getBytes());
            // and the closest I know how to get to a source line count
            md5.update(getLineCountEstimate().toString().getBytes());
            // add in the method signatures of all the
            md5.update(getMethodSignature("migrate").getBytes());
            md5.update(getMethodSignature("loadTimes").getBytes());
            md5.update(getMethodSignature("getChecksum").getBytes());
            md5.update(getMethodSignature("getMethodSignature").getBytes());
            md5.update(getMethodSignature("getLineCountEstimate").getBytes());
            return new HashCodeBuilder().append(md5.digest()).hashCode();
        } catch (Exception e) {
            if (!(e instanceof IllegalStateException)) {
                e = new IllegalStateException(e);
            }
            throw (IllegalStateException)e;
        }
    }

    /**
     * Get a rendering of a named method's signature.
     *
     * <p>We combine the method's name with a list of the fully-qualified class names of all its parameters.</p>
     *
     * <p>This works only when the method is declared by this class, and it is the only method with that name
     * declared by the class.</p>
     *
     * @param name name of method
     * @return method's signature (e.g. "getMethodSignature(java.lang.String name)"
     */
    private String getMethodSignature(String name) {
        List<Method> candidates = Stream.of(getClass().getDeclaredMethods())
                .filter(m -> m.getName().equals(name))
                .collect(Collectors.toList());
        if (candidates.size() == 1) {
            String parms = Stream.of(candidates.get(0).getParameters())
                    .map(p -> p.getType().getName() + " " + p.getName())
                    .collect(Collectors.joining(","));
            return candidates.get(0).getName() + "(" + parms + ")";
        } else {
            throw new IllegalStateException(
                    String.format("Failed to obtain method signature for method '%s': %d methods found",
                            name, candidates.size()));
        }
    }

    /**
     * Get something close to the number of lines in this class's source file.
     *
     * <p>To get the closest estimate possible, this method should appear at the end of the class. We obtain the
     * line number from our entry on a stack trace.</p>
     *
     * @return approximate line count
     */
    private Integer getLineCountEstimate() {
        // code to get our stack frame
        Supplier<StackTraceElement> getFrame = () -> Thread.currentThread().getStackTrace()[1];
        // Do a quick check to make sure our method is getting the stack frame we intend it to. (It could put us
        // in quite a pickle if we inadvertently incorporated information from some other class's stack frame!)
        if (!getFrame.get().getClassName().equals(getClass().getName())) {
            throw new IllegalStateException(
                    "Attempt to read our stack trace frame failed - found unexpected class " + getFrame.get().getClassName());
        }
        // re-fetch our stack frame using same method, so we're as close as possible to end of source
        return getFrame.get().getLineNumber();
    }
}
