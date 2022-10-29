package com.vmturbo.topology.processor.identity.staleoidmanager;

import static com.vmturbo.topology.processor.identity.recurrenttasks.RecurrentTask.RecurrentTasksEnum.OID_TIMESTAMP_UPDATE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.jooq.tools.jdbc.MockDataProvider;
import org.jooq.tools.jdbc.MockExecuteContext;
import org.jooq.tools.jdbc.MockResult;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.topology.processor.db.Tables;
import com.vmturbo.topology.processor.identity.StaleOidManagerImpl;
import com.vmturbo.topology.processor.identity.StaleOidManagerTest;

/**
 * jOOQ {@link DataProvider} implementation to support {@link StaleOidManagerTest}.
 *
 * <p>The overall approach implemented in this class is, for each query, executed by a live instance
 * of {@link StaleOidManagerImpl}:
 * <ol>
 *     <li>Categorize the query based on its SQL</li>
 *     <li>
 *         Create an instance of a category-specific "capture" class to capture bindings and make
 *         them conveniently accessible.
 *     </li>
 *     <li>Construct query results suitable to the query.</li>
 * </ol>
 *
 * <p>The capture objects can be examined by the test and are a the primary means by which tests
 * validate the execution</p>
 *
 * <p>In most tests, a single scheduled execution of the oid management cycle is executed, and the
 * {@link DataProvider} instance fails simulated execution of any query that arrives after that
 * single cycle is complete. It is up to the test to detect the end of the first cycle and
 * communicate that to the {@link DataProvider}. First cycle completion is communicated via a
 * {@link Future} that becomes "done" when the first cycle is complete.</p>
 */
public class DataProvider implements MockDataProvider {

    private final List<QueryCapture> allQueries = new ArrayList<>();
    private final Map<QueryCategory, List<QueryCapture>> queriesByCategory = new HashMap<>();

    private final SQLDialect dialect;
    private ScheduledFuture<?> cycleCompletionFuture;
    private boolean done;
    private boolean forceFail;
    private int successfulTaskCount;

    private static DataProvider currentDataProvider;
    private boolean allowMultipleCycles;
    private Collection<Long> currentOids;

    public static DataProvider getCurrentDataProvider() {
        return currentDataProvider;
    }

    /**
     * Create a new instance.
     *
     * @param dialect {@link SQLDialect} that will be used to construct queries
     */
    public DataProvider(SQLDialect dialect) {
        this.dialect = dialect;
        currentDataProvider = this;
    }

    /**
     * Provide values that affect how the instance behaves, to provide for various execution
     * scenarios under test.
     *
     * <p>These values are not available at the time in the test class where the
     * {@link DataProvider} must be constructed, so they cannot be provided in the constructor.</p>
     *
     * @param cycleCompletionFuture future that, when done, signals first-cycle completion
     * @param forceFail             true if all queries should be made to fail with an exception
     * @param allowMultipleCycles   true if executions should be allowed after the first cycle
     * @param successfulTaskCount   number of successful tasks to be returned by any
     *                              {@link QueryCategory#COUNT_SUCCESSFUL_RECURRENT_TASKS} executed
     *                              during this test
     * @param currentOids           list of oids that are currently present (i.e. in the entity
     *                              store, in non-test execution)
     */
    public void init(ScheduledFuture<?> cycleCompletionFuture, boolean forceFail,
            boolean allowMultipleCycles, int successfulTaskCount, Collection<Long> currentOids) {
        this.cycleCompletionFuture = cycleCompletionFuture;
        this.allowMultipleCycles = allowMultipleCycles;
        this.forceFail = forceFail;
        this.successfulTaskCount = successfulTaskCount;
        this.currentOids = currentOids;
    }

    @Override
    public synchronized MockResult[] execute(MockExecuteContext ctx) {
        if (!done && cycleCompletionFuture != null && cycleCompletionFuture.isDone()
                && !allowMultipleCycles) {
            this.done = true;
        }
        if (!done && !forceFail) {
            return runQuery(ctx);
        } else if (done) {
            throw new IllegalStateException("StaleOidManagerTest has already completed one cycle");
        } else {
            throw new IllegalStateException("Test is configured to fail all queries");
        }
    }

    private MockResult[] runQuery(MockExecuteContext ctx) {
        QueryCapture capture = QueryCategory.capture(ctx, dialect);
        allQueries.add(capture);
        queriesByCategory.computeIfAbsent(capture.category, cat -> new ArrayList<>()).add(capture);
        // special case to convey current oids (provided in DataProvider#init(...) to the
        // GetExpiredEntities query for use in producing results
        if (capture.getCategory() == QueryCategory.GET_EXPIRED_ENTITIES) {
            ((GetExpiredEntities)capture).addCurrentOids(currentOids);
        }
        return capture.results();
    }

    public List<QueryCapture> getAllQueries() {
        return allQueries;
    }

    /**
     * Retrieve {@link QueryCapture} objects for all queries of a given category executed by this
     * {@link DataProvider}.
     *
     * @param category {@link QueryCategory} whose queries are needed
     * @return list of capture objects, in order of execution
     */
    public List<QueryCapture> getQueries(QueryCategory category) {
        return queriesByCategory.getOrDefault(category, Collections.emptyList());
    }

    public int getSuccessfulTaskCount() {
        return successfulTaskCount;
    }

    /**
     * Categories of queries executed by the tests.
     */
    public enum QueryCategory {
        /** Query to retrieve records from the `recurrent_tasks` table. */
        GET_RECURRENT_TASKS(GetRecurrentTasks::new),
        /** Count the number of successful recurrent tasks recorded in `recurrent_tasks`. */
        COUNT_SUCCESSFUL_RECURRENT_TASKS(CountSuccessfulRecurrentTasks::new),
        /** Add a record to `recurrent_tasks` table. */
        INSERT_RECURRENT_TASK(InsertRecurrentTask::new),
        /** Delete records from the `recurrent_operations` table. */
        DELETE_RECURRENT_OPERATIONS(DeleteRecurrentOperations::new),
        /** Set the `last_seen` timestamp in records for currently active entities. */
        SET_ENTITIES_LAST_SEEN(SetEntitiesLastSeen::new),
        /** Retrieve oids of entities that have not been seen since the current expiration date. */
        GET_EXPIRED_ENTITIES(GetExpiredEntities::new),
        /** Set the `expired` flag in records that have become expired. */
        SET_EXPIRED_ENTITIES(SetExpiredEntities::new),
        /**
         * Delete records marked as expired and that have not been seen since current deletion
         * date.
         */
        DELETE_EXPIRED_ENTITIES(DeleteExpiredEntities::new);

        private final Function<Pair<MockExecuteContext, SQLDialect>, QueryCapture> captureFactory;

        QueryCategory(Function<Pair<MockExecuteContext, SQLDialect>, QueryCapture> captureFactory) {
            this.captureFactory = captureFactory;
        }

        /**
         * Create a {@link QueryCapture} instance to represent a query of this category.
         *
         * @param context {@link DSLContext} attached to this {@link DataProvider}
         * @param dialect {@link SQLDialect} being used for query execution
         * @return instance of the category-specific {@link QueryCapture} subclass
         */
        public static QueryCapture capture(MockExecuteContext context, SQLDialect dialect) {
            QueryCategory category = getQueryCategory(context.sql(), dialect);
            return category.captureFactory.apply(Pair.of(context, dialect));
        }

        /**
         * Determine the {@link QueryCategory} for the current query, based on its SQL.
         *
         * <p>We use a simple match of the full SQL, which means that if any of the SQL changes,'
         * this method will need to be updated accordingly. The new SQL in this case will show up in
         * the logs, in the context of the exception thrown by this message, for easy
         * copy/paste.</p>
         *
         * <p>In some cases, the SQL for a given category may appear in multiple variations.</p>
         *
         * @param sql     SQL being executed
         * @param dialect {@link SQLDialect} used by jOOQ to construct SQL
         * @return query category
         */
        private static QueryCategory getQueryCategory(String sql, SQLDialect dialect) {
            // SQL naturally differs based on category.
            switch (dialect) {
                case MARIADB:
                case MYSQL:
                    switch (sql) {
                        case "select `topology_processor`.`recurrent_tasks`.`execution_time`, `topology_processor`.`recurrent_tasks`.`operation_name`, `topology_processor`.`recurrent_tasks`.`successful`, `topology_processor`.`recurrent_tasks`.`affected_rows`, `topology_processor`.`recurrent_tasks`.`summary`, `topology_processor`.`recurrent_tasks`.`errors` from `topology_processor`.`recurrent_tasks` order by `topology_processor`.`recurrent_tasks`.`execution_time` desc limit ?":
                            return GET_RECURRENT_TASKS;
                        case "select count(*) from `topology_processor`.`recurrent_tasks` where (`topology_processor`.`recurrent_tasks`.`execution_time` > ? and `topology_processor`.`recurrent_tasks`.`operation_name` = ? and `topology_processor`.`recurrent_tasks`.`successful` = true)":
                            return COUNT_SUCCESSFUL_RECURRENT_TASKS;
                        case "insert into `topology_processor`.`recurrent_tasks` (`execution_time`, `operation_name`, `affected_rows`, `successful`, `summary`, `errors`) values (?, ?, ?, ?, ?, ?)":
                            return INSERT_RECURRENT_TASK;
                        case "delete from `topology_processor`.`recurrent_operations` where `topology_processor`.`recurrent_operations`.`execution_time` < ?":
                            return DELETE_RECURRENT_OPERATIONS;
                        case "update `topology_processor`.`assigned_identity` set `topology_processor`.`assigned_identity`.`last_seen` = ? where `topology_processor`.`assigned_identity`.`id` in (?, ?)":
                            return SET_ENTITIES_LAST_SEEN;
                        case "select `topology_processor`.`assigned_identity`.`id` from `topology_processor`.`assigned_identity` where (`topology_processor`.`assigned_identity`.`last_seen` < ? and `topology_processor`.`assigned_identity`.`expired` = false)":
                        case "select `topology_processor`.`assigned_identity`.`id` from `topology_processor`.`assigned_identity` where (`topology_processor`.`assigned_identity`.`last_seen` < ? and `topology_processor`.`assigned_identity`.`expired` = false and not(`topology_processor`.`assigned_identity`.`entity_type` in (?, ?)))":
                        case "select `topology_processor`.`assigned_identity`.`id` from `topology_processor`.`assigned_identity` where (`topology_processor`.`assigned_identity`.`last_seen` < ? and `topology_processor`.`assigned_identity`.`expired` = false and `topology_processor`.`assigned_identity`.`entity_type` = ?)":
                            return GET_EXPIRED_ENTITIES;
                        case "update `topology_processor`.`assigned_identity` set `topology_processor`.`assigned_identity`.`expired` = ? where (`topology_processor`.`assigned_identity`.`last_seen` < ? and `topology_processor`.`assigned_identity`.`expired` = false)":
                        case "update `topology_processor`.`assigned_identity` set `topology_processor`.`assigned_identity`.`expired` = ? where (`topology_processor`.`assigned_identity`.`last_seen` < ? and `topology_processor`.`assigned_identity`.`expired` = false and not(`topology_processor`.`assigned_identity`.`entity_type` in (?, ?)))":
                        case "update `topology_processor`.`assigned_identity` set `topology_processor`.`assigned_identity`.`expired` = ? where (`topology_processor`.`assigned_identity`.`last_seen` < ? and `topology_processor`.`assigned_identity`.`expired` = false and `topology_processor`.`assigned_identity`.`entity_type` = ?)":
                            return SET_EXPIRED_ENTITIES;
                        case "delete from `topology_processor`.`assigned_identity` where (`topology_processor`.`assigned_identity`.`last_seen` < ? and `topology_processor`.`assigned_identity`.`expired` = true) limit ?":
                            return DELETE_EXPIRED_ENTITIES;
                        default:
                            break;
                    }
                    break;
                case POSTGRES:
                    switch (sql) {
                        case "select \"topology_processor\".\"recurrent_tasks\".\"execution_time\", \"topology_processor\".\"recurrent_tasks\".\"operation_name\", \"topology_processor\".\"recurrent_tasks\".\"successful\", \"topology_processor\".\"recurrent_tasks\".\"affected_rows\", \"topology_processor\".\"recurrent_tasks\".\"summary\", \"topology_processor\".\"recurrent_tasks\".\"errors\" from \"topology_processor\".\"recurrent_tasks\" order by \"topology_processor\".\"recurrent_tasks\".\"execution_time\" desc limit ?":
                            return GET_RECURRENT_TASKS;
                        case "select count(*) from \"topology_processor\".\"recurrent_tasks\" where (\"topology_processor\".\"recurrent_tasks\".\"execution_time\" > cast(? as timestamp) and \"topology_processor\".\"recurrent_tasks\".\"operation_name\" = ? and \"topology_processor\".\"recurrent_tasks\".\"successful\" = true)":
                            return COUNT_SUCCESSFUL_RECURRENT_TASKS;
                        case "insert into \"topology_processor\".\"recurrent_tasks\" (\"execution_time\", \"operation_name\", \"affected_rows\", \"successful\", \"summary\", \"errors\") values (cast(? as timestamp), ?, ?, ?, ?, ?)":
                            return INSERT_RECURRENT_TASK;
                        case "delete from \"topology_processor\".\"recurrent_operations\" where \"topology_processor\".\"recurrent_operations\".\"execution_time\" < cast(? as timestamp)":
                            return DELETE_RECURRENT_OPERATIONS;
                        case "update \"topology_processor\".\"assigned_identity\" set \"last_seen\" = cast(? as timestamp) where \"topology_processor\".\"assigned_identity\".\"id\" in (?, ?)":
                            return SET_ENTITIES_LAST_SEEN;
                        case "select \"topology_processor\".\"assigned_identity\".\"id\" from \"topology_processor\".\"assigned_identity\" where (\"topology_processor\".\"assigned_identity\".\"last_seen\" < cast(? as timestamp) and \"topology_processor\".\"assigned_identity\".\"expired\" = false)":
                        case "select \"topology_processor\".\"assigned_identity\".\"id\" from \"topology_processor\".\"assigned_identity\" where (\"topology_processor\".\"assigned_identity\".\"last_seen\" < cast(? as timestamp) and \"topology_processor\".\"assigned_identity\".\"expired\" = false and \"topology_processor\".\"assigned_identity\".\"entity_type\" = ?)":
                        case "select \"topology_processor\".\"assigned_identity\".\"id\" from \"topology_processor\".\"assigned_identity\" where (\"topology_processor\".\"assigned_identity\".\"last_seen\" < cast(? as timestamp) and \"topology_processor\".\"assigned_identity\".\"expired\" = false and not(\"topology_processor\".\"assigned_identity\".\"entity_type\" in (?, ?)))":
                            return GET_EXPIRED_ENTITIES;
                        case "update \"topology_processor\".\"assigned_identity\" set \"expired\" = ? where (\"topology_processor\".\"assigned_identity\".\"last_seen\" < cast(? as timestamp) and \"topology_processor\".\"assigned_identity\".\"expired\" = false)":
                        case "update \"topology_processor\".\"assigned_identity\" set \"expired\" = ? where (\"topology_processor\".\"assigned_identity\".\"last_seen\" < cast(? as timestamp) and \"topology_processor\".\"assigned_identity\".\"expired\" = false and not(\"topology_processor\".\"assigned_identity\".\"entity_type\" in (?, ?)))":
                        case "update \"topology_processor\".\"assigned_identity\" set \"expired\" = ? where (\"topology_processor\".\"assigned_identity\".\"last_seen\" < cast(? as timestamp) and \"topology_processor\".\"assigned_identity\".\"expired\" = false and \"topology_processor\".\"assigned_identity\".\"entity_type\" = ?)":
                            return SET_EXPIRED_ENTITIES;
                        case "delete from \"topology_processor\".\"assigned_identity\" where \"topology_processor\".\"assigned_identity\".\"id\" in (select \"topology_processor\".\"assigned_identity\".\"id\" from \"topology_processor\".\"assigned_identity\" where (\"topology_processor\".\"assigned_identity\".\"last_seen\" < cast(? as timestamp) and \"topology_processor\".\"assigned_identity\".\"expired\" = true) limit ?)":
                            return DELETE_EXPIRED_ENTITIES;
                        default:
                            break;
                    }
                    break;
                default:
                    throw new IllegalArgumentException(
                            "Dialect is not supported: " + dialect.name());
            }
            // no match was found for the query
            throw new IllegalArgumentException("SQL query could not be categorized; "
                    + "QueryCategory.getQueryCategory may need to be updated if queries have changed: "
                    + sql);
        }
    }

    /**
     * Base class for category-specific classes that represent queries that have been executed by
     * this {@link DataProvider}. The capture classes break down binding values and make them
     * available in appropriately-typed accessors for use by tests. They are also responsible for
     * constructing query results.
     */
    public abstract static class QueryCapture {
        protected final String sql;
        protected final Object[] bindings;
        protected final MockExecuteContext context;
        private final QueryCategory category;

        /**
         * Create a new instance.
         *
         * @param context {@link MockExecuteContext} for execution of this query
         * @param dialect {@link SQLDialect} used in SQL generation
         */
        public QueryCapture(MockExecuteContext context, SQLDialect dialect) {
            this.context = context;
            this.sql = context.sql();
            this.category = QueryCategory.getQueryCategory(sql, dialect);
            this.bindings = context.bindings();
        }

        /**
         * Collects the tail end of the bindings into a {@link List}.
         *
         * @param skipBindingCount position of first binding to collect
         * @param <T>              type top which collected bindings will be cast
         * @return list of collected bindings
         */
        @NotNull
        protected <T> List<T> collectRemainingBindings(int skipBindingCount) {
            //noinspection unchecked
            return Arrays.stream(Arrays.copyOfRange(bindings, skipBindingCount, bindings.length))
                    .map(value -> (T)value)
                    .collect(Collectors.toList());
        }

        /**
         * Provide results for the SQL execution.
         *
         * @return query results
         */
        public abstract MockResult[] results();

        public QueryCategory getCategory() {
            return category;
        }
    }

    /**
     * Specialization of {@link QueryCapture} for GET_RECURRENT_TASKS query category.
     */
    public static class GetRecurrentTasks extends QueryCapture {
        private final Long limit;

        /**
         * Create a new instance.
         *
         * @param pair {@link Pair} containing execution context and dialect
         */
        public GetRecurrentTasks(Pair<MockExecuteContext, SQLDialect> pair) {
            super(pair.getLeft(), pair.getRight());
            assertThat(bindings.length, is(1));
            this.limit = (Long)bindings[0];
        }

        @Override
        public MockResult[] results() {
            ResultCreator resultCreator = new ResultCreator(Tables.RECURRENT_TASKS.fields());
            resultCreator.add(LocalDateTime.now(), OID_TIMESTAMP_UPDATE.toString(), true, 10, "",
                    "");
            return resultCreator.getMockResults();
        }
    }

    /**
     * Specialization of {@link QueryCapture} for COUNT_SUCCESSFUL_RECURRENT_TASKS query category.
     */
    public static class CountSuccessfulRecurrentTasks extends QueryCapture {
        private final Timestamp executionTime;
        private final String operationName;

        /**
         * Create a new instance.
         *
         * @param pair {@link Pair} containing execution context and dialect
         */
        public CountSuccessfulRecurrentTasks(Pair<MockExecuteContext, SQLDialect> pair) {
            super(pair.getLeft(), pair.getRight());
            assertThat(bindings.length, is(2));
            this.executionTime = (Timestamp)bindings[0];
            this.operationName = (String)bindings[1];
        }

        @Override
        public MockResult[] results() {
            return ResultCreator.makeIntMockResults(
                    DataProvider.getCurrentDataProvider().getSuccessfulTaskCount());
        }
    }

    /**
     * Specialization of {@link QueryCapture} for INSERT_RECURRENT_TASK query category.
     */
    public static class InsertRecurrentTask extends QueryCapture {
        private final Timestamp executionTime;
        private final String operationName;
        private final Integer affectedRows;
        private final Boolean isSuccess;
        private final String summary;
        private final String errors;

        /**
         * Create a new instance.
         *
         * @param pair {@link Pair} containing execution context and dialect
         */
        public InsertRecurrentTask(Pair<MockExecuteContext, SQLDialect> pair) {
            super(pair.getLeft(), pair.getRight());
            assertThat(bindings.length, is(6));
            //`execution_time`, `operation_name`, `affected_rows`, `successful`, `summary`, `errors`
            this.executionTime = (Timestamp)bindings[0];
            this.operationName = (String)bindings[1];
            this.affectedRows = (Integer)bindings[2];
            this.isSuccess = (Boolean)bindings[3];
            this.summary = (String)bindings[4];
            this.errors = (String)bindings[5];
        }

        @Override
        public MockResult[] results() {
            return ResultCreator.makeIntMockResults(1);
        }

        public Timestamp getExecutionTime() {
            return executionTime;
        }

        public String getOperationName() {
            return operationName;
        }
    }

    /**
     * Specialization of {@link QueryCapture} for DELETE_RECURRENT_OPERATIONS query category.
     */
    public static class DeleteRecurrentOperations extends QueryCapture {
        private final Timestamp deletionTimestamp;

        /**
         * Create a new instance.
         *
         * @param pair {@link Pair} containing execution context and dialect
         */
        public DeleteRecurrentOperations(Pair<MockExecuteContext, SQLDialect> pair) {
            super(pair.getLeft(), pair.getRight());
            assertThat(bindings.length, is(1));
            this.deletionTimestamp = (Timestamp)bindings[0];
        }

        @Override
        public MockResult[] results() {
            return ResultCreator.makeIntMockResults(10);
        }
    }

    /**
     * Specialization of {@link QueryCapture} for SET_ENTITIES_LAST_SEEN query category.
     */
    public static class SetEntitiesLastSeen extends QueryCapture {

        private final Timestamp lastSeenTimestamp;
        private final List<Long> updatedOids;

        /**
         * Create a new instance.
         *
         * @param pair {@link Pair} containing execution context and dialect
         */
        public SetEntitiesLastSeen(Pair<MockExecuteContext, SQLDialect> pair) {
            super(pair.getLeft(), pair.getRight());
            assertThat(bindings.length, greaterThanOrEqualTo(1));
            this.lastSeenTimestamp = (Timestamp)bindings[0];
            this.updatedOids = collectRemainingBindings(1);
        }

        @Override
        public MockResult[] results() {
            return ResultCreator.makeIntMockResults(updatedOids.size());
        }

        public List<Long> getUpdatedOids() {
            return updatedOids;
        }

        public Timestamp getLastSeenTimestamp() {
            return lastSeenTimestamp;
        }
    }

    /**
     * Specialization of {@link QueryCapture} for GET_EXPIRED_ENTITIES query category.
     */
    public static class GetExpiredEntities extends QueryCapture {
        private final Timestamp expiryTimestamp;
        private final Set<Long> currentOids = new HashSet<>();

        /**
         * Create a new instance.
         *
         * @param pair {@link Pair} containing execution context and dialect
         */
        public GetExpiredEntities(Pair<MockExecuteContext, SQLDialect> pair) {
            super(pair.getLeft(), pair.getRight());
            assertThat(bindings.length, greaterThanOrEqualTo(1));
            this.expiryTimestamp = (Timestamp)bindings[0];
            this.currentOids.addAll(collectRemainingBindings(1));
            // handle per-entity-type expiration
        }

        @Override
        public MockResult[] results() {
            ResultCreator resultCreator = new ResultCreator(Tables.ASSIGNED_IDENTITY.ID);
            currentOids.stream().filter(oid -> oid >= 0).forEach(resultCreator::add);
            return resultCreator.getMockResults();
        }

        public Timestamp getExpiryTimestamp() {
            return expiryTimestamp;
        }

        /**
         * Provide oids to be considered deleted in this execution.
         *
         * <p>This is needed because there are no oids conveyed in the query, which could otherwise
         * be used for this purpose.</p>
         *
         * @param oids oids to be deleted
         */
        public void addCurrentOids(Collection<Long> oids) {
            this.currentOids.addAll(oids);
        }
    }

    /**
     * Specialization of {@link QueryCapture} for SET_ENTITIES_LAST_SEEN query category.
     */
    public static class SetExpiredEntities extends QueryCapture {
        private final Boolean expired;
        private final Timestamp expiryTimestamp;
        private final List<Long> expiredOids;
        private Integer entityType;

        /**
         * Create a new instance.
         *
         * @param pair {@link Pair} containing execution context and dialect
         */
        public SetExpiredEntities(Pair<MockExecuteContext, SQLDialect> pair) {
            super(pair.getLeft(), pair.getRight());
            assertThat(bindings.length, greaterThanOrEqualTo(2));
            this.expired = (Boolean)bindings[0];
            this.expiryTimestamp = (Timestamp)bindings[1];
            if (bindings.length > 2 && bindings[2] instanceof Integer) {
                this.entityType = (Integer)bindings[2];
                this.expiredOids = collectRemainingBindings(3);
            } else {
                this.expiredOids = collectRemainingBindings(2);
            }
        }

        @Override
        public MockResult[] results() {
            return ResultCreator.makeIntMockResults(expiredOids.size());
        }

        public Timestamp getExpiryTimestamp() {
            return expiryTimestamp;
        }

        public boolean isExpired() {
            return expired;
        }

        public String getEntityTypeName() {
            return entityType != null ? EntityType.forNumber(entityType).name() : null;
        }
    }

    /**
     * Specialization of {@link QueryCapture} for DELETE_EXPIRED_ENTITIES query category.
     */
    public static class DeleteExpiredEntities extends QueryCapture {
        private final Timestamp deletionTimestamp;
        private final Integer limit;

        /**
         * Create a new instance.
         *
         * @param pair {@link Pair} containing execution context and dialect
         */
        public DeleteExpiredEntities(Pair<MockExecuteContext, SQLDialect> pair) {
            super(pair.getLeft(), pair.getRight());
            this.deletionTimestamp = (Timestamp)bindings[0];
            this.limit = (Integer)bindings[1];
        }

        @Override
        public MockResult[] results() {
            return ResultCreator.makeIntMockResults(limit);
        }
    }

    /**
     * Utility class to construct query result records and package them up in a
     * {@link MockResult}[].
     */
    private static class ResultCreator {

        private final Field<?>[] fields;
        private final DSLContext creator;
        private final Result<Record> result;

        /**
         * Create a new instance.
         *
         * @param fields fields present in result records
         */
        ResultCreator(Field<?>... fields) {
            this.fields = fields;
            this.creator = DSL.using(SQLDialect.DEFAULT);
            this.result = creator.newResult(fields);
        }

        /**
         * Create a result containing a single int value.
         *
         * @param value the int value to be returned
         * @return query result
         */
        public static MockResult[] makeIntMockResults(int value) {
            ResultCreator resultCreator = new ResultCreator(
                    // simple way to create an int field
                    DSL.count());
            resultCreator.add(value);
            return resultCreator.getMockResults();
        }

        /**
         * Create a result record with the given values (must be in same order as corresponding
         * fields passed to {@link ResultCreator#ResultCreator(Field[])}).
         *
         * @param values values to appear in result record
         */
        public void add(Object... values) {
            Record record = creator.newRecord(fields);
            record.from(values);
            result.add(record);
        }

        /**
         * Package result records into a {@link MockResult}[].
         *
         * @return mock results
         */
        public MockResult[] getMockResults() {
            return new MockResult[]{new MockResult(result.size(), result)};
        }
    }
}
