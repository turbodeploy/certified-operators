package com.vmturbo.history.misc;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import com.google.common.collect.ImmutableSet;

import org.jooq.Table;

import com.vmturbo.history.db.EntityType;
import com.vmturbo.testbases.dropdead.DbObject;
import com.vmturbo.testbases.dropdead.DbObject.ProcObject;
import com.vmturbo.testbases.dropdead.DbObject.TableObject;
import com.vmturbo.testbases.dropdead.DeadDbObjectTestBase;

/**
 * Scan for dead DB objects in history component.
 *
 * <p>The single test is @Ignored, but the class configured to use a custom test runner that will
 * run ignored tests if `RUN_IGNORED_TESTS` env var or system property is defined. So tests based on
 * this class</p>
 *
 * <p>The test itself prints DB statements that can be collected into a migration to remove dead
 * objects, but the list should be carefully scrutinized and tested due to the possibility of false
 * positives. In particular, objects named with no underscores are not subjected to liveness tests
 * involving their appearance in non-jOOQ-generated SQL, either in Java code or in the definitions
 * of other live DB objects. This is due to the high likelihood of such names appearing in code for
 * other reasons. Another reason for false positives is that the process of collecting all classes
 * accessible from the classpath appears to be somewhat flaky, and usages in classes that are not
 * scanned will not prevent the used DB objects from being treated as dead.</p>
 *
 * <p>Despite the limitations, this can be a great help in providing a starting point for a
 * clean-up effort.</p>
 *
 * <p>N.B. This can take several minutes to complete.</p>
 */
public class DeadDbObjectTest extends DeadDbObjectTestBase {

    /**
     * Create a new instance.
     *
     * @throws SQLException if there's a problem scanning the DB for objects
     * @throws IOException  if there's a problem discovering classes
     */
    public DeadDbObjectTest() throws SQLException, IOException {
        super(DriverManager.getConnection("jdbc:mysql://localhost:3306/vmtdb", "root", "vmturbo"),
                "vmtdb", Collections.emptySet(), forcedLiveObjects(),
                "com.vmturbo.history.schema.abstraction", "com.vmturbo.history"
        );
    }

    private static Set<DbObject> forcedLiveObjects() {
        return ImmutableSet.<DbObject>builder()
                .addAll(entityStatsTables())
                .add(new ProcObject("aggregateSpend", null))
                .add(new TableObject("notifications"))
                .build();
    }

    private static Set<DbObject> entityStatsTables() {
        final HashSet<DbObject> results = new HashSet<>();
        EntityType.allEntityTypes().forEach(type -> {
            type.getLatestTable().map(DeadDbObjectTest::toDbObject).ifPresent(results::add);
            type.getHourTable().map(DeadDbObjectTest::toDbObject).ifPresent(results::add);
            type.getDayTable().map(DeadDbObjectTest::toDbObject).ifPresent(results::add);
            type.getMonthTable().map(DeadDbObjectTest::toDbObject).ifPresent(results::add);
        });
        return results;
    }

    private static TableObject toDbObject(Table<?> t) {
        return new TableObject(t.getName());
    }
}
