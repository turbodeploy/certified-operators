package com.vmturbo.testbases.dropdead;

import java.io.IOException;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;

import com.vmturbo.testbases.dropdead.DbObject.EventObject;
import com.vmturbo.testbases.dropdead.DbObject.FunctionObject;
import com.vmturbo.testbases.dropdead.DbObject.ProcObject;
import com.vmturbo.testbases.dropdead.DbObject.TableObject;
import com.vmturbo.testbases.dropdead.DbObject.ViewObject;
import com.vmturbo.testbases.dropdead.DeadDbObjectTestBase.RunIgnoredTestRunner;

/**
 * Scan for dead DB objects in a component. For a given component, create a test class that extends
 * this class and provide appropriate values in the constructor.
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
 */
@RunWith(RunIgnoredTestRunner.class)
public class DeadDbObjectTestBase {
    protected static final Logger logger = LogManager.getLogger();
    private static DeadDbObjectScanner scanner;

    /**
     * Create a new instance.
     *
     * @param conn              DB connection to use when scanning for objects
     * @param schemaName        name of DB schema to scan
     * @param deadObjects       list of {@link DbObject} instances for objects to be considered dead
     *                          regardless of analysis results
     * @param liveObjects       list of {@link DbObject} instances for objects to be considered live
     *                          regarldess of analysis results
     * @param schemaPackage     package in which jOOQ-generated classes are located
     * @param otherPackageSpecs any other packages to include/exclude for discovery (see {@link
     *                          Decompiler} for details
     * @throws IOException if there's a problem during discovery
     */
    public DeadDbObjectTestBase(Connection conn, String schemaName,
            Set<DbObject> deadObjects, Set<DbObject> liveObjects,
            String schemaPackage, String... otherPackageSpecs) throws IOException {
        if (scanner == null) {
            scanner = new DeadDbObjectScanner(conn, schemaName, deadObjects, liveObjects,
                    schemaPackage, otherPackageSpecs);
        }
        scanner.searchForLife();
    }

    /**
     * Test whether any dead objects were found, and if so, print DROP statements in a form suitable
     * for inclusion in a migration file.
     */
    @Ignore
    @Test
    public void testForDeadObjects() {
        final List<EventObject> deadEvents = scanner.getDeadEvents();
        if (!deadEvents.isEmpty()) {
            logger.info("Found {} dead procedures", deadEvents.size());
        }
        final List<FunctionObject> deadFunctions = scanner.getDeadFunctions();
        if (!deadFunctions.isEmpty()) {
            logger.info("Found {} dead functions", deadFunctions.size());
        }
        final List<ProcObject> deadProcs = scanner.getDeadProcs();
        if (!deadProcs.isEmpty()) {
            logger.info("Found {} dead procedures", deadProcs.size());
        }
        final List<ViewObject> deadViews = scanner.getDeadViews();
        if (!deadViews.isEmpty()) {
            logger.info("Found {} dead views", deadViews.size());
        }
        final List<TableObject> deadTables = scanner.getDeadTables();
        if (!deadTables.isEmpty()) {
            logger.info("Found {} dead tables", deadTables.size());
        }
        List<String> dropStatements = new ArrayList<>();
        final List<? extends DbObject> deadObjects = Stream.of(deadEvents, deadFunctions, deadProcs, deadViews, deadTables)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
        if (deadObjects.isEmpty()) {
            logger.info("No dead DB objects found");
        } else {

            dropStatements.add("SET FOREIGN_KEY_CHECKS = 0;\n");
            deadObjects.stream()
                    .map(DbObject::dropStatement)
                    .map(s -> s + ";\n")
                    .forEach(dropStatements::add);
            dropStatements.add("SET FOREIGN_KEY_CHECKS = 1;\n");
            logger.info("Statements to drop dead objects:\n{}", String.join("", dropStatements));
        }
    }

    /**
     * Test runner class that will run ignored tests if signaled by presence of eitehr a system
     * property or an environment variable.
     */
    public static class RunIgnoredTestRunner extends BlockJUnit4ClassRunner {

        private static final String RUN_IGNORED_TESTS_PROPERTY = "RUN_IGNORED_TESTS";

        /**
         * Constructs a new instance of the default runner.
         *
         * @param klass test class
         * @throws InitializationError if the super-constructor fails
         */
        public RunIgnoredTestRunner(final Class<?> klass) throws InitializationError {
            super(klass);
        }

        @Override
        protected boolean isIgnored(final FrameworkMethod child) {
            if (super.isIgnored(child)) {
                return System.getProperty(RUN_IGNORED_TESTS_PROPERTY) == null
                        && !System.getenv().containsKey(RUN_IGNORED_TESTS_PROPERTY);
            } else {
                return false;
            }
        }
    }
}
