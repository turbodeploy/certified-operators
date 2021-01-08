package com.vmturbo.testbases.dropdead;

import java.io.IOException;
import java.sql.Connection;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.impl.DSL;

import com.vmturbo.testbases.dropdead.DbObject.EventObject;
import com.vmturbo.testbases.dropdead.DbObject.FunctionObject;
import com.vmturbo.testbases.dropdead.DbObject.ProcObject;
import com.vmturbo.testbases.dropdead.DbObject.TableObject;
import com.vmturbo.testbases.dropdead.DbObject.ViewObject;

/**
 * Detect DB objects that are created by a component's migrations but are never used.
 *
 * <p>This class assumes that most if not all uses of tables, views, functions, and procedures will
 * be via jOOQ generated classes, and it scans Java code found in the classpath (within specified
 * packages) for references of such classes. It also scans disassembled classes for instances of of
 * the raw object names, in an attempt to catch uses in raw SQL.</p>
 *
 * <p>In addition to scanning Java code, definitions of certain SQL objects are also scanned for
 * references to other SQL objects. Specifically: view definitions are scanned for table references,
 * and routine and event definitions are scanned for table, view, and routine references.</p>
 */
public class DeadDbObjectScanner {
    private static final Logger logger = LogManager.getLogger();

    private final Connection conn;
    private final String schemaName;
    private final Decompiler decompiler;
    private final String schemaPackage;

    private final Map<String, TableObject> tables = new HashMap<>();
    private final Map<String, ViewObject> views = new HashMap<>();
    private final Map<String, FunctionObject> functions = new HashMap<>();
    private final Map<String, ProcObject> procedures = new HashMap<>();
    private final Map<String, EventObject> events = new HashMap<>();

    /**
     * Create a new test class instance.
     *
     * <p>All the hard work is done by the constructor. The DB is scanned for DB objects, source
     * classes are discovered and decompiled, sources and DB object definitions are scanned for uses
     * of DB objects under consideration, and forced dead/live objects are marked as such.</p>
     *
     * <p>See {@link Decompiler} documentation for information on how to specify included and
     * excluded packages for the `otherPackages` parameter.</p>
     *
     * @param conn              connection to a live database built from the component's migrations
     * @param schemaName        name of schema to be analyzed
     * @param deadObjects       objects that are to be considered dead regardless of scan result
     * @param liveObjects       objects that are to be considered live regardless of scan result
     * @param schemaPackage     package in which jOOQ-generated classes are found - will be used as
     *                          an excluded package name for discovery
     * @param otherPackageSpecs additional inclucded or excluded pckage names
     * @throws IOException if there's a problem with discovery
     */
    public DeadDbObjectScanner(Connection conn, String schemaName,
            Collection<DbObject> deadObjects, Collection<DbObject> liveObjects,
            String schemaPackage, String... otherPackageSpecs) throws IOException {
        this.conn = conn;
        this.schemaName = schemaName;
        this.schemaPackage = schemaPackage;
        List<String> packageSpecs = ImmutableList.<String>builder()
                .add("!" + schemaPackage)
                .addAll(Arrays.asList(otherPackageSpecs))
                .build();
        this.decompiler = new Decompiler(packageSpecs);
        scanForObjects();
        // implement forced dead and live results
        deadObjects.forEach(this::markDead);
        liveObjects.forEach(this::markLive);
        markLive(new TableObject("schema_version"));
    }

    /**
     * Analyze the loaded DB objects to identify which appear to be live (i.e. usages are
     * detected).
     */
    public void searchForLife() {
        searchForLiveEvents();
        searchForLiveRoutines();
        searchForLiveViews();
        searchForLiveTables();
    }

    private void searchForLiveEvents() {
        for (final EventObject event : events.values()) {
            if (!event.isDead()) {
                event.setLive();
            }
        }
    }


    private void searchForLiveRoutines() {
        final List<DbObject> allRoutines = Stream.of(functions.values(), procedures.values())
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
        searchForJooqUsages(allRoutines);
        searchForNonJooqUsages(allRoutines);
        //noinspection StatementWithEmptyBody
        while (searchForInSchemaUsages(allRoutines, events.values(), allRoutines)) {
        }
        killUndecided(allRoutines);
    }

    private void searchForLiveViews() {
        searchForJooqUsages(views.values());
        searchForNonJooqUsages(views.values());
        //noinspection StatementWithEmptyBody
        while (searchForInSchemaUsages(views.values(),
                events.values(), functions.values(), procedures.values(), views.values())) {
        }
        killUndecided(views.values());
    }

    private void searchForLiveTables() {
        searchForJooqUsages(tables.values());
        searchForNonJooqUsages(tables.values());
        searchForInSchemaUsages(tables.values(),
                events.values(), functions.values(), procedures.values(), views.values());
        killUndecided(tables.values());
    }

    private boolean searchForJooqUsages(Collection<? extends DbObject> dbos) {
        final List<? extends DbObject> todo =
                dbos.stream().filter(DbObject::isUndecided).collect(Collectors.toList());
        boolean anyMatch = false;
        for (final String cls : decompiler.discoveredClassNames()) {
            if (todo.isEmpty()) {
                break;
            }
            final String code = decompiler.decompile(cls);
            for (Iterator<? extends DbObject> iter = todo.iterator(); iter.hasNext(); ) {
                final DbObject dbo = iter.next();
                if (dbo.jooqNames(schemaPackage).stream().anyMatch(code::contains)) {
                    dbo.setLive();
                    iter.remove();
                    logger.debug("{} has jOOQ usage in {}; marked live", dbo, cls);
                    anyMatch = true;
                }
            }
        }
        return anyMatch;
    }

    private boolean searchForNonJooqUsages(Collection<? extends DbObject> dbos) {
        final List<? extends DbObject> todo =
                dbos.stream().filter(DbObject::isUndecided).collect(Collectors.toList());
        boolean anyMatch = false;
        for (final String cls : decompiler.discoveredClassNames()) {
            if (todo.isEmpty()) {
                break;
            }
            final String code = decompiler.decompile(cls).toLowerCase();
            for (Iterator<? extends DbObject> iter = todo.iterator(); iter.hasNext(); ) {
                final DbObject dbo = iter.next();
                if (dbo.nonJooqNames().stream().anyMatch(code::contains)) {
                    dbo.setLive();
                    iter.remove();
                    logger.debug("{} has non-jOOQ usage in {}; marked live", dbo, cls);
                    anyMatch = true;
                }
            }
        }
        return anyMatch;
    }

    @SafeVarargs
    private final boolean searchForInSchemaUsages(Collection<? extends DbObject> dbos,
            Collection<? extends DbObject>... searchDbos) {
        final List<? extends DbObject> todo =
                dbos.stream().filter(DbObject::isUndecided).collect(Collectors.toList());
        boolean anyMatch = false;
        for (final DbObject searchDbo : Iterables.concat(searchDbos)) {
            if (todo.isEmpty()) {
                break;
            }
            if (!searchDbo.isLive()) {
                // we can't count a usage from an object that hasn't yet been determined to
                // be live
                continue;
            }
            final String code = searchDbo.getDefinition().toLowerCase();
            for (Iterator<? extends DbObject> iter = todo.iterator(); iter.hasNext(); ) {
                final DbObject dbo = iter.next();
                if (dbo.nonJooqNames().stream().anyMatch(code::contains)) {
                    dbo.setLive();
                    iter.remove();
                    logger.debug("{} has in-schema usage in {}; marked live", dbo, searchDbo);
                    anyMatch = true;
                }
            }
        }
        return anyMatch;
    }

    private void killUndecided(Collection<? extends DbObject> dbos) {
        dbos.stream().filter(DbObject::isUndecided).forEach(DbObject::setDead);
    }

    public List<TableObject> getDeadTables() {
        return getDeadObjects(tables);
    }

    public List<ViewObject> getDeadViews() {
        return getDeadObjects(views);
    }

    public List<FunctionObject> getDeadFunctions() {
        return getDeadObjects(functions);
    }

    public List<ProcObject> getDeadProcs() {
        return getDeadObjects(procedures);
    }

    public List<EventObject> getDeadEvents() {
        return getDeadObjects(events);
    }

    private <T extends DbObject> List<T> getDeadObjects(Map<String, T> scannedObjects) {
        return scannedObjects.values().stream()
                .filter(DbObject::isDead)
                .sorted()
                .collect(Collectors.toList());
    }

    private void scanForObjects() {
        scanForTables();
        scanForViews();
        scanForFunctions();
        scanForProcs();
        scanForEvents();
    }

    private void scanForTables() {
        final String sql = "SELECT table_name AS name FROM information_schema.tables "
                + "WHERE table_schema = '" + schemaName + "' "
                + "AND table_type = 'BASE TABLE' "
                + "AND (temporary is NULL OR temporary = 'N')";
        DSL.using(conn).fetch(sql).stream()
                .map(r -> new TableObject(r.getValue("name", String.class)))
                .peek(t -> tables.put(t.getName(), t))
                .forEach(t -> logger.debug("Discovered {}", t));

    }

    private void scanForViews() {
        final String sql = "SELECT table_name AS name, view_definition as definition "
                + "FROM information_schema.views "
                + "WHERE table_schema = '" + schemaName + "' ";
        DSL.using(conn).resultQuery(sql).stream()
                .map(r -> new ViewObject(
                        r.getValue("name", String.class), r.getValue("definition", String.class)))
                .peek(v -> views.put(v.getName(), v))
                .forEach(v -> logger.debug("Discovered {}", v));
    }

    private void scanForFunctions() {
        final String sql = "SELECT routine_name AS name, routine_definition as definition "
                + "FROM information_schema.routines "
                + "WHERE routine_schema = '" + schemaName + "' "
                + "AND routine_type = 'FUNCTION'";
        DSL.using(conn).resultQuery(sql).stream()
                .map(r -> new FunctionObject(
                        r.getValue("name", String.class), r.getValue("definition", String.class)))
                .peek(f -> functions.put(f.getName(), f))
                .forEach(f -> logger.debug("Discovered {}", f));
    }

    private void scanForProcs() {
        final String sql = "SELECT routine_name AS name, routine_definition as definition "
                + "FROM information_schema.routines "
                + "WHERE routine_schema = '" + schemaName + "' "
                + "AND routine_type = 'PROCEDURE'";
        DSL.using(conn).resultQuery(sql).stream()
                .map(r -> new ProcObject(
                        r.getValue("name", String.class), r.getValue("definition", String.class)))
                .peek(p -> procedures.put(p.getName(), p))
                .forEach(p -> logger.debug("Discovered {}", p));
    }

    private void scanForEvents() {
        final String sql = "SELECT event_name AS name, event_definition as definition "
                + "FROM information_schema.events "
                + "WHERE event_schema = '" + schemaName + "'";
        DSL.using(conn).resultQuery(sql).stream()
                .map(r -> new EventObject(
                        r.getValue("name", String.class), r.getValue("definition", String.class)))
                .peek(e -> events.put(e.getName(), e))
                .forEach(e -> logger.info("Discovered {}", e));
    }

    private void markDead(DbObject dbo) {
        final DbObject found = findScannedDbObject(dbo);
        if (found != null) {
            found.setDead();
        } else {
            logger.warn("Cannot mark {} {} dead: not found in schema {}",
                    dbo.getType(), dbo.getName(), schemaName);
        }
    }

    private void markLive(DbObject dbo) {
        final DbObject found = findScannedDbObject(dbo);
        if (found != null) {
            found.setLive();
        } else {
            logger.warn("Cannot mark {} {} live: not found in schema {}",
                    dbo.getType(), dbo.getName(), schemaName);
        }
    }

    private <T extends DbObject> T findScannedDbObject(T dbo) {
        switch (dbo.getType()) {
            case TABLE:
                return findScannedDbObject(dbo, tables);
            case VIEW:
                return findScannedDbObject(dbo, views);
            case FUNCTION:
                return findScannedDbObject(dbo, functions);
            case PRODCEDURE:
                return findScannedDbObject(dbo, procedures);
            case EVENT:
                return findScannedDbObject(dbo, events);
            default:
                throw new IllegalArgumentException();
        }
    }

    private <T extends DbObject> T findScannedDbObject(T dbo, Map<String, ? extends
            DbObject> scanned) {
        @SuppressWarnings("unchecked")
        final T scannedDbo = (T)scanned.get(dbo.getName());
        return scannedDbo;
    }
}
