import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.nio.file.Files;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.function.BiPredicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.CRC32;

import com.google.common.base.Charsets;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

/**
 * This is a one-off program created to perform clean-up of the very large V1.0 migration of history
 * component.
 *
 * <p>That migration file was essentially pasted from mysqldump output, apparently when the classic
 * opsmgr reports database was ported to an early XL prototype, and it has lived on since then. That
 * had to change when it was discovered that there was code in that migration that used - albeit
 * transiently - the MyISAM database engine. This was preventing XL from being deployed on Azure
 * using Azure DB Service, since that service disallowed use of MyISAM.</p>
 *
 * <p>Since the response (see https://vmturbo.atlassian.net/browse/OM-55595) was going to require
 * modification of this (and one other) migration file, it was decided that this would be a good
 * time to try to clean up the migration in general, dropping a large amount of pointless
 * boilerplate, backtick-quoting of all identifiers, executable, comments, etc. The result is
 * far ore readable.</p>
 *
 * <p>A small number of general alterations were made, though these will have no effect on existing
 * installations (since they will not re-run this migration):</p>
 * <ul>
 *     <li>Character set utf8mb4 is set as the default for everything.</li>
 *     <li>Collation utfmb4_unicode_ci is likewise set as the default.</li>
 *     <li>SQL_MODE is set to what we now use as a default value.</li>
 *     <li>Local overrides for these and other settings are no longer used.</li>
 * </ul>
 *
 * <p>This program operates by reading the existing V1.0 migration (and all the rest of them as
 * well) and performing a few basic transformations. If these result in any significant change,
 * additional transformations are applied. Otherwise the migration is left as-is. This process
 * ended up changing the V1.0, V1.10, and V1.16 migrations. The latter two were then hand-edited,
 * as they were quite small. The V1.0 migration transformation is fully automated.</p>
 *
 * <p>While this program was created as a one-off utility, it is left here (though not included
 * in the history component jar file or image) in case the techniques or code might find use in
 * some future scneario.</p>
 */
public class StripSqlExecutableComments {

    private StripSqlExecutableComments() {
    }

    // match executable comments (like /*!40101 ... */) with group 1 left containing the
    // interior of the executable comment
    private static Pattern executableCommentPat = Pattern.compile(
            "/\\*!\\d{5}(((?!\\*/).)*)\\*/",
            Pattern.MULTILINE + Pattern.DOTALL);
    // match the beginning of a trigger definition, so we can insert a comment prior to the
    // definition
    private static Pattern triggerPat = Pattern.compile(
            "^\\s*DELIMITER\\s*;;\\s*CREATE\\s*TRIGGER.*?INSERT ON\\s+(\\w+)\\s*$",
            Pattern.MULTILINE + Pattern.DOTALL);
    // match the beginning of an event definition so we can insert a comment prior to the definition
    private static Pattern eventPat = Pattern.compile(
            "^(\\s*DROP\\s+EVENT.*?(\\w+)\\s*;?;)\\s*$",
            Pattern.MULTILINE);
    // match the beginning of a proc definition so we can insert a comment prior to the definition
    private static Pattern procPat = Pattern.compile(
            "^(\\s*DROP\\s+PROCEDURE.*?(\\w+)\\s*;)\\s*$",
            Pattern.MULTILINE);
    // match the beginning of a function definition so we can insert a comment prior to he definition
    private static Pattern funcPat = Pattern.compile(
            "^(\\s*DROP\\s+FUNCTION.*?(\\w+)\\s*;)\\s*$",
            Pattern.MULTILINE);

    // match the end of a definition with ";;" used as delimiters, so we can ensure there's a
    // following statement to reset the delimiter to ";"
    private static Pattern endDblDelim = Pattern.compile(
            "^\\s*END\\s*;;\\s*$",
            Pattern.MULTILINE);
    // remove duplicated "DELIMITER ;" statement that results from adding them when processing
    // above pattern, since sometimes they were already there
    private static Pattern multiDelim = Pattern.compile(
            "^(\\s*DELIMITER\\s+;\\s*\n)+",
            Pattern.MULTILINE);

    /**
     * Transform migration files in a given directory.
     *
     * <p>Any modified migration file is rewritten in-place. Ensure current code is checked in
     * prior to running this program.</p>
     *
     * @param args path to migration directory
     * @throws IOException if there's a problem reading or writing a migration file
     */
    public static void main(String[] args) throws IOException {

        File migrationDir = new File(args[0]);
        for (File file : migrationDir.listFiles()) {
            if (file.getName().matches("V\\d+(\\.\\d+)*__.*\\.(?i:sql)")) {
                processFile(file);
            }
        }
    }

    /**
     * Process a single migration file and, if it is changed as a result, overwrite it with its
     * new content.
     *
     * @param file migration file to process
     * @throws IOException if there's a problem reading or writing the file
     */
    private static void processFile(final File file) throws IOException {
        String migration = new String(Files.readAllBytes(file.toPath()), Charsets.UTF_8);
        // remove executable comments, leaving behind the enclosed body
        String stripped = replaceAll(migration, executableCommentPat, "$1");
        // and remove all mention of MyISAM engine (the main driver of this cleanup
        stripped = stripped.replaceAll("\\s*ENGINE\\s*=\\s*MyISAM\\s*", "");

        if (!stripped.equals(migration)) {
            // only do additional modifications if main items above were changed, to
            // reduce the number of migrations we need to hack
            stripped = stripped
                    // get rid of unneeded identifier quoting
                    .replaceAll("`", "")
                    // except we do have a column named "column" that needs quoting!
                    .replaceAll("column\\s+tinyint", "`column` tinyint")
                    // InnnoDB is default, so we don't need to specify it
                    .replaceAll("ENGINE\\s*=\\s*InnoDB\\s*", "")
                    // we're changing default settings for charset and collation so we don't
                    // need these throughout the schema definition
                    .replaceAll("(DEFAULT\\s+)?CHARSET=utf8\\s*", "")
                    .replaceAll("(DEFAULT\\s+)?COLLATE=utf8_(unicode|general)_ci\\s*", "")
                    // and this is the default, so we don't need to specify it
                    .replaceAll("DEFINER\\s*=\\s*CURRENT_USER\\s*", "")
                    // mysqldump version used in creating some of our migrations created
                    // tables for all views prior to later creating the views, so that forward
                    // references to other view definitions wouldn't break. We handle it differently
                    // and have no need for these.
                    .replaceAll("(?m)(?s)^\\s*--\\s+Temporary table.*?^\\s*\\);\\s*$", "");

            // now apply a bunch of alterations to individual lines of the schema definition,
            // mostly a bunch of unneeded boilerplate that saves and restores various settings
            // before and after various types of object definitions.
            List<String> lines = Stream.of(stripped.split("(?m)^"))
                    .map(l -> l.replaceAll("^\\s*SET\\s+@OLD_.*,\\s*", "SET "))
                    .filter(l -> !hasPrefix(l, "SET\\s+SQL_MODE\\s*="))
                    .filter(l -> !hasPrefix(l, "SET\\s+TIME_ZONE\\s*="))
                    .filter(l -> !hasPrefix(l, "SET\\s+NAMES\\s+"))
                    .filter(l -> !hasPrefix(l, "SET\\s+@OLD_"))
                    .filter(l -> !hasPrefix(l, "SET.*=\\s*@OLD_"))
                    .filter(l -> !hasPrefix(l, "SET\\s+\\@saved?_"))
                    .filter(l -> !hasPrefix(l, "SET.*=\\s+@saved?_"))
                    .filter(l -> !hasPrefix(l, "SET\\s+character_set_client"))
                    .filter(l -> !hasPrefix(l, "SET\\s+character_set_results"))
                    .filter(l -> !hasPrefix(l, "SET\\s+collation_connection"))
                    .filter(l -> !hasPrefix(l, "SET\\s+sql_mode"))
                    // we don't need these optimizations when creating table data - this is
                    // always happening at a time when no other accesss to the database is
                    // possible
                    .filter(l -> !hasPrefix(l, "(UN)?LOCK\\s+TABLES"))
                    .filter(l -> !hasPrefix(l, "ALTER\\s+TABLE.*(DIS|EN)ABLE\\s+KEYS"))
                    // get rid of some useless comments since we'll use comments later to split
                    // apart individual object definitions
                    .filter(l -> !l.matches("^\\s*(--)?\\s*$"))
                    .filter(l -> !hasPrefix(l, "--\\s*Dumping events"))
                    .filter(l -> !hasPrefix(l, "--\\s*Dumping routines"))
                    // these are sprinkled through the original schema definition, with inconsistent
                    // case
                    .filter(l -> !hasPrefix(l, "(?i)set\\s+time_zone.*system"))
                    .collect(Collectors.toList());

            // add some global settings at the top of the file and get back to a single
            // multiline string
            Stream.of(
                    "SET NAMES utf8mb4;\n",
                    "SET TIME_ZONE='+00:00';",
                    "SET CHARACTER_SET_CLIENT = utf8mb4;",
                    "SET COLLATION_CONNECTION = utf8mb4_unicode_ci;",
                    "SET SQL_MODE='IGNORE_SPACE,STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION';"
            ).forEach(line -> lines.add(0, line + "\n"));
            stripped = lines.stream().collect(Collectors.joining());

            // place marker comments in front of some object definitions that don't already
            // have them
            stripped = replaceAll(stripped, triggerPat, "-- Trigger for $1\n$0");
            stripped = replaceAll(stripped, eventPat, "-- Event $2\n$1");
            stripped = replaceAll(stripped, procPat, "-- Proc $2\n$1");
            stripped = replaceAll(stripped, funcPat, "-- Func $2\n$1");
            stripped = replaceAll(stripped, endDblDelim, "$0\nDELIMITER ;");
            stripped = replaceAll(stripped, multiDelim, "DELIMITER ;\n");

            // now chop up the schema into individual object definitions and spit them back out
            // in an order that will avoid all backward references (so we don't need the dummy
            // table definition hack for views that mysqldump resorts to)
            stripped = new Definitions(stripped.split("\n")).dump();

            // as a convenience, log the old and new checksums for the altered files
            System.out.printf("File %s changed: orig = %s stripped = %s\n", file,
                    calculateChecksum(migration), calculateChecksum(stripped));
            Files.write(file.toPath(), stripped.getBytes(Charsets.UTF_8));
        }
    }

    /**
     * Encasulates the {@link Pattern} class's appendReplacement/appendTail process for performing
     * repeated replacements on a subject string.
     *
     * @param s   the string on which to perform replacements
     * @param pat pattern defining substrings to be replaced
     * @param rep replacement string, including match-group identifiers if needed (see
     *            {@link Matcher#appendReplacement(StringBuffer, String)} for details)
     * @return the resulting string
     */
    private static String replaceAll(final String s, Pattern pat, String rep) {
        final Matcher m = pat.matcher(s);
        final StringBuffer sb = new StringBuffer();
        while (m.find()) {
            m.appendReplacement(sb, rep);
        }
        m.appendTail(sb);
        return sb.toString();
    }

    /**
     * Test whether a line (possibly with a line terminator) starts with text that matches the
     * given pattern (possibly after some whitespace).
     *
     * <p>The DOTALL option is included so that dot will match any line terminator present.</p>
     *
     * @param line  line to be tested
     * @param regex pattern to check
     * @return true if the pattern matches the beginnning of the line, not counting initial whitespace
     */
    private static boolean hasPrefix(String line, String regex) {
        return line.matches("(?s)^\\s*" + regex + ".*$");
    }

    /**
     * Calculates the checksum of this string.
     *
     * <p>This code is lifted from a Flyway class that's part of their standard distribution.</p>
     *
     * @param str The string to calculate the checksum for.
     * @return The crc-32 checksum of the bytes.
     */
    /* private -> for testing */
    static int calculateChecksum(String str) {
        final CRC32 crc32 = new CRC32();

        BufferedReader bufferedReader = new BufferedReader(new StringReader(str));
        try {
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                crc32.update(line.getBytes("UTF-8"));
            }
        } catch (IOException e) {
            String message = "Unable to calculate checksum";
        }

        return (int)crc32.getValue();
    }

    /**
     * Class to manage a set of DB object definitions (tables, views, procs, etc.).
     */
    private static class Definitions {

        private Map<DefinitionType, List<Definition>> defs = new HashMap<>();
        // we start with an active "Start" definition, which we'll fill until a line matches
        // the start pattern for some other object. This way we collect any "front matter" in
        // the migration file.
        private Definition currentDef = new Definition(DefinitionType.Start, null);

        /**
         * Create a new instance, with lines constituting a (largely cleaned up) SQL file obtained
         * from a migration being transformed.
         *
         * <p>This class uses patterns to identify individaul object definitions and collects them
         * into lists organized by object type. It can then be used to dump the collected objects
         * for output, ordered so as to avoid forward references.</p>
         *
         * @param lines lines from a migration file
         */
        Definitions(String[] lines) {
            for (String line : lines) {
                // does this line start a new object?
                final Optional<Definition> def = DefinitionType.match(line);
                if (def.isPresent()) {
                    // Yes... tie off the current one
                    saveCurrent();
                    // and start collecting the new one.
                    currentDef = def.get();
                    currentDef.setFirstLine(line);
                } else {
                    // no, add this line to the current object
                    currentDef.append(line);
                }
            }
            saveCurrent();
        }

        /**
         * Save the currently accumlating definition object in our map structure.
         */
        private void saveCurrent() {
            currentDef.done();
            final DefinitionType type = currentDef.getType();
            defs.computeIfAbsent(type, t -> new ArrayList<>());
            defs.get(type).add(currentDef);
        }

        /**
         * Dump all the accumulated definitions in an order that will not involve any forward
         * references.
         *
         * @return the concatenation of all the object definitions
         */
        String dump() {
            StringBuffer sb = new StringBuffer();
            // Process the object types in an order that will not require inter-type forward
            // referencesx
            for (DefinitionType type : Arrays.asList(
                    DefinitionType.Start,
                    DefinitionType.Table,
                    DefinitionType.Trigger,
                    DefinitionType.Data,
                    DefinitionType.Func,
                    DefinitionType.View,
                    DefinitionType.Proc,
                    DefinitionType.Event,
                    DefinitionType.End)) {
                defs.computeIfAbsent(type, t -> new ArrayList<>());
                List<Definition> defList = defs.get(type);
                System.out.printf("Dumping %d definitions of type %s\n", defList.size(), type);
                if (defList.size() > 1) {
                    // topologically sort all definitions of the current type, to prevent forward
                    // references.
                    defList = topSort(defList);
                }
                // now output all the definitions of this type
                defList.forEach(d -> {
                    System.out.printf("  %s %s\n", type, d.getName());
                    sb.append("\n");
                    final String body = d.getBody();
                    // skip definitions that are empty
                    if (!body.isEmpty()) {
                        final String firstLine = d.getFirstLine();
                        if (firstLine != null) {
                            sb.append("--\n" + firstLine + "\n--\n");
                        }
                        sb.append(body);
                    }
                });
            }
            return sb.toString();
        }

        /**
         * Topologically sort a list of definitions of a given type.
         *
         * <p>The definitions are formulated as a graph, where each definition is a node, and
         * there's an edge from definition n1 to definition n2 if n2's body references n1's name.
         * In the sorted result, this will ensure that n1 appears before n2, so n2's reference will
         * be a backward reference. The name appearing in n2's body must be in the context of word
         * boundaries at both ends, so e.g. "foobar" will not produce a false match for "foo".</p>
         *
         * @param defList list of definitions to be sorted
         * @return list of definitions in topologically-sorted order
         */
        private List<Definition> topSort(List<Definition> defList) {
            defList.sort(Comparator.comparing(StripSqlExecutableComments.Definition::getName));
            Graph<Definition> graph = new Graph<Definition>(defList,
                    (a, b) -> b.getBody().matches("(?ms).*\\b" + a.getName() + "\\b.*"));
            return graph.topSort();
        }
    }

    /**
     * Utility class to create a simple graph structure for nodes of a given type.
     *
     * @param <T> node type
     */
    static class Graph<T> {
        // the nodes of the graph
        final List<T> nodes = new ArrayList<>();
        // edges, represented as lists of successors for all nodes
        final Multimap<T, T> successors = HashMultimap.<T, T>create();
        // # of unprocessed in-edges for each node, adjusted as the graph is processed
        final Map<T, Integer> inDegrees = new HashMap<>();

        /**
         * Create a new graph instance.
         *
         * @param nodes    the graph nodes
         * @param precedes a predicate that, given two nodes, determines whether an edge exists
         *                 from the first to the second
         */
        Graph(List<T> nodes, BiPredicate<T, T> precedes) {
            // save all our nodes and compute successors and initial in-degrees
            this.nodes.addAll(nodes);
            nodes.forEach(n -> inDegrees.put(n, 0));
            for (int i = 0; i < nodes.size(); i++) {
                for (int j = i + 1; j < nodes.size(); j++) {
                    final T n1 = nodes.get(i);
                    final T n2 = nodes.get(j);
                    if (precedes.test(n1, n2)) {
                        successors.put(n1, n2);
                        inDegrees.compute(n2, (n, deg) -> deg + 1);
                    }
                    if (precedes.test(n2, n1)) {
                        successors.put(n2, n1);
                        inDegrees.compute(n1, (n, deg) -> deg + 1);
                    }
                }
            }
        }

        /**
         * Topologically sort the graph.
         *
         * <p>This is an implementation of Kahn's algorithm for topological sort.</p>
         *
         * <p>If there are any cycles, unplaced nodes are added at the end of the result
         * list. This may or may not be lead to successful processing of the resulting
         * migration file by Flyway.</p>
         *
         * @return the graph nodes, in top-sorted order
         */
        List<T> topSort() {
            Queue<T> toVisit = new ArrayDeque<>();
            nodes.stream().filter(n -> inDegrees.get(n) == 0).forEach(toVisit::add);
            System.out.printf("Initial toVisit(%d): %s\n", toVisit.size(), toVisit);
            List<T> result = new ArrayList<>();
            // keep adding nodes with no visited in-edges until there are no more
            while (!toVisit.isEmpty()) {
                T node = toVisit.remove();
                System.out.printf("Visting: %s\n", node);
                result.add(node);
                for (T succ : successors.get(node)) {
                    if (inDegrees.compute(succ, (s, i) -> i - 1) == 0) {
                        toVisit.add(succ);
                        System.out.printf("Added to toVisit: %s\n", succ);
                    }
                }
            }
            // there must have been cycles... append unplaced nodes and issue a warning
            if (result.size() < nodes.size()) {
                List<T> unsorted = new ArrayList<>();
                unsorted.addAll(nodes);
                unsorted.removeAll(result);
                System.out.printf("%d unsorted nodes: %s", unsorted.size(), unsorted);
                result.addAll(unsorted);
            }
            return result;
        }
    }

    /**
     * Definition types, with patterns that will be used to detect their first lines in the
     * migration file.
     */
    private enum DefinitionType {
        Start(null),
        Table("^-- Table structure for table (\\w+).*"),
        Data("^-- Dumping data for table (\\w+).*"),
        Trigger("^-- Trigger for (\\w+).*"),
        View("^-- Final view structure for view (\\w+).*"),
        Func("^-- Func (\\w+).*"),
        Proc("^-- Proc (\\w+).*"),
        Event("^-- Event (\\w+).*"),
        End("^-- Dump completed on.*");


        private final Pattern pat;

        DefinitionType(String pat) {
            this.pat = pat != null ? Pattern.compile(pat) : null;
        }

        /**
         * Check whehter the given line matches any of the first-line patterns for our
         * definition types.
         *
         * @param line line to be checked
         * @return matching definition type, if any
         */
        static Optional<Definition> match(String line) {
            for (DefinitionType type : values()) {
                if (type.pat != null) {
                    final Matcher m = type.pat.matcher(line);
                    if (m.matches()) {
                        final String name = m.groupCount() > 0 ? m.group(1) : null;
                        return Optional.of(new Definition(type, name));
                    }
                }
            }
            return Optional.empty();
        }
    }

    /**
     * A database object definition.
     */
    private static class Definition {

        private final String name;
        private final DefinitionType type;
        private StringBuffer bodyBuf = new StringBuffer();
        private String body = null;
        private String firstLine;

        /**
         * Create a new instance of a given type and with a given name.
         *
         * @param type object type
         * @param name object name
         */
        Definition(DefinitionType type, String name) {
            this.type = type;
            this.name = name;
        }

        /**
         * Save the first line of this object - the one that matched this object's pattern.
         *
         * <p>This is saved separately so we can detect objects that have empty bodies (the
         * first line does not count.</p>
         *
         * @param firstLine this definition's first line
         */
        void setFirstLine(String firstLine) {
            this.firstLine = firstLine;
        }

        /**
         * Add a line to this definition's body.
         *
         * @param s line to add
         */
        void append(String s) {
            bodyBuf.append(s + "\n");
        }

        String getName() {
            return name;
        }

        DefinitionType getType() {
            return type;
        }

        String getFirstLine() {
            return firstLine;
        }

        String getBody() {
            return Objects.requireNonNull(body);
        }

        /**
         * Finish off this definition.
         */
        void done() {
            this.body = bodyBuf.toString();
            this.bodyBuf = null;
        }

        @Override
        public String toString() {
            return String.format("%s:%s", type, name);
        }
    }
}
