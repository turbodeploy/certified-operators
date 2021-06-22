package com.vmturbo.extractor.bin;

import java.io.File;
import java.io.IOException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator.Feature;
import com.google.common.base.Charsets;
import com.google.common.collect.Streams;
import com.google.common.io.Files;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * File to convert JSON files representing Grafana dashboards to YAML.
 *
 * <p>This conversion slightly complicates the task of incorporating a new dashboard, or edits
 * made to a dashboard from within the Grafana UI, since this program must be run afterward to
 * convert any exported JSON files to YAML. The justification is that we can use YAML's multi-line
 * block literals so that nicely formatted complex SQL statements become far easier to understand in
 * code reviews.</p>
 */
public class ConvertJsonToYaml {
    private static final Logger logger = LogManager.getLogger();

    private ConvertJsonToYaml() {
    }


    private static final String[] DIRS_TO_SCAN = new String[]{"dashboards/general"};

    private static final ObjectMapper jsonMapper = new ObjectMapper();
    private static final ObjectMapper yamlMapper = createYamlMapper();

    /**
     * Scan directories and convert any JSON files with equivalent YAML files.
     *
     * <p>Directories to be scanned are according to the paths in DIRS_TO_SCAN, relative to a
     * root directory. That root directory can be specified on the command line, but if it is
     * omitted, an attempt will be made to infer the correct root directory based on the location of
     * this class' resources.</p>
     *
     * <p>The auto-discovery of root directory works well when the program is executed in Intellij
     * with the classpath to that of the extractor module.</p>
     *
     * @param args optional 1st arg supplies root directory for scans, else the class will attempt
     *             to auto-detect the correct root
     * @throws IOException if there's a problem
     */
    public static void main(String[] args) throws IOException {
        final File rootDir;
        if (args.length > 0) {
            rootDir = new File(args[0]);
        } else {
            File classDir = new File(ConvertJsonToYaml.class.getResource("/").getFile());
            rootDir = new File(classDir, "../../src/main/resources");
        }
        if (rootDir.isDirectory()) {
            for (final String dir : DIRS_TO_SCAN) {
                scanAndConvert(new File(rootDir, dir));
            }
        }
    }

    private static ObjectMapper createYamlMapper() {
        final YAMLFactory factory = new YAMLFactory();
        factory.enable(Feature.MINIMIZE_QUOTES);
        factory.enable(Feature.LITERAL_BLOCK_STYLE);
        return new ObjectMapper(factory);
    }

    private static void scanAndConvert(final File dir) throws IOException {
        if (dir.isDirectory()) {
            logger.info("Scanning directory {}", dir);
            final String[] files = dir.list();
            for (final String file : files != null ? files : new String[0]) {
                if (Files.getFileExtension(file).equalsIgnoreCase("json")) {
                    convertFile(new File(dir, file).getCanonicalFile());
                }
            }
        } else {
            logger.error("Cannot scan dir {}: not a directory", dir);
        }
    }

    private static void convertFile(final File in) throws IOException {
        if (in.canRead()) {
            File out = new File(in.getCanonicalPath().replaceAll("(?i)\\.json$", ".yaml"));
            String json = FileUtils.readFileToString(in, Charsets.UTF_8);
            String yaml = jsonToYaml(json);
            FileUtils.write(out, yaml, Charsets.UTF_8);
            logger.info("Converted file {} to {}", in, out);
        } else {
            logger.error("Cannot convert file {}; file not readable", in);
        }
    }

    /**
     * Convert a JSON string to an equivalent YAML string, taking extra precautions to ensure that
     * multi-line strings are rendered using block literals.
     *
     * @param json the JSON string
     * @return the YAML string
     * @throws JsonProcessingException if the input is invalid JSON
     */
    public static String jsonToYaml(final String json) throws JsonProcessingException {
        // whitespace at ends of lines makes it impossible to use YAML's block literal,
        // so we get rid of it here. Tabs within indentation are also poisonous, so we
        // convert them to spaces.
        JsonNode tree = jsonMapper.readTree(json);
        tree = cleanStrings(tree);
        return yamlMapper.writeValueAsString(tree);
    }

    private static JsonNode cleanStrings(JsonNode tree) {
        if (tree.isTextual()) {
            String text = tree.asText();
            text = untabify(text.replaceAll("(?m)[ \\t]+$", ""), 8);
            return JsonNodeFactory.instance.textNode(text);
        } else if (tree.isArray()) {
            return Streams.stream(tree.elements())
                    .map(ConvertJsonToYaml::cleanStrings)
                    .collect(JsonNodeFactory.instance::arrayNode, ArrayNode::add, ArrayNode::addAll);
        } else if (tree.isObject()) {
            Streams.stream(tree.fields()).forEach(e -> e.setValue(cleanStrings(e.getValue())));
            return tree;
        } else {
            return tree;
        }
    }

    /**
     * Convert a YAML string to an equivalent JSON string.
     *
     * @param yaml the YAML string
     * @return the JSON string
     * @throws JsonProcessingException if the input is not valid YAML
     */
    public static String yamlToJson(final String yaml) throws JsonProcessingException {
        final JsonNode tree = yamlMapper.readTree(yaml);
        return jsonMapper.writeValueAsString(tree);
    }

    /**
     * Replace tabs with equivalent spaces in the given multi-line string.
     *
     * @param s        String to be untabified
     * @param tabWidth # of chrarcters between tab positions
     * @return untabified string
     */
    static String untabify(String s, int tabWidth) {
        StringBuilder sb = new StringBuilder();
        int pos = 0;
        for (int i = 0; i < s.length(); i++) {
            final char c = s.charAt(i);
            if (c == '\t') {
                do {
                    sb.append(' ');
                } while (++pos % tabWidth != 0);
            } else if (c == '\n') {
                sb.append(c);
                pos = 0;
            } else {
                sb.append(c);
                pos += 1;
            }
        }
        return sb.toString();
    }
}
