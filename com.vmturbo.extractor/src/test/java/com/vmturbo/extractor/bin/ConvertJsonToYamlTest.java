package com.vmturbo.extractor.bin;

import static com.google.common.io.Resources.getResource;
import static com.vmturbo.extractor.bin.ConvertJsonToYaml.jsonToYaml;
import static com.vmturbo.extractor.bin.ConvertJsonToYaml.untabify;
import static com.vmturbo.extractor.bin.ConvertJsonToYaml.yamlToJson;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.io.Resources;

import org.junit.Test;

/**
 * Tests of {@link ConvertJsonToYaml} class.
 */
public class ConvertJsonToYamlTest {

    /**
     * Tests that the untabify method works.
     */
    @Test
    public void testUnTabify() {
        // test no escape chars
        assertThat(untabify("abcdefghijklmnopqrstuvwxyz", 3), is("abcdefghijklmnopqrstuvwxyz"));
        // test correct number of spaces for tabs
        assertThat(untabify("\t", 8), is("        "));
        assertThat(untabify(" \t", 8), is("        "));
        assertThat(untabify("  \t", 8), is("        "));
        assertThat(untabify("   \t", 8), is("        "));
        assertThat(untabify("    \t", 8), is("        "));
        assertThat(untabify("     \t", 8), is("        "));
        assertThat(untabify("      \t", 8), is("        "));
        assertThat(untabify("       \t", 8), is("        "));
        // test with a width other than 8
        assertThat(untabify("\t", 3), is("   "));
        assertThat(untabify(" \t", 3), is("   "));
        assertThat(untabify("  \t", 3), is("   "));
        assertThat(untabify("   \t", 3), is("      "));
        // test multiple tabs in same line
        assertThat(untabify("   \t\t", 3), is("         "));
        assertThat(untabify("   \ta\t", 3), is("      a  "));
        assertThat(untabify("   \ta\tb", 3), is("      a  b"));
        // test embedded newlines
        assertThat(untabify("   \t\n\t", 3), is("      \n   "));
        assertThat(untabify("   \t\n\t\n", 3), is("      \n   \n"));
        // test tabs after non-blank text
        assertThat(untabify("a\t", 3), is("a  "));
        assertThat(untabify("ab\t", 3), is("ab "));
        assertThat(untabify("abc\t", 3), is("abc   "));
        // test quoted backslashes
        assertThat(untabify("\\", 3), is("\\"));
        assertThat(untabify("\\\t\t", 3), is("\\     "));
    }

    /**
     * Test that we can convert from JSON to correct YAML.
     *
     * @throws IOException if there's an issue
     */
    @Test
    public void testJsonToYaml() throws IOException {
        // basic types
        assertThat(jsonToYaml("1"), is("--- 1\n"));
        assertThat(jsonToYaml("\"a\""), is("--- a\n"));
        assertThat(jsonToYaml("null"), is("--- null\n"));
        assertThat(jsonToYaml("true"), is("--- true\n"));
        assertThat(jsonToYaml("false"), is("--- false\n"));
        // simple objects
        assertThat(jsonToYaml("{\"a\": 1}"), is("---\na: 1\n"));
        assertThat(jsonToYaml("{\"a\": \"b\"}"), is("---\na: b\n"));
        assertThat(jsonToYaml("{\"a\": null}"), is("---\na: null\n"));
        assertThat(jsonToYaml("{\"a\": true}"), is("---\na: true\n"));
        assertThat(jsonToYaml("{\"a\": false}"), is("---\na: false\n"));
        assertThat(jsonToYaml("{\"a\": {}}"), is("---\na: {}\n"));
        assertThat(jsonToYaml("{\"a\": []}"), is("---\na: []\n"));
        assertThat(jsonToYaml("{\"a\": \"null\"}"), is("---\na: \"null\"\n"));
        assertThat(jsonToYaml("{\"a\": \"true\"}"), is("---\na: \"true\"\n"));
        assertThat(jsonToYaml("{\"a\": \"false\"}"), is("---\na: \"false\"\n"));
        // simple arrays
        assertThat(jsonToYaml("[]"), is("--- []\n"));
        assertThat(jsonToYaml("[1, 2, 3]"), is("---\n- 1\n- 2\n- 3\n"));
        assertThat(jsonToYaml("[1, \"a\"]"), is("---\n- 1\n- a\n"));
        assertThat(jsonToYaml("[true, null, false]"), is("---\n- true\n- null\n- false\n"));
        // multi-line strings
        assertThat(jsonToYaml("\"a\\nb\\nc\""), is("--- |-\n  a\n  b\n  c\n"));
        assertThat(jsonToYaml("\"a\\n b\\n  c\""), is("--- |-\n  a\n   b\n    c\n"));
        assertThat(jsonToYaml("\"a\\n\\tb\\n\\t\\tc\""),
                is("--- |-\n  a\n          b\n                  c\n"));
        // multi-line strings with terminal newline - use "|" instead of "|-" in block literal
        assertThat(jsonToYaml("\"a\\nb\\nc\\n\""), is("--- |\n  a\n  b\n  c\n"));
        assertThat(jsonToYaml("\"a\\n b\\n  c\\n\""), is("--- |\n  a\n   b\n    c\n"));
        assertThat(jsonToYaml("\"a\\n\\tb\\n\\t\\tc\\n\""),
                is("--- |\n  a\n          b\n                  c\n"));
        // multi-line strings with trailing spaces
        assertThat(jsonToYaml("\"a \\n\\tb \\t \\n\\t\\tc \""),
                is("--- |-\n  a\n          b\n                  c\n"));
        assertThat(jsonToYaml("\"a x\\n\\tb x\\n\\t\\tc x\""),
                is("--- |-\n  a x\n          b x\n                  c x\n"));
        // complex structures
        assertThat(jsonToYaml("{\"a\": {\"x\": [1,2,3], \"y\": \"hello \\n  \\tgood-bye\\t \\n\"}, \"b\": true}"),
                is("---\na:\n  x:\n  - 1\n  - 2\n  - 3\n  y: |\n    hello\n            good-bye\nb: true\n"));
        assertThat(jsonToYaml("[true, {\"x\": 1, \"y\": {\"a\": null}}, 1234567890]"),
                is("---\n- true\n- x: 1\n  y:\n    a: null\n- 1234567890\n"));
        // an actual dashboard
        String json = Resources.toString(getResource(getClass(), "/sample-docs/sample.json"), Charsets.UTF_8);
        String yaml = Resources.toString(getResource(getClass(), "/sample-docs/sample.yaml"), Charsets.UTF_8);
        assertThat(jsonToYaml(json), is(yaml));
    }

    /**
     * Test that we can convert from YAML to JSON.
     *
     * @throws IOException if there's a problem
     */
    @Test
    public void testYamlToJson() throws IOException {
        // sample file is pretty-printed, but we don't bother doing that with yaml2Json, so we need
        // to un-prettyprint what we read in
        String json = Resources.toString(getResource(getClass(), "/sample-docs/sample.json"), Charsets.UTF_8);
        ObjectMapper mapper = new ObjectMapper();
        json = mapper.writeValueAsString(mapper.readTree(json));
        String yaml = Resources.toString(getResource(getClass(), "/sample-docs/sample.yaml"), Charsets.UTF_8);
        assertThat(yamlToJson(yaml), is(json));

    }
}
