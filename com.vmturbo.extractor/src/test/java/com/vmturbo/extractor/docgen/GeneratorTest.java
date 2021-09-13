package com.vmturbo.extractor.docgen;

import static com.github.npathai.hamcrestopt.OptionalMatchers.isEmpty;
import static com.github.npathai.hamcrestopt.OptionalMatchers.isPresentAnd;
import static com.vmturbo.extractor.docgen.DocGenUtils.JSON_TYPE;
import static com.vmturbo.extractor.schema.Tables.ENTITY;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.MissingNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.vmturbo.extractor.docgen.DocGen.CmdLine;
import com.vmturbo.extractor.docgen.sections.TableSection;
import com.vmturbo.extractor.schema.json.common.Constants;

/**
 * Tests for the {@link Generator} class.
 */
public class GeneratorTest {

    private final JsonNodeFactory jsonFac = JsonNodeFactory.instance;

    /**
     * We use a temp directory for doc-tree and generated doc files.
     */
    @Rule
    public TemporaryFolder tmp = new TemporaryFolder();

    /**
     * Test that all sections are generated as expected.
     *
     * @throws IOException if there's a problem
     */
    @Test
    public void testGeneratesAllSections() throws IOException {
        final File docTreeFile = createDocTree("/tables/entity/intro", "intro-text");
        genDocs(docTreeFile);
        JsonNode rewrite = new YAMLMapper().readTree(docTreeFile);
        // verify enums section
        assertThat(rewrite.at("/enums/action_category/intro"), isA(NullNode.class));
        assertThat(rewrite.at("/enums/action_category/items/PERFORMANCE_ASSURANCE/Notes"), isA(NullNode.class));
        assertThat(rewrite.at("/enums/AttachmentState/items/ATTACHED/Notes"), isA(NullNode.class));

        // verify tables section
        assertThat(rewrite.at("/tables/entity/intro").asText(), is("intro-text"));
        assertThat(rewrite.at("/tables/entity/columns/oid/Description"), isA(NullNode.class));
        assertThat(rewrite.at("/tables/entity/columns/type/Description"), isA(NullNode.class));
        assertThat(rewrite.at("/tables/entity/columns/type/Type").asText(), is(DocGenUtils.ENUM_TYPE));
        assertThat(rewrite.at("/tables/entity/columns/type/Reference").asText(), is("/enums/entity_type"));
        assertThat(rewrite.at("/tables/entity/columns/name/Description"), isA(NullNode.class));
        assertThat(rewrite.at("/tables/entity/columns/environment/Description"), isA(NullNode.class));
        assertThat(rewrite.at("/tables/entity/columns/attrs/Description"), isA(NullNode.class));
        assertThat(rewrite.at("/tables/entity/columns/attrs/Reference").asText(), is("/shared/entity.attrs|/shared/group.attrs"));
        assertThat(rewrite.at("/tables/entity/columns/first_seen/Description"), isA(NullNode.class));
        assertThat(rewrite.at("/tables/entity/columns/last_seen/Description"), isA(NullNode.class));

        // verify table_data section
        assertThat(rewrite.at("/table_data/action.attrs/fields/moveInfo/Description"), isA(NullNode.class));
        assertThat(rewrite.at("/table_data/action.attrs/fields/moveInfo/Type").asText(), is(JSON_TYPE));
        assertThat(rewrite.at("/table_data/action.attrs/fields/moveInfo/Reference").asText(), is("/shared/MoveChange"));
        assertThat(rewrite.at("/table_data/action.attrs/fields/moveInfo/MapKeyType").asText(), is("/enums/entity_type"));
        assertThat(rewrite.at("/table_data/action.attrs/fields/buyRiInfo/MapKeyType"), isA(MissingNode.class));

        // verify shared section
        assertThat(rewrite.at("/shared/entity.attrs/fields/guest_os_type/Type").asText(), is("String"));
        assertThat(rewrite.at("/shared/entity.attrs/fields/guest_os_type/Reference").asText(), is("/enums/OSType"));
        assertThat(rewrite.at("/shared/entity.attrs/fields/guest_os_type/SupportedEntityTypes").asText(), is("VIRTUAL_MACHINE"));
        assertThat(rewrite.at("/shared/entity.attrs/fields/tags/SupportedEntityTypes").asText(), is("ALL"));
        assertThat(rewrite.at("/shared/entity.attrs/fields/targets/Repeated").asText(), is("true"));
        assertThat(rewrite.at("/shared/Target/intro"), isA(NullNode.class));

        assertThat(rewrite.at("/shared/MoveChange/fields/from/Type").asText(), is(JSON_TYPE));
        assertThat(rewrite.at("/shared/MoveChange/fields/from/Reference").asText(), is("/shared/ActionImpactedEntity"));

        // verify exporter section
        assertThat(rewrite.at("/exporter/intro"), isA(NullNode.class));
        assertThat(rewrite.at("/exporter/fields/timestamp/Format").asText(), is(Constants.TIMESTAMP_PATTERN));
        assertThat(rewrite.at("/exporter/fields/entity/Type").asText(), is(JSON_TYPE));
        assertThat(rewrite.at("/exporter/fields/entity/Reference").asText(), is("/exporter/Entity"));
        assertThat(rewrite.at("/exporter/Entity/intro"), isA(NullNode.class));
        assertThat(rewrite.at("/exporter/Entity/fields/state/Type").asText(), is("String"));
        assertThat(rewrite.at("/exporter/Entity/fields/state/Reference").asText(), is("/enums/entity_state"));
        assertThat(rewrite.at("/exporter/Entity/fields/related/Type").asText(), is(JSON_TYPE));
        assertThat(rewrite.at("/exporter/Entity/fields/related/Reference").asText(), is("/exporter/RelatedEntity"));
        assertThat(rewrite.at("/exporter/Entity/fields/related/MapKeyType").asText(), is("/enums/entity_type"));
        assertThat(rewrite.at("/exporter/Entity/fields/related/Repeated").asText(), is("true"));
        assertThat(rewrite.at("/exporter/CostByCategory/fields/source/Type").asText(), is("Float"));
        assertThat(rewrite.at("/exporter/CostByCategory/fields/source/MapKeyType").asText(), is("/enums/cost_source"));
        assertThat(rewrite.at("/exporter/CostByCategory/fields/total/Type").asText(), is("Float"));
        assertThat(rewrite.at("/exporter/Action/intro"), isA(NullNode.class));
        assertThat(rewrite.at("/exporter/Group/intro"), isA(NullNode.class));
    }

    /**
     * Test that prefilled fields in the yaml file are not overwritten if the generator was run
     * again.
     *
     * @throws IOException if there's a problem
     */
    @Test
    public void testNotOverwritingPrefilledFields() throws IOException {
        final File docTreeFile = createDocTree(
                "/enums/action_category/intro", "intro1",
                "/tables/entity/intro", "intro2",
                "/exporter/entity/Description", "description1");
        genDocs(docTreeFile);
        JsonNode rewrite = new YAMLMapper().readTree(docTreeFile);
        assertThat(rewrite.at("/enums/action_category/intro").asText(), is("intro1"));
        assertThat(rewrite.at("/tables/entity/intro").asText(), is("intro2"));
        assertThat(rewrite.at("/exporter/entity/Description").asText(), is("description1"));
    }

    /**
     * Check that the method that fetches doc snippets from the doc-tree handles various node times
     * properly.
     */
    @Test
    public void testThatDOcTreeEdgeCasesAreHandled() {
        final JsonPointer ptr = JsonPointer.compile("/a/b/c");

        // missing node
        ObjectNode docTree = jsonFac.objectNode();
        Section<?> section = new TableSection<>(ENTITY, docTree);
        assertThat(section.getDocText(ptr), isEmpty());
        assertThat(docTree.at("/a/b/c"), is(jsonFac.nullNode()));

        // null node
        docTree = jsonFac.objectNode();
        docTree.putObject("a").putObject("b").set("c", jsonFac.nullNode());
        section = new TableSection<>(ENTITY, docTree);
        assertThat(section.getDocText(ptr), isEmpty());

        // text node
        docTree = jsonFac.objectNode();
        docTree.putObject("a").putObject("b").set("c", jsonFac.textNode("hi there"));
        section = new TableSection<>(ENTITY, docTree);
        assertThat(section.getDocText(ptr), isPresentAnd(is("hi there")));

        // something altogehter different
        docTree = jsonFac.objectNode();
        docTree.putObject("a").putObject("b").set("c", jsonFac.numberNode(42));
        section = new TableSection<>(ENTITY, docTree);
        assertThat(section.getDocText(ptr), isEmpty());
    }

    private void genDocs(File docTreeFile) throws IOException {
        final File docsDir = new File(tmp.getRoot(), "docs");
        docsDir.mkdirs();
        List<String> args = Arrays.asList("--doc-tree", docTreeFile.getPath());
        final CmdLine cmdLine = CmdLine.of(args.toArray(new String[0]));
        new Generator(cmdLine).generate();
    }

    private File createDocTree(String... args) throws IOException {
        File outFile = new File(tmp.getRoot(), "doc-tree.yaml");
        JsonNode docTree = jsonFac.objectNode();
        for (int i = 0; i < args.length; i += 2) {
            final JsonPointer path = JsonPointer.compile(args[i]);
            Section.addNodeAt(docTree, path, jsonFac.textNode(args[i + 1]));
        }
        new YAMLMapper().writerWithDefaultPrettyPrinter().writeValue(outFile, docTree);
        return outFile;
    }
}
