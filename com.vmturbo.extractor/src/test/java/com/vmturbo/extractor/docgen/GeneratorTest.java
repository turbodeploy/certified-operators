package com.vmturbo.extractor.docgen;

import static com.github.npathai.hamcrestopt.OptionalMatchers.isEmpty;
import static com.github.npathai.hamcrestopt.OptionalMatchers.isPresentAnd;
import static javax.xml.xpath.XPathConstants.NODESET;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;

import java.io.File;
import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;

import org.apache.commons.lang3.SystemUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.vmturbo.extractor.docgen.DocGen.CmdLine;
import com.vmturbo.extractor.docgen.Section.RegisteredSection;
import com.vmturbo.extractor.docgen.sections.EntityAttrsSection;
import com.vmturbo.extractor.docgen.sections.EnumTypeSection;
import com.vmturbo.extractor.docgen.sections.TableSection;
import com.vmturbo.extractor.patchers.PrimitiveFieldsOnTEDPatcher;
import com.vmturbo.extractor.schema.enums.EntityType;
import com.vmturbo.extractor.schema.tables.Entity;
import com.vmturbo.platform.common.dto.CommonDTOREST.EntityDTO;

/**
 * Tests for the {@link Generator} class.
 */
public class GeneratorTest {

    private final JsonNodeFactory jsonFac = JsonNodeFactory.instance;
    private final DocumentBuilderFactory docBuilderFac = DocumentBuilderFactory.newInstance();
    private final XPathFactory xpathFac = XPathFactory.newInstance();

    /**
     * We use a temp directory for doc-tree and generated doc files.
     */
    @Rule
    public TemporaryFolder tmp = new TemporaryFolder();

    /**
     * To capture exceptions.
     */
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    /**
     * Test that generation fails fast if the provided output directory is not writable.
     *
     * @throws IOException if there's a problem other than unwritable output dir)
     */
    @Test
    public void testGeneratorRejectsNonWritableOutputDir() throws IOException {
        final File unwritableDir = new File(tmp.getRoot(), "unwritable");
        final File docTree = createDocTree();
        unwritableDir.mkdir();
        unwritableDir.setReadOnly();
        final CmdLine cmdLine = CmdLine.of("--write-to", unwritableDir.getPath(),
                "--doc-tree", docTree.getPath());

        // exception is thrown during construction, no need to invoke generation
        // On windows we can write to non-readable folders
        if (!SystemUtils.IS_OS_WINDOWS)  {
            expectedException.expect(AccessDeniedException.class);
        }

        new Generator(cmdLine);
    }

    /**
     * Test that we generate docs for selected sections when they're explicitly provided on the
     * command line.
     *
     * @throws IOException if there's a problem
     */
    @Test
    public void testGeneratorGeneratesSelectedSections() throws IOException {
        final File docTreeFile = createDocTree();
        final File docsDir = genDocs(docTreeFile, null,
                RegisteredSection.EntityTypeEnum, RegisteredSection.EntityTable);
        final List<String> actual = Arrays.asList(docsDir.list());
        final String[] expected =
                Stream.of(RegisteredSection.EntityTypeEnum, RegisteredSection.EntityTable)
                        .map(rs -> rs.create(jsonFac.objectNode()))
                        .map(Section::getName)
                        .map(name -> name + ".xml")
                        .toArray(String[]::new);
        assertThat(actual, containsInAnyOrder(expected));
    }

    /**
     * Test that all sections are generated hwen the command line does not explicitly call for any.
     *
     * @throws IOException if there's a problem
     */
    @Test
    public void testGeneratorGeneratesAllSectionsIfNoneSpecified() throws IOException {
        final File docTreeFile = createDocTree();
        final File docsDir = genDocs(docTreeFile, null);
        final List<String> actual = Arrays.asList(docsDir.list());
        final String[] expected = Arrays.stream(RegisteredSection.values())
                .map(rs -> rs.create(jsonFac.objectNode()))
                .map(Section::getName)
                .map(name -> name + ".xml")
                .toArray(String[]::new);
        assertThat(actual, containsInAnyOrder(expected));
    }

    /**
     * Test that when the `--rewrite-tree` option is given without an argument, the original
     * doc-tree file is rewritten after generation, with null values for missing paths.
     *
     * @throws IOException if there's a problem
     */
    @Test
    public void testThatRewriteToSameFileWorks() throws IOException {
        final File docTreeFile = createDocTree("/tables/entity/intro", "intro-text");
        genDocs(docTreeFile, null, RegisteredSection.EntityTable);
        JsonNode rewrite = new YAMLMapper().readTree(docTreeFile);
        assertThat(rewrite.at("/tables/entity/intro").asText(), is("intro-text"));
        assertThat(rewrite.at("/tables/entity/columns/oid/Description"), isA(NullNode.class));
        assertThat(rewrite.at("/tables/entity/columns/type/Description"), isA(NullNode.class));
        assertThat(rewrite.at("/tables/entity/columns/name/Description"), isA(NullNode.class));
        assertThat(rewrite.at("/tables/entity/columns/environment/Description"), isA(NullNode.class));
        assertThat(rewrite.at("/tables/entity/columns/attrs/Description"), isA(NullNode.class));
        assertThat(rewrite.at("/tables/entity/columns/first_seen/Description"), isA(NullNode.class));
        assertThat(rewrite.at("/tables/entity/columns/last_seen/Description"), isA(NullNode.class));
    }

    /**
     * Make sure that if `--rewrite-tree` option is provided with a file path, the augmnted doc-tree
     * file is written to that, rather than the overwriting the original doc-tree.
     *
     * @throws IOException if there's a problem
     */
    @Test
    public void testThatRewriteToOtherFileWorks() throws IOException {
        final File docTreeFile = createDocTree("/tables/entity/intro", "intro-text");
        final File rewriteFile = new File(tmp.getRoot(), "rewrite.yaml");
        genDocs(docTreeFile, rewriteFile, RegisteredSection.EntityTable);
        JsonNode rewrite = new YAMLMapper().readTree(rewriteFile);
        assertThat(rewrite.at("/tables/entity/intro").asText(), is("intro-text"));
        assertThat(rewrite.at("/tables/entity/columns/oid/Description"), isA(NullNode.class));
        assertThat(rewrite.at("/tables/entity/columns/type/Description"), isA(NullNode.class));
        assertThat(rewrite.at("/tables/entity/columns/name/Description"), isA(NullNode.class));
        assertThat(rewrite.at("/tables/entity/columns/environment/Description"), isA(NullNode.class));
        assertThat(rewrite.at("/tables/entity/columns/attrs/Description"), isA(NullNode.class));
        assertThat(rewrite.at("/tables/entity/columns/first_seen/Description"), isA(NullNode.class));
        assertThat(rewrite.at("/tables/entity/columns/last_seen/Description"), isA(NullNode.class));
    }

    /**
     * Test that the {@link EnumTypeSection} section generator produces correct documents.
     *
     * <p>We closely examine the `entity_type` generated doc to ensure all required elements and
     * attributes are present and have correct values.</p>
     *
     * @throws IOException                  If there's a probelm
     * @throws ParserConfigurationException if there's an XML parser problem
     * @throws SAXException                 If the generated file is not proper XML
     * @throws XPathExpressionException     if we try to use an incorrect XPath expression
     */
    @Test
    public void testThatEnumDocsAreCorrect()
            throws IOException, ParserConfigurationException, SAXException, XPathExpressionException {
        final File docTreeFile = createDocTree(
                "/types/entity_type/intro", " entity-type intro ",
                "/types/entity_type/items/APPLICATION/Notes", " app-notes ");
        final File docsDir = genDocs(docTreeFile, null, RegisteredSection.EntityTypeEnum);
        Document doc = parseDoc(new File(docsDir, "entity_type.xml"));
        // validate root node
        final Element root = doc.getDocumentElement();
        assertThat(root.getNodeName(), is("GeneratedDoc"));
        assertThat(root.getAttributes().getLength(), is(2));
        assertThat(root.getAttribute("name"), is("entity_type"));
        assertThat(root.getAttribute("type"), is("enum-type"));
        // validate Intro element
        final NodeList introElts = root.getElementsByTagName("Intro");
        assertThat(introElts.getLength(), is(1));
        final Element introElt = (Element)introElts.item(0);
        assertThat(introElt.getAttributes().getLength(), is(0));
        assertThat(introElt.getTextContent(), is(" entity-type intro "));
        // validate Items element
        final NodeList itemsElts = root.getElementsByTagName("Items");
        assertThat(itemsElts.getLength(), is(1));
        final Element itemsElt = (Element)itemsElts.item(0);
        assertThat(itemsElt.getAttributes().getLength(), is(0));
        final NodeList itemElts = itemsElt.getElementsByTagName("Item");
        assertThat(itemElts.getLength(), is(EntityType.values().length));
        // validate the APPLICATION element
        final NodeList appElts = (NodeList)xpathFac.newXPath()
                .compile("/GeneratedDoc/Items/Item[@name='APPLICATION']")
                .evaluate(doc, NODESET);
        assertThat(appElts.getLength(), is(1));
        final Element appElt = (Element)appElts.item(0);
        assertThat(appElt.getAttributes().getLength(), is(1));
        assertThat(appElt.getAttribute("name"), is("APPLICATION"));
        // check fields
        final NodeList fldElts = appElt.getElementsByTagName("Field");
        assertThat(fldElts.getLength(), is(1));
        final Element fldElt = (Element)fldElts.item(0);
        assertThat(fldElt.getAttributes().getLength(), is(1));
        assertThat(fldElt.getAttribute("name"), is("Notes"));
        assertThat(fldElt.getTextContent(), is(" app-notes "));
        // whew!
    }

    /**
     * Test that the {@link TableSection} generator works correctly.
     *
     * <p>We test this by closely examining the doc generated for the `entity` table.</p>
     *
     * @throws IOException                  If there's a probelm
     * @throws ParserConfigurationException if there's an XML parser problem
     * @throws SAXException                 If the generated file is not proper XML
     * @throws XPathExpressionException     if we try to use an incorrect XPath expression
     */
    @Test
    public void testThatTableDocsAreCorrect()
            throws IOException, ParserConfigurationException, SAXException, XPathExpressionException {
        final File docTreeFile = createDocTree(
                "/tables/entity/intro", " entity-table intro ",
                "/tables/entity/columns/oid/Description", "\n oid-description \n ");
        final File docsDir = genDocs(docTreeFile, null, RegisteredSection.EntityTable);
        Document doc = parseDoc(new File(docsDir, "entity.xml"));
        // validate root node
        final Element root = doc.getDocumentElement();
        assertThat(root.getNodeName(), is("GeneratedDoc"));
        assertThat(root.getAttributes().getLength(), is(2));
        assertThat(root.getAttribute("name"), is("entity"));
        assertThat(root.getAttribute("type"), is("table"));
        // validate Intro element
        final NodeList introElts = root.getElementsByTagName("Intro");
        assertThat(introElts.getLength(), is(1));
        final Element introElt = (Element)introElts.item(0);
        assertThat(introElt.getAttributes().getLength(), is(0));
        assertThat(introElt.getTextContent(), is(" entity-table intro "));
        // validate Items element
        final NodeList itemsElts = root.getElementsByTagName("Items");
        assertThat(itemsElts.getLength(), is(1));
        final Element itemsElt = (Element)itemsElts.item(0);
        assertThat(itemsElt.getAttributes().getLength(), is(0));
        final NodeList itemElts = itemsElt.getElementsByTagName("Item");
        assertThat(itemElts.getLength(), is(Entity.ENTITY.fields().length));
        // validate the oid element
        final NodeList oidElts = (NodeList)xpathFac.newXPath()
                .compile("/GeneratedDoc/Items/Item[@name='oid']")
                .evaluate(doc, NODESET);
        assertThat(oidElts.getLength(), is(1));
        final Element oidElt = (Element)oidElts.item(0);
        assertThat(oidElt.getAttributes().getLength(), is(1));
        assertThat(oidElt.getAttribute("name"), is("oid"));
        // check fields
        final NodeList fldElts = oidElt.getElementsByTagName("Field");
        assertThat(fldElts.getLength(), is(4));
        Element fldElt = (Element)fldElts.item(0);
        assertThat(fldElt.getAttributes().getLength(), is(1));
        assertThat(fldElt.getAttribute("name"), is("Type"));
        assertThat(fldElt.getTextContent(), is("bigint"));
        fldElt = (Element)fldElts.item(1);
        assertThat(fldElt.getAttributes().getLength(), is(1));
        assertThat(fldElt.getAttribute("name"), is("Nullable"));
        assertThat(fldElt.getTextContent(), is("false"));
        fldElt = (Element)fldElts.item(2);
        assertThat(fldElt.getAttributes().getLength(), is(1));
        assertThat(fldElt.getAttribute("name"), is("Primary"));
        assertThat(fldElt.getTextContent(), is("true"));
        fldElt = (Element)fldElts.item(3);
        assertThat(fldElt.getAttributes().getLength(), is(1));
        assertThat(fldElt.getAttribute("name"), is("Description"));
        assertThat(fldElt.getTextContent(), is("\n oid-description \n "));
    }

    /**
     * Test that the {@link EntityAttrsSection} generator works correctly.
     *
     * @throws IOException                  If there's a probelm
     * @throws ParserConfigurationException if there's an XML parser problem
     * @throws SAXException                 If the generated file is not proper XML
     * @throws XPathExpressionException     if we try to use an incorrect XPath expression
     */
    @Test
    public void testThatEntityAttrsDocIsCorrect()
            throws IOException, ParserConfigurationException, SAXException, XPathExpressionException {
        final File docTreeFile = createDocTree(
                "/table_data/entity.attrs/intro", " entity.attrs intro ",
                "/table_data/entity.attrs/items/APPLICATION/tags/Description", "app-tags-description");
        final File docsDir = genDocs(docTreeFile, null, RegisteredSection.EntityAttrs);
        Document doc = parseDoc(new File(docsDir, "entity.attrs.xml"));
        // validate root node
        final Element root = doc.getDocumentElement();
        assertThat(root.getNodeName(), is("GeneratedDoc"));
        assertThat(root.getAttributes().getLength(), is(2));
        assertThat(root.getAttribute("name"), is("entity.attrs"));
        assertThat(root.getAttribute("type"), is("table_data"));
        // validate Intro element
        final NodeList introElts = root.getElementsByTagName("Intro");
        assertThat(introElts.getLength(), is(1));
        final Element introElt = (Element)introElts.item(0);
        assertThat(introElt.getAttributes().getLength(), is(0));
        assertThat(introElt.getTextContent(), is(" entity.attrs intro "));
        // validate Items element
        final NodeList itemsElts = root.getElementsByTagName("Items");
        assertThat(itemsElts.getLength(), is(1));
        final Element itemsElt = (Element)itemsElts.item(0);
        assertThat(itemsElt.getAttributes().getLength(), is(1));
        assertThat(itemsElt.getAttribute("groupBy"), is("Entity_Type"));
        final NodeList itemElts = itemsElt.getElementsByTagName("Item");
        assertThat(itemElts.getLength(), is((int)(
                // we have a "tags" entry for every entity type, constructed by hack...
                EntityDTO.EntityType.values().length
                        // plus one for every metadata configured for json data...
                        + PrimitiveFieldsOnTEDPatcher.getJsonbColumnMetadataByEntityType()
                        .values().stream()
                        .mapToLong(List::size)
                        .sum())));
        // validate the APPLICATION.tags item
        final NodeList oidElts = (NodeList)xpathFac.newXPath()
                .compile("/GeneratedDoc/Items/Item[@name='APPLICATION.tags']")
                .evaluate(doc, NODESET);
        assertThat(oidElts.getLength(), is(1));
        final Element oidElt = (Element)oidElts.item(0);
        assertThat(oidElt.getAttributes().getLength(), is(1));
        assertThat(oidElt.getAttribute("name"), is("APPLICATION.tags"));
        // check fields
        final NodeList fldElts = oidElt.getElementsByTagName("Field");
        assertThat(fldElts.getLength(), is(4));
        Element fldElt = (Element)fldElts.item(0);
        assertThat(fldElt.getAttributes().getLength(), is(1));
        assertThat(fldElt.getAttribute("name"), is("Entity_Type"));
        assertThat(fldElt.getTextContent(), is("APPLICATION"));
        fldElt = (Element)fldElts.item(1);
        assertThat(fldElt.getAttributes().getLength(), is(1));
        assertThat(fldElt.getAttribute("name"), is("Attribute_Name"));
        assertThat(fldElt.getTextContent(), is("tags"));
        fldElt = (Element)fldElts.item(2);
        assertThat(fldElt.getAttributes().getLength(), is(1));
        assertThat(fldElt.getAttribute("name"), is("Attribute_Type"));
        assertThat(fldElt.getTextContent(), is("TEXT_MULTIMAP"));
        fldElt = (Element)fldElts.item(3);
        assertThat(fldElt.getAttributes().getLength(), is(1));
        assertThat(fldElt.getAttribute("name"), is("Description"));
        assertThat(fldElt.getTextContent(), is("app-tags-description"));
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
        Section<?> section = RegisteredSection.values()[0].create(docTree);
        assertThat(section.getDocText(ptr), isEmpty());
        assertThat(docTree.at("/a/b/c"), is(jsonFac.nullNode()));

        // null node
        docTree = jsonFac.objectNode();
        docTree.putObject("a").putObject("b").set("c", jsonFac.nullNode());
        section = RegisteredSection.values()[0].create(docTree);
        assertThat(section.getDocText(ptr), isEmpty());

        // text node
        docTree = jsonFac.objectNode();
        docTree.putObject("a").putObject("b").set("c", jsonFac.textNode("hi there"));
        section = RegisteredSection.values()[0].create(docTree);
        assertThat(section.getDocText(ptr), isPresentAnd(is("hi there")));

        // something altogehter different
        docTree = jsonFac.objectNode();
        docTree.putObject("a").putObject("b").set("c", jsonFac.numberNode(42));
        section = RegisteredSection.values()[0].create(docTree);
        assertThat(section.getDocText(ptr), isEmpty());
    }

    private File genDocs(File docTreeFile, File rewriteFile, RegisteredSection... sections)
            throws IOException {
        final File docsDir = new File(tmp.getRoot(), "docs");
        docsDir.mkdirs();
        List<String> args = new ArrayList<>(Arrays.asList(
                "--doc-tree", docTreeFile.getPath(),
                "--write-to", docsDir.getPath(),
                "--rewrite-tre"));
        if (rewriteFile != null) {
            args.add(rewriteFile.getPath());
        }
        for (final RegisteredSection section : sections) {
            args.add("--section");
            args.add(section.name());
        }
        final CmdLine cmdLine = CmdLine.of(args.toArray(new String[0]));
        new Generator(cmdLine).generate();
        return docsDir;
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

    private Document parseDoc(File xmlFile) throws ParserConfigurationException, IOException, SAXException {
        DocumentBuilder builder = docBuilderFac.newDocumentBuilder();
        return builder.parse(xmlFile);
    }
}
