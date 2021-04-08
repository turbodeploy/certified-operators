package com.vmturbo.extractor.docgen;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.AccessDeniedException;
import java.time.OffsetDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.xml.stream.XMLEventFactory;
import javax.xml.stream.XMLEventWriter;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.Attribute;
import javax.xml.stream.events.Characters;
import javax.xml.stream.events.Comment;
import javax.xml.stream.events.EndElement;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;

import jersey.repackaged.com.google.common.collect.Iterators;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.extractor.docgen.DocGen.CmdLine;

/**
 * Perform document generation for one or more documentation sections.
 *
 * <p>Generated documentation takes the form of XML files that can subsequently be transformed to
 * target format for publication.</p>
 */
public class Generator {
    private static final Logger logger = LogManager.getLogger();

    private final JsonNode docTree;
    private final File outDir;
    private final List<Section<?>> sections;
    private final CmdLine cmdLine;

    /**
     * Create a new instance.
     *
     * @param cmdLine command line parser with results
     * @throws IOException if there's a problem reading or writing any files
     */
    public Generator(final CmdLine cmdLine) throws IOException {
        this.cmdLine = cmdLine;
        // Load YAML file containing doc snippets to be plugged into generated files
        this.docTree = new YAMLMapper().readTree(cmdLine.valueOf(cmdLine.docTreeOption()));
        this.outDir = cmdLine.valueOf(cmdLine.writeToOption());
        checkWritableDir(outDir);
        this.sections = getSections(cmdLine, docTree);
    }

    /**
     * Perform generation.
     */
    public void generate() {
        // generate each section doc
        sections.forEach(this::generateSection);
        // if rewrite requested for doc tree, do that now;
        // any missing entries were added with null values as a side-effect of generation
        if (cmdLine.has(cmdLine.rewriteTreeOption())) {
            // target file defaults to rewriting the file that was originally loaded
            File newTree = cmdLine.hasArgument(cmdLine.rewriteTreeOption())
                    ? cmdLine.valueOf(cmdLine.rewriteTreeOption())
                    : cmdLine.valueOf(cmdLine.docTreeOption());
            try {
                new YAMLMapper().writerWithDefaultPrettyPrinter().writeValue(newTree, docTree);
            } catch (IOException e) {
                logger.error("Failed to rewrite doc-tree to {}", newTree, e);
            }
        }
    }


    private <T> void generateSection(final Section<T> section) {
        File outFile = new File(outDir, section.getName() + ".xml");
        try (OutputStream out = new FileOutputStream(outFile)) {
            final XMLEventWriter ew = XMLOutputFactory.newInstance().createXMLEventWriter(out);
            final XMLEventFactory fac = XMLEventFactory.newInstance();
            final IndentingEventWriter writer = new IndentingEventWriter(fac, ew);
            writer.add(fac.createStartDocument());
            writer.add(fac.createComment("Generated on " + OffsetDateTime.now()));
            writer.add(getStartElement(fac, section));
            addIntro(section, writer);
            addItemList(section, fac, writer);
            writer.add(fac.createEndElement("", "", "GeneratedDoc"));
            writer.add(fac.createEndDocument());
        } catch (XMLStreamException | IOException e) {
            logger.error("Failed to generate doc section {}", section.getName(), e);
        }
    }

    private <T> void addItemList(
            final Section<T> section, final XMLEventFactory fac, final IndentingEventWriter writer)
            throws XMLStreamException {
        List<Attribute> itemAttrs = Collections.emptyList();
        if (!section.getGroupByFields().isEmpty()) {
            String commaList = String.join(",", section.getGroupByFields());
            itemAttrs = attributesFor(fac, ImmutableMap.of("groupBy", commaList));
        }
        writer.add(fac.createStartElement("", "", "Items",
                itemAttrs.iterator(), Iterators.emptyIterator()));
        for (final T item : section.getItems()) {
            final List<Attribute> attrs = attributesFor(fac, ImmutableMap.of(
                    "name", section.getItemName(item)));
            writer.add(fac.createStartElement("", "", "Item",
                    attrs.iterator(), Iterators.emptyIterator()));
            for (final String fieldName : section.getFieldNames()) {
                final ImmutableMap<String, String> fAttrMap = ImmutableMap.of("name", fieldName);
                final String value = section.getFieldValue(item, fieldName).orElse("");
                writer.addTextElement("Field", fAttrMap, value);
            }
            writer.add(fac.createEndElement("", "", "Item"));
        }
        writer.add(fac.createEndElement("", "", "Items"));
    }

    private StartElement getStartElement(final XMLEventFactory fac, final Section<?> section) {
        final List<Attribute> attrs = attributesFor(fac, ImmutableMap.of(
                "name", section.getName(),
                "type", section.getType()));
        return fac.createStartElement(
                "", "", "GeneratedDoc", attrs.iterator(), Iterators.emptyIterator());
    }

    private List<Attribute> attributesFor(XMLEventFactory fac, Map<String, String> map) {
        return map.entrySet().stream()
                .map(e -> fac.createAttribute(e.getKey(), e.getValue()))
                .collect(Collectors.toList());
    }

    private void checkWritableDir(final File outDir) throws AccessDeniedException {
        if (!outDir.exists() || !outDir.isDirectory() || !outDir.canWrite()) {
            throw new AccessDeniedException("Output path is not an existing, writable directory: " + outDir);
        }
    }

    private List<Section<?>> getSections(final CmdLine cmdLine, JsonNode docTree) {
        return cmdLine.valuesOf(cmdLine.sectionOption()).stream()
                .map(s -> s.create(docTree))
                .collect(Collectors.toList());
    }

    private void addIntro(final Section<?> section, final IndentingEventWriter writer)
            throws XMLStreamException {
        final String introText = section.getIntroText().orElse("");
        writer.addTextElement("Intro", Collections.emptyMap(), introText);
    }

    /**
     * Utility class that wraps an {@link XMLEventWriter} and injections newlines and indentation
     * where they are semantically immaterial.
     */
    private class IndentingEventWriter {

        private final XMLEventFactory fac;
        private final XMLEventWriter writer;
        private final Characters nl;
        private final Characters tab;
        private int indent = 0;

        IndentingEventWriter(XMLEventFactory fac, XMLEventWriter writer) {
            this.fac = fac;
            this.writer = writer;
            this.nl = fac.createCharacters("\n");
            this.tab = fac.createCharacters("\t");
        }

        public void add(final XMLEvent event) throws XMLStreamException {
            // we always drop indentation level before an end element, so that element is
            // affected
            if (event.isEndElement()) {
                indent -= 1;
            }
            // we add current indentation in front of any start or end element or comment
            if (event.isStartElement() || event.isEndElement() || event instanceof Comment) {
                for (int i = 0; i < indent; i++) {
                    writer.add(tab);
                }
            }
            writer.add(event);
            // then we add a newline after start/end elements including doc start, and comments
            if (event.isStartDocument() || event.isStartElement() || event.isEndElement()
                    || event instanceof Comment) {
                writer.add(nl);
            }
            // and we bump indentation _after_ emitting a start element
            if (event.isStartElement()) {
                indent += 1;
            }
        }

        /**
         * Utility method to add an element with text content, taking care not to add superfluous
         * whitespace within the element.
         *
         * <p>The start tag gets indentation, the end tag does not, and the prevailing indentation
         * is unchanged.</p>
         *
         * @param name    element name
         * @param attrMap map of attrs and values to include in start tag
         * @param content text content to occupy entirety of element
         * @throws XMLStreamException if there's a problem
         */
        private void addTextElement(String name, Map<String, String> attrMap, String content)
                throws XMLStreamException {
            final List<Attribute> attrs = attrMap != null ? attributesFor(fac, attrMap)
                    : Collections.emptyList();
            final StartElement start = fac.createStartElement("", "", name,
                    attrs.iterator(), Iterators.emptyIterator());
            final EndElement end = fac.createEndElement("", "", name);
            writer.add(fac.createCharacters(Strings.repeat("\t", indent)));
            writer.add(start);
            if (!Strings.isNullOrEmpty(content)) {
                writer.add(fac.createCharacters(content));
            }
            writer.add(end);
            writer.add(fac.createCharacters("\n"));
        }
    }
}
