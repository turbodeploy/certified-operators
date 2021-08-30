package com.vmturbo.extractor.docgen;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.EnumType;

import com.vmturbo.extractor.docgen.DocGen.CmdLine;
import com.vmturbo.extractor.docgen.sections.EntityAttrsSection;
import com.vmturbo.extractor.docgen.sections.EnumTypeSection;
import com.vmturbo.extractor.docgen.sections.GroupAttrsSection;
import com.vmturbo.extractor.docgen.sections.JsonSection;
import com.vmturbo.extractor.docgen.sections.SdkEnumTypeSection;
import com.vmturbo.extractor.docgen.sections.TableSection;
import com.vmturbo.extractor.export.ExportUtils;
import com.vmturbo.extractor.patchers.PrimitiveFieldsOnTEDPatcher;
import com.vmturbo.extractor.schema.Extractor;
import com.vmturbo.extractor.schema.json.export.ExportedObject;
import com.vmturbo.extractor.schema.json.export.Target;
import com.vmturbo.extractor.schema.json.reporting.ReportingActionAttributes;
import com.vmturbo.search.metadata.SearchMetadataMapping;

/**
 * Perform document generation for one or more documentation sections.
 *
 * <p>Generated documentation takes the form of XML files that can subsequently be transformed to
 * target format for publication.</p>
 */
public class Generator {
    private static final Logger logger = LogManager.getLogger();

    private final JsonNode docTree;
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
        this.sections = getSections(docTree);
    }

    /**
     * Perform generation.
     */
    public void generate() {
        // generate each section doc
        sections.forEach(this::generateSection);
        // target file defaults to rewriting the file that was originally loaded
        File newTree = cmdLine.valueOf(cmdLine.docTreeOption());
        try {
            new YAMLMapper().enable(YAMLGenerator.Feature.MINIMIZE_QUOTES)
                    .writerWithDefaultPrettyPrinter().writeValue(newTree, docTree);
        } catch (IOException e) {
            logger.error("Failed to rewrite doc-tree to {}", newTree, e);
        }
    }

    private <T> void generateSection(final Section<T> section) {
        addIntro(section);
        addItemList(section);
    }

    private <T> void addItemList(final Section<T> section) {
        for (final T item : section.getItems()) {
            for (final String fieldName : section.getFieldNames(item)) {
                section.addItemFieldValue(item, fieldName);
            }
        }
    }

    private List<Section<?>> getSections(JsonNode docTree) {
        List<Section<?>> allSections = new ArrayList<>();

        // add all enum sections by going through all columns in the tables in extractor schema
        List<EnumTypeSection> enumSections = Extractor.EXTRACTOR.getTables().stream()
                .flatMap(table -> Arrays.stream(table.fields()))
                .filter(field -> field.getType().isEnum())
                .map(field -> (Class<EnumType>)field.getDataType().getType())
                .distinct()
                .sorted(Comparator.comparing(Class::getSimpleName))
                .map(cls -> new EnumTypeSection(cls, docTree))
                .collect(Collectors.toList());
        for (EnumTypeSection<?> enumSection : enumSections) {
            allSections.add(enumSection);
        }

        // add all sdk enums used in attrs, like AttachmentState, OSType, etc.
        PrimitiveFieldsOnTEDPatcher.getJsonbColumnMetadataByEntityType().values().stream()
                .flatMap(List::stream)
                .map(SearchMetadataMapping::getEnumClass)
                .filter(Objects::nonNull)
                .distinct()
                .sorted(Comparator.comparing(Class::getSimpleName))
                .forEach(enumClass -> allSections.add(new SdkEnumTypeSection<>(enumClass, docTree)));

        // add sections for embedded reporting
        // all tables
        Extractor.EXTRACTOR.getTables().stream()
                .filter(table -> !DocGenUtils.NON_REPORTING_TABLES.contains(table))
                .forEach(table -> allSections.add(new TableSection<>(table, docTree)));
        // table_data: entity attrs (shared between embedded reporting and data exporter)
        allSections.add(new EntityAttrsSection(docTree));
        // group attrs
        allSections.add(new GroupAttrsSection(docTree));

        final Set<Class<?>> sharedClasses = new HashSet<>();
        // table_data: action attrs
        allSections.addAll(DocGenUtils.getSections(ReportingActionAttributes.class,
                DocGenUtils.ACTION_ATTRS_SECTION_NAME,
                DocGenUtils.TABLE_DATA_DOC_PREFIX, docTree, sharedClasses));

        // add sections for data exporter
        allSections.addAll(DocGenUtils.getSections(ExportedObject.class, "",
                DocGenUtils.DATA_EXPORTER_DOC_PREFIX, docTree, sharedClasses));

        // add all shared sections
        // add "Target" which is used in entity attrs
        allSections.add(new JsonSection(docTree,
                ExportUtils.objectMapper.getTypeFactory().constructType(Target.class),
                Target.class.getSimpleName(), DocGenUtils.SHARED_DOC_PREFIX));
        // other shared
        sharedClasses.forEach(sharedClass -> {
            allSections.addAll(DocGenUtils.getSections(sharedClass, sharedClass.getSimpleName(),
                    DocGenUtils.SHARED_DOC_PREFIX, docTree, null));
        });

        return allSections;
    }

    private void addIntro(final Section<?> section) {
        section.getIntroText();
    }
}
