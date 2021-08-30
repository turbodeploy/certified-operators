package com.vmturbo.extractor.docgen.sections;

import static com.vmturbo.extractor.docgen.DocGenUtils.NOTES_FIELD;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.databind.JsonNode;

import com.vmturbo.extractor.docgen.DocGenUtils;
import com.vmturbo.extractor.docgen.Section;

/**
 * Document second generator to document a DB enum type. Each enum value is individually
 * documented.
 *
 * @param <E> sdk enum defined in protobuf.
 */
public class SdkEnumTypeSection<E extends Enum> extends Section<E> {

    private final Class<E> enumClass;

    /**
     * Create a new instance.
     *
     * @param enumClass class object for jOOQ generated class
     * @param docTree   JSON structure with doc snippets to be substituted
     */
    public SdkEnumTypeSection(Class<E> enumClass, JsonNode docTree) {
        super(enumClass.getSimpleName(), DocGenUtils.ENUMS_DOC_PREFIX + "/" + enumClass.getSimpleName(), docTree);
        this.enumClass = enumClass;
    }

    @Override
    public List<E> getItems() {
        return Arrays.stream(enumClass.getEnumConstants())
                .sorted(Comparator.comparing(Enum::name))
                .collect(Collectors.toList());
    }

    @Override
    public List<String> getFieldNames(E item) {
        return Collections.singletonList(NOTES_FIELD);
    }

    @Override
    public JsonPointer getItemDocPath(E item) {
        return docPathPrefix.append(JsonPointer.compile("/items/" + item.name()));
    }

    @Override
    public JsonNode getItemFieldValue(E item, String fieldName) {
        return null;
    }
}
