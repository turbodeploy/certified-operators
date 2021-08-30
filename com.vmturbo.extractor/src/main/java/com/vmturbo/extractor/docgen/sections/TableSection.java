package com.vmturbo.extractor.docgen.sections;

import static com.vmturbo.extractor.docgen.DocGenUtils.ACTION_ATTRS_SECTION_NAME;
import static com.vmturbo.extractor.docgen.DocGenUtils.DESCRIPTION;
import static com.vmturbo.extractor.docgen.DocGenUtils.EMBEDDED_REPORTING_TABLE_DOC_PREFIX;
import static com.vmturbo.extractor.docgen.DocGenUtils.ENTITY_ATTRS_SECTION_PATH;
import static com.vmturbo.extractor.docgen.DocGenUtils.ENUMS_DOC_PREFIX;
import static com.vmturbo.extractor.docgen.DocGenUtils.GROUP_ATTRS_SECTION_PATH;
import static com.vmturbo.extractor.docgen.DocGenUtils.NULLABLE;
import static com.vmturbo.extractor.docgen.DocGenUtils.PRIMARY;
import static com.vmturbo.extractor.docgen.DocGenUtils.REFERENCE;
import static com.vmturbo.extractor.docgen.DocGenUtils.TABLE_DATA_DOC_PREFIX;
import static com.vmturbo.extractor.docgen.DocGenUtils.TYPE;
import static com.vmturbo.extractor.schema.tables.CompletedAction.COMPLETED_ACTION;
import static com.vmturbo.extractor.schema.tables.Entity.ENTITY;
import static com.vmturbo.extractor.schema.tables.PendingAction.PENDING_ACTION;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Lists;

import org.jooq.Configuration;
import org.jooq.DataType;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.SQLDialect;
import org.jooq.Table;
import org.jooq.TableField;
import org.jooq.UniqueKey;
import org.jooq.impl.DefaultConfiguration;

import com.vmturbo.extractor.docgen.Section;

/**
 * Documentation section generator for a schema table.
 *
 * @param <R> Record type of table
 */
public class TableSection<R extends Record> extends Section<TableField<R, ?>> {

    private static final Configuration POSTGRES_CONFIG
            = new DefaultConfiguration().set(SQLDialect.POSTGRES);

    private static final com.google.common.collect.Table<Table, TableField, String> COLUMNS_WITH_REFERENCE =
            new ImmutableTable.Builder<Table, TableField, String>()
                    .put(ENTITY, ENTITY.ATTRS, ENTITY_ATTRS_SECTION_PATH + " or " + GROUP_ATTRS_SECTION_PATH)
                    .put(PENDING_ACTION, PENDING_ACTION.ATTRS, TABLE_DATA_DOC_PREFIX + "/" + ACTION_ATTRS_SECTION_NAME)
                    .put(COMPLETED_ACTION, COMPLETED_ACTION.ATTRS, TABLE_DATA_DOC_PREFIX + "/" + ACTION_ATTRS_SECTION_NAME)
                    .build();

    private final Table<R> table;

    /**
     * Create a new instance.
     *
     * @param table   an instance of the table to be documented
     * @param docTree JSON structure containing document snippets
     */
    public TableSection(Table<R> table, final JsonNode docTree) {
        super(table.getName(), EMBEDDED_REPORTING_TABLE_DOC_PREFIX + "/" + table.getName(), docTree);
        this.table = table;
    }

    @Override
    public List<TableField<R, ?>> getItems() {
        return Arrays.stream(table.fields())
                .map(f -> (TableField<R, ?>)f)
                .collect(Collectors.toList());
    }


    @Override
    public List<String> getFieldNames(final TableField<R, ?> field) {
        List<String> fields = Lists.newArrayList(TYPE, NULLABLE, PRIMARY, DESCRIPTION);
        // special reference for the jsonb column in entity and action table
        if (COLUMNS_WITH_REFERENCE.contains(table, field)) {
            fields.add(REFERENCE);
        }
        return ImmutableList.copyOf(fields);
    }

    @Override
    public JsonNode getItemFieldValue(TableField<R, ?> item, String fieldName) {
        switch (fieldName) {
            case TYPE:
                String typeSpec = getTypeSpec(item);
                if (item.getDataType(POSTGRES_CONFIG).isEnum()) {
                    typeSpec = ENUMS_DOC_PREFIX + "/" + typeSpec;
                }
                return JsonNodeFactory.instance.textNode(typeSpec);
            case NULLABLE:
                final boolean nullable = item.getDataType().nullability().nullable();
                return JsonNodeFactory.instance.booleanNode(nullable);
            case PRIMARY:
                final UniqueKey<?> primaryKey = table.getPrimaryKey();
                if (primaryKey != null) {
                    final boolean isPrimary = primaryKey.getFields().contains(item);
                    return JsonNodeFactory.instance.booleanNode(isPrimary);
                } else {
                    return JsonNodeFactory.instance.booleanNode(false);
                }
            case REFERENCE:
                return JsonNodeFactory.instance.textNode(COLUMNS_WITH_REFERENCE.get(table, item));
            default:
                return JsonNodeFactory.instance.nullNode();
        }
    }

    private String getTypeSpec(final Field<?> field) {
        final DataType<?> type = field.getDataType(POSTGRES_CONFIG);
        final String name = type.getTypeName(POSTGRES_CONFIG);
        String lenPrecScale = "";
        if (type.hasLength()) {
            lenPrecScale = "(" + type.length() + ")";
        } else if (type.hasPrecision() && !(type.isTemporal() && type.precision() == 0)) {
            lenPrecScale = "(" + type.precision();
            if (type.hasScale()) {
                lenPrecScale += ", " + type.scale();
            }
            lenPrecScale += ")";
        }
        String identity = type.identity() ? " GENERATED BY DEFAULT AS IDENTITY" : "";
        return name + lenPrecScale + identity;
    }

    @Override
    public JsonPointer getItemDocPath(final TableField<R, ?> field) {
        return docPathPrefix.append(JsonPointer.compile("/columns/" + field.getName()));
    }
}
