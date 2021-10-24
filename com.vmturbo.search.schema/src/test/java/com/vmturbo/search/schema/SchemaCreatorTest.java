package com.vmturbo.search.schema;

import java.util.LinkedList;
import java.util.List;

import org.jooq.CreateTableColumnStep;
import org.jooq.CreateTableConstraintStep;
import org.jooq.DSLContext;
import org.jooq.DropTableStep;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * Unit tests for SchemaCreator.
 */
public class SchemaCreatorTest {
    /**
     * Test how the schema creation queries are generated.
     */
    @Test
    public void testSchemaCreateQueries() {
        DSLContext dsl = Mockito.mock(DSLContext.class);
        CreateTableColumnStep col = Mockito.mock(CreateTableColumnStep.class);
        ColumnRecordingAnswer colAnswer = new ColumnRecordingAnswer(col);
        Mockito.when(dsl.createTable(Mockito.anyString())).thenAnswer(colAnswer);
        Mockito.when(dsl.dropTableIfExists(Mockito.anyString())).thenReturn(Mockito.mock(DropTableStep.class));
        Mockito.when(col.column(Mockito.anyString(), Mockito.any())).thenAnswer(colAnswer);
        CreateTableConstraintStep constraint = Mockito.mock(CreateTableConstraintStep.class);
        Mockito.when(col.constraint(Mockito.any())).thenReturn(constraint);

        ISchemaCreator sc = new SchemaCreator(dsl);
        List<String> queries = sc.createWithoutIndexes("", null);
        Assert.assertEquals(4 + 4, queries.size());
        // search_entity: id, oid, type, name, state, environment
        // search_entity_action: id, oid, type, severity, num_actions
        // numeric and string: id, oid, field, value
        Assert.assertEquals(4 + 6 + 5 + 4 + 4, colAnswer.getColumns().size());
    }

    /**
     * Holds columns provided by generator.
     */
    private static class ColumnRecordingAnswer implements Answer<CreateTableColumnStep> {
        private final List<String> columns = new LinkedList<>();
        private final CreateTableColumnStep col;

        ColumnRecordingAnswer(CreateTableColumnStep col) {
            this.col = col;
        }

        @Override
        public CreateTableColumnStep answer(InvocationOnMock invocation) {
            columns.add(invocation.getArgumentAt(0, String.class));
            return col;
        }

        public List<String> getColumns() {
            return columns;
        }
    }
}
