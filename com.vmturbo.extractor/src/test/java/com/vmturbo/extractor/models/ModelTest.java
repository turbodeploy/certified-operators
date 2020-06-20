package com.vmturbo.extractor.models;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

import java.util.stream.Collectors;

import org.junit.Test;

/**
 * Tests for the {@link Model} class.
 */
public class ModelTest {

    /**
     * Test that the model builder properly constructs a model.
     */
    @Test
    public void testModelBuilder() {
        Model model = Model.named("m").withTables(
                Table.named("t1").withColumns(
                        Column.intColumn("c1"),
                        Column.stringColumn("c2")
                ).build(),
                Table.named("t2").withColumns(
                        Column.longArrayColumn("c3")
                ).build()
        ).build();
        assertThat(model.getName(), is("m"));
        assertThat(model.getTables().stream().map(Table::getName).collect(Collectors.toList()),
                contains("t1", "t2"));
        assertThat(model.getTables().stream()
                        .flatMap(t -> t.getColumns().stream().map(Column::getName))
                        .collect(Collectors.toList()),
                contains("c1", "c2", "c3"));

    }
}
