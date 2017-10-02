package com.vmturbo.repository.topology;

import javaslang.Function1;
import org.derive4j.Data;
import org.derive4j.Derive;
import org.derive4j.ExportAsPublic;
import org.derive4j.FieldNames;
import org.derive4j.Flavour;
import org.derive4j.Visibility;

/**
 * A thin type to represent a topology database.
 */
@Data(flavour = Flavour.Javaslang, value = @Derive(withVisibility = Visibility.Smart))
public abstract class TopologyDatabase {
    public abstract <R> R match(@FieldNames("dbName") Function1<String, R> dbName);

    @ExportAsPublic
    public static TopologyDatabase from(final String databaseName) {
        return TopologyDatabases.dbName0(databaseName);
    }

    @Override
    public abstract String toString();
}
