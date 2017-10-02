package com.vmturbo.repository.graph.executor;

import java.util.Collection;
import java.util.Objects;

import org.derive4j.Data;
import org.derive4j.Derive;
import org.derive4j.ExportAsPublic;
import org.derive4j.FieldNames;
import org.derive4j.Flavour;
import org.derive4j.Visibility;

import javaslang.Function2;

/**
 * A thin type for AQL.
 */
@Data(flavour = Flavour.Javaslang, value = @Derive(withVisibility = Visibility.Smart))
public abstract class AQL {
    public abstract <R> R match(@FieldNames({"query", "bindVars"}) Function2<String, Collection<String>, R> aql);

    @ExportAsPublic
    static AQL of(final String query, final Collection<String> bindVars) {
        Objects.requireNonNull(query, "query cannot be null");
        Objects.requireNonNull(bindVars, "bindVars cannot be null");

        return AQLs.aql0(query, bindVars);
    }

    @Override
    public abstract String toString();
}
