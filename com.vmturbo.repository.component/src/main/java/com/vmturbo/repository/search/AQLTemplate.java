package com.vmturbo.repository.search;

import com.github.jknack.handlebars.Handlebars;
import com.github.jknack.handlebars.Template;
import com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.util.Collection;
import java.util.EnumMap;
import java.util.Map;

/**
 * Contain a mapping from {@link com.vmturbo.repository.search.Filter.Type} to {@link Template}.
 *
 * The template is used when one needs to convert an object to AQL.
 */
public class AQLTemplate {

    /**
     * Template for property search. It works for both string and numeric properties.
     */
    public static final String PROPERTY_SEARCH_TEMPLATE =
            "LET all_entities = FIRST(@inputs) == @allKeyword ? true : false\n" +
            "FOR service_entity IN @@serviceEntityCollection\n" +
            "FILTER all_entities OR service_entity._id IN @inputs\n" +
            "{{#filters}}\n" +
            "{{{filter}}}\n" +
            "{{/filters}}\n" +
            "{{pagination}}\n" +
            "RETURN service_entity._id";

    /**
     * The bind vars for {@link #PROPERTY_SEARCH_TEMPLATE}.
     */
    public static final Collection<String> PROPERTY_SEARCH_BIND_VARS =
            ImmutableList.of("inputs", "allKeyword", "@serviceEntityCollection");

    /**
     * Template for traversal based on number of hops.
     */
    public static final String TRAVERSAL_HOP_TEMPLATE =
            "FOR origin IN @inputs\n" +
            "FOR v,e,p IN 1..{{hops}} {{direction}} origin GRAPH @graph\n" +
            "FILTER LENGTH(p.edges) == {{hops}}\n" +
            "LET service_entity = LAST(p.vertices)\n" +
            "{{#filters}}\n" +
            "{{{filter}}}\n" +
            "{{/filters}}\n" +
            "{{pagination}}\n" +
            "RETURN service_entity._id";

    /**
     * The bind vars for {@link #TRAVERSAL_HOP_TEMPLATE}.
     */
    public static final Collection<String> TRAVERSAL_HOP_BIND_VARS =
            ImmutableList.of("inputs", "graph");

    /**
     * Template for traversal based on a property filter.
     */
    public static final String TRAVERSAL_SEARCH_TEMPLATE =
            "FOR origin IN @inputs\n" +
            "FOR v,e,p IN 1..100 {{direction}} origin GRAPH @graph\n" +
            "LET service_entity = v\n" +
            "{{{condition}}}\n" +
            "{{#filters}}\n" +
            "{{{filter}}}\n" +
            "{{/filters}}\n" +
            "{{pagination}}\n" +
            "RETURN service_entity._id";

    /**
     * The bind vars for  {@link #TRAVERSAL_SEARCH_TEMPLATE}.
     */
    public static final Collection<String> TRAVERSAL_COND_BIND_VARS =
            ImmutableList.of("inputs", "graph");

    public static final Map<Filter.Type, Template> templateMapper = new EnumMap<>(Filter.Type.class);

    public static final Map<Filter.Type, Collection<String>> bindVarsMapper = new EnumMap<>(Filter.Type.class);

    static {
        try {
            // Compile the templates and create the mappings at startup.
            final Handlebars handlebars = new Handlebars();
            final Template propSearch = handlebars.compileInline(PROPERTY_SEARCH_TEMPLATE);
            final Template traversalHop = handlebars.compileInline(TRAVERSAL_HOP_TEMPLATE);
            final Template traversalCond = handlebars.compileInline(TRAVERSAL_SEARCH_TEMPLATE);
            templateMapper.put(Filter.Type.PROPERTY_STRING, propSearch);
            templateMapper.put(Filter.Type.PROPERTY_NUMERIC, propSearch);
            templateMapper.put(Filter.Type.TRAVERSAL_HOP, traversalHop);
            templateMapper.put(Filter.Type.TRAVERSAL_COND, traversalCond);

            bindVarsMapper.put(Filter.Type.PROPERTY_STRING, PROPERTY_SEARCH_BIND_VARS);
            bindVarsMapper.put(Filter.Type.PROPERTY_NUMERIC, PROPERTY_SEARCH_BIND_VARS);
            bindVarsMapper.put(Filter.Type.TRAVERSAL_HOP, TRAVERSAL_HOP_BIND_VARS);
            bindVarsMapper.put(Filter.Type.TRAVERSAL_COND, TRAVERSAL_COND_BIND_VARS);
        } catch (IOException e) {
            // rethow
            throw new Error(e);
        }
    }
}
