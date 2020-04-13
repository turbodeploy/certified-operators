package com.vmturbo.topology.processor.template;

/**
 * Exception thrown if there are templates missing when users try to query templates.
 */
public class TemplatesNotFoundException extends TopologyEntityConstructorException {

    private static final long serialVersionUID = -4963419544915049766L;

    public TemplatesNotFoundException(final long expect, final long real) {
        super("Can not find all templates, expect to find " + expect
                +  " templates, but only found " + real + " templates");
    }
}
