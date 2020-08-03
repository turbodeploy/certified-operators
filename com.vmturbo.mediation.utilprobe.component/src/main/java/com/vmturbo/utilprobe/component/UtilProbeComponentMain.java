package com.vmturbo.utilprobe.component;

/**
 * Helper class for starting UtilProbeComponent.
 * Since MediationComponentMain already has main method and we cannot override static methods,
 * there is a problem with creating main method in its derived class UtilProbeComponent.
 * So, this helper class has only one main method invoking the start of UtilProbeComponent.
 */
public class UtilProbeComponentMain {

    private UtilProbeComponentMain() {
        throw new IllegalStateException();
    }

    /**
     * Main method for the component.
     *
     * @param args
     *      Program arguments
     */
    public static void main(String[] args) {
        UtilProbeComponent.start();
    }
}
