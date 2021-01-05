package com.vmturbo.history.stats.snapshots;

/**
 * {@link InformationState} is an interface make sure that
 * all concrete classes implementing it have the required methods.
 */
public interface InformationState {

    /**
     * Whether information is stored in
     * {@InformationState} or not.
     *
     * @return boolean
     */
    boolean isEmpty();

    /**
     * returns if information state is
     * in multiple key state.
     *
     * @return boolean
     * */
    boolean isMultiple();
}
