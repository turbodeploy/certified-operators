package com.vmturbo.mediation.actionscript.executor;

/**
 * Status of an Action Script execution managed by an {@link ActionScriptExecutor}
 */
public enum ActionScriptExecutionStatus {
    // new ASE, never scheduled for execution
    NEW,
    // script is currently running on remote host
    RUNNING,
    // script has completed execution with normal exit status
    COMPLETE,
    // script completed execution with an abnormal exit status
    FAILED,
    // script is queued for execution but has not yet started
    QUEUED,
    // script was canceled while queued
    CANCELED_FROM_QUEUE,
    // script was canceled after it started running
    CANCELED,
    // script is malformed and cannot be scheduled for execution, or an exception was thrown
    // by the SSH library attempting to execute the script remotely.
    ERROR
}
