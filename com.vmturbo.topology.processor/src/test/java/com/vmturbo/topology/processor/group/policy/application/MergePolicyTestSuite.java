package com.vmturbo.topology.processor.group.policy.application;



import org.junit.runners.Suite;
import org.junit.runner.RunWith;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        MergePolicyComputationClusterTest.class,
        MergePolicyStorageClusterTest.class,
        MergePolicyDataecenterTest.class})
public class MergePolicyTestSuite {
    //nothing
}