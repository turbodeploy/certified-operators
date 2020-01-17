package com.vmturbo.mediation.client.it;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.vmturbo.mediation.common.ProbeConfigurationLoadException;
import com.vmturbo.mediation.common.tests.probes.AccountValuesWithDflt;
import com.vmturbo.mediation.common.tests.probes.EternalWorkingProbe;
import com.vmturbo.mediation.common.tests.probes.NullAccountDefinitionsProbe;
import com.vmturbo.mediation.common.tests.probes.NullSupplyChainProbe;
import com.vmturbo.mediation.common.tests.probes.PredefinedAccountProbe;
import com.vmturbo.mediation.common.tests.probes.ProbeWithDefaultAccValues;
import com.vmturbo.mediation.common.tests.util.NotValidatableProbe;
import com.vmturbo.mediation.common.tests.util.ProbeConfig;
import com.vmturbo.mediation.common.tests.util.IntegrationTestProbeConfiguration;
import com.vmturbo.mediation.common.tests.util.ProbeValidator;
import com.vmturbo.mediation.common.tests.util.SdkProbe;
import com.vmturbo.mediation.common.tests.util.SdkTarget;
import com.vmturbo.mediation.common.tests.util.TestConstants;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;
import com.vmturbo.platform.common.dto.Discovery.ValidationResponse;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.platform.sdk.common.PredefinedAccountDefinition;

/**
 * Test case to perform integration testing of communications between remote mediation container and
 * VMTurbo main server.
 */
public class ExternalProbeTestIT extends AbstractIntegrationTest {

    private ProbeValidator validator;
    private Collection<SdkTarget> registeredTargets;
    private Collection<SdkContainer> registeredContainers;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void createDefaultMediationContainer() throws Exception {
        validator = new ProbeValidator(getRemoteMediationInterface(), getThreadPool());
        registeredTargets = new ArrayList<>();
        registeredContainers = new ArrayList<>();
    }

    /**
     * Creates container, holding a probe. Probe registration is not finished, when this method
     * exits.
     *
     * @param probeConfig probe configuration to create container for
     * @param probeType probe type
     * @return probe representation
     * @throws Exception if some errors occur.
     */
    private SdkProbe registerProbe(IntegrationTestProbeConfiguration probeConfig, String probeType)
                    throws Exception {
        getLogger().debug("Creating probe with id: " + probeType);

        final SdkProbe probe = new SdkProbe(probeConfig, probeType);
        final SdkTarget target = new SdkTarget(probe, "target-" + probeType);
        final SdkContainer container = startSdkComponent(probe);
        registeredContainers.add(container);
        registeredTargets.add(target);
        return probe;
    }

    /**
     * Tests responding probes. Mediation container does not go down and connection should never be
     * timed out.
     *
     * @throws Exception on test exception
     */
    @Test
    public void testRespondingProbes() throws Exception {
        registerProbe(ProbeConfig.Empty, "EMPTY-PROBE");
        registerProbe(ProbeConfig.Critical, "INVALID-PROBE");
        registerProbe(ProbeConfig.Warning, "WARNING-PROBE");
        registerProbe(ProbeConfig.Failing, "FAILING-PROBE");
        registerProbe(ProbeConfig.ErrorThrowing, "THROW-ERROR-PROBE");
        registerProbe(ProbeConfig.Long, "LONG-WORKING-PROBE");
        awaitContainersRegister();

        validator.checkTargets(registeredTargets);
        checkTargetIdentifiers();
    }

    /**
     * Tests probe with complex names (probe types), containing various special symbols.
     *
     * @throws Exception on exception occurred
     */
    @Test
    public void testSpecialSymbols() throws Exception {
        registerProbe(ProbeConfig.Empty, "Probe with spaces");
        registerProbe(ProbeConfig.Empty, "Punctuation.,!*ddd_+=-||/\\");
        registerProbe(ProbeConfig.Empty, "ProbeWithNumbers0123456789");
        registerProbe(ProbeConfig.Empty, "0123456789");
        awaitContainersRegister();

        validator.checkTargets(registeredTargets);
        checkTargetIdentifiers();
    }

    @Test
    public void testNullSupplyChain() throws Exception {
        final SdkProbe probe = registerProbe(new NotValidatableProbe(NullSupplyChainProbe.class),
                        "NullSupplyChain");
        Thread.sleep(1000);
        Assert.assertFalse(isProbeRegistered(probe));
    }

    /**
     * Tests that probe with null account definitions will not be registered in probe store.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testAccountDefinition() throws Exception {
        registerProbe(new NotValidatableProbe(NullAccountDefinitionsProbe.class),
                "NullAccountDefinitionChain");
        awaitContainersRegister();
        Assert.assertEquals(Collections.emptyMap(), getProbeStore().getProbes());
    }

    /**
     * Tets for correct discovery of probe with predefined fields in account values.
     *
     * @throws Exception on exceptions occur
     */
    @Test
    public void testPredefinedFields() throws Exception {
        final SdkProbe probe = registerProbe(
                        new NotValidatableProbe(PredefinedAccountProbe.class, ProbeConfig.Empty),
                        testName.getMethodName());
        awaitContainersRegister();
        final String targetId = "Entity-1";
        final SdkTarget target = new SdkTarget(probe, targetId);
        target.getAccountValues().add(SdkTarget.makeAccountValue(
                        PredefinedAccountDefinition.UseSSL.getValueKey(), Boolean.toString(true)));
        final DiscoveryResponse response1 = validator.requestDiscovery(target).get();
        Assert.assertEquals(1, response1.getEntityDTOCount());
        Assert.assertEquals(1, response1.getErrorDTOCount());
        Assert.assertEquals(targetId, response1.getEntityDTO(0).getId());
        Assert.assertEquals("true", response1.getErrorDTO(0).getDescription());
    }

    /**
     * Tests for correct discovery of probe with default fields (both set and unset).
     *
     * @throws Exception on exceptions occur
     */
    @Test
    public void testDefaultFields() throws Exception {
        final SdkProbe probe = registerProbe(
                new NotValidatableProbe(ProbeWithDefaultAccValues.class, ProbeConfig.Empty),
                testName.getMethodName());
        awaitContainersRegister();
        final String targetId = "Entity-1";
        final SdkTarget targetDflt = new SdkTarget(probe, targetId);
        final DiscoveryResponse response1 = validator.requestDiscovery(targetDflt).get();
        Assert.assertEquals(1, response1.getEntityDTOCount());
        Assert.assertEquals(AccountValuesWithDflt.DFLT_FIELD_VAL, response1.getEntityDTO(0)
                .getId());

        final String fieldVal =  "some-test-string";
        final SdkTarget target2 = new SdkTarget(probe, targetId);
        target2.getAccountValues()
                .add(SdkTarget.makeAccountValue(AccountValuesWithDflt.DFLT_FIELD_NAME, fieldVal));
        final DiscoveryResponse response2 = validator.requestDiscovery(target2).get();
        Assert.assertEquals(1, response2.getEntityDTOCount());
        Assert.assertEquals(fieldVal, response2.getEntityDTO(0)
                .getId());
    }

    /**
     * Tests mediation container is shut down during validation request processing
     *
     * @throws Exception on exception occurred
     */
    @Test
    public void testContainerShutDownValidate() throws Exception {
        final SdkProbe longProbe = registerProbe(new NotValidatableProbe(EternalWorkingProbe.class),
                "TIMEOUT-VLD-PROBE");
        final SdkTarget target = new SdkTarget(longProbe, "tgt-" + testName.getMethodName());
        awaitContainersRegister();
        final Future<ValidationResponse> validation = validator.requestValidation(target);
        EternalWorkingProbe.getLatch(target.getTargetId())
                .await(TestConstants.TIMEOUT, TimeUnit.SECONDS);
        closeAllContainers();
        try {
            validation.get(TestConstants.TIMEOUT, TimeUnit.SECONDS);
            Assert.fail("Expected connection closed exception");
        } catch (Exception e) {
            assertConnectionClosed(e);
        }
    }

    /**
     * Tests mediation container is shut down during discovery request processing
     *
     * @throws Exception on exception occurred
     */
    @Test
    public void testContainerShutDownDiscover() throws Exception {
        final SdkProbe longProbe = registerProbe(new NotValidatableProbe(EternalWorkingProbe.class),
                "TIMEOUT-DSC-PROBE");
        final SdkTarget target = new SdkTarget(longProbe, "tgt-" + testName.getMethodName());
        awaitContainersRegister();
        final Future<DiscoveryResponse> discovery = validator.requestDiscovery(target);
        EternalWorkingProbe.getLatch(target.getTargetId())
                .await(TestConstants.TIMEOUT, TimeUnit.SECONDS);
        closeAllContainers();
        try {
            discovery.get(TestConstants.TIMEOUT, TimeUnit.SECONDS);
            Assert.fail("Expected connection closed exception");
        } catch (ExecutionException e) {
            assertConnectionClosed(e);
        }
    }

    /**
     * Method expects, that one of its causes identify that connection has been closed.
     *
     * @param exception exception to examine
     */
    private void assertConnectionClosed(final Exception exception) {
        int causeLevel = 0;
        Throwable nextException = exception;
        while (nextException != null) {
            if (nextException.getMessage()
                    .contains("Communication transport to remote probe closed")) {
                return;
            }
            nextException = nextException.getCause();
            if (causeLevel++ > 100) {
                Assert.fail("Expected 'transport closed' exception");
            }
        }
        Assert.fail("Expected 'transport closed' exception");
    }


    // TODO: skiped tests here are:
    // testProbeWithProperties
    // testProbeWTemplates

    private void awaitContainersRegister() throws InterruptedException {
        for (SdkContainer container : registeredContainers) {
            container.awaitRegistered();
        }
    }

    private void closeAllContainers() throws Exception {
        for (SdkContainer container: registeredContainers) {
            container.close();
        }
        for (SdkContainer container: registeredContainers) {
            container.awaitUnregistered();
        }
    }

    /**
     * Method checks, that probes has successfully and correctly reported their target identifier
     * fields.
     */
    private void checkTargetIdentifiers() {
        for (SdkTarget target : registeredTargets) {
            final SdkProbe probe = target.getProbe();
            final long probeId = getProbeId(probe);
            final ProbeInfo probeInfo = getProbeStore().getProbe(probeId).get();
            Assert.assertEquals(
                    Collections.singletonList(probe.getProbeConfig().getTargetIdentifier()),
                            probeInfo.getTargetIdentifierFieldList());
        }
    }

    /**
     * Matcher that goes through all the causes of the thrown exception to match at least one of
     * them with the specified exception class.
     */
    private class CauseMatcher extends BaseMatcher<Throwable> {

        private final Class<? extends Throwable> clazz;
        private final Set<Throwable> found = new HashSet<>();

        CauseMatcher(Class<? extends Throwable> clazz) {
            this.clazz = clazz;
        }

        @Override
        public boolean matches(Object item) {
            final Throwable throwable = (Throwable)item;
            Throwable next = throwable;
            while (next != null) {
                if (!found.add(next)) {
                    return false;
                }
                if (clazz.isAssignableFrom(next.getClass())) {
                    return true;
                }
                next = next.getCause();
            }
            return false;
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("does not have cause " + clazz);
        }

    }
}
