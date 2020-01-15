package com.vmturbo.clustermgr;

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.annotation.DirtiesContext.ClassMode;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.AnnotationConfigContextLoader;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.web.client.HttpClientErrorException;

/**
 * Test the exclusion for the ClusterMgrService - calls to either /cluster/diagnostics or
 * /admin/diagnostics must not overlap. A second call made while the first call is still
 * running must be rejected with HttpClientErrorException(TOO_MANY_REQUESTS).
 */
@RunWith(SpringJUnit4ClassRunner.class)
@WebAppConfiguration
@ContextConfiguration(loader = AnnotationConfigContextLoader.class,
    classes = {ClusterMgrServiceTestConfiguration.class})
@DirtiesContext(classMode = ClassMode.BEFORE_EACH_TEST_METHOD)
public class ClusterDiagsTest {
    private Logger log = LogManager.getLogger();
    @Autowired
    ConsulService consulServiceMock;
    @Autowired
    private ClusterMgrService clusterMgrService;

    /**
     * Test exclusion for collectComponentDiagnostics. If one is running, then a second call
     * should fail immediately with an HttpClientErrorException(TOO_MANY_REQUESTS) exception.
     *
     * @throws InterruptedException if the join() at the end of the test is interrupted
     */
    @Test
    public void testDiagsInProgressExclusion() throws InterruptedException {

        // no real output required
        final OutputStream mockStream = Mockito.mock(OutputStream.class);

        // the semaphore starts with 1 reservation already, so thread 1 will block
        // in the getValues() call mocked below
        Semaphore thread2finished = new Semaphore(1);
        thread2finished.acquire();
        log.info("thread2finished locked...");


        // Set a semaphore for thread2 to wait for before starting
        Semaphore thread1Started = new Semaphore(1);
        thread1Started.acquire();
        log.info("thread1started locked...");

        // collectComponentDiagnostics will make a  "getValues()" call to fetch the component types.
        // Set this call to block until thread 2 has finished.
        when(consulServiceMock.getAllServiceInstances()).thenAnswer((Answer)invocation -> {
            // signal that we've entered the first operation, under thread 1
            log.info("releasing thread1Started");
            thread1Started.release();
            // wait for thread 2 to finish
            log.info("waiting for thread2");
            thread2finished.acquire();
            log.info("thread2 finished...throwing BailException");
            // throw an unchecked exception, crashing out of the collectComponentDiagnostics()
            // call since we don't need the actual invocation
            throw new BailException("Bail!");
        });

        // place to store "test pass" from within thread 2 - assume false;
        final AtomicBoolean lockSucceeded = new AtomicBoolean(false);

        // thread 1 makes a collectComponentDiagnostics() call which will block
        Thread thread1 = new Thread(() -> {
            try {
                log.info("thread1 calling collectComponentDiagnostics()");
                clusterMgrService.collectComponentDiagnostics(mockStream);
                log.info("thread1 done with collectComponentDiagnostics()");
            } catch (IOException e) {
                // not expected
                e.printStackTrace();
            } catch (BailException e) {
                // expected! the actual collectComponentDiagnostics() execution isn't necessary
                log.info("accepted thread success");
            }
        }, "thread-1");

        // thread 2 makes a collectComponentDiagnostics() call, and since thread 1 is already
        // running the call will be rejected immediately with:
        // HttpClientErrorException(TOO_MANY_REQUESTS)
        // In either case, the semaphore is released, allowing thread 1 to finish.
        Thread thread2 = new Thread(() -> {
            try {
                log.info("thread2 calling collectComponentDiagnostics()");
                clusterMgrService.collectComponentDiagnostics(mockStream);
                log.info("thread2 done with collectComponentDiagnostics()");
            } catch (IOException e) {
                log.error("IOEException in thread 2: " , e);
            } catch (HttpClientErrorException e) {
                // test that the HTTP Status is as expected
                log.info("HttpClientErrorException", e);
                if (e.getStatusCode() == HttpStatus.TOO_MANY_REQUESTS) {
                    log.info("thread 2 succeeded");
                    lockSucceeded.set(true);
                }
            } finally {
                log.info("releasing thread2");
                thread2finished.release();
            }
        }, "thread-2");

        // start both threads; thread 1 will block waiting for thread2 to finish
        log.info("starting thread 1");
        thread1.start();
        // wait for thread1 to be running
        log.info("waiting for thread 1 to start");
        thread1Started.acquire();
        // now thread 2 can run; thread1 is waiting for thread2finished.
        log.info("starting thread 2");
        thread2.start();

        // wait for both threads to finish
        log.info("waiting for thread 1 to finish");
        thread1.join();
        log.info("waiting for thread 2 to finish");
        thread2.join();
        log.info("thread 2 finished");

        // check that thread 2 was rejected immediately with the correct HTTP exception.
        assertTrue(lockSucceeded.get());
    }

    /**
     * This unchecked exception is thrown from within the first test thread above, at the time
     * when the second test thread has released the semaphore and the first test thread
     * may exit (without exercising any of the "real" code to fetch diagnostics.
     */
    private static class BailException extends RuntimeException {
        BailException(String message) {
            super(message);
        }
    }
}