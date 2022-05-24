package com.vmturbo.kibitzer;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Import;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.PropertiesPropertySource;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.stereotype.Component;

import com.vmturbo.components.common.BaseVmtComponent.ContextConfigurationException;
import com.vmturbo.components.common.config.PropertiesLoader;
import com.vmturbo.components.common.featureflags.FeatureFlagManager;
import com.vmturbo.components.common.featureflags.PropertiesLoaderFeatureFlagEnablementStore;
import com.vmturbo.kibitzer.KibitzerDb.DbMode;
import com.vmturbo.kibitzer.activities.ActivityRegistry;
import com.vmturbo.kibitzer.activities.KibitzerActivity;
import com.vmturbo.kibitzer.activities.KibitzerActivity.KibitzerActivityException;
import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.sql.utils.DbEndpointResolver;
import com.vmturbo.sql.utils.DbEndpointsConfig;

/**
 * Kibitzer is designed to run alongside another component, in its own pod. (This architectural
 * choice may morph, e.g. into a side-car container in the associated pod, with zero replicas by
 * default.)
 *
 * <p>Kibitzers are not part of a normal deployment - and in fact, they are not currently managed
 * by operator. They can be used to launch advanced diagnostics, performance tests, etc, in a a live
 * environment, without building those capabilities into the components themselves.</p>
 *
 * <p>The kibitzer has full access to the code and resources of the component to which it attaches,
 * by means of a (transitive) dependency on the component's module. This means, for example, that
 * the kibitzer can gain access to the component's database or - more cautiously - a copy of that
 * database created on-the-fly by flyway.</p>
 *
 * <p>At present there are a few places where Kibitzer is limited from attaching to any component
 * other than history, but the intent is to remove that limitation.</p>
 */
@Component
@Import({KibitzerDb.class, DbEndpointsConfig.class, ActivityRegistry.class})
public class Kibitzer implements ApplicationListener<ContextRefreshedEvent> {
    private static final Logger logger = LogManager.getLogger();
    private static KibitzerComponent component;
    private static String[] args;

    @Autowired
    private KibitzerDb db;

    @Autowired
    ApplicationContext context;

    @Autowired
    ActivityRegistry activityRegistry;

    @Value("${enableKibitzerChatter:false}")
    boolean enableKibitzerChatter;

    private final KibitzerChatter chatter = new KibitzerChatter();

    /**
     * Launch a kibitzer as a main program.
     *
     * @param args argument lists - not used at present
     * @throws ContextConfigurationException if there's a problem loading config properties
     */
    public static void main(String[] args)
            throws ContextConfigurationException {
        if (args[0].equals("--describe")) {
            new ActivityDescriber(Arrays.copyOfRange(args, 1, args.length)).describeAll();
        } else {
            Kibitzer.component = KibitzerComponent.named(args[0]);
            Kibitzer.args = Arrays.copyOfRange(args, 1, args.length);
            runKibitzer();
        }
    }

    private static void runKibitzer() throws ContextConfigurationException {
        ConfigurableEnvironment env = new StandardEnvironment();
        PropertiesLoader.addConfigurationPropertySources(env, component.getName());
        applyKibitzerOverrides(env);
        FeatureFlagManager.setStore(new PropertiesLoaderFeatureFlagEnablementStore(env));
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
        context.setEnvironment(env);
        List<Class<?>> configClasses = new ArrayList<>();
        configClasses.add(Kibitzer.class);
        for (KibitzerComponent component : KibitzerComponent.nonWildcardValues()) {
            configClasses.add(component.info().getDbPropertyProviderClass());
        }
        context.register(configClasses.toArray(new Class<?>[0]));
        context.refresh();
    }

    private static void applyKibitzerOverrides(ConfigurableEnvironment env) {
        Properties props = new Properties();
        props.setProperty(DbEndpointResolver.USE_CONNECTION_POOL, "false");
        env.getPropertySources().addFirst(
                new PropertiesPropertySource("kibitzer-overrides", props));
    }

    /**
     * Handle an application event.
     *
     * @param event the event to respond to
     */
    @Override
    public void onApplicationEvent(@Nonnull ContextRefreshedEvent event) {
        Runtime.getRuntime().addShutdownHook(new Thread(this::onShutdown));
        if (enableKibitzerChatter) {
            chatter.start();
        }
        List<KibitzerActivity<?, ?>> activities = configureActivities();
        Map<KibitzerActivity<?, ?>, DSLContext> dsls = seedEndpoints(activities);
        initActivities(activities, dsls);
        Map<KibitzerActivity<?, ?>, Optional<?>> runResults = runActivities(activities);
        reportResultsForActivities(runResults);

        // In more recent versions of Spring, the `AnnotationConfigApplicationContext` class we
        // use in Kibitzer extend `ConfigurableApplicationContext`, which has a `close()` method
        // that can be used to terminate the application. But we can't do that here, so we rely on
        // the termination hook Spring registers with the JVM
        System.exit(0);
    }

    private void reportResultsForActivities(Map<KibitzerActivity<?, ?>, Optional<?>> results) {
        results.forEach((activity, optResult) -> optResult.ifPresent(
                result -> reportActivityResult(activity, result)));
    }

    private <T, U> void reportActivityResult(KibitzerActivity<T, U> activity, Object result) {
        //noinspection unchecked
        U castResult = (U)result;
        activity.report(castResult);
    }

    private List<KibitzerActivity<?, ?>> configureActivities() {
        Map<String, AtomicInteger> idMap = new HashMap<>();
        return Arrays.stream(args)
                .map(spec -> configureActivity(spec, idMap))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());
    }

    private Optional<KibitzerActivity<?, ?>> configureActivity(
            KibitzerActivity<?, ?> activity, String[] configSegments)
            throws KibitzerActivityException {
        return Optional.of(activity.newInstance())
                .map(a -> fixComponent(a, activity.getComponent()))
                .flatMap(a -> a.configure(configSegments));
    }

    private Optional<KibitzerActivity<?, ?>> configureActivity(
            String spec, Map<String, AtomicInteger> idMap) {
        String[] segments = spec.split(":");
        String tag = segments[0];
        return activityRegistry.getActivity(component, tag)
                .flatMap(this::newActivityInstance)
                .flatMap(activity -> {
                    activity.setIdNo(idMap.computeIfAbsent(tag, t -> new AtomicInteger(0))
                            .incrementAndGet());
                    String[] configSegments = Arrays.copyOfRange(segments, 1, segments.length);
                    try {
                        return configureActivity(activity, configSegments);
                    } catch (KibitzerActivityException e) {
                        logger.error("Invalid configuration spec '{}'", spec, e);
                    }
                    return Optional.empty();
                });
    }

    private Optional<KibitzerActivity<?, ?>> newActivityInstance(KibitzerActivity<?, ?> activity) {
        try {
            return Optional.of(activity.newInstance());
        } catch (KibitzerActivityException e) {
            logger.info("Failed to create new instance of {} activity ({})",
                    activity.getTag(), activity.getClass().getSimpleName());
            return Optional.empty();
        }
    }

    private <T, U> KibitzerActivity<T, U> fixComponent(KibitzerActivity<T, U> activity,
            KibitzerComponent templateComponent) {
        if (activity.getComponent() == KibitzerComponent.ANY) {
            activity.setComponent(component);
        }
        return activity;
    }

    private Map<KibitzerActivity<?, ?>, Optional<?>> runActivities(
            List<KibitzerActivity<?, ?>> activities) {
        Map<ExecutorService, KibitzerActivity<?, ?>> executors = new HashMap<>();
        Map<KibitzerActivity<?, ?>, List<?>> runResultsByActivity = new HashMap<>();
        Map<KibitzerActivity<?, ?>, Optional<?>> resultsByActivity = new HashMap<>();
        for (KibitzerActivity<?, ?> activity : activities) {
            logger.info("Activity {} is ready to execute", activity);
        }
        for (KibitzerActivity<?, ?> activity : activities) {
            Pair<ExecutorService, List<?>> runObjects = runActivity(activity);
            executors.put(runObjects.getLeft(), activity);
            runResultsByActivity.put(activity, runObjects.getRight());
        }
        while (!executors.isEmpty()) {
            ExecutorService executor = executors.keySet().iterator().next();
            if (Thread.currentThread().isInterrupted()) {
                executor.shutdownNow();
                executors.remove(executor);
            } else {
                try {
                    boolean terminated = executor.awaitTermination(Long.MAX_VALUE,
                            TimeUnit.MILLISECONDS);
                    if (terminated) {
                        KibitzerActivity<?, ?> activity = executors.get(executor);
                        try {
                            resultsByActivity.put(activity, finishActivity(activity,
                                    runResultsByActivity.get(activity)));
                        } catch (KibitzerActivityException e) {
                            logger.error("Activity {} failed during finalization",
                                    activity, e);
                        }
                        executors.remove(executor);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
        return resultsByActivity;
    }

    private <T, U> Optional<U> finishActivity(KibitzerActivity<T, U> activity, List<?> runResults)
            throws KibitzerActivityException {
        //noinspection unchecked
        List<T> castRunResults = (List<T>)runResults;
        Optional<U> activityResult = activity.finish(castRunResults);
        if (activity.getDbMode() == DbMode.COPY) {
            db.onActivityComplete(activity);
        }
        return activityResult;
    }

    private Map<KibitzerActivity<?, ?>, DSLContext> seedEndpoints(
            List<KibitzerActivity<?, ?>> activities) {
        Map<KibitzerActivity<?, ?>, DSLContext> dsls = new HashMap<>();
        for (KibitzerActivity<?, ?> activity : activities) {
            DbEndpoint endpoint = db.getDatabase(activity, context).orElse(null);
            try {
                dsls.put(activity, endpoint != null ? endpoint.dslContext() : null);
            } catch (SQLException | UnsupportedDialectException | InterruptedException e) {
                logger.error("DSL context seeding failed for activity {}",
                        activity.getTag(), e);
            }
        }
        return dsls;
    }

    private void initActivities(
            List<KibitzerActivity<?, ?>> activities, Map<KibitzerActivity<?, ?>, DSLContext> dsls) {
        List<KibitzerActivity<?, ?>> fails = new ArrayList<>();
        for (KibitzerActivity<?, ?> activity : activities) {
            try {
                activity.init(dsls.get(activity), db, context);
            } catch (KibitzerActivityException e) {
                logger.error("Activity {} failed initialization", activity.getTag(), e);
                fails.add(activity);
            }
        }
        if (!fails.isEmpty()) {
            logger.warn("Activities with failed initialization will not run: {}", fails);
            activities.removeAll(fails);
            fails.forEach(dsls::remove);
        }
    }

    private <T> Pair<ExecutorService, List<?>> runActivity(KibitzerActivity<T, ?> activity) {
        final int[] repetition = new int[]{0};
        final int maxRepetition = activity.getRuns();
        final boolean forever = maxRepetition == 0;
        final List<T> runResults = new ArrayList<>();
        ThreadFactory fac = new ThreadFactoryBuilder()
                .setNameFormat("kibitzact-" + activity.getTag())
                .build();
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(1, fac);
        final Runnable runnable = () -> {
            if (forever || repetition[0]++ < maxRepetition) {
                try {
                    activity.run(repetition[0]).ifPresent(runResults::add);
                } catch (KibitzerActivityException e) {
                    logger.error("Failed to execute activity {}", activity.getTag(), e);
                }
                if (!forever && repetition[0] >= maxRepetition) {
                    executor.shutdown();
                }
            }
        };
        long period = Math.max(activity.getSchedule().toMillis(), 1L);
        executor.scheduleAtFixedRate(runnable, 0L, period, TimeUnit.MILLISECONDS);
        return Pair.of(executor, runResults);
    }

    private void onShutdown() {
        if (enableKibitzerChatter) {
            chatter.stop();
        }
    }
}
