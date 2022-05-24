package com.vmturbo.kibitzer;

import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

/**
 * A class to provide helpful commentary during execution of the Kibitzer. It's what a kibitzer
 * does, after all.
 */
public class KibitzerChatter {
    private static final Logger logger = LogManager.getLogger();

    private static final String[] KIBITZ_MESSAGES = new String[]{
            "Are you sure you want to do that?",
            "I was tossing and turning all night. Didn't sleep a wink!",
            "I heard you broke up with Pat. Good move! I never liked Pat.",
            "I don't think you're doing that right. Want me to do it for you? I don't mind.",
            "[Yaaaaawwwnnn...]",
            "Ugh! My back is killing me. Can we switch chairs?",
            "Are you planning to finish that sandwich?",
            "You don't work out much, do you?",
            "You're right, you're right. I'll shut up. Not another word, I promise!",
            "Wait, wait... lemme... umm... I think... umm... Oh, never mind, we're good!",
            "OMG, what was that?... Wait, you didn't hear that? Seriously?!!",
            "... Purple haze... dum dum dum...  da da dum dum... 'scuze me while I kiss this guy",
            "You should really get that checked out. I'm sure it's nothing, but still..."
    };

    private final Random rand = new Random();
    private ScheduledExecutorService threadPool;

    /**
     * Start producing "helpful" commentary.
     */
    public void start() {
        this.threadPool = Executors.newScheduledThreadPool(1, getChatterThreadFactory());
        logger.info("Hey, mind if I watch? I know you're busy, so I'll be quiet as a mouse. "
                + "You won't even know I'm here.");
        scheduleChatter();
    }

    /**
     * Stop the chatter.
     */
    public void stop() {
        threadPool.shutdownNow();
        logger.info("Wow, that was amazing! I'd say we both learned a lot. Want to get a coffee?");
    }

    @NotNull
    private ThreadFactory getChatterThreadFactory() {
        return new ThreadFactoryBuilder().setDaemon(true)
                .setNameFormat("kibitzer-chatter")
                .build();
    }

    private void scheduleChatter() {
        // schedule our next helpful comment for anywhere from 5 to 20 minutes from mow
        int sleepSecs = 300 + rand.nextInt(900);
        threadPool.schedule(this::speak, sleepSecs, TimeUnit.SECONDS);
    }

    private void speak() {
        logger.info(KIBITZ_MESSAGES[rand.nextInt(KIBITZ_MESSAGES.length)]);
        scheduleChatter();
    }
}
