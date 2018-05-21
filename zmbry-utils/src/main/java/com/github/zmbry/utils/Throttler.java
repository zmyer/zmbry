package com.github.zmbry.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zifeng
 *
 */
public class Throttler {
    private double desiredRatePerSec;
    private long checkIntervalMs;
    private boolean throttleDown;
    private final Object lock = new Object();
    private final Object waitGuard = new Object();
    private long periodStartNs;
    private double observedSoFar;
    private Logger logger = LoggerFactory.getLogger(getClass());
    private Time time;
    private boolean enabled;

    /**
     * @param desiredRatePerSec: The rate we want to hit in units/sec
     * @param checkIntervalMs: The interval at which to check our rate. If < 0, rate is checked on every call
     * @param throttleDown: Does throttling increase or decrease our rate?
     * @param time: The time implementation to use
     **/
    public Throttler(double desiredRatePerSec, long checkIntervalMs, boolean throttleDown, Time time) {
        this.desiredRatePerSec = desiredRatePerSec;
        this.checkIntervalMs = checkIntervalMs;
        this.throttleDown = throttleDown;
        this.time = time;
        this.observedSoFar = 0.0;
        this.periodStartNs = time.nanoseconds();
        this.enabled = true;
    }

    /**
     * Throttle if required
     * @param observed the newly observed units since the last time this method was called.
     */
    public void maybeThrottle(double observed) throws InterruptedException {
        synchronized (lock) {
            observedSoFar += observed;
            long now = time.nanoseconds();
            long elapsedNs = now - periodStartNs;

            // if we have completed an interval AND we have observed something, maybe
            // we should take a little nap
            if ((checkIntervalMs < 0 || elapsedNs > checkIntervalMs * Time.NsPerMs) && observedSoFar > 0) {
                double rateInSecs = elapsedNs > 0 ? (observedSoFar * Time.NsPerSec) / elapsedNs : Double.MAX_VALUE;
                if (throttleDown == rateInSecs > desiredRatePerSec) {
                    // solve for the amount of time to sleep to make us hit the desired rate
                    double desiredRateMs = desiredRatePerSec / Time.MsPerSec;
                    double elapsedMs = elapsedNs / Time.NsPerMs;
                    long sleepTime = Math.round(observedSoFar / desiredRateMs - elapsedMs);
                    if (sleepTime > 0) {
                        logger.trace(
                                "Natural rate is {} per second but desired rate is {}, sleeping for {} ms to compensate.",
                                rateInSecs, desiredRatePerSec, sleepTime);
                        synchronized (waitGuard) {
                            if (enabled) {
                                time.wait(waitGuard, sleepTime);
                            }
                        }
                    }
                }
                periodStartNs = now;
                observedSoFar = 0;
            }
        }
    }

    /**
     * Disable the throttler for good.
     */
    public void disable() {
        synchronized (waitGuard) {
            enabled = false;
            waitGuard.notify();
        }
    }

}
