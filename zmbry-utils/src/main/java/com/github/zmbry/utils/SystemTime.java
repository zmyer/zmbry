package com.github.zmbry.utils;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

/**
 * @author zifeng
 *
 */
public class SystemTime extends Time {

    private static SystemTime time = new SystemTime();

    public static Time getInstance() {
        return time;
    }

    private SystemTime() {
    }

    @Override
    public long milliseconds() {
        return System.currentTimeMillis();
    }

    @Override
    public long nanoseconds() {
        return System.nanoTime();
    }

    @Override
    public long seconds() {
        return System.currentTimeMillis() / MsPerSec;
    }

    @Override
    public void sleep(long ms) throws InterruptedException {
        Thread.sleep(ms);
    }

    @Override
    public void wait(Object o, long ms) throws InterruptedException {
        o.wait(ms);
    }

    @Override
    public void await(Condition c, long ms) throws InterruptedException {
        c.await(ms, TimeUnit.MILLISECONDS);
    }
}
