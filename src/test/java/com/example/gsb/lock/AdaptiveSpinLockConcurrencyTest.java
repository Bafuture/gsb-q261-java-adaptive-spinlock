package com.example.gsb.lock;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class AdaptiveSpinLockConcurrencyTest {

    @Test
    @Timeout(120)
    void sharedCounterHasNoLostUpdatesAndCriticalSectionIsExclusive() throws Exception {
        int threads = 8;
        int perThread = 50_000;

        AdaptiveSpinLock lock = new AdaptiveSpinLock();
        long[] counter = {0};
        AtomicInteger inCriticalSection = new AtomicInteger();
        AtomicInteger maxConcurrent = new AtomicInteger();

        List<Thread> workers = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            Thread worker = new Thread(() -> {
                for (int i = 0; i < perThread; i++) {
                    lock.lock();
                    try {
                        int inside = inCriticalSection.incrementAndGet();
                        maxConcurrent.accumulateAndGet(inside, Math::max);
                        counter[0]++;
                        inCriticalSection.decrementAndGet();
                    } finally {
                        lock.unlock();
                    }
                }
            });
            worker.setDaemon(true);
            workers.add(worker);
        }
        workers.forEach(Thread::start);
        for (Thread worker : workers) {
            worker.join();
        }

        assertThat(counter[0]).isEqualTo((long) threads * perThread);
        assertThat(maxConcurrent.get()).isEqualTo(1);
        assertThat(inCriticalSection.get()).isZero();
        assertThat(lock.isLocked()).isFalse();
    }

    @Test
    @Timeout(60)
    void manyTimedOutWaitersLeaveNoResidualState() throws Exception {
        AdaptiveSpinLock lock = new AdaptiveSpinLock();
        lock.lock();
        try {
            int waiters = 6;
            AtomicInteger failures = new AtomicInteger();
            List<Thread> threads = new ArrayList<>();
            for (int i = 0; i < waiters; i++) {
                Thread t = new Thread(() -> {
                    try {
                        if (!lock.tryLock(80, TimeUnit.MILLISECONDS)) {
                            failures.incrementAndGet();
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
                t.setDaemon(true);
                threads.add(t);
            }
            threads.forEach(Thread::start);
            for (Thread t : threads) {
                t.join();
            }
            assertThat(failures.get()).isEqualTo(waiters);
            assertThat(lock.isLocked()).isTrue();
        } finally {
            lock.unlock();
        }
        // 全部超时后，取消节点应已被惰性清理，锁可被新等待者正常获取。
        Thread successor = new Thread(() -> {
            lock.lock();
            lock.unlock();
        });
        successor.start();
        successor.join(20_000);
        assertThat(successor.isAlive()).isFalse();
        assertThat(lock.isLocked()).isFalse();
    }

    @Test
    @Timeout(60)
    void manyInterruptedWaitersLeaveNoResidualState() throws Exception {
        AdaptiveSpinLock lock = new AdaptiveSpinLock();
        lock.lock();
        try {
            int waiters = 6;
            AtomicInteger interrupted = new AtomicInteger();
            List<Thread> threads = new ArrayList<>();
            for (int i = 0; i < waiters; i++) {
                Thread t = new Thread(() -> {
                    try {
                        lock.lockInterruptibly();
                        lock.unlock();
                    } catch (InterruptedException e) {
                        interrupted.incrementAndGet();
                    }
                });
                t.setDaemon(true);
                threads.add(t);
            }
            threads.forEach(Thread::start);
            Thread.sleep(200);
            for (Thread t : threads) {
                t.interrupt();
            }
            for (Thread t : threads) {
                t.join();
            }
            assertThat(interrupted.get()).isEqualTo(waiters);
            assertThat(lock.isLocked()).isTrue();
        } finally {
            lock.unlock();
        }
        Thread successor = new Thread(() -> {
            lock.lock();
            lock.unlock();
        });
        successor.start();
        successor.join(20_000);
        assertThat(successor.isAlive()).isFalse();
        assertThat(lock.isLocked()).isFalse();
    }

    @Test
    @Timeout(120)
    void mixedLockOperationsKeepCounterConsistent() throws Exception {
        int threads = 6;
        int perThread = 10_000;

        AdaptiveSpinLock lock = new AdaptiveSpinLock();
        long[] counter = {0};
        AtomicReference<Throwable> error = new AtomicReference<>();

        List<Thread> workers = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            final int index = t;
            Thread worker = new Thread(() -> {
                try {
                    for (int i = 0; i < perThread; i++) {
                        if (i % 7 == 0) {
                            if (!lock.tryLock(50, TimeUnit.MILLISECONDS)) {
                                continue;
                            }
                        } else if (index % 2 == 0) {
                            lock.lock();
                        } else {
                            lock.lockInterruptibly();
                        }
                        try {
                            counter[0]++;
                        } finally {
                            lock.unlock();
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (Throwable e) {
                    error.compareAndSet(null, e);
                }
            });
            worker.setDaemon(true);
            workers.add(worker);
        }
        workers.forEach(Thread::start);
        for (Thread worker : workers) {
            worker.join();
        }

        assertThat(error.get()).isNull();
        assertThat(counter[0]).isBetween((long) (threads * perThread * 0.9),
                (long) threads * perThread);
        assertThat(lock.isLocked()).isFalse();
    }
}
