package com.example.gsb.lock;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AdaptiveSpinLockTest {

    // ----------------------------------------------------------- 互斥

    @Test
    @Timeout(30)
    void mutualExclusionBetweenTwoThreads() throws InterruptedException {
        AdaptiveSpinLock lock = new AdaptiveSpinLock();
        long[] counter = {0};
        int iterations = 100_000;

        Thread t1 = new Thread(() -> {
            for (int i = 0; i < iterations; i++) {
                lock.lock();
                try {
                    counter[0]++;
                } finally {
                    lock.unlock();
                }
            }
        });
        Thread t2 = new Thread(() -> {
            for (int i = 0; i < iterations; i++) {
                lock.lock();
                try {
                    counter[0]++;
                } finally {
                    lock.unlock();
                }
            }
        });
        t1.start();
        t2.start();
        t1.join();
        t2.join();

        assertThat(counter[0]).isEqualTo(2L * iterations);
    }

    @Test
    void tryLockFailsWhileHeldByAnotherThread() throws InterruptedException {
        AdaptiveSpinLock lock = new AdaptiveSpinLock();
        lock.lock();
        try {
            AtomicBoolean acquired = new AtomicBoolean(true);
            Thread t = new Thread(() -> acquired.set(lock.tryLock()));
            t.start();
            t.join();
            assertThat(acquired).isFalse();
        } finally {
            lock.unlock();
        }
    }

    // ----------------------------------------------------------- 可重入

    @Test
    void reentrantLockingRequiresMatchingUnlocks() throws InterruptedException {
        AdaptiveSpinLock lock = new AdaptiveSpinLock();
        lock.lock();
        lock.lock();
        lock.lock();
        assertThat(lock.isLocked()).isTrue();

        lock.unlock();
        lock.unlock();
        // 重入计数未归零，锁仍被持有，其他线程抢不到。
        assertThat(lock.isLocked()).isTrue();
        AtomicBoolean acquired = new AtomicBoolean(true);
        Thread t = new Thread(() -> acquired.set(lock.tryLock()));
        t.start();
        t.join();
        assertThat(acquired).isFalse();

        lock.unlock();
        assertThat(lock.isLocked()).isFalse();
    }

    @Test
    void reentrantTryLockIncrementsHoldCount() {
        AdaptiveSpinLock lock = new AdaptiveSpinLock();
        assertThat(lock.tryLock()).isTrue();
        assertThat(lock.tryLock()).isTrue();
        lock.unlock();
        assertThat(lock.isLocked()).isTrue();
        lock.unlock();
        assertThat(lock.isLocked()).isFalse();
    }

    @Test
    void unlockWithoutHoldingThrows() throws InterruptedException {
        AdaptiveSpinLock lock = new AdaptiveSpinLock();
        assertThatThrownBy(lock::unlock).isInstanceOf(IllegalMonitorStateException.class);

        lock.lock();
        try {
            AtomicReference<Throwable> error = new AtomicReference<>();
            Thread t = new Thread(() -> {
                try {
                    lock.unlock();
                } catch (Throwable e) {
                    error.set(e);
                }
            });
            t.start();
            t.join();
            assertThat(error.get()).isInstanceOf(IllegalMonitorStateException.class);
        } finally {
            lock.unlock();
        }
    }

    // ----------------------------------------------------------- 自适应

    @Test
    @Timeout(30)
    void spinLimitShrinksOnParkThenGrowsAfterAcquire() throws InterruptedException {
        int initial = 64;
        AdaptiveSpinLock lock = new AdaptiveSpinLock(initial, true);

        lock.lock();
        Thread waiter = new Thread(() -> {
            lock.lock();
            lock.unlock();
        });
        waiter.start();
        // 保证等待线程自旋 64 轮耗尽并 park。
        Thread.sleep(100);
        int shrunk = lock.currentSpinLimit();
        // 自旋耗尽被迫挂起：上限收缩（64 -> 32）。
        assertThat(shrunk).isLessThan(initial);
        lock.unlock();
        waiter.join();

        assertThat(lock.parkCount()).isGreaterThanOrEqualTo(1);
        // 被唤醒后自旋抢到锁：上限重新放大（32 -> 64）。
        assertThat(lock.currentSpinLimit()).isGreaterThan(shrunk);
    }

    @Test
    @Timeout(30)
    void spinSuccessIsCountedAndAffectsAverage() throws InterruptedException {
        AdaptiveSpinLock lock = new AdaptiveSpinLock(64, true);

        lock.lock();
        Thread waiter = new Thread(() -> {
            lock.lock();
            lock.unlock();
        });
        waiter.start();
        Thread.sleep(100);
        lock.unlock();
        waiter.join();

        // 等待线程被唤醒后重新自旋并抢到锁，记为一次自旋成功。
        assertThat(lock.spinSuccessCount()).isGreaterThanOrEqualTo(1);
        assertThat(lock.averageSpinRounds()).isGreaterThan(0.0);
    }

    @Test
    void uncontendedLockDoesNotTouchSpinStats() {
        AdaptiveSpinLock lock = new AdaptiveSpinLock();
        lock.lock();
        lock.unlock();
        assertThat(lock.spinSuccessCount()).isZero();
        assertThat(lock.parkCount()).isZero();
        assertThat(lock.averageSpinRounds()).isZero();
    }

    // ----------------------------------------------------------- 退让模式

    @Test
    void backoffModeFollowsCpuCount() {
        assertThat(new AdaptiveSpinLock(64, true).isMultiCoreMode()).isTrue();
        assertThat(new AdaptiveSpinLock(64, false).isMultiCoreMode()).isFalse();
        boolean expected = Runtime.getRuntime().availableProcessors() > 1;
        assertThat(new AdaptiveSpinLock().isMultiCoreMode()).isEqualTo(expected);
    }

    // ----------------------------------------------------------- 超时

    @Test
    @Timeout(30)
    void tryLockTimesOutAndLeavesNoLockState() throws InterruptedException {
        AdaptiveSpinLock lock = new AdaptiveSpinLock();
        lock.lock();
        try {
            AtomicBoolean acquired = new AtomicBoolean(true);
            AtomicLongHolder elapsed = new AtomicLongHolder();
            Thread t = new Thread(() -> {
                long start = System.nanoTime();
                try {
                    acquired.set(lock.tryLock(150, TimeUnit.MILLISECONDS));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                elapsed.value = System.nanoTime() - start;
            });
            t.start();
            t.join();

            assertThat(acquired).isFalse();
            assertThat(elapsed.value).isGreaterThanOrEqualTo(TimeUnit.MILLISECONDS.toNanos(100));
            // 超时返回后锁仍属于主线程，等待线程不持有任何锁状态。
            assertThat(lock.isLocked()).isTrue();
        } finally {
            lock.unlock();
        }
        // 主线程释放后，之前的超时线程没有残留持锁，锁可正常再获取。
        assertThat(lock.tryLock()).isTrue();
        lock.unlock();
    }

    @Test
    void tryLockWithNonPositiveTimeoutDoesNotBlock() throws InterruptedException {
        AdaptiveSpinLock lock = new AdaptiveSpinLock();
        lock.lock();
        try {
            AtomicBoolean acquired = new AtomicBoolean(true);
            Thread t = new Thread(() -> {
                try {
                    acquired.set(lock.tryLock(0, TimeUnit.MILLISECONDS));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            t.start();
            t.join();
            assertThat(acquired).isFalse();
        } finally {
            lock.unlock();
        }
    }

    @Test
    @Timeout(30)
    void tryLockSucceedsBeforeTimeoutWhenLockReleased() throws InterruptedException {
        AdaptiveSpinLock lock = new AdaptiveSpinLock();
        lock.lock();
        AtomicBoolean acquired = new AtomicBoolean(false);
        Thread t = new Thread(() -> {
            try {
                acquired.set(lock.tryLock(5, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            if (acquired.get()) {
                lock.unlock();
            }
        });
        t.start();
        Thread.sleep(100);
        lock.unlock();
        t.join();
        assertThat(acquired).isTrue();
        assertThat(lock.isLocked()).isFalse();
    }

    // ----------------------------------------------------------- 中断

    @Test
    @Timeout(30)
    void lockInterruptiblyThrowsAndLeavesNoLockState() throws InterruptedException {
        AdaptiveSpinLock lock = new AdaptiveSpinLock();
        lock.lock();
        try {
            AtomicBoolean interrupted = new AtomicBoolean(false);
            CountDownLatch parked = new CountDownLatch(1);
            Thread t = new Thread(() -> {
                try {
                    parked.countDown();
                    lock.lockInterruptibly();
                } catch (InterruptedException e) {
                    interrupted.set(true);
                }
            });
            t.start();
            parked.await();
            Thread.sleep(100);
            t.interrupt();
            t.join();

            assertThat(interrupted).isTrue();
            // 中断后锁仍归主线程，被中断线程没有留下持锁状态。
            assertThat(lock.isLocked()).isTrue();
        } finally {
            lock.unlock();
        }
        assertThat(lock.tryLock()).isTrue();
        lock.unlock();
        assertThat(lock.isLocked()).isFalse();
    }

    @Test
    void lockInterruptiblyWithPreSetInterruptFlagThrowsImmediately() {
        AdaptiveSpinLock lock = new AdaptiveSpinLock();
        Thread.currentThread().interrupt();
        try {
            assertThatThrownBy(lock::lockInterruptibly)
                    .isInstanceOf(InterruptedException.class);
            assertThat(lock.isLocked()).isFalse();
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    @Timeout(30)
    void plainLockIgnoresInterruptAndKeepsWaiting() throws InterruptedException {
        AdaptiveSpinLock lock = new AdaptiveSpinLock();
        lock.lock();
        AtomicBoolean acquired = new AtomicBoolean(false);
        Thread t = new Thread(() -> {
            lock.lock();
            acquired.set(true);
            lock.unlock();
        });
        t.start();
        Thread.sleep(50);
        t.interrupt();
        Thread.sleep(50);
        // lock() 不响应中断：线程仍在等待。
        assertThat(acquired).isFalse();
        lock.unlock();
        t.join();
        assertThat(acquired).isTrue();
        assertThat(lock.isLocked()).isFalse();
    }

    private static final class AtomicLongHolder {
        volatile long value;
    }
}
