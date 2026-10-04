package com.example.gsb.lock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

class AdaptiveSpinLockTest {

    // ------------------------------------------------------------------
    // 互斥 + 共享计数器不丢更新
    // ------------------------------------------------------------------

    @Test
    @Timeout(60)
    void mutualExclusionAndNoLostUpdatesUnderStress() throws InterruptedException {
        AdaptiveSpinLock lock = new AdaptiveSpinLock();
        long[] counter = {0};
        AtomicBoolean violation = new AtomicBoolean(false);
        AtomicInteger inside = new AtomicInteger();

        int threads = Math.max(4, Runtime.getRuntime().availableProcessors());
        int iterations = 20_000;
        List<Thread> workers = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            Thread worker = new Thread(() -> {
                for (int i = 0; i < iterations; i++) {
                    lock.lock();
                    try {
                        if (inside.getAndIncrement() != 0) {
                            violation.set(true); // 临界区内同时存在两个线程
                        }
                        counter[0]++;
                        inside.decrementAndGet();
                    } finally {
                        lock.unlock();
                    }
                }
            });
            workers.add(worker);
            worker.start();
        }
        for (Thread worker : workers) {
            worker.join();
        }

        assertThat(violation).as("临界区互斥被破坏").isFalse();
        assertThat(counter[0]).as("共享计数器丢更新").isEqualTo((long) threads * iterations);
        assertThat(lock.isLocked()).isFalse();
        assertThat(lock.getQueueLength()).isZero();
    }

    // ------------------------------------------------------------------
    // 可重入
    // ------------------------------------------------------------------

    @Test
    void reentrantLockingAndHoldCount() {
        AdaptiveSpinLock lock = new AdaptiveSpinLock();
        assertThat(lock.getHoldCount()).isZero();

        lock.lock();
        assertThat(lock.getHoldCount()).isEqualTo(1);
        assertThat(lock.isHeldByCurrentThread()).isTrue();

        lock.lock(); // 重入第二次
        lock.lock(); // 重入第三次
        assertThat(lock.getHoldCount()).isEqualTo(3);

        lock.unlock();
        assertThat(lock.getHoldCount()).isEqualTo(2);
        assertThat(lock.isLocked()).as("重入未完全退出时锁仍被持有").isTrue();

        lock.unlock();
        lock.unlock();
        assertThat(lock.getHoldCount()).isZero();
        assertThat(lock.isLocked()).isFalse();
    }

    @Test
    void unlockWithoutHoldingThrows() {
        AdaptiveSpinLock lock = new AdaptiveSpinLock();
        assertThatThrownBy(lock::unlock).isInstanceOf(IllegalMonitorStateException.class);
    }

    @Test
    void tryLockIsReentrant() {
        AdaptiveSpinLock lock = new AdaptiveSpinLock();
        assertThat(lock.tryLock()).isTrue();
        assertThat(lock.tryLock()).isTrue(); // 重入
        assertThat(lock.getHoldCount()).isEqualTo(2);
        lock.unlock();
        lock.unlock();
    }

    // ------------------------------------------------------------------
    // 自适应调整
    // ------------------------------------------------------------------

    @Test
    @Timeout(60)
    void spinBudgetGrowsOnSpinSuccess() throws InterruptedException {
        // 初始预算取最小值：持续竞争下大量加锁会走自旋路径并成功，
        // 按“成功则翻倍（采样）”的策略，预算应显著增长
        AdaptiveSpinLock lock = new AdaptiveSpinLock(AdaptiveSpinLock.MIN_SPIN, false);
        int initial = lock.currentSpinBudget();

        int threads = 8;
        int iterations = 50_000;
        AtomicLong counter = new AtomicLong();
        List<Thread> workers = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            Thread worker = new Thread(() -> {
                for (int i = 0; i < iterations; i++) {
                    lock.lock();
                    try {
                        counter.incrementAndGet();
                    } finally {
                        lock.unlock();
                    }
                }
            });
            workers.add(worker);
            worker.start();
        }
        for (Thread worker : workers) {
            worker.join();
        }

        assertThat(counter.get()).isEqualTo((long) threads * iterations);
        assertThat(lock.stats().spinSuccesses()).as("竞争下应出现自旋成功").isPositive();
        assertThat(lock.currentSpinBudget())
                .as("自旋成功后预算应增大（翻倍封顶）")
                .isGreaterThan(initial);
    }

    @Test
    @Timeout(60)
    void spinBudgetShrinksWhenSpinningFails() throws InterruptedException {
        AdaptiveSpinLock lock = new AdaptiveSpinLock(AdaptiveSpinLock.MAX_SPIN, false);
        int initial = lock.currentSpinBudget();

        // 预算是按 1/16 采样减半的，需要足够多的“自旋耗尽转挂起”事件保证调整必然发生
        int workers = 256;
        List<Thread> threads = new ArrayList<>();
        lock.lock(); // 主线程长时间持锁，工作线程必然自旋耗尽后转挂起
        try {
            for (int i = 0; i < workers; i++) {
                Thread worker = new Thread(() -> {
                    lock.lock();
                    lock.unlock();
                });
                threads.add(worker);
                worker.start();
            }
            awaitCondition(() -> lock.stats().spinFailures() >= workers, 30_000);
            awaitCondition(() -> lock.getQueueLength() == workers, 30_000);

            assertThat(lock.currentSpinBudget())
                    .as("大量自旋失败转挂起后预算应减小（减半保底）")
                    .isLessThan(initial);
            assertThat(lock.currentSpinBudget()).isGreaterThanOrEqualTo(AdaptiveSpinLock.MIN_SPIN);
        } finally {
            lock.unlock(); // 释放后所有工作线程依次被唤醒并完成
        }
        for (Thread worker : threads) {
            worker.join(10_000);
            assertThat(worker.isAlive()).as("工作线程应全部完成").isFalse();
        }
        assertThat(lock.isLocked()).isFalse();
        assertThat(lock.getQueueLength()).isZero();
    }

    // ------------------------------------------------------------------
    // 超时
    // ------------------------------------------------------------------

    @Test
    @Timeout(30)
    void tryLockTimesOutAndLeavesNoHeldState() throws InterruptedException {
        AdaptiveSpinLock lock = new AdaptiveSpinLock();
        lock.lock(); // 主线程持锁
        AtomicBoolean acquired = new AtomicBoolean(true);
        AtomicReference<Throwable> error = new AtomicReference<>();
        try {
            Thread worker = new Thread(() -> {
                try {
                    acquired.set(lock.tryLock(200, TimeUnit.MILLISECONDS));
                } catch (InterruptedException e) {
                    error.set(e);
                }
            });
            worker.start();
            worker.join(5_000);

            assertThat(error.get()).isNull();
            assertThat(acquired).as("超时后应返回 false").isFalse();
            assertThat(lock.getQueueLength()).as("超时后不应残留等待者").isZero();
            assertThat(worker.isAlive()).isFalse();
        } finally {
            lock.unlock();
        }
        // 锁必须仍可被获取，说明超时线程没有留下持锁状态
        assertThat(lock.tryLock()).isTrue();
        assertThat(lock.getHoldCount()).isEqualTo(1);
        lock.unlock();
        assertThat(lock.isLocked()).isFalse();
    }

    @Test
    void tryLockWithZeroTimeoutIsNonBlocking() throws InterruptedException {
        AdaptiveSpinLock lock = new AdaptiveSpinLock();
        lock.lock();
        try {
            assertThat(lock.tryLock(0, TimeUnit.MILLISECONDS)).isTrue(); // 重入，立即成功
            lock.unlock();
        } finally {
            lock.unlock();
        }
        assertThat(lock.isLocked()).isFalse();
    }

    // ------------------------------------------------------------------
    // 中断
    // ------------------------------------------------------------------

    @Test
    @Timeout(30)
    void lockInterruptiblyReleasesWaitOnInterrupt() throws InterruptedException {
        AdaptiveSpinLock lock = new AdaptiveSpinLock();
        lock.lock(); // 主线程持锁
        AtomicBoolean interrupted = new AtomicBoolean(false);
        try {
            Thread worker = new Thread(() -> {
                try {
                    lock.lockInterruptibly();
                } catch (InterruptedException e) {
                    interrupted.set(true);
                }
            });
            worker.start();
            awaitCondition(() -> lock.getQueueLength() == 1, 10_000);
            worker.interrupt();
            worker.join(5_000);

            assertThat(worker.isAlive()).as("被中断的线程应退出").isFalse();
            assertThat(interrupted).as("应抛出 InterruptedException").isTrue();
            assertThat(lock.getQueueLength()).as("中断后不应残留等待者").isZero();
        } finally {
            lock.unlock();
        }
        // 锁必须仍可被获取，说明被中断线程没有留下持锁状态
        assertThat(lock.tryLock()).isTrue();
        lock.unlock();
        assertThat(lock.isLocked()).isFalse();
    }

    @Test
    @Timeout(30)
    void uninterruptibleLockDefersInterruptUntilAcquired() throws InterruptedException {
        AdaptiveSpinLock lock = new AdaptiveSpinLock();
        lock.lock();
        AtomicBoolean acquired = new AtomicBoolean(false);
        try {
            Thread worker = new Thread(() -> {
                lock.lock(); // 不可中断：中断只应被记录，最终仍拿到锁
                acquired.set(true);
                lock.unlock();
            });
            worker.start();
            awaitCondition(() -> lock.getQueueLength() == 1, 10_000);
            worker.interrupt();
            Thread.sleep(100);
            assertThat(acquired).as("不可中断加锁不应因中断放弃").isFalse();
        } finally {
            lock.unlock(); // 释放后工作线程应拿到锁
        }
        awaitCondition(acquired::get, 10_000);
    }

    // ------------------------------------------------------------------
    // 单核退让分支
    // ------------------------------------------------------------------

    @Test
    @Timeout(30)
    void singleCoreModeYieldsAndStillWorks() throws InterruptedException {
        AdaptiveSpinLock lock = new AdaptiveSpinLock(AdaptiveSpinLock.MIN_SPIN, true);
        AtomicLong counter = new AtomicLong();
        int threads = 4;
        int iterations = 5_000;
        List<Thread> workers = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            Thread worker = new Thread(() -> {
                for (int i = 0; i < iterations; i++) {
                    lock.lock();
                    try {
                        counter.incrementAndGet();
                    } finally {
                        lock.unlock();
                    }
                }
            });
            workers.add(worker);
            worker.start();
        }
        for (Thread worker : workers) {
            worker.join();
        }
        assertThat(counter.get()).isEqualTo((long) threads * iterations);
    }

    // ------------------------------------------------------------------
    // 工具
    // ------------------------------------------------------------------

    private static void busyWork(int iterations) {
        long value = 0;
        for (int i = 0; i < iterations; i++) {
            value += i * 31L;
        }
        if (value == Long.MIN_VALUE) { // 防止 JIT 优化掉循环
            throw new AssertionError();
        }
    }

    private static void awaitCondition(Check check, long timeoutMillis) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (!check.passed()) {
            if (System.nanoTime() >= deadline) {
                throw new AssertionError("等待条件超时");
            }
            Thread.sleep(5);
        }
    }

    @FunctionalInterface
    private interface Check {
        boolean passed();
    }
}
