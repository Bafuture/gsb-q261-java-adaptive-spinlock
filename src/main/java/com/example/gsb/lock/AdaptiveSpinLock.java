package com.example.gsb.lock;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.LockSupport;

/**
 * 自适应可重入自旋锁。
 *
 * <p>设计要点：
 * <ul>
 *   <li><b>CAS 加锁</b>：锁状态为 {@code AtomicReference<Thread>}，CAS(null → owner) 抢锁；
 *       可重入通过“当前持有线程 + holdCount 计数”实现，每重入一次计数 +1，unlock 一次 -1，
 *       计数归零才真正释放锁。</li>
 *   <li><b>自适应自旋</b>：每次加锁先自旋 {@link #spinBudget} 次，失败则 park 挂起；
 *       自旋成功则预算翻倍（封顶 MAX_SPIN），自旋耗尽则预算减半（保底 MIN_SPIN），
 *       自旋预算随历史成功情况动态调整。</li>
 *   <li><b>自旋退让</b>：多核（availableProcessors &gt; 1）用 {@link Thread#onSpinWait()} 忙等待提示
 *       （x86 对应 PAUSE）；单核直接 {@link Thread#yield()} 让出 CPU——单核上持锁线程必须先被
 *       调度进来才能释放锁，继续自旋毫无意义。</li>
 *   <li><b>超时与中断</b>：{@code tryLock(timeout)} 与 {@code lockInterruptibly()} 在超时/中断时
 *       直接返回 false / 抛 InterruptedException；线程只有在 CAS 抢到锁后才算持有，超时与中断
 *       退出路径上均未持锁，不留下持锁状态。</li>
 *   <li><b>统计</b>：自旋成功次数、自旋失败转挂起次数、平均自旋轮数，见 {@link LockStats}。</li>
 * </ul>
 */
public final class AdaptiveSpinLock implements Lock {

    /** 自旋预算下界：至少自旋这么多次再考虑挂起。 */
    public static final int MIN_SPIN = 16;
    /** 自旋预算上界：避免在严重竞争下空转太久。 */
    public static final int MAX_SPIN = 1024;
    /** 初始自旋预算。 */
    public static final int DEFAULT_SPIN = 256;
    /** 自适应调整的采样率：每 SAMPLE_RATE 次事件平均调整一次预算。 */
    static final int SAMPLE_RATE = 16;

    /** 锁状态：null 表示空闲，否则为持有线程。所有互斥语义由该引用的 CAS 保证。 */
    private final AtomicReference<Thread> owner = new AtomicReference<>();

    /**
     * 重入计数：仅由持有线程读写，无需原子类。
     * 可见性由 owner 引用的 CAS/set 释放-获取语义保证：解锁时 owner.set(null) 之前的所有写，
     * 对随后 CAS(null, current) 抢到锁的线程可见。
     */
    private int holdCount;

    /** 当前自旋预算，随历史成功情况自适应调整。 */
    private final AtomicInteger spinBudget;

    /** 等待线程队列（FIFO），用该对象自身的 monitor 保护，每次只唤醒队首，避免惊群。 */
    private final Deque<Thread> waiters = new ArrayDeque<>();

    /** 等待者数量的提示值：为空时 unlock 可完全跳过同步块（无锁快路径）。 */
    private final AtomicInteger waiterCount = new AtomicInteger();

    /** 是否按单核处理：决定自旋阶段用 onSpinWait 还是 yield。 */
    private final boolean singleCore;

    // ---- 统计 ----
    private final LongAdder spinSuccesses = new LongAdder();
    private final LongAdder spinFailures = new LongAdder();
    private final LongAdder totalSpinRounds = new LongAdder();
    private final LongAdder spinAttempts = new LongAdder();

    public AdaptiveSpinLock() {
        this(DEFAULT_SPIN, Runtime.getRuntime().availableProcessors() <= 1);
    }

    /**
     * @param initialSpinBudget 初始自旋预算（会被夹取到 [MIN_SPIN, MAX_SPIN]）
     * @param singleCore        是否按单核处理：true 时自旋阶段直接 yield，false 时用 onSpinWait 忙等待提示
     */
    public AdaptiveSpinLock(int initialSpinBudget, boolean singleCore) {
        this.spinBudget = new AtomicInteger(clamp(initialSpinBudget));
        this.singleCore = singleCore;
    }

    private static int clamp(int budget) {
        return Math.max(MIN_SPIN, Math.min(MAX_SPIN, budget));
    }

    // ------------------------------------------------------------------
    // 加锁
    // ------------------------------------------------------------------

    @Override
    public void lock() {
        Thread current = Thread.currentThread();
        if (tryAcquire(current)) {
            return;
        }
        int rounds = spinOnce(current, Long.MAX_VALUE);
        if (rounds >= 0) {
            onSpinSuccess(rounds);
            return;
        }
        onSpinExhausted();
        boolean interrupted = parkPhase(current, Long.MAX_VALUE, false);
        if (interrupted) {
            // 不可中断锁：吞掉中断，但按 Lock.lock() 约定恢复中断标记
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {
        Thread current = Thread.currentThread();
        if (Thread.interrupted()) {
            throw new InterruptedException();
        }
        if (tryAcquire(current)) {
            return;
        }
        int rounds = spinOnce(current, Long.MAX_VALUE);
        if (rounds >= 0) {
            onSpinSuccess(rounds);
            return;
        }
        onSpinExhausted();
        boolean interrupted = parkPhase(current, Long.MAX_VALUE, true);
        if (interrupted) {
            // parkPhase 保证返回 true 时当前线程未持锁，直接抛出
            throw new InterruptedException();
        }
    }

    @Override
    public boolean tryLock() {
        return tryAcquire(Thread.currentThread());
    }

    @Override
    public boolean tryLock(long timeout, TimeUnit unit) throws InterruptedException {
        Thread current = Thread.currentThread();
        if (Thread.interrupted()) {
            throw new InterruptedException();
        }
        if (tryAcquire(current)) {
            return true;
        }
        long timeoutNanos = unit.toNanos(timeout);
        if (timeoutNanos <= 0L) {
            return false;
        }
        long deadline = System.nanoTime() + timeoutNanos;
        int rounds = spinOnce(current, deadline);
        if (rounds >= 0) {
            onSpinSuccess(rounds);
            return true;
        }
        if (rounds == -2) {
            // 自旋阶段即超时：未持锁，直接失败
            return false;
        }
        onSpinExhausted();
        boolean interrupted = parkPhase(current, deadline, true);
        if (interrupted) {
            throw new InterruptedException();
        }
        return owner.get() == current; // 超时返回 false 时 owner 必然不是自己
    }

    // ------------------------------------------------------------------
    // 解锁
    // ------------------------------------------------------------------

    @Override
    public void unlock() {
        Thread current = Thread.currentThread();
        if (owner.get() != current) {
            throw new IllegalMonitorStateException("当前线程不持有锁");
        }
        if (--holdCount > 0) {
            return; // 重入尚未完全退出，不释放也不唤醒
        }
        owner.set(null); // 真正释放锁：此前的写对下一个抢到锁的线程可见
        if (waiterCount.get() > 0) { // 无等待者时跳过同步块（常见快路径）
            Thread next;
            synchronized (waiters) {
                next = waiters.peekFirst();
            }
            if (next != null) {
                LockSupport.unpark(next); // 只唤醒队首，避免惊群
            }
        }
    }

    @Override
    public java.util.concurrent.locks.Condition newCondition() {
        throw new UnsupportedOperationException("自适应自旋锁不支持 Condition");
    }

    // ------------------------------------------------------------------
    // 内部实现
    // ------------------------------------------------------------------

    /** 尝试获取一次锁：空闲则 CAS 抢锁并置重入计数为 1；被自己持有则重入计数 +1。 */
    private boolean tryAcquire(Thread current) {
        if (owner.compareAndSet(null, current)) {
            holdCount = 1;
            return true;
        }
        if (owner.get() == current) {
            holdCount++;
            return true;
        }
        return false;
    }

    /**
     * 自旋阶段：最多自旋当前预算轮，每轮先尝试抢锁，失败则退让。
     *
     * @param deadlineNanos 截止时间；Long.MAX_VALUE 表示不限时
     * @return 抢到锁返回实际自旋轮数（&ge;0）；预算耗尽返回 -1；超时返回 -2
     */
    private int spinOnce(Thread current, long deadlineNanos) {
        int budget = spinBudget.get();
        for (int round = 1; round <= budget; round++) {
            // TTAS：先普通读，看到锁空闲才 CAS，避免每轮 CAS 造成的缓存行乒乓
            if (owner.get() == null && tryAcquire(current)) {
                return round;
            }
            // 每 64 轮检查一次时间，避免 nanoTime 开销进入热点
            if (deadlineNanos != Long.MAX_VALUE && (round & 63) == 0
                    && System.nanoTime() >= deadlineNanos) {
                return -2;
            }
            backoff();
        }
        return -1;
    }

    /**
     * 自旋退让策略。
     * 多核：{@link Thread#onSpinWait()}——向 CPU 发出忙等待提示（x86 上为 PAUSE 指令），
     * 降低流水线冲刷与功耗，同时保持极低唤醒延迟。
     * 单核：{@link Thread#yield()}——单核上持锁线程只有被调度进来才能释放锁，自旋纯属浪费，
     * 直接让出 CPU。
     */
    private void backoff() {
        if (singleCore) {
            Thread.yield();
        } else {
            Thread.onSpinWait();
        }
    }

    /**
     * 挂起阶段：进入 FIFO 等待队列，被唤醒（或超时/中断）后先出队再尝试抢锁。
     *
     * @param deadlineNanos 截止时间；Long.MAX_VALUE 表示无限等待
     * @param interruptible 是否响应中断
     * @return true 表示等待期间被中断（此时保证未持锁）；false 表示正常返回（已持锁或超时）
     */
    private boolean parkPhase(Thread current, long deadlineNanos, boolean interruptible) {
        boolean interrupted = false;
        synchronized (waiters) {
            waiters.addLast(current);
            waiterCount.incrementAndGet();
        }
        try {
            for (;;) {
                if (interruptible && Thread.interrupted()) {
                    return true; // 未持锁，由调用方抛出 InterruptedException
                }
                if (owner.get() == null && tryAcquire(current)) {
                    return interrupted; // 已持锁；不可中断模式把中断标记带回去恢复
                }
                if (deadlineNanos != Long.MAX_VALUE) {
                    long remaining = deadlineNanos - System.nanoTime();
                    if (remaining <= 0L) {
                        return interrupted; // 超时，未持锁
                    }
                    LockSupport.parkNanos(Math.min(remaining, TimeUnit.MILLISECONDS.toNanos(1)));
                } else {
                    LockSupport.park();
                }
                if (!interruptible && Thread.interrupted()) {
                    interrupted = true; // 不可中断模式：记录中断，继续等待
                }
            }
        } finally {
            Thread next = null;
            synchronized (waiters) {
                waiters.remove(current); // 抢到锁/超时/中断都必须离开等待队列
                if (waiterCount.decrementAndGet() > 0) {
                    next = waiters.peekFirst();
                }
            }
            // 级联唤醒：自己因超时/中断退出时锁可能已空闲，不唤醒新队首会造成丢唤醒
            if (next != null && owner.get() == null) {
                LockSupport.unpark(next);
            }
        }
    }

    // ------------------------------------------------------------------
    // 自适应调整与统计
    // ------------------------------------------------------------------

    /**
     * 自旋成功：记录统计；以 1/16 概率把预算翻倍（封顶），说明当前竞争下自旋值得等待。
     * 采样调整是为了避免每次加锁都对共享的 spinBudget 做 CAS（高竞争下会成为瓶颈），
     * 统计上仍保持“成功则增大、失败则减小”的适应方向。
     */
    private void onSpinSuccess(int rounds) {
        spinSuccesses.increment();
        totalSpinRounds.add(rounds);
        spinAttempts.increment();
        if (rounds > 1 && ThreadLocalRandom.current().nextInt(SAMPLE_RATE) == 0) {
            spinBudget.getAndUpdate(budget -> Math.min(MAX_SPIN, budget * 2));
        }
    }

    /** 自旋耗尽转挂起：记录统计；以 1/16 概率把预算减半（保底），说明继续自旋是在浪费 CPU。 */
    private void onSpinExhausted() {
        spinFailures.increment();
        if (ThreadLocalRandom.current().nextInt(SAMPLE_RATE) == 0) {
            spinBudget.getAndUpdate(budget -> Math.max(MIN_SPIN, budget / 2));
        }
    }

    // ------------------------------------------------------------------
    // 观测接口（统计与状态查询）
    // ------------------------------------------------------------------

    /** 当前自旋预算（自适应调整后的值）。 */
    public int currentSpinBudget() {
        return spinBudget.get();
    }

    /** 当前线程的重入计数（未持有返回 0）。 */
    public int getHoldCount() {
        return owner.get() == Thread.currentThread() ? holdCount : 0;
    }

    /** 是否被当前线程持有。 */
    public boolean isHeldByCurrentThread() {
        return owner.get() == Thread.currentThread();
    }

    /** 是否被任意线程持有。 */
    public boolean isLocked() {
        return owner.get() != null;
    }

    /** 当前等待队列长度（用于测试与监控）。 */
    public int getQueueLength() {
        synchronized (waiters) {
            return waiters.size();
        }
    }

    /** 统计快照。 */
    public LockStats stats() {
        long attempts = spinAttempts.sum();
        long rounds = totalSpinRounds.sum();
        return new LockStats(
                spinSuccesses.sum(),
                spinFailures.sum(),
                attempts == 0 ? 0.0 : (double) rounds / attempts);
    }

    /** 统计快照：自旋成功次数、自旋失败转挂起次数、平均自旋轮数。 */
    public record LockStats(long spinSuccesses, long spinFailures, double averageSpinRounds) {
        @Override
        public String toString() {
            return String.format("LockStats{spinSuccesses=%d, spinFailures=%d, avgSpinRounds=%.2f}",
                    spinSuccesses, spinFailures, averageSpinRounds);
        }
    }
}
