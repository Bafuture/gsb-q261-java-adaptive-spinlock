package com.example.gsb.lock;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.LockSupport;

/**
 * 自适应自旋锁（Adaptive Spin Lock）。
 *
 * <h2>总体结构</h2>
 * <ul>
 *   <li>{@code owner}：{@link AtomicReference}&lt;{@link Thread}&gt;，保存锁的持有者。
 *       加锁用 {@code compareAndSet(null, current)} 抢占，解锁用 {@code compareAndSet(t, null)}
 *       归还，全部基于 CAS，无 {@code synchronized}。</li>
 *   <li>{@code holds}：重入计数。只有持有者线程访问，普通 {@code int} 即可，无需 volatile；
 *       加锁时若发现 owner 是自己则 +1，解锁时 -1，归零才真正 CAS 释放锁。</li>
 *   <li>{@code spinLimit}：当前自旋轮数上限（自适应值）。加锁先自旋，自旋耗尽后进入挂起。</li>
 * </ul>
 *
 * <h2>自适应策略</h2>
 * <p>每一轮自旋 = 检查一次 owner 并尝试一次 CAS。策略根据"最近一次竞争结果"在线更新 spinLimit：
 * <ul>
 *   <li>自旋阶段直接抢到锁：近期临界区短、自旋划算，spinLimit 加倍（×2），多自旋少挂起；</li>
 *   <li>自旋耗尽、被迫 park：临界区较长，自旋浪费 CPU，spinLimit 减半（×0.5），更早挂起；</li>
 *   <li>挂起入栈后的复查中直接拿到锁（锁恰好在自旋耗尽后释放）：spinLimit 加倍。</li>
 * </ul>
 * 取值被限制在 [{@link #MIN_SPIN_LIMIT}, {@link #MAX_SPIN_LIMIT}] 之间。
 * 对 spinLimit 的并发更新用 CAS 去重，同一次竞争最多被调整一次。
 *
 * <h2>自旋阶段的退让</h2>
 * <p>每轮自旋失败后根据 CPU 拓扑选择退让方式，判定依据是 JVM 可见的可用处理器数
 * {@link Runtime#availableProcessors()}（容器 / CPU 亲和性限制下该接口通常反映配额核数）：
 * <ul>
 *   <li>多核（&gt;1）：调用 {@link Thread#onSpinWait()}，x86 上编译为 {@code PAUSE}、
 *       AArch64 上为 {@code YIELD}。PAUSE 次数随轮数指数增长（1,2,4,... 直到
 *       {@link #MAX_PAUSE} 上限），让争抢同一缓存行的多个自旋者错开节奏；
 *       不主动放弃 CPU，等待延迟最低——这是短临界区想要的效果；</li>
 *   <li>单核（==1）：自旋毫无意义（持锁线程无法并行执行），直接 {@link Thread#yield()}
 *       让出执行权，避免空转烧 CPU。</li>
 * </ul>
 *
 * <h2>自旋者配额</h2>
 * <p>TTAS 自旋的瓶颈是所有自旋者反复读取 owner 所在的缓存行：自旋者越多，锁交接越慢。
 * 因此用 {@code spinnerCount} 限制同时自旋的线程数（默认 {@code max(2, CPU/8)}），
 * 拿不到配额的线程跳过自旋直接挂起，把缓存行让给少数自旋者。
 *
 * <h2>挂起队列</h2>
 * <p>用 {@link WaiterNode} 组成无锁 Treiber 栈（{@code waiters} 做 push/pop CAS）。
 * 入栈后必须再检查一次锁状态，防止"锁刚释放、还没 park"的丢唤醒竞态。
 * unlock 从栈顶弹出节点并 {@link LockSupport#unpark} 对应线程，跳过已取消节点。
 * 超时 / 中断放弃的等待者把节点标记为 cancelled：若节点在栈顶则尽力 CAS 弹出，
 * 否则留给后续 unlock 的弹出循环惰性清理，避免并发摘除中间节点的竞态。
 *
 * <h2>中断与超时的持锁保证</h2>
 * <p>{@link #tryLock(long, TimeUnit)} 超时、{@link #lockInterruptibly()} 收到中断时，
 * 调用线程若尚未抢到锁只会返回 false / 抛 {@link InterruptedException}，绝不触碰
 * {@code holds}；每轮挂起使用全新节点。因此失败退出后锁状态（owner/holds）
 * 与该线程完全无关，不存在任何残留持锁状态。
 */
public class AdaptiveSpinLock implements Lock {

    private static final int MIN_SPIN_LIMIT = 32;
    private static final int MAX_SPIN_LIMIT = 100_000;

    /** 单轮自旋退让的最大 PAUSE 次数（指数退让上限）。 */
    private static final int MAX_PAUSE = 256;

    /**
     * 允许同时自旋的最大线程数。自旋者过多会同时 hammer 锁变量所在的缓存行，
     * 反而拖慢交接；超出配额的线程直接进入挂起等待。
     */
    private static int defaultMaxSpinners() {
        return Math.max(2, Runtime.getRuntime().availableProcessors() / 8);
    }

    private static final int DEFAULT_INITIAL_SPIN_LIMIT = 1_024;

    private final AtomicReference<Thread> owner = new AtomicReference<>();
    private int holds;

    private final AtomicInteger spinLimit;

    private final AtomicReference<WaiterNode> waiters = new AtomicReference<>();

    private final boolean multiCore;

    private final int maxSpinners;

    private final AtomicInteger spinnerCount = new AtomicInteger();

    private final LongAdder spinSuccessCount = new LongAdder();
    private final LongAdder parkCount = new LongAdder();
    private final LongAdder totalSpinRounds = new LongAdder();
    private final LongAdder spinAttempts = new LongAdder();

    public AdaptiveSpinLock() {
        this(DEFAULT_INITIAL_SPIN_LIMIT,
                Runtime.getRuntime().availableProcessors() > 1,
                defaultMaxSpinners());
    }

    /** 测试用构造器：可指定初始自旋上限与退让模式（自旋者配额取默认值）。 */
    AdaptiveSpinLock(int initialSpinLimit, boolean multiCore) {
        this(initialSpinLimit, multiCore, defaultMaxSpinners());
    }

    /** 测试用构造器：可指定初始自旋上限、退让模式与自旋者配额。 */
    AdaptiveSpinLock(int initialSpinLimit, boolean multiCore, int maxSpinners) {
        if (initialSpinLimit < MIN_SPIN_LIMIT || initialSpinLimit > MAX_SPIN_LIMIT) {
            throw new IllegalArgumentException("initialSpinLimit out of range: " + initialSpinLimit);
        }
        if (maxSpinners < 1) {
            throw new IllegalArgumentException("maxSpinners must be >= 1");
        }
        this.spinLimit = new AtomicInteger(initialSpinLimit);
        this.multiCore = multiCore;
        this.maxSpinners = maxSpinners;
    }

    // --------------------------------------------------------------- 加锁

    @Override
    public void lock() {
        Thread current = Thread.currentThread();
        if (tryAcquire(current)) {
            return;
        }
        try {
            acquire(current, false, 0L);
        } catch (InterruptedException e) {
            // 不可中断模式不会抛出，这里只是恢复中断标志。
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {
        if (Thread.interrupted()) {
            throw new InterruptedException();
        }
        Thread current = Thread.currentThread();
        if (tryAcquire(current)) {
            return;
        }
        acquire(current, true, 0L);
    }

    @Override
    public boolean tryLock() {
        return tryAcquire(Thread.currentThread());
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
        if (unit == null) {
            throw new NullPointerException("unit");
        }
        if (Thread.interrupted()) {
            throw new InterruptedException();
        }
        Thread current = Thread.currentThread();
        if (tryAcquire(current)) {
            return true;
        }
        if (time <= 0) {
            return false;
        }
        return acquire(current, true, System.nanoTime() + unit.toNanos(time));
    }

    /**
     * 核心获取流程：先自旋，自旋耗尽则挂起等待，循环直到成功 / 超时 / 被中断。
     *
     * @param interruptible 是否响应中断
     * @param deadline      超时时刻（nanoTime）；0 表示不限时
     * @return 是否获取成功（仅限时模式可能返回 false）
     */
    private boolean acquire(Thread current, boolean interruptible, long deadline)
            throws InterruptedException {
        int observedLimit = spinLimit.get();

        for (;;) {
            if (interruptible && Thread.interrupted()) {
                throw new InterruptedException();
            }
            if (deadline != 0L && System.nanoTime() - deadline >= 0) {
                return false;
            }

            // ---------- 阶段 1：自旋 ----------
            SpinResult result = spinPhase(current, interruptible, deadline, observedLimit);
            switch (result) {
                case ACQUIRED:
                    return true;
                case TIMED_OUT:
                    return false;
                case INTERRUPTED:
                    throw new InterruptedException();
                case EXHAUSTED:
                    observedLimit = shrink(observedLimit);
                    break;
                case SKIPPED:
                    // 自旋者配额已满：不自旋，直接去挂起。
                    break;
                default:
                    throw new AssertionError(result);
            }

            // ---------- 阶段 2：挂起等待 ----------
            WaiterNode node = new WaiterNode(current);
            pushWaiter(node);
            try {
                // 入栈后必须复查：锁可能在入栈与 park 之间被释放。
                if (owner.get() == null && owner.compareAndSet(null, current)) {
                    holds = 1;
                    grow(spinLimit.get());
                    return true;
                }
                if (interruptible && Thread.interrupted()) {
                    throw new InterruptedException();
                }
                if (deadline != 0L) {
                    long remaining = deadline - System.nanoTime();
                    if (remaining <= 0) {
                        return false;
                    }
                    parkCount.increment();
                    LockSupport.parkNanos(this, remaining);
                } else {
                    parkCount.increment();
                    LockSupport.park(this);
                }
                if (Thread.interrupted()) {
                    if (interruptible) {
                        throw new InterruptedException();
                    }
                    // lock() 不响应中断：恢复中断标志，继续抢锁。
                    Thread.currentThread().interrupt();
                }
            } finally {
                cancelWaiter(node);
            }

            // 被唤醒 / 超时后回到循环顶部：重新检查中断与截止时间，可能再自旋一轮。
        }
    }

    private enum SpinResult { ACQUIRED, TIMED_OUT, INTERRUPTED, EXHAUSTED, SKIPPED }

    /**
     * 自旋阶段。自旋者配额未满才参与自旋，获取自旋配额与归还（finally）配对，
     * 保证在自旋中成功 / 超时 / 中断退出时都不泄漏配额。
     */
    private SpinResult spinPhase(Thread current, boolean interruptible, long deadline, int limit)
            throws InterruptedException {
        int currentSpinners;
        boolean spinning = false;
        while ((currentSpinners = spinnerCount.get()) < maxSpinners) {
            if (spinnerCount.compareAndSet(currentSpinners, currentSpinners + 1)) {
                spinning = true;
                break;
            }
        }
        if (!spinning) {
            return SpinResult.SKIPPED;
        }
        try {
            int pause = 1;
            for (int round = 0; round < limit; round++) {
                if (owner.get() == null && owner.compareAndSet(null, current)) {
                    holds = 1;
                    totalSpinRounds.add(round + 1L);
                    spinAttempts.increment();
                    spinSuccessCount.increment();
                    grow(limit);
                    return SpinResult.ACQUIRED;
                }
                if (interruptible && Thread.interrupted()) {
                    totalSpinRounds.add(round + 1L);
                    spinAttempts.increment();
                    return SpinResult.INTERRUPTED;
                }
                if (deadline != 0L && (round & 15) == 0
                        && System.nanoTime() - deadline >= 0) {
                    totalSpinRounds.add(round + 1L);
                    spinAttempts.increment();
                    return SpinResult.TIMED_OUT;
                }
                pause = backoff(pause);
            }
            totalSpinRounds.add(limit);
            spinAttempts.increment();
            return SpinResult.EXHAUSTED;
        } finally {
            spinnerCount.decrementAndGet();
        }
    }

    private boolean tryAcquire(Thread current) {
        if (owner.compareAndSet(null, current)) {
            holds = 1;
            return true;
        }
        if (owner.get() == current) {
            holds++;
            return true;
        }
        return false;
    }

    // --------------------------------------------------------------- 解锁

    @Override
    public void unlock() {
        Thread current = Thread.currentThread();
        if (owner.get() != current) {
            throw new IllegalMonitorStateException("current thread does not hold the lock");
        }
        int remaining = --holds;
        if (remaining > 0) {
            return;
        }
        if (remaining < 0) {
            throw new IllegalMonitorStateException("negative reentrancy count");
        }
        if (!owner.compareAndSet(current, null)) {
            throw new IllegalMonitorStateException("owner changed unexpectedly");
        }
        signalWaiter();
    }

    /** 弹出栈顶等待者并唤醒；跳过并清理已取消的节点。 */
    private void signalWaiter() {
        for (;;) {
            WaiterNode head = waiters.get();
            if (head == null) {
                return;
            }
            if (waiters.compareAndSet(head, head.next)) {
                if (!head.cancelled) {
                    LockSupport.unpark(head.thread);
                    return;
                }
                // 栈顶已取消：继续弹出下一个，直到唤醒一个有效等待者或栈空。
            }
        }
    }

    // --------------------------------------------------------------- 等待栈

    private void pushWaiter(WaiterNode node) {
        for (;;) {
            WaiterNode head = waiters.get();
            node.next = head;
            if (waiters.compareAndSet(head, node)) {
                return;
            }
        }
    }

    /**
     * 取消一个等待节点：标记 cancelled；若它正好是栈顶则尽力 CAS 弹出，
     * 否则留给 {@link #signalWaiter()} 的弹出循环惰性清理。
     */
    private void cancelWaiter(WaiterNode node) {
        node.cancelled = true;
        for (int i = 0; i < 4; i++) {
            WaiterNode head = waiters.get();
            if (head != node) {
                return;
            }
            if (waiters.compareAndSet(node, node.next)) {
                return;
            }
        }
    }

    // --------------------------------------------------------------- 自适应

    private void grow(int observed) {
        int updated = Math.min(MAX_SPIN_LIMIT, Math.max(MIN_SPIN_LIMIT, observed * 2));
        spinLimit.compareAndSet(observed, updated);
    }

    private int shrink(int observed) {
        int updated = Math.max(MIN_SPIN_LIMIT, observed / 2);
        if (spinLimit.compareAndSet(observed, updated)) {
            return updated;
        }
        return spinLimit.get();
    }

    /**
     * 自旋退让，返回下一轮使用的 PAUSE 次数。
     * 多核：指数递增的 {@link Thread#onSpinWait()}（x86 PAUSE / AArch64 YIELD），
     * 让争抢同一缓存行的自旋者错开节奏，避免 16 个核同时 hammer 锁变量；
     * 单核：直接 {@link Thread#yield()} 让出 CPU，自旋无意义。
     */
    private int backoff(int pause) {
        if (multiCore) {
            for (int i = 0; i < pause; i++) {
                Thread.onSpinWait();
            }
            return Math.min(pause << 1, MAX_PAUSE);
        }
        Thread.yield();
        return pause;
    }

    // --------------------------------------------------------------- 统计与检视

    /** 自旋阶段成功获取锁的次数。 */
    public long spinSuccessCount() {
        return spinSuccessCount.sum();
    }

    /** 自旋耗尽转入挂起（park）的次数。 */
    public long parkCount() {
        return parkCount.sum();
    }

    /** 每次自旋尝试平均花费的轮数（重入快速路径不计入）。 */
    public double averageSpinRounds() {
        long attempts = spinAttempts.sum();
        return attempts == 0 ? 0.0 : (double) totalSpinRounds.sum() / attempts;
    }

    /** 当前自适应自旋轮数上限。 */
    public int currentSpinLimit() {
        return spinLimit.get();
    }

    /** 是否按多核策略退让（onSpinWait）；false 表示单核策略（yield）。 */
    public boolean isMultiCoreMode() {
        return multiCore;
    }

    /** 是否有任意线程持有该锁（测试与诊断用）。 */
    public boolean isLocked() {
        return owner.get() != null;
    }

    @Override
    public Condition newCondition() {
        throw new UnsupportedOperationException();
    }

    private static final class WaiterNode {
        final Thread thread;
        volatile WaiterNode next;
        volatile boolean cancelled;

        WaiterNode(Thread thread) {
            this.thread = thread;
        }
    }
}
