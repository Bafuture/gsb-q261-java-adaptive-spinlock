package com.example.gsb.benchmark;

import com.example.gsb.lock.AdaptiveSpinLock;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 自适应自旋锁 vs {@code synchronized} 吞吐对比基准。
 *
 * <p>两种负载：
 * <ul>
 *   <li>短临界区：仅一次计数器自增（纳秒级）。此时阻塞/唤醒开销占主导，
 *       自适应自旋锁的自旋路径应明显占优；</li>
 *   <li>长临界区：临界区内忙等约 1 微秒（模拟一次小的本地计算）。此时自旋
 *       大概率耗尽转入挂起，两者吞吐应接近，自适应锁会自动调小自旋上限。</li>
 * </ul>
 *
 * <p>运行方式：{@code ./mvnw -q compile exec:java} 或先 package 后
 * {@code java -cp target/classes com.example.gsb.benchmark.SpinLockBenchmark}。
 * 单测中通过 {@link #runWorkload(Workload, int, long)} 做缩短版冒烟对比。
 */
public final class SpinLockBenchmark {

    /** 工作负载类型。 */
    public enum Workload {
        /** 短临界区：一次自增。 */
        SHORT,
        /** 长临界区：自增 + 约 1µs 忙等。 */
        LONG
    }

    /** 一次基准的结果。 */
    public static final class Result {
        public final String name;
        public final long operations;
        public final long elapsedNanos;
        public final long counterValue;

        Result(String name, long operations, long elapsedNanos, long counterValue) {
            this.name = name;
            this.operations = operations;
            this.elapsedNanos = elapsedNanos;
            this.counterValue = counterValue;
        }

        public double opsPerSecond() {
            return operations * 1_000_000_000.0 / elapsedNanos;
        }
    }

    private static final AtomicLong BLACKHOLE = new AtomicLong();

    /** 校准出忙等约 1µs 所需的自旋轮数（先预热 JIT，再按实测速率线性外推）。 */
    static int calibrateSpinRoundsForOneMicros() {
        for (int i = 0; i < 200_000; i++) {
            busySpin(64);
        }
        int sample = 100_000;
        long start = System.nanoTime();
        busySpin(sample);
        long elapsed = System.nanoTime() - start;
        return (int) Math.max(1, sample * 1_000L / Math.max(elapsed, 1));
    }

    private static void busySpin(int rounds) {
        long acc = 0;
        for (int i = 0; i < rounds; i++) {
            acc += i;
        }
        BLACKHOLE.addAndGet(acc);
    }

    /**
     * 跑一组对比：同一负载下分别测自适应自旋锁与 synchronized。
     *
     * @param workload    负载类型
     * @param threads     并发线程数
     * @param durationMillis 每种锁的测量时长
     * @return 两个结果，[0] 为自适应自旋锁，[1] 为 synchronized
     */
    public static Result[] runWorkload(Workload workload, int threads, long durationMillis)
            throws InterruptedException {
        int spinRounds = workload == Workload.LONG ? calibrateSpinRoundsForOneMicros() : 0;
        long warmupMillis = Math.max(200, durationMillis / 2);
        measure("warmup-spin", workload, threads, warmupMillis, spinRounds, true);
        Result spin = measure("adaptive-spin", workload, threads, durationMillis, spinRounds, true);
        measure("warmup-sync", workload, threads, warmupMillis, spinRounds, false);
        Result sync = measure("synchronized ", workload, threads, durationMillis, spinRounds, false);
        return new Result[]{spin, sync};
    }

    private static Result measure(String name, Workload workload, int threads,
                                  long durationMillis, int spinRounds, boolean useSpinLock)
            throws InterruptedException {
        AdaptiveSpinLock spinLock = useSpinLock ? new AdaptiveSpinLock() : null;
        Object monitor = new Object();
        long[] counter = {0};
        CountDownLatch ready = new CountDownLatch(threads);
        CountDownLatch start = new CountDownLatch(1);
        AtomicLong ops = new AtomicLong();
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(durationMillis);

        List<Thread> workers = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            Thread worker = new Thread(() -> {
                long localOps = 0;
                ready.countDown();
                try {
                    start.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                while (System.nanoTime() < deadline) {
                    if (useSpinLock) {
                        spinLock.lock();
                        try {
                            counter[0]++;
                            if (workload == Workload.LONG) {
                                busySpin(spinRounds);
                            }
                        } finally {
                            spinLock.unlock();
                        }
                    } else {
                        synchronized (monitor) {
                            counter[0]++;
                            if (workload == Workload.LONG) {
                                busySpin(spinRounds);
                            }
                        }
                    }
                    localOps++;
                }
                ops.addAndGet(localOps);
            });
            worker.setDaemon(true);
            workers.add(worker);
            worker.start();
        }

        ready.await();
        long begin = System.nanoTime();
        start.countDown();
        for (Thread worker : workers) {
            worker.join();
        }
        long elapsed = System.nanoTime() - begin;
        return new Result(name, ops.get(), elapsed, counter[0]);
    }

    public static void main(String[] args) throws InterruptedException {
        int threads = args.length > 1
                ? Integer.parseInt(args[1])
                : Math.max(2, Runtime.getRuntime().availableProcessors());
        long duration = args.length > 0 ? Long.parseLong(args[0]) : 3_000;

        System.out.printf("threads=%d, duration per lock=%dms%n", threads, duration);
        System.out.printf("%-16s %-14s %16s %14s%n", "workload", "lock", "ops", "ops/sec");

        for (Workload workload : Workload.values()) {
            Result[] results = runWorkload(workload, threads, duration);
            for (Result r : results) {
                System.out.printf("%-16s %-14s %,16d %,14.0f%n",
                        workload, r.name, r.operations, r.opsPerSecond());
            }
            double ratio = results[0].opsPerSecond() / results[1].opsPerSecond();
            System.out.printf("%-16s %-14s %16s %13.2fx%n", workload, "spin/sync", "", ratio);
        }
    }

    private SpinLockBenchmark() {
    }
}
