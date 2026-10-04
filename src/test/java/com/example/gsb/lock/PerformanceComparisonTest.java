package com.example.gsb.lock;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * 短临界区 / 长临界区两种负载下，AdaptiveSpinLock 与 synchronized 的吞吐对比。
 *
 * <p>报告同时打印到控制台并写入 {@code target/benchmark-results.txt}。
 * 不断言谁更快（取决于 CPU、调度与 JIT），只断言压测结果正确、吞吐为正，
 * 并要求两种负载都观察到自旋失败转挂起的统计，证明自适应策略真正生效。
 */
class PerformanceComparisonTest {

    private static final int THREADS = Math.max(4, Runtime.getRuntime().availableProcessors());
    private static final int SHORT_OPS_PER_THREAD = 100_000;
    private static final int LONG_OPS_PER_THREAD = 2_000;
    /** 长临界区目标持锁时长（约 2 微秒），运行时标定忙等轮数。 */
    private static final long LONG_SECTION_NANOS = 2_000;

    @Test
    @Timeout(300)
    void compareThroughputShortAndLongCriticalSections() throws Exception {
        int longWorkIterations = calibrate(LONG_SECTION_NANOS);

        List<String> report = new ArrayList<>();
        report.add("================ 自适应自旋锁 vs synchronized 吞吐对比 ================");
        report.add(String.format("线程数: %d, JVM: %s %s",
                THREADS, System.getProperty("java.vm.name"), System.getProperty("java.version")));
        report.add("--------------------------------------------------------------------");
        report.add(String.format("%-28s %14s %14s %10s",
                "负载", "自适应自旋锁", "synchronized", "锁/同步比"));

        Result shortAdaptive = runAdaptive(SHORT_OPS_PER_THREAD, 0);
        Result shortSynchronized = runSynchronized(SHORT_OPS_PER_THREAD, 0);
        Result longAdaptive = runAdaptive(LONG_OPS_PER_THREAD, longWorkIterations);
        Result longSynchronized = runSynchronized(LONG_OPS_PER_THREAD, longWorkIterations);

        report.add(row("短临界区(仅计数器++)", shortAdaptive, shortSynchronized));
        report.add(row("长临界区(约2us计算)", longAdaptive, longSynchronized));
        report.add("--------------------------------------------------------------------");
        report.add("单位: ops/s（越大越好）；锁/同步比 > 1 表示自适应自旋锁更快");
        report.add("");
        report.add("短临界区统计: " + shortAdaptive.stats);
        report.add("长临界区统计: " + longAdaptive.stats);

        String text = String.join(System.lineSeparator(), report);
        System.out.println(System.lineSeparator() + text);

        Path out = Path.of("target", "benchmark-results.txt");
        Files.createDirectories(out.getParent());
        Files.writeString(out, text + System.lineSeparator(),
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);

        // 正确性：共享计数器不丢更新
        assertThat(shortAdaptive.counter).isEqualTo((long) THREADS * SHORT_OPS_PER_THREAD);
        assertThat(longAdaptive.counter).isEqualTo((long) THREADS * LONG_OPS_PER_THREAD);
        assertThat(shortSynchronized.counter).isEqualTo((long) THREADS * SHORT_OPS_PER_THREAD);
        assertThat(longSynchronized.counter).isEqualTo((long) THREADS * LONG_OPS_PER_THREAD);

        // 吞吐为正
        assertThat(shortAdaptive.opsPerSecond).isPositive();
        assertThat(shortSynchronized.opsPerSecond).isPositive();
        assertThat(longAdaptive.opsPerSecond).isPositive();
        assertThat(longSynchronized.opsPerSecond).isPositive();

        // 高竞争压测下两种负载都应出现自旋成功，且长临界区应大量触发“自旋失败转挂起”
        assertThat(shortAdaptive.stats.spinSuccesses())
                .as("应出现自旋成功").isPositive();
        assertThat(longAdaptive.stats.spinFailures())
                .as("长临界区竞争下应出现自旋失败转挂起，证明自旋不会无限浪费 CPU")
                .isPositive();
        assertThat(longAdaptive.stats.averageSpinRounds()).isGreaterThanOrEqualTo(1.0);
    }

    private static String row(String label, Result adaptive, Result synch) {
        double ratio = adaptive.opsPerSecond / synch.opsPerSecond;
        return String.format("%-28s %,14.0f %,14.0f %10.2f",
                label, adaptive.opsPerSecond, synch.opsPerSecond, ratio);
    }

    /** 标定 busy-loop 轮数，使单轮自旋约消耗 targetNanos 纳秒。 */
    private static int calibrate(long targetNanos) {
        long dummy = 0;
        long start = System.nanoTime();
        int iterations = 0;
        do {
            for (int i = 0; i < 1_000; i++) {
                dummy += i * 31L;
            }
            iterations += 1_000;
        } while (System.nanoTime() - start < Math.max(targetNanos, 500));
        if (dummy == Long.MIN_VALUE) {
            throw new AssertionError();
        }
        return (int) (iterations * targetNanos / Math.max(1, System.nanoTime() - start));
    }

    private static Result runAdaptive(int opsPerThread, int longWorkIterations) throws Exception {
        AdaptiveSpinLock lock = new AdaptiveSpinLock();
        long[] counter = {0};
        // 预热
        runWorkers(() -> {
            lock.lock();
            try {
                counter[0]++;
                doWork(longWorkIterations);
            } finally {
                lock.unlock();
            }
        }, THREADS, Math.max(1_000, opsPerThread / 20));
        counter[0] = 0;

        long elapsedNanos = runWorkers(() -> {
            lock.lock();
            try {
                counter[0]++;
                doWork(longWorkIterations);
            } finally {
                lock.unlock();
            }
        }, THREADS, opsPerThread);

        long totalOps = (long) THREADS * opsPerThread;
        return new Result(counter[0],
                totalOps * 1_000_000_000.0 / elapsedNanos,
                lock.stats());
    }

    private static Result runSynchronized(int opsPerThread, int longWorkIterations) throws Exception {
        Object monitor = new Object();
        long[] counter = {0};
        Runnable task = () -> {
            synchronized (monitor) {
                counter[0]++;
                doWork(longWorkIterations);
            }
        };
        runWorkers(task, THREADS, Math.max(1_000, opsPerThread / 20));
        counter[0] = 0;

        long elapsedNanos = runWorkers(task, THREADS, opsPerThread);

        long totalOps = (long) THREADS * opsPerThread;
        return new Result(counter[0],
                totalOps * 1_000_000_000.0 / elapsedNanos,
                null);
    }

    private static void doWork(int iterations) {
        long value = 0;
        for (int i = 0; i < iterations; i++) {
            value += i * 31L;
        }
        if (value == Long.MIN_VALUE) {
            throw new AssertionError();
        }
    }

    /** 启动 threads 个线程，每个执行 iterations 次任务，返回总耗时（纳秒）。 */
    private static long runWorkers(Runnable task, int threads, int iterations) throws InterruptedException {
        List<Thread> workers = new ArrayList<>(threads);
        for (int t = 0; t < threads; t++) {
            workers.add(new Thread(() -> {
                for (int i = 0; i < iterations; i++) {
                    task.run();
                }
            }));
        }
        long start = System.nanoTime();
        for (Thread worker : workers) {
            worker.start();
        }
        for (Thread worker : workers) {
            worker.join();
        }
        return System.nanoTime() - start;
    }

    private record Result(long counter, double opsPerSecond, AdaptiveSpinLock.LockStats stats) {
    }
}
