package com.example.gsb.benchmark;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 短/长两种负载下自适应自旋锁与 synchronized 的吞吐对比（冒烟版）。
 *
 * <p>测试本身不强制断言"自旋锁一定更快"（虚机与机器负载会影响结果），但会打印
 * ops/sec 对比表；完整时长（3 秒）结果可用 main 方法复现：
 * {@code java -cp target/classes com.example.gsb.benchmark.SpinLockBenchmark}。
 */
class SpinLockBenchmarkComparisonTest {

    @Test
    @Timeout(120)
    void compareThroughputUnderShortAndLongCriticalSections() throws Exception {
        int threads = Math.min(4, Runtime.getRuntime().availableProcessors());
        long durationMillis = 400;

        System.out.printf("%n=== 吞吐对比（冒烟，threads=%d, %dms/锁）===%n",
                threads, durationMillis);
        System.out.printf("%-8s %-16s %14s %14s %10s%n",
                "负载", "锁", "ops", "ops/sec", "spin/sync");

        for (SpinLockBenchmark.Workload workload : SpinLockBenchmark.Workload.values()) {
            SpinLockBenchmark.Result[] results =
                    SpinLockBenchmark.runWorkload(workload, threads, durationMillis);
            for (SpinLockBenchmark.Result r : results) {
                System.out.printf("%-8s %-16s %,14d %,14.0f%n",
                        workload, r.name, r.operations, r.opsPerSecond());
            }
            double ratio = results[0].opsPerSecond() / results[1].opsPerSecond();
            System.out.printf("%-8s %-16s %14s %14s %9.2fx%n",
                    workload, "ratio", "", "", ratio);

            for (SpinLockBenchmark.Result r : results) {
                assertThat(r.counterValue)
                        .as("临界区互斥：%s 下 %s 的共享计数器不丢更新", workload, r.name)
                        .isEqualTo(r.operations);
                assertThat(r.opsPerSecond()).isGreaterThan(0.0);
            }
            // 防止严重退化（例如自旋锁退化到无法获取锁）：至少达到 synchronized 的 20%。
            assertThat(ratio)
                    .as("%s 负载下自旋锁吞吐不应灾难性低于 synchronized", workload)
                    .isGreaterThan(0.20);
        }
        System.out.println("=== 对比结束（短临界区自旋占优；长临界区两者接近）===");
    }
}
