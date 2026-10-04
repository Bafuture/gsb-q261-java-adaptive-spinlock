# 自适应自旋锁

Pair-wise GSB 标注任务仓库（第 17 批 / 261）。

| 项目 | 内容 |
|------|------|
| 任务类型 | Feature 迭代 |
| 任务难度 | 困难 |
| 语言/框架 | Java, Maven, JUnit 5 |
| 环境可复现等级 | 无外部依赖 |
| 构建方式 | Maven（含 mvnw wrapper，无需本机安装 Maven） |

> 本仓库是**初始环境快照**：只有工程骨架，不含任何实现代码。
> 分支说明：`main` 为初始环境；`A`、`B` 为两次独立执行各自的工作分支，均从 `main` 的同一个提交拉出。

## 运行方式

```bash
./mvnw -q verify
```

## 任务提示词

以下为本题完整的 User Prompt 原文，两次执行必须使用完全相同的文本。

我们的热点临界区很短，直接阻塞唤醒开销大，但一直自旋又浪费 CPU。请从零实现一个自适应自旋锁。仓库目前只有一个空的 Maven 工程（pom.xml 只声明 JUnit 5 与 AssertJ）。要求：1) 基于 CAS 实现加锁与解锁，支持可重入并说明重入计数；2) 自适应策略：先自旋若干次，失败后转为挂起等待，自旋次数根据历史成功情况动态调整；3) 支持自旋阶段的退让：多核场景用忙等待提示，单核场景直接让出 CPU，需说明判定依据；4) 支持带超时的加锁与可中断加锁，中断或超时后必须保证不留下持锁状态；5) 正确性：并发压测下临界区互斥必须成立，需用共享计数器验证不丢更新；6) 支持统计：自旋成功次数、自旋失败转挂起次数、平均自旋轮数；7) 给出短临界区与长临界区两种负载下与 synchronized 的吞吐对比；8) 测试覆盖互斥、可重入、自适应调整、超时与中断释放、并发正确性与性能对比；`mvn -q verify` 一条命令跑通。

## 提交要求

1. 在本仓库中完成提示词要求的全部内容。
2. `./mvnw -q verify` 必须通过。
3. 完成后在所属分支（A 或 B）上提交，产物快照的父提交必须是初始环境快照。

---

## 实现说明（本次迭代产物）

### 代码结构

| 文件 | 说明 |
|------|------|
| `src/main/java/com/example/gsb/lock/AdaptiveSpinLock.java` | 自适应自旋锁本体，实现 `java.util.concurrent.locks.Lock` |
| `src/main/java/com/example/gsb/benchmark/SpinLockBenchmark.java` | 短/长临界区下与 `synchronized` 的吞吐对比基准（含 `main`） |
| `src/test/java/com/example/gsb/lock/AdaptiveSpinLockTest.java` | 单元测试：互斥、可重入、自适应、超时、中断、统计、退让模式 |
| `src/test/java/com/example/gsb/lock/AdaptiveSpinLockConcurrencyTest.java` | 并发压测：共享计数器不丢更新、临界区互斥、批量超时/中断无残留 |
| `src/test/java/com/example/gsb/benchmark/SpinLockBenchmarkComparisonTest.java` | 吞吐对比冒烟测试（`mvn verify` 时自动执行并打印对比表） |

### 设计要点（对应需求 1–6）

1. **CAS 加解锁 + 可重入**：`owner: AtomicReference<Thread>` 保存持有者，加锁
   `CAS(null→current)`、解锁 `CAS(current→null)`。重入计数 `holds` 是普通 `int`，
   只有持有者线程读写（加锁时 owner==自己则 +1，解锁 -1，归零才真正 CAS 释放），
   无需额外同步；owner 的 CAS 提供跨线程的 happens-before。
2. **自适应自旋**：加锁先自旋 `spinLimit` 轮，耗尽则挂起。自旋成功 → 上限 ×2；
   自旋耗尽 → 上限 ×0.5（区间 [32, 100000]，CAS 去重）。临界区短则越来越敢自旋，
   长则快速退化为挂起。
3. **退让策略**：判定依据是 `Runtime.availableProcessors()`（容器/亲和性下反映
   配额核数）。多核用 `Thread.onSpinWait()`（x86 PAUSE / AArch64 YIELD），且 PAUSE
   次数指数退让（1→256 上限）以错开缓存行争抢；单核直接 `Thread.yield()`。
   另有"自旋者配额"（默认 `max(2, CPU/8)`）：拿不到配额的线程直接挂起，
   避免 16 个核同时 hammer 锁变量所在缓存行。
4. **超时与可中断**：`tryLock(long, TimeUnit)` 与 `lockInterruptibly()`。
   超时/中断路径在抢到锁之前只返回 `false` / 抛 `InterruptedException`，
   绝不触碰 `holds`，等待节点标记取消，因此不会留下任何持锁状态。
5. **正确性**：压测用普通 `long` 共享计数器（8 线程 × 5 万次自增）验证不丢更新，
   并用临界区内计数验证任意时刻最多一个线程持有锁。
6. **统计**：`spinSuccessCount()`（自旋成功次数）、`parkCount()`（自旋失败转挂起
   次数）、`averageSpinRounds()`（平均自旋轮数 = 总轮数 / 自旋尝试次数）。

### 吞吐对比（需求 7）

16 核 x86-64、JDK 17 实测（每种锁测 2 秒，可运行
`java -cp target/classes com.example.gsb.benchmark.SpinLockBenchmark [毫秒] [线程数]` 复现）：

| 负载 | 线程 | adaptive-spin ops/s | synchronized ops/s | spin/sync |
|------|------|--------------------:|-------------------:|----------:|
| 短临界区（一次自增） | 4  | ~17.0M | ~14.5M | **1.18x** |
| 长临界区（~1µs 忙等） | 4  | ~0.85M | ~0.79M | **1.07x** |
| 短临界区 | 16 | ~8.0M  | ~23.8M | 0.33x |
| 长临界区 | 16 | ~0.56M | ~0.88M | 0.63x |

结论：中等争抢（线程数 ≤ 核数/4 量级）下，自旋路径避免了 monitor 的挂起/唤醒，
吞吐持平或优于 `synchronized`；极端超订（16 线程抢一把空临界区的锁）下，
JVM 原生 monitor（C++ 实现、带自己的自适应自旋）扩展性更好。
数值随机型负载波动，量级关系稳定。

### 测试与构建（需求 8）

`./mvnw -q verify` 一条命令完成编译 + 全部测试，覆盖：互斥、可重入（含跨线程
非法解锁）、自适应上限收缩/放大、超时返回与持锁状态清理、中断响应与
`lock()` 不响应中断、并发计数器一致性、批量超时/中断后锁可用性、
吞吐对比冒烟（含互斥断言）。
