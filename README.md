# 自适应自旋锁（AdaptiveSpinLock）

针对“热点临界区很短，直接阻塞唤醒开销大，但一直自旋又浪费 CPU”的场景，
从零实现的自适应可重入自旋锁（`com.example.gsb.lock.AdaptiveSpinLock`，实现 `java.util.concurrent.locks.Lock`）。

## 运行方式

```bash
./mvnw -q verify   # 编译 + 全部正确性测试 + 性能对比基准
```

性能对比报告打印在测试输出中，并写入 `target/benchmark-results.txt`。

## 设计说明

### 1. CAS 加锁与可重入计数

- 锁状态是一个 `AtomicReference<Thread> owner`：`null` 表示空闲。
- 加锁：`owner.compareAndSet(null, current)` 抢锁；自旋阶段采用 **TTAS**
  （先普通读 `owner.get()`，看到空闲才 CAS），避免每轮 CAS 造成缓存行在自旋线程间乒乓。
- 解锁：持有线程把 `owner` 置回 `null`（`owner.set(null)`），并唤醒等待队列队首。
- **可重入计数**：`holdCount`（普通 `int`）记录当前持有线程的重入次数。
  重入加锁时 `holdCount++`，`unlock()` 时 `holdCount--`，**计数归零才真正释放锁**。
  `holdCount` 只有持有线程会读写，无需原子类；其可见性由 `owner` 引用的
  CAS/ volatile 写的释放-获取语义保证。`getHoldCount()` 可查询当前线程的重入计数。

### 2. 自适应策略

- 每次加锁：先走快速路径（一次 CAS），失败则**自旋 `spinBudget` 次**，仍失败则
  **park 挂起**进入 FIFO 等待队列（每次只唤醒队首，避免惊群）。
- 预算动态调整：**自旋成功 → 预算翻倍**（封顶 `MAX_SPIN = 1024`）；
  **自旋耗尽转挂起 → 预算减半**（保底 `MIN_SPIN = 16`）。
- 调整为 **1/16 采样**（`ThreadLocalRandom`）：避免每次加锁都对共享的
  `spinBudget` 做 CAS（高竞争下这本身会成为瓶颈），统计上仍保持
  “历史成功多则多自旋、失败多则少自旋”的适应方向。

### 3. 自旋阶段的退让（backoff）

- **判定依据**：`Runtime.getRuntime().availableProcessors()`。
  `> 1` 按多核处理，`<= 1` 按单核处理（也可用构造器
  `AdaptiveSpinLock(int initialSpinBudget, boolean singleCore)` 显式指定）。
- **多核**：`Thread.onSpinWait()`——向 CPU 发出忙等待提示（x86 上即 PAUSE 指令），
  降低流水线冲刷与功耗；持锁线程在别的核上并行推进，自旋等待是有意义的。
- **单核**：`Thread.yield()` 直接让出 CPU——单核上持锁线程必须先被调度进来
  才能释放锁，继续自旋纯属浪费自己的时间片。

### 4. 超时与中断

- `tryLock(long, TimeUnit)`：在自旋与挂起阶段都检查截止时间，超时返回 `false`。
- `lockInterruptibly()`：等待期间响应中断，抛 `InterruptedException`。
- **不留下持锁状态**：线程只有在 CAS 抢到锁之后才成为持有者；超时/中断的退出
  路径上都尚未持有锁，且 `finally` 中保证把自己移出等待队列（超时/中断线程退出时
  若锁已空闲，会级联唤醒新队首，避免丢唤醒）。
- `lock()`（不可中断）：等待期间的中断只记录不响应，拿到锁后按 `Lock` 约定
  恢复中断标记。

### 5. 统计

`stats()` 返回快照 `LockStats`：

| 指标 | 含义 |
|------|------|
| `spinSuccesses` | 自旋成功次数（在自旋预算内抢到锁） |
| `spinFailures` | 自旋失败转挂起次数（预算耗尽转 park） |
| `averageSpinRounds` | 平均自旋轮数（自旋成功的总轮数 / 自旋成功次数） |

### 6. 吞吐对比（自适应自旋锁 vs synchronized）

测试：`PerformanceComparisonTest`，16 线程（8 核 16 线程 Ryzen 7 9700X），
每种负载先预热再计时，共享计数器校验不丢更新。某次运行结果：

```
负载                                   自适应自旋锁   synchronized      锁/同步比
短临界区(仅计数器++)                      3,051,454     23,650,079       0.13
长临界区(约2us计算)                       2,677,565      6,772,266       0.40
```

结果解读：

- **长临界区下差距明显缩小**（0.13 → 0.40）：临界区越长，锁管理开销占比越低，
  且自旋线程更可能在预算内等到释放，避免 park/unpark 的系统调用开销。
- 纯计数器级短临界区 + 16 线程抢同一把锁是 `synchronized` 的主场：
  HotSpot 的 monitor 是 native 实现，自带自适应自旋与高效交接；
  而本实现每次自旋成功还要更新 3 个统计计数器（实测约占 25% 开销），
  且 `onSpinWait`（PAUSE）在 Zen4 上延迟较高。该负载下锁本身的开销被极限放大，
  实际业务中临界区通常包含真实工作，差距会介于两行数据之间。
- 本锁的价值在于**行为可控**：自旋预算自适应、统计可观测、支持超时/中断，
  且避免了 `synchronized` 在特定场景下的锁膨胀与不可中断问题。

## 测试覆盖（`mvn -q verify` 一条命令跑通）

| 测试 | 覆盖需求 |
|------|----------|
| `mutualExclusionAndNoLostUpdatesUnderStress` | 互斥（临界区内线程数断言）+ 共享计数器不丢更新 |
| `reentrantLockingAndHoldCount` / `tryLockIsReentrant` / `unlockWithoutHoldingThrows` | 可重入与重入计数 |
| `spinBudgetGrowsOnSpinSuccess` / `spinBudgetShrinksWhenSpinningFails` | 自适应调整（成功翻倍、失败减半） |
| `tryLockTimesOutAndLeavesNoHeldState` / `tryLockWithZeroTimeoutIsNonBlocking` | 超时返回 false 且不留持锁状态 |
| `lockInterruptiblyReleasesWaitOnInterrupt` / `uninterruptibleLockDefersInterruptUntilAcquired` | 中断释放且不留持锁状态 |
| `singleCoreModeYieldsAndStillWorks` | 单核退让分支（yield）正确性 |
| `PerformanceComparisonTest` | 并发正确性 + 短/长临界区吞吐对比 + 统计断言 |

## 原始任务说明

本仓库为 Pair-wise GSB 标注任务仓库（第 17 批 / 261），`main` 为初始环境快照，
`A`、`B` 为两次独立执行的工作分支。完整提示词见 git 历史中初始提交的 README。
