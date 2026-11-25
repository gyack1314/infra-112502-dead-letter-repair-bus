# 死信修复通道的因果回放与副作用隔离

消息包含业务命令和修订。消费组在完成外部作用后拥有独立凭据。演练端可提供外部执行事实。

本仓库现在包含一个只依赖 Python 3.11 标准库的 `repair_bus` 中间件原型：它把**正常消费、外部处理回执、修复申请、更正状态、定向放行和重启恢复**都做成了可运行入口，并用本地仿真消费端模拟扣件/配件账等副作用。

## 快速运行

```bash
# 校验原始演练资料
python3 scenario_reader.py examples

# 回放完整演练；inspect 事件会输出 JSON 报告
python3 -m repair_bus replay examples/unknown-effect-then-amend.jsonl \
  --store runtime/unknown.json

# 用值班员视图查询同一状态
python3 -m repair_bus inspect --repair r1 --store runtime/unknown.json
python3 -m repair_bus dead-letter --command take --store runtime/unknown.json
python3 -m repair_bus amendments --command take --store runtime/unknown.json
python3 -m repair_bus pending --store runtime/unknown.json

# 环与无关键
python3 -m repair_bus replay examples/cycle-with-unrelated-work.jsonl \
  --store runtime/cycle.json
python3 -m repair_bus inspect --repair cycle --store runtime/cycle.json

# 全量测试
python3 -m unittest discover -v
```

也可以使用根目录入口：

```bash
python3 repair_middleware.py overview --store runtime/unknown.json
```

全局状态路径可放在子命令后，写作 `--store` 或 `--state-file`；放在子命令前时可用原始形式 `--state`：

```bash
python3 -m repair_bus --state runtime/unknown.json overview
```

## 运营修复通路

中间件状态保存在单个原子替换的 JSON 快照中。每次发布、领取、死信、回执、更正、补偿、修复申请和定向放行后都会落盘，因此进程重启后仍能找到未消解的外部回执查询。

核心对象是精确修订凭据：

```text
group/command@rev
```

例如 `ledger/take@1` 与 `ledger/take@2` 是两条不同凭据：

- 只有 `ledger` 组拿到 `applied` 回执和 `effect_id`，`ledger/take@1` 才成立。
- `schedule` 或其他组处理过 `take` 的进度不能代 `ledger` 签名。
- 旧修订的 applied 回执可用于引用 `@1` 的后续，防止重复执行同一扣件影响。
- 旧确认不能自动抵作 `@2` 的完成凭据；新版必须有自己的真实回执。
- `unknown` 结果会形成持久化 receipt，重启后仍要求外部系统给出 `applied(effect_id)` 或 `not_applied`。
- `not_applied` 只用于确认旧 attempt 无影响，之后才允许安全重试。
- 已 applied 的同一 `effect_id` 重复确认不会产生第二个影响或第二笔补偿。

### 常用入口

```bash
# 正常发布与本地仿真消费
python3 -m repair_bus publish --key stock --command take --rev 1 \
  --consumers ledger --store runtime/ops.json
python3 -m repair_bus consume --group ledger --command take \
  --attempt a1 --store runtime/ops.json

# 死信和外部回执
python3 -m repair_bus dead --attempt a1 --reason unknown --store runtime/ops.json
python3 -m repair_bus effect --group ledger --command take --rev 1 \
  --attempt a1 --state applied --effect-id h1 --store runtime/ops.json

# 修复申请与更正
python3 -m repair_bus repair r1 take finish --store runtime/ops.json
python3 -m repair_bus amend --command take --rev 2 --changed effect \
  --store runtime/ops.json
python3 -m repair_bus compensated --effect-id h1 \
  --compensation-id undo-h1 --store runtime/ops.json

# 只给指定消费组/命令补授凭据并放行
python3 -m repair_bus release --group schedule --command finish \
  --store runtime/ops.json

# 旧修订依赖由来源决定换新版或撤销
python3 -m repair_bus retarget --delivery schedule/finish@1 \
  --new-ref ledger/take@2 --store runtime/ops.json
python3 -m repair_bus withdraw --delivery schedule/finish@1 \
  --store runtime/ops.json

# 模拟重启并恢复未消解回执
python3 -m repair_bus restart --store runtime/ops.json
```

`consume` 是本地仿真入口，会完成“领取 → 本地业务处理 → 外部 effect 回执”。也可用 `--fail unavailable|incomplete|unknown` 模拟失败。

## 报告如何指导值班

`inspect --repair <id>` 和 `dead-letter` 会给出：

- `unknown`：哪条前置或当前死信仍结果不明；
- `compensations`：哪个 `effect_id` 应补偿、补偿是否已确认、哪些重叠修复单共享该动作；
- `do_not_repeat`：哪些操作已经 applied，不能再执行一次；
- `continuable` / `releasable`：哪个消费方或哪条后续现在可以继续；
- `cycles` / `cycle_blocked_branches`：环及等待环的分支；
- `unaffected`：不在环/环等待分支上且没有阻塞的消息；
- 每个投递的 `blockers` 和 `next_action`，例如：
  - `query_external_result`
  - `compensate`
  - `continue_original`
  - `source_retarget_or_withdraw`
  - `break_cycle_by_source_decision`
  - `release_and_claim`

依赖旧修订且尚未执行的消息会得到 `obsolete_dependency`。即使旧凭据仍 applied，也不会自动升级成新修订凭据；必须由来源把依赖改到新凭据，或撤销本次处理。

## 输入消息契约

每个 JSONL 文件是一段独立输入记录。每行包括整数秒点 `at`、消息名称 `kind` 与对象 `record`。`at` 代表中间件收到消息的时刻，同一秒内按行序提交。业务时间和对应回执引用写在 `record` 中。

- **publish**：`key`、`command`、`rev` 为业务键、稳定业务命令和修订，`consumers` 描述订阅者。`needs` 是对真实完成凭据的依赖引用，写作 `group/command@rev`。
- **claim**：`group` 消费指定 `command`，`attempt` 为领取标识。
- **dead**：`group`、`command`、`attempt`、`dead_reason`；原因可为 `unavailable`、`incomplete` 或 `unknown`。
- **effect**：仿真业务端提供 `group/command@rev` 对应的 result。`state` 为 `applied`、`not_applied` 或 `unknown`；applied 额外带证据 `effect_id`。not_applied 声明本次旧 attempt 不会再产生效果。
- **amend**：对命令发布新的 `rev` 与 `changed`。`changed=effect` 表示更正命中已经确认的外部作用；`context` 表示不改变先前作用的事实说明。
- **compensated**：外部业务端确认指定 `effect_id` 的补偿结果，携带 `compensation_id`。
- **repair**：运营用唯一 `id` 向 `commands` 列表申请恢复。
- **inspect**：查看 `repair`、`command`、`group` 或死信。
- **restart**：模拟修复中间件重启；状态从 `--store` 快照恢复，仿真业务端处理事实也仍在状态中。

## 示例

- `examples/unknown-effect-then-amend.jsonl`：unknown 死信、重叠修复单、effect 类更正、旧影响补偿、新版依赖和重启。
- `examples/cycle-with-unrelated-work.jsonl`：`a`/`b` 循环依赖，而 `c` 是不受影响且已兑现的操作。

## 代码结构

```text
repair_bus/
  model.py       # 引用解析、原因常量、初始状态
  store.py       # 原子 JSON 快照
  graph.py       # 依赖图、Tarjan SCC 环检测、反向等待分支
  engine.py      # 发布/消费/回执/更正/补偿/修复/报告状态机
  simulation.py  # 本地消费端与幂等外部账务仿真
  reports.py     # 中文文本报告
  replay.py      # JSONL 演练回放与 restart 重载
  cli.py         # python -m repair_bus 运行入口
tests/           # 因果链、幂等、环、重启和示例回放测试
```

资料读取器仍会列出各文件包含的消息和时间范围。演练记录只描述各参与方交给系统的输入；修复后的继续操作可通过 CLI/API 执行。
