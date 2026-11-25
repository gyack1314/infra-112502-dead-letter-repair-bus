# 死信修复通道的因果回放与副作用隔离

消息包含业务命令和修订。消费组在完成外部作用后拥有独立凭据。演练端可提供外部执行事实。

## 输入消息

每个 JSONL 文件是一段独立输入记录。每行包括整数秒点 at、消息名称 kind 与对象 record。at 代表中间件收到消息的时刻，同一秒内按行序提交。业务时间和对应回执引用写在 record 中。

- **publish**　key、command、rev 为业务命令和修订，consumers 描述订阅者。needs 是对真实完成凭据的依赖引用，写作 group/command@rev。
- **claim**　group 消费指定 command，attempt 为领取标识。
- **dead**　group 上报该 attempt 的 dead_reason，可为 unavailable、incomplete 或 unknown。
- **effect**　仿真业务端提供 group/command@rev 对应的 result。state 为 applied、not_applied 或 unknown，applied 额外带证据 effect_id。not_applied 声明本次旧 attempt 不会再产生效果。
- **amend**　对命令发布 rev 与 changed。changed 标为 effect 或 context，前者表示影响已经确认的外部作用。context 可用于不改变先前作用的事实说明。
- **compensated**　外部业务端确认指定 effect_id 的补偿结果，携带 compensation_id。
- **repair**　运营向列表 commands 申请恢复，使用 id 唯一标记。
- **inspect**　查看 group、command 或 repair。restart 为中继进程重启。
- **restart**　模拟修复中间件重启，仿真业务端保留处理事实。

## 示例

- unknown-effect-then-amend.jsonl
- cycle-with-unrelated-work.jsonl

## 读取演练资料

运行环境为 Python 3.11，只使用标准库。在根目录运行以下命令可核对文件格式及读取接口。

    python3 scenario_reader.py examples
    python3 -m unittest discover -v

资料读取器会列出各文件包含的消息和时间范围。演练记录只描述各参与方交给系统的输入。
