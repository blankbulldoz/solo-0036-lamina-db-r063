# LaminaDB 持久运行演练方案

本目录把 LaminaDB 的容器部署固定为一套可演练（rehearsable）的持久运行方案，并附带真实数据验证脚本 `drill.py`。方案围绕一条不变量展开：

> **已对外确认的提交，在容器重建之后仍然可以逐条核对。**

这条不变量由在线事务提交（日志 fsync 先于确认）和备份恢复（镜像校验 + 恢复基点）共同维护，而不是由停止宽限期维护。

## 组成

| 文件 | 作用 |
| --- | --- |
| `../Dockerfile` | 非特权用户 10001、无 shell 依赖的自检（`laminad -healthcheck`）、`STOPSIGNAL SIGTERM`、ENTRYPOINT/CMD 分离 |
| `docker-compose.yml` | 只读根文件系统、独立可写数据卷、有界停止、健康检查、资源上限、一次性还原 profile |
| `drill.py` | 演练脚本：驱动完整生命周期并用真实数据核对上述不变量 |
| `drill-work/` | 演练产物（台账、备份、报告、沙箱、日志），不入库 |

## 策略选择（可说明的取舍）

### 1. 耐久性来自提交路径，不来自停止宽限

提交顺序是校验 → 完整帧写入 → fsync → 内存发布 → 返回客户端。因此**任何已确认的提交不依赖优雅退出**：`stop_grace_period` 到期后编排器发 SIGKILL 也只影响未确认的请求。演练 P3 在写入负载下直接 SIGKILL，重启后逐条核对已确认提交，证明这一点——我们不通过无限延长终止等待来假装耐久。

### 2. 有界停止：排空是礼节，不是耐久机制

- SIGTERM/SIGINT 到达后立即停止接受新连接，进行中的请求（包括慢检查点）在 `-drain-timeout`（默认 10s，compose 显式设置）内完成。
- 预算耗尽则强制关闭连接并记 WARN 日志；被中断的只有未确认请求，其结果按协议本就“不确定”，客户端重启后核对。
- 进程退出的上界 = 排空预算 + 一个在飞检查点/提交（镜像上限 128MiB，有界）+ 日志关闭。`stop_grace_period: 30s` 覆盖该上界并留有余量。
- 演练 P2 在提交负载 + 在飞检查点中发 SIGTERM，实测停止耗时（本环境约 0.02–0.04s）远小于上界，并核对全部已确认提交。

### 3. 恢复核验先于“可用”宣告

容器重建后，编排器只保证进程活着；**数据正确性由客户端台账核对宣告**。演练 P1 演示核对流程，顺序固定：

1. 台账（`ledger.json`，保存在容器外）记录每个已确认提交的序号与写集规模；
2. 重建后 `GET /stats` 的 `sequence` 必须等于最后确认序号；
3. `GET /changes` 从 `history_base` 到 head 必须连续无缺口，且覆盖台账全部序号；
4. 全表查询必须等于仅由已确认提交重放出的客户端模型；
5. 历史快照（`at`）在重建后仍可读。

任何一步失败都视为不可用，而不是“启动成功即可用”。

### 4. 请求排空、恢复核验、可用性之间的取舍

- **排空**：只给进行中的请求有界时间；新请求在信号到达后即被拒绝（连接重置），客户端重试交给重启后的实例。
- **核验**：重建后先过台账核对再承载流量；核验用只读接口（stats/changes/query），不改变数据。
- **可用性**：`restart: unless-stopped` 让崩溃后自动拉起；健康检查（镜像内自检，不依赖 curl）把未通过核验前的实例标记为未就绪。还原 profile 故意 `restart: "no"`——还原目标必须不存在，重跑失败一次好过崩溃循环假装可用。

### 5. 同目录第二实例

数据目录由 Unix 文件锁独占（`lamina.lock`）。同一卷上启动第二个容器/进程会在启动期失败（`data directory already open`），主实例不受影响。命名卷跨容器共享同一内核文件锁，容器间同样生效。演练 P4 验证拒绝行为与主实例连续性。

### 6. 异卷还原后的游标边界

- 还原目标是**另一个卷上的新目录**；还原保留镜像内的提交序号与历史读版本，但不含源库历史日志。
- 变更消费者的旧游标若小于还原实例的 `history_base`，`GET /changes` 返回 410（`snapshot`），消费者必须在 `history_base` 重新对齐；新提交从 `history_base + 1` 继续。
- 向已初始化的目录还原会被拒绝（`restore requires a new directory`），防止把备份拼接到活目录上。演练 P5 覆盖以上全部边界。

### 7. 只读根文件系统与写隔离

`read_only: true` + 独立可写卷 `/data` + 小型 tmpfs `/tmp`。进程运行时只写数据目录（日志帧、检查点临时文件与原子替换、锁文件），不依赖 HOME 或 /tmp。演练 P6 在本地用只读沙箱（根与 HOME 均为 0555）实际运行全部阶段并逐文件比对，证明写入被限制在数据卷内；compose 运行时则通过 `docker inspect` 核对加固项。

## 运行演练

```sh
# 本地运行时（无需 Docker，只读沙箱内跑完整生命周期）
go build -o bin/laminad ./cmd/laminad
python3 deploy/drill.py --runtime local

# 容器运行时（需要 Docker，驱动 deploy/docker-compose.yml）
python3 deploy/drill.py --runtime compose
```

每个核对点输出一行 JSON，最终 `summary` 行给出总判定；任何失败以非零码退出。产物在 `deploy/drill-work/`：`ledger.json`（已确认提交台账）、`backup.lmi`（冻结备份）、`report.json`（完整报告）、`logs/`（实例日志）。

手动容器运行（等价于 compose 主服务）：

```sh
docker build -t lamina-db .
docker run -d --name laminadb \
  --read-only --tmpfs /tmp:noexec,nosuid,nodev,size=16m \
  --user 10001:10001 --cap-drop ALL --security-opt no-new-privileges:true \
  -p 127.0.0.1:7386:7386 -v lamina-data:/data \
  --stop-signal SIGTERM --stop-timeout 30 \
  lamina-db
```

## 已知边界（与产品规范一致）

- 单进程单卷；没有复制、自动切换或加密，备份只缓解存储故障。
- 未确认的提交结果不确定，客户端必须重启后核对；基线没有请求级去重协议。
- 检查点不截短日志，日志与镜像随数据量增长；镜像上限 128MiB。
