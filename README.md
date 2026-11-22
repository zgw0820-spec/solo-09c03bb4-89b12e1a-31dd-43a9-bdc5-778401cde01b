# 退役构建机镜像层复用配对（layer-matcher）

CI 值班员工具：构建机退役后，找出待重跑任务能从哪些**在役构建机**复用镜像层。
上传两份 CSV，系统按"层摘要"做等值连接，把相同摘要在两表中的**所有行对**列出，
每对只出现一次，产出 `任务号, 来源构建机, 层摘要` 清单 CSV。

## 需求对照

| 要求 | 实现 |
| --- | --- |
| Flask 网页上传两份 CSV | `app/app.py` + `app/templates/`，支持中文/英文列名别名 |
| 相同摘要所有行对、每对一次 | 分桶 + 分块嵌套循环等值连接，输出前不做去重、不丢行（自测校验唯一性） |
| 内存最多暂存 256 条输入记录、256 条结果 | 每侧块大小 128（合计 256）；结果满 256 立即刷盘；`BoundedMemoryGuard` 运行时强制，越界即抛异常；自测断言整次运行峰值 |
| 只统计重启后仍能保留的配对 | 配对数始终由磁盘上已确认的结果批次重数（`count_pairs_on_disk`），不依赖内存 |
| 写盘中断可续跑、已确认批次不重算 | 工作单元 = (桶, 任务块, 机器块)；单元结果原子写盘后才推进 `state.json`（原子 rename + fsync）；续跑按状态跳过已确认单元，重算前清理本单元残留 |
| 全部完成后才开放 CSV 下载 | 仅当最终 `pairs.csv` 生成后下载路由返回 200，否则 409 |
| 清单列出任务号、来源构建机、层摘要 | 最终 CSV 表头即这三列 |
| Docker Compose 启动 | `docker compose up --build`，命名卷 `layer_matcher_data` 持久化断点状态 |

## 算法与内存边界（`app/join_engine.py`）

1. **分桶（partition）**：流式读两份 CSV，用跨进程稳定的 FNV-1a 哈希把记录路由到
   64 个磁盘分桶文件；该阶段内存只持有当前 1 条记录。
2. **分块嵌套循环连接（block nested-loop join）**：逐桶处理，每侧每次最多载入
   128 条（两侧合计 ≤ 256 条输入记录），任务块 × 机器块做笛卡尔匹配。
   热点摘要（同摘要海量行对）只会产生更多磁盘结果批次，内存驻留不增长。
3. **结果刷盘**：结果缓冲每 256 条原子写一个批次文件（临时文件 + `os.replace`），
   内存中结果永不超过 256。
4. **断点续跑**：每个 (桶, 任务块, 机器块) 单元完成并刷盘后，原子更新 `state.json`
   记录游标；进程重启后从磁盘状态恢复，跳过已确认单元。分桶阶段中断则整侧重写
   （先删旧分桶文件），join 阶段中断只重算未确认的当前单元。
5. **最终合并**：流式拼接所有批次为 `pairs.csv`，不整体载入内存。

## 目录结构

```
app/
  app.py            # Flask 路由、后台线程、启动恢复、下载门禁
  join_engine.py    # 受限内存外部连接引擎（分桶 + 分块 BNLJ + 断点续跑）
  templates/        # 上传页 / 作业详情页 / 错误页
  Dockerfile
  requirements.txt
cli/run.py          # 命令行入口，支持 --crash-after 故障注入
tests/              # unittest 自测（引擎 7 项 + Web 3 项）
examples/           # 两份示例 CSV
docker-compose.yml
```

## 用 Docker Compose 启动

```bash
docker compose up --build
# 浏览器打开 http://localhost:8000
```

作业数据写入命名卷 `layer_matcher_data`（容器内 `/data/jobs`）。
容器/进程重启后，中断的作业会在首次访问时自动从断点继续，已确认批次不重算。

## 本地开发与自测

无需 Docker（本机无容器运行时时）：

```bash
# 安装 Flask 到独立目录（不污染系统环境）
python3 -m pip install --target=.pylibs flask==3.0.3   # 或使用已有的 get-pip

# 引擎自测（仅标准库，不需要 Flask）
python3 -I -m unittest discover -s tests -t .

# 含 Web 层的全部自测
PYTHONPATH=.pylibs:app python3 -m unittest discover -s tests -t .

# 命令行跑一个作业（作业目录内需有 tasks.csv / machines.csv）
PYTHONPATH=.pylibs:app python3 -I cli/run.py /path/to/job
# 故障注入：第 N 个检查点模拟写盘中断（退出码 3），再次执行即续跑
PYTHONPATH=.pylibs:app python3 -I cli/run.py /path/to/job --crash-after 1610
```

## 输入 CSV 格式

- 任务表：`任务号, 层摘要`（别名：task_id/job/task + digest/layer/sha256/hash）
- 构建机表：`来源构建机, 层摘要`（别名：machine/host/builder + digest/...）
- 空摘要行无法参与等值连接，会跳过并在作业页计数展示。

## 自测覆盖（10 项，全部通过）

- 正确性：多摘要全部行对、每对一次；空摘要跳过；无匹配产出仅表头文件
- **热点摘要**：600×600 同摘要（36 万对）+ 冷数据，注入内存守卫跑完整流程，
  断言输入记录峰值 ≤ 256、结果峰值 = 256（达上限即刷盘）
- **中断续跑**：进程内故障注入后续跑结果与一次跑完一致；
  两个独立子进程模拟真实写盘中断与重启接力；第三次执行验证幂等无重复；
  分桶阶段中断同样可恢复
- Web：上传→状态轮询→完成前下载 409、完成后 200 且列名正确；
  坏表头 400；构造 joining 中断态后调用启动恢复，已确认批次保留
- 真实 CLI 冒烟：800×800 数据在 join 阶段中断（4711 条已确认），
  新进程续跑至 17306 条，与手工期望值一致且无重复

## 已知限制 / 剩余事项

- 当前交付环境没有 Docker 守护进程，`docker compose up` 仅做了 compose/Dockerfile
  静态校验，未实际构建镜像；需在有 Docker 的环境执行一次冒烟。
- 容器内用 Flask 自带开发服务器（单进程多线程，符合"同进程后台线程 + 状态文件"
  的设计）；上生产建议换 gunicorn 单 worker（多 worker 需加作业级分布式锁）。
- 上传大小上限 50MB（`app.py` 中 `MAX_UPLOAD_BYTES` 可调）。
