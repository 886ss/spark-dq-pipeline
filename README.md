# Spark DQ Pipeline — 分布式数据质量验证流水线

[![GitHub](https://img.shields.io/badge/GitHub-886ss%2Fspark--dq--pipeline-blue?logo=github)](https://github.com/886ss/spark-dq-pipeline)
[![Docker](https://img.shields.io/badge/Docker-Ready-2496ED?logo=docker)](https://www.docker.com/)
[![Spark](https://img.shields.io/badge/Spark-3.4.0-E25A1C?logo=apachespark)](https://spark.apache.org/)
[![Python](https://img.shields.io/badge/Python-3.9-3776AB?logo=python)](https://www.python.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-4169E1?logo=postgresql)](https://www.postgresql.org/)
[![License](https://img.shields.io/badge/License-MIT-green)](./LICENSE)

基于 **Apache Spark + Docker** 的分布式数据质量验证框架，支持 **18 项 DQ 检查**，覆盖 7 大类别，YAML 配置驱动，百万级数据集 **37 秒**完成全量检测。

---

## 目录

- [项目背景](#项目背景)
- [技术栈](#技术栈)
- [架构设计](#架构设计)
- [快速开始](#快速开始)
- [DQ 检查清单](#dq-检查清单)
- [数据清洗流水线](#数据清洗流水线)
- [合成数据与错误注入](#合成数据与错误注入)
- [运行结果](#运行结果)
- [项目结构](#项目结构)
- [设计模式](#设计模式)
- [Bug 调试记录](#bug-调试记录)
- [面试要点](#面试要点)

---

## 项目背景

在大数据 ETL 流程中，数据质量监控是确保下游分析和决策正确性的关键环节。本项目构建了一个**可配置、可扩展、容器化**的 DQ 验证框架，具备以下能力：

- 无需修改代码即可通过 YAML 配置文件增删检查项
- 支持单机百万行级别数据的秒级验证
- 清洗 → 验证 → 报告 → 持久化 全链路自动化
- 合成数据中注入 6 类真实错误，验证检查有效性

**应用场景**：数据仓库入库校验、ETL 管道质量门禁、数据湖合规性审计、实时流数据异常检测。

---

## 技术栈

| 层级 | 技术 | 版本 | 用途 |
|------|------|------|------|
| 分布式计算 | Apache Spark (PySpark) | 3.4.0 | 分布式数据读取、清洗、DQ 检查 |
| 编程语言 | Python | 3.9 | DQ 检查逻辑、报告生成 |
| 容器化 | Docker + Docker Compose | latest | 环境一致性、一键部署 |
| 数据库 | PostgreSQL | 15 | DQ 结果持久化存储 (JSONB 格式) |
| 配置管理 | YAML | — | DQ 检查项、报告参数声明式配置 |
| 关键依赖 | Pandas, NumPy, SciPy, Tabulate | — | 数据生成、统计计算、格式化输出 |

---

## 架构设计

```
                        ┌──────────────────────────┐
                        │  docker-compose.yml       │
                        │  ┌─────────────────────┐  │
                        │  │  postgres-db (pg15)  │  │
                        │  │  dq_results_log 表   │  │
                        │  └──────────┬──────────┘  │
                        │             │              │
                        │  ┌──────────▼──────────┐  │
                        │  │  spark-dq-app        │  │
                        │  │  ┌────────────────┐  │  │
                        │  │  │ main_pipeline  │  │  │
                        │  │  │ 1.Load Config  │  │  │
                        │  │  │ 2.Init Spark   │  │  │
                        │  │  │ 3.Load Parquet │  │  │
                        │  │  │ 4.Clean Data   │  │  │
                        │  │  │ 5.Run 18 DQ    │──┼──┼──► reports/*.json
                        │  │  │ 6.Write Report  │  │  │    reports/*.csv
                        │  │  │ 7.Log to PG     │──┼──┼──► PostgreSQL
                        │  │  └────────────────┘  │  │
                        │  │  dq_checks/          │  │
                        │  │  ├ completeness      │  │
                        │  │  ├ uniqueness        │  │
                        │  │  ├ validity          │  │
                        │  │  ├ consistency       │  │
                        │  │  ├ format            │  │
                        │  │  └ stats             │  │
                        │  └──────────────────────┘  │
                        └──────────────────────────┘
```

### 流水线执行流程

1. **加载配置** — 从 `config/dq_checks_config.yaml` 读取 18 项 DQ 检查定义
2. **初始化 Spark** — 创建本地 `SparkSession` (driver 2GB, local[*])
3. **加载数据** — 从 Parquet 文件读取原始数据 (~1M 行)
4. **数据清洗** — 空值标记、负数归零、无效邮箱/国家码修复、日期交换检测 (6 步清洗)
5. **去重** — Window 函数 `ROW_NUMBER() OVER (PARTITION BY transaction_id)` + 空键分离
6. **执行 18 项 DQ 检查** — 策略模式路由，在 `cleaned_df.cache()` 上依次执行
7. **生成报告** — JSON + CSV 双格式输出到 `reports/`
8. **持久化日志** — 将检查结果写入 PostgreSQL `dq_results_log` 表

---

## 快速开始

### 前置要求

- **Docker Desktop** (推荐 ≥ 4GB 内存)

### 1. 构建镜像

```powershell
docker compose build
```

### 2. 启动 PostgreSQL

```powershell
docker compose up -d postgres-db
```

### 3. 初始化数据库表（仅首次）

```powershell
docker compose exec postgres-db psql -U dq_user -d dq_db -c "
CREATE TABLE IF NOT EXISTS dq_results_log (
  log_id SERIAL PRIMARY KEY,
  check_run_timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
  check_name VARCHAR(255) NOT NULL,
  dq_check_timestamp TIMESTAMPTZ,
  status VARCHAR(10) NOT NULL CHECK (status IN ('PASS', 'FAIL', 'ERROR')),
  metric DOUBLE PRECISION,
  message TEXT,
  parameters JSONB,
  details JSONB,
  data_source VARCHAR(1024),
  pipeline_run_id VARCHAR(100)
);
CREATE INDEX IF NOT EXISTS idx_dq_log_check_name ON dq_results_log (check_name);
CREATE INDEX IF NOT EXISTS idx_dq_log_run_ts ON dq_results_log (check_run_timestamp);
CREATE INDEX IF NOT EXISTS idx_dq_log_status ON dq_results_log (status);
"
```

### 4. 生成合成数据

```powershell
docker compose run --rm spark-dq-app python3 src/data_generator.py
docker compose run --rm spark-dq-app python3 src/generate_users_data.py
```

### 5. 运行 DQ 流水线

```powershell
docker compose run --rm spark-dq-app python3 src/main_pipeline.py
```

### 6. 查看报告

```powershell
# JSON 报告
ls reports/

# 数据库日志
docker compose exec postgres-db psql -U dq_user -d dq_db -c "
  SELECT check_name, status, metric, message FROM dq_results_log ORDER BY log_id DESC LIMIT 18;
"
```

---

## DQ 检查清单

共 **18 项检查**，分为 **7 个类别**：

### 1. 完整性 Completeness (3 项)

| # | 检查项 | 列 | 阈值 | 原理 |
|---|--------|-----|------|------|
| 1 | `check_user_id_not_null` | user_id | 0% | `isNull() \| isnan()` 计数 |
| 2 | `check_price_not_null` | price | 6% | 同上，容忍 5% 注入的 NaN |
| 3 | `check_quantity_not_null` | quantity | 6% | 同上 |

### 2. 唯一性 Uniqueness (2 项)

| # | 检查项 | 列 | 阈值 | 原理 |
|---|--------|-----|------|------|
| 4 | `check_user_id_unique` | user_id | 0% | `groupBy + count > 1` 检测重复 |
| 5 | `check_transaction_id_unique_post_dedupe` | transaction_id | 0.1% | 去重后唯一性验证 |

### 3. 有效性 Validity (4 项)

| # | 检查项 | 列 | 阈值 | 原理 |
|---|--------|-----|------|------|
| 6 | `check_price_range_post_clean` | price | 0.1% | 范围检查 `price < 0` |
| 7 | `check_email_format_post_clean` | email | 0.1% | 正则 `rlike()` 验证邮箱格式 |
| 8 | `check_country_code_values_post_clean` | country_code | 0.1% | `isin()` 枚举值校验 |
| 9 | `check_order_status_values_post_clean` | order_status | 0.1% | 同上 |

### 4. 一致性 Consistency (2 项)

| # | 检查项 | 条件 | 阈值 | 原理 |
|---|--------|------|------|------|
| 10 | `check_end_date_after_start_date_final` | `end_date >= start_date` | 12% | `F.expr()` 自定义 SQL 条件 |
| 11 | `check_user_id_exists_in_users_ref` | user_id ∈ ref.user_id | 3% | `left_anti` join 参照完整性 |

### 5. 格式 Format (5 项)

| # | 检查项 | 列 | 阈值 | 原理 |
|---|--------|-----|------|------|
| 12 | `check_product_id_length` | product_id | 1% | `length()` 固定长度检查 |
| 13 | `check_country_code_length_post_clean` | country_code | 0.1% | `length()` 最小/最大长度 |
| 14 | `check_user_id_data_type` | user_id | 0% | `schema[col].dataType` 类型校验 |
| 15 | `check_price_data_type_post_clean` | price | 0% | 同上 |
| 16 | `check_start_date_data_type` | start_date | 0% | 同上 |
| 17 | `check_dates_marked_inconsistent_data_type` | dates_marked_inconsistent | 0% | 同上 |

### 6. 统计 Stats (1 项)

| # | 检查项 | 列 | 阈值 | 原理 |
|---|--------|-----|------|------|
| 18 | `check_price_mean_deviation` | price | 3σ | Z-score：`|actual_mean - expected_mean| / stddev` |

**检查函数分发**：`main_pipeline.py` 中的 `get_check_function()` 使用 **策略模式 (Strategy Pattern)**，根据 `check_type` + `sub_type` 动态路由到对应的检查函数。

---

## 数据清洗流水线

在执行 DQ 检查前，原始数据经过 **6 步清洗**（`apply_data_cleaning` 函数）：

| 步骤 | 操作 | 实现方式 |
|------|------|----------|
| 1. 保存原始值 | `price_original`, `email_original`, `country_code_original`, `order_status_original` | `withColumn` 备份 |
| 2. 价格修复 | 负价格 → 0.0, NaN 保持 | `F.when(price < 0, 0.0)` |
| 3. 邮箱修复 | 无效格式 → NULL | `F.when(rlike(regex), keep).otherwise(None)` |
| 4. 国家码修复 | 不在白名单 → "UNKNOWN" | `isin(allowed_countries)` |
| 5. 订单状态修复 | 不在白名单 → "UNKNOWN" | `isin(allowed_statuses)` |
| 6. 日期标记 | `end_date < start_date` → `dates_marked_inconsistent=True` | `F.when()` 布尔标记 |

### 去重策略

```
transaction_id 去重：ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY start_date DESC)
```

- **空键分离**：`transaction_id` 或 `start_date` 为 NULL 的行不参与去重，直接保留
- **保留最新**：相同 `transaction_id` 只保留 `start_date` 最新的那条
- **去重效果**：100 万行 → 97 万行 (去除 3 万条重复，与注入的 3% 一致)

---

## 合成数据与错误注入

使用 `data_generator.py` 生成 **100 万行**模拟电商交易数据，并注入 **6 类真实错误**：

| 错误类型 | 注入比例 | 注入方式 | 检测项 |
|----------|----------|----------|--------|
| **NaN 空值** | 5% | `df.loc[sample(5%), 'price'] = np.nan` | `check_price_not_null` |
| **负价格** | 1% | `price *= -1` | `check_price_range_post_clean` |
| **无效邮箱** | 2% | 去除 `@` 符号 | `check_email_format_post_clean` |
| **枚举错误** | 内置 | `'INVALID'`, `'UNK'` 替代合法值 | `check_country_code_values_post_clean` |
| **日期交换** | 10% | start_date ↔ end_date 互换 | `check_end_date_after_start_date_final` |
| **重复 ID** | 3% | 随机复制 transaction_id | `check_transaction_id_unique_post_dedupe` |

### 参考数据生成

`generate_users_data.py` 生成 **98.5 万**参考用户记录（`users.parquet`），其中 2% 的 user_id 被故意移除，用于测试参照完整性检查。

---

## 运行结果

### 最新运行指标 (2026-06-05)

```
数据集: 100 万行合成交易数据 (970,000 行去重后)
运行时: 37.16 秒 (完整流水线)
结果:   18/18 PASS ✅
```

### DQ 检查详情

| 检查项 | 状态 | 实测值 | 阈值 |
|--------|------|--------|------|
| `check_user_id_not_null` | ✅ PASS | 0.00% | 0.00% |
| `check_price_not_null` | ✅ PASS | 5.01% | 6.00% |
| `check_quantity_not_null` | ✅ PASS | 5.01% | 6.00% |
| `check_user_id_unique` | ✅ PASS | 0.00% | 0.00% |
| `check_transaction_id_unique_post_dedupe` | ✅ PASS | 0.00% | 0.10% |
| `check_price_range_post_clean` | ✅ PASS | 0.00% | 0.10% |
| `check_email_format_post_clean` | ✅ PASS | 0.00% | 0.10% |
| `check_country_code_values_post_clean` | ✅ PASS | 0.00% | 0.10% |
| `check_order_status_values_post_clean` | ✅ PASS | 0.00% | 0.10% |
| `check_end_date_after_start_date_final` | ✅ PASS | 10.05% | 12.00% |
| `check_product_id_length` | ✅ PASS | 0.00% | 1.00% |
| `check_country_code_length_post_clean` | ✅ PASS | 0.00% | 0.10% |
| `check_user_id_data_type` | ✅ PASS | 0.00% | 0.00% |
| `check_price_data_type_post_clean` | ✅ PASS | 0.00% | 0.00% |
| `check_start_date_data_type` | ✅ PASS | 0.00% | 0.00% |
| `check_dates_marked_inconsistent_data_type` | ✅ PASS | 0.00% | 0.00% |
| `check_price_mean_deviation` | ✅ PASS | 0.035σ | 3.00σ |
| `check_user_id_exists_in_users_ref` | ✅ PASS | 2.00% | 3.00% |

### 关键验证

- 注入的 5% NaN 被精确检测（5.01%），阈值 6% 合理包容
- 注入的 10% 日期交换被精确检测（10.05%），阈值 12% 合理包容
- 注入的 2% 缺失参照被精确检测（2.00%），阈值 3% 合理包容
- 清洗后价格范围错误降为 0%，验证了数据修复有效性

---

## 项目结构

```
spark-dq-pipeline/
├── config/
│   └── dq_checks_config.yaml      # DQ 检查配置 (18 项 + 报告 + 数据库)
├── src/
│   ├── main_pipeline.py           # 主流水线编排器 (328 行)
│   ├── data_generator.py          # 合成数据生成 (100 万行 + 6 类错误注入)
│   ├── generate_users_data.py     # 用户参考数据生成 (98.5 万行)
│   ├── analyze_failures.py        # 失败分析脚本
│   ├── dq_checks/
│   │   ├── __init__.py
│   │   ├── completeness.py        # 空值检查
│   │   ├── uniqueness.py          # 唯一性检查
│   │   ├── validity.py            # 有效性检查 (范围/正则/枚举)
│   │   ├── consistency.py         # 一致性检查 (自定义条件/参照完整性)
│   │   ├── format.py              # 格式检查 (长度/日期/类型)
│   │   └── stats.py               # 统计检查 (Z-score 均值偏差)
│   └── utils/
│       ├── __init__.py
│       ├── spark_utils.py         # Spark Session 管理
│       └── db_utils.py            # PostgreSQL 日志持久化
├── Dockerfile                     # Spark 应用镜像定义
├── docker-compose.yml             # 2 服务编排 (PostgreSQL + Spark)
├── requirements.txt               # Python 依赖
├── .dockerignore                  # Docker 构建排除
├── .gitignore                     # Git 版本控制排除
├── data/                          # (运行时生成) Parquet 数据文件
├── reports/                       # (运行时生成) JSON/CSV 报告
└── 运行全记录.md                   # 完整运行文档 (9 章节)
```

---

## 设计模式

### 1. 策略模式 (Strategy Pattern)

```python
# src/main_pipeline.py - get_check_function()
def get_check_function(check_type, sub_type=None):
    if check_type == "completeness":
        return completeness.check_not_null
    elif check_type == "uniqueness":
        return uniqueness.check_uniqueness
    elif check_type == "validity":
        if sub_type == "range":    return validity.check_range
        if sub_type == "regex":    return validity.check_regex
        if sub_type == "categorical": return validity.check_allowed_values
    # ...
```

YAML 配置中的 `check_type` + `sub_type` 在运行时动态映射到具体检查函数，新增检查类型无需修改流水线主逻辑。

### 2. 配置驱动 (Configuration-Driven)

所有 DQ 检查通过 YAML 声明式定义，支持添加/删除/调整阈值而无需重新构建镜像（配置文件通过 volume 挂载）。

### 3. 窗口去重 (Window Deduplication)

`ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY start_date DESC)` 保留每组最新记录，空键单独处理避免被误删。

### 4. left_anti Join 参照完整性

```sql
SELECT * FROM main_df
LEFT ANTI JOIN ref_df ON main_df.user_id = ref_df.user_id
```

比 `NOT IN` 子查询高效，充分利用 Spark 分布式 join 优化。

### 5. 缓存策略

清洗后的 DataFrame 执行 `.cache()`，18 项检查共享同一份内存数据，避免重复计算清洗步骤。

---

## Bug 调试记录

| # | Bug | 根因 | 解决方案 |
|---|-----|------|----------|
| 1 | `bitnami/spark` 镜像拉取失败 | Docker Hub GFW 限制 | 换用 `apache/spark-py:latest`，配置代理 |
| 2 | `pip install` 权限拒绝 | 默认 spark 用户无写权限 | `Dockerfile` 添加 `USER root` |
| 3 | `TypeError: 'JavaPackage' object is not callable` | pip 安装了 pyspark 4.1.2 但镜像 JVM 是 Spark 3.4.0 | 固定 `pyspark==3.4.0` |
| 4 | `python` 命令不存在 | `apache/spark-py` 只有 `python3` | 使用 `python3` 执行脚本 |
| 5 | `check_price_mean_deviation` 返回 `mean=nan` | **Spark NaN ≠ NULL**：`F.mean()` 包含 NaN 值导致结果为 NaN | 计算前过滤 `~F.isnan(col)` |

> Bug 5 是最关键的学习点：Spark 中 `NaN` 不是 `NULL`，不可依赖 `isNotNull()` 过滤 NaN。

---

## 面试要点

### 项目亮点 (STAR 法则)

- **Situation**: 需要构建一个面向大数据场景的 DQ 质量监控系统
- **Task**: 在 Docker 环境中使用 Spark 实现可配置、可扩展的 DQ 验证框架
- **Action**: 设计 7 类 18 项检查，使用策略模式 + YAML 配置驱动架构，实现清洗→验证→报告→持久化全链路
- **Result**: 100 万行数据 37 秒完成全量检测，6 类注入错误全部准确检出

### JD 匹配关键词

`Spark` `Python` `Scala` `ETL` `数据质量` `Hive` `数据仓库` `Flink` `Kafka` `大模型`

完整面试准备材料见：[`reports/interview-pack/`](reports/interview-pack/)

### 本项目涉及的大数据核心概念

- **分布式计算**：Spark RDD/DataFrame, lazy evaluation, partition, shuffle
- **数据质量**：完整性/唯一性/有效性/一致性/格式/统计 六大维度
- **数据清洗**：ETL 中的 T (Transform)，Window 去重，数据修复策略
- **容器化**：Docker 多服务编排，环境一致性
- **配置驱动**：声明式 YAML 配置，代码与逻辑解耦
- **持久化**：JDBC 写入 PostgreSQL，JSONB 灵活存储

---

## License

MIT

---

> 完整运行记录、调试过程、代码走读参见：[`运行全记录.md`](运行全记录.md)
