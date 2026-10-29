# s3shift —— 真正 serverless 的 Redshift

> **Redshift Serverless** 一有查询就开始按小时计费（最低 8 RPU ≈ $3/小时，每次查询最少按 60 秒算）。
> **s3shift** 在 **Lambda 里用 DuckDB 直接查你 S3 和 RDS 上的数据**，跑的是 Redshift SQL，没人查询时 ≈ $0。
> 客户端 API 一样（redshift_connector）；哪天长大了，改一行 import 就回到 Redshift——数据从没搬过家。

[English](README.md) · [兼容性报告](tests/compat/REPORT.md) · [任务](tests/compat/TASKS.md) · [更新日志](CHANGELOG.md)

![架构](docs/architecture.svg)

| | Redshift Serverless | s3shift |
|---|---|---|
| 空闲费用 / 月 | 空闲 $0，但一有查询 ≈ $3/小时（8 RPU），每次最少 60 秒 | ≈ $0；一次查询 = Lambda 秒数 + S3 读取 |
| 端点 | host:5439，PostgreSQL 协议，要进 VPC | Lambda Function URL，HTTPS，哪里都能连 |
| 数据 | 导进仓库，或用 Spectrum 查 S3 | 原地读：S3 文件和 RDS 表，什么都不导入 |
| 并发 | 查询共享 RPU，要排队 | 一个查询一个 Lambda：多个分析师互不等待 |
| 单次查询上限 | TB 级 | 一个 Lambda：最多 10 GB 内存、15 分钟 |
| 客户端 | `import redshift_connector` | `from s3shift import redshift_connector` |
| 控制台 | Query Editor v2 | 自带查询编辑器（Lambda URL） |
| 兼容性 | — | [见报告](tests/compat/REPORT.md) |

## 为什么

大多数团队用 Redshift 做日报、即席查询、定时 ETL，数据本来就在 S3 和 RDS 里。这些查询是一阵一阵的，仓库——
哪怕是 serverless 的——跑的时候按计算小时收钱。DuckDB 在一个进程里就能对同样的文件跑同样的 SQL；放进 Lambda，
每个查询一个引擎、按毫秒计费、不用保温。

## 原理

代码（或浏览器控制台）用 HTTP Basic 把 SQL 发到 Lambda Function URL。Lambda 开一个内存里的 DuckDB，在一个小目录
（S3 上一个 JSON）里查这条语句用到的表并挂上：外部表直接读你 S3 上的 Parquet / CSV / JSON（按 ETag 缓存在 /tmp），
RDS 表通过标准客户端（pymysql / psycopg2）读，托管表是 s3shift 自己写的 Parquet 对象。Redshift 特有写法（`DISTKEY`、
`NVL`、`GETDATE()`、`DATEADD(day, …)`、`LISTAGG … WITHIN GROUP`、`COPY`、`UNLOAD`）在交给 DuckDB 之前翻译或处理掉。
查询返回，引擎就没了。

## 快速开始

### 1. 部署

```bash
cp terraform/example.json terraform/my.json      # 改 aws_profile、env、user、password、read_buckets
terraform -chdir=terraform init
terraform -chdir=terraform apply -var-file=my.json
terraform -chdir=terraform output                 # console_url、bucket
terraform -chdir=terraform destroy -var-file=my.json   # 全部拆掉（托管表也一起删）
```

`apply` 会用 `pip` 下载 Linux 版的 DuckDB 和 RDS 客户端（本机要有 `python3`），打成一个 layer 发布。它会建一个 S3 桶
（目录、托管表、大结果保留一天）、一个带公网 Function URL 的 Lambda（3 GB 内存、15 分钟超时、10 GB /tmp）、一个能读
`read_buckets` 里那些桶的 IAM role。要查私有 RDS，设 `vpc_subnet_ids` / `vpc_security_group_ids`（这时 Lambda 需要
S3 网关终端节点）。CloudFormation：计划中（task 2）。

### 2. 打开查询编辑器

浏览器打开 `console_url`，用 `user` / `password` 登录。粘下面这段就有数据可以点（按顺序执行；最后两句把桶换成
`read_buckets` 里的一个，就能试你自己的数据）：

```sql
CREATE TABLE sales (id INT, region VARCHAR(20), product VARCHAR(20), amount DECIMAL(10,2), day DATE) DISTKEY(id) SORTKEY(day);
INSERT INTO sales VALUES (1, 'us', 'apple', 10.50, '2026-01-01'), (2, 'us', 'pear', 4.00, '2026-01-02'),
  (3, 'eu', 'apple', 7.25, '2026-01-02'), (4, 'eu', 'kiwi', 1.00, '2026-02-01'), (5, 'asia', 'kiwi', 12.00, '2026-02-03');
SELECT region, SUM(amount) AS total, LISTAGG(product, ',') WITHIN GROUP (ORDER BY product) AS products
  FROM sales GROUP BY region ORDER BY total DESC;
SELECT TO_CHAR(day, 'YYYY-MM') AS month, COUNT(*), SUM(amount) FROM sales GROUP BY 1 ORDER BY 1;
-- CREATE EXTERNAL TABLE events (user_id INT, kind VARCHAR) STORED AS PARQUET LOCATION 's3://my-lake/events/';
-- SELECT kind, COUNT(*) FROM events GROUP BY kind;
```

左边列出表、外部表、视图和 RDS schema，点一下预览。每次查询显示服务端耗时和从 S3 读了多少字节；结果可导出 CSV。

### 3. 程序连接

```bash
pip install s3shift
```

```python
from s3shift import redshift_connector   # 以后换 Redshift:`import redshift_connector`,其他不动

conn = redshift_connector.connect(host="xxx.lambda-url.us-east-2.on.aws", database="dev",
                                  user="admin", password="...")
cur = conn.cursor()
cur.execute("CREATE EXTERNAL SCHEMA app FROM POSTGRES DATABASE 'shop' URI 'db.xxx.rds.amazonaws.com' "
            "USER 'reader' PASSWORD '...'")
cur.execute("""SELECT c.country, SUM(o.amount) FROM orders o JOIN app.customers c ON c.id = o.customer_id
               WHERE o.day >= %s GROUP BY 1 ORDER BY 2 DESC""", ["2026-01-01"])
cur.fetchall()                 # ([country, Decimal(...)], ...)
cur.fetch_dataframe()          # 装了 pandas 就能用
cur.execute("UNLOAD ('SELECT * FROM orders WHERE day >= ''2026-01-01''') TO 's3://my-lake/exports/orders_' FORMAT AS PARQUET")
```

## 兼容性

[tests/compat/REPORT.md](tests/compat/REPORT.md) 每次跑测试自动生成。Redshift 从 PostgreSQL 继承的 SQL 在 Docker 里的真
PostgreSQL 16 和 s3shift 上各跑一遍，逐行比对结果和 Python 类型；Redshift 特有的语句（`COPY`、`UNLOAD`、外部表和外部
schema、`DISTKEY`、`NVL` …）按 Redshift 文档的行为验证。

| 状态 | 含义 |
|---|---|
| ✅ verified | 与参照一致，可放心依赖 |
| ❌ gap | 已知差异，备注指向 [TASKS.md](tests/compat/TASKS.md) 里修它的任务 |
| ⚪ quirk | PostgreSQL 和 Redshift 不一样的地方，s3shift 跟 Redshift |
| ⛔ unsupported | 明确拒绝（`NotSupportedError`），绝不静默走样 |

自己跑一遍——和 CI 每次 push 跑的一样：

```bash
python3 -m pip install pytest boto3 moto duckdb psycopg2-binary
python3 -m pytest tests -q                 # 不碰 Docker：PostgreSQL 在跑就实时比对，否则跳过兼容性测试
python3 -m pytest tests -q --docker up     # 在 Docker 里启动 PostgreSQL，等健康后测试；测完保持运行
python3 -m pytest tests -q --docker run    # 同上，全部通过就关掉；有失败就保持运行并提示
python3 -m pytest tests -q --docker down   # 同上，不管结果一律关掉
COMPAT_DRY=1 python3 -m pytest tests/compat -q -s        # 不用 Docker:只跑 s3shift 这边
```

**不支持的**——抛 `NotSupportedError`：存储过程、UDF、用户 / GRANT、Glue Data Catalog schema、物化视图。

## 保证

| | 靠什么 |
|---|---|
| **数据还是你的** | S3 文件和 RDS 表原地读；s3shift 只写自己的桶（以及你 `UNLOAD` 指定的地方） |
| **写入原子** | 写托管表 = 一次 PUT 替换它的 Parquet 对象，带 If-Match：并发写的那一方报错，绝不丢更新 |
| **目录一致** | `catalog.json` 同样用 If-Match 更新 |
| **结果完整** | 超过 4 MB 的结果存一天，`execute()` 里逐页取完；绝不截断 |
| **绝不重复写** | 驱动只在"请求确定没送达"时重发 |
| **可用性** | Lambda 和 S3 是区域级、多 AZ：没有集群会挂、要扩容或要暂停 |

## 适合谁

对 S3 和 RDS 上的数据做报表、即席分析、定时 ETL、日志分析——从几 MB 到分区好的几百 GB——用 Python 代码、notebook、
Python 仪表盘（Streamlit、Dash）。很多分析师同时查没问题：每个查询自己一个 Lambda。两条边界：单次查询要装得进一个
Lambda（10 GB 内存、15 分钟），以及没有 JDBC / PostgreSQL 协议，Java 的 BI 工具（QuickSight、Tableau、Metabase）连不上。
碰到任何一条，就是该换 Redshift 的时候——见 [MIGRATION.md](MIGRATION.md)。

## 目录

```
src/server/       服务本体:lambda_function.py(HTTP API + 控制台)、engine.py(DuckDB 上的 Redshift SQL)、index.html
client/           pip 包 s3shift:s3shift/redshift_connector.py、pyproject.toml
terraform/        部署(带 DuckDB 的 layer 在 apply 时由 pip 构建)
tests/            回归测试(moto);tests/compat/ 兼容性测试、REPORT.md、TASKS.md
```

## 发版

CI 每次 push 跑全部测试（含 PostgreSQL 对比）。发版就是打 tag：改 `client/pyproject.toml` 和 `client/s3shift/__init__.py`
的版本号，写 CHANGELOG，提交，然后 `git tag vX.Y.Z && git push --tags`（测试 → 构建 → PyPI；tag 必须等于版本号）。

---

**[Redefine AWS Serverless](https://github.com/topics/redefine-aws-serverless)** 系列之一——把空闲也计费的 AWS 服务，
用空闲 ≈ $0 的原语重做一遍。MIT。
