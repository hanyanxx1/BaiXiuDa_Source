pscc_vos_etl/             <-- 项目根目录
│
├── sql/                  <-- 存放所有数据库相关的 SQL 脚本
│   ├── 01_init_schema.sql      (把你刚才复制的建库、建表语句放这里)
│   ├── 02_extract_cc.sql       (存放提取 CC 库数据的那个带 LEFT JOIN 的查询 SQL)
│   └── 03_extract_vos.sql      (存放提取 VOS 库数据的查询 SQL)
│
├── scripts/              <-- 存放即将要写的 Python 清洗脚本
│   └── main_sync.py            (先建个空文件占位)
│
├── .gitignore            <-- Git 忽略文件（极其重要，防密码泄露）
└── README.md             <-- 项目说明文档（用来记录咱们扒出来的“有效话单”底层逻辑）


# PSCC & VOS3000 跨库话单对账系统 (ETL)

## 核心业务逻辑备忘
1. **CC 侧有效话单定义**：绝对不能用 `recording` 字段！真正的有效话单标准是 `answered = 1`。
2. **数据隔离**：CC 库必须强制加上 `admin_id = 1`。
3. **跨库匹配核心算法**：
   - 匹配基准：`CC.telephone` = `VOS.calleee164`
   - 时间容差：CC 开始时间与 VOS 起始时间误差在 ±3 秒内。