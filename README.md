# kb-migrator · 组织知识飞书集中沉淀与持续治理工具集

把分散在 **SharePoint / 本地文件夹 / 企业微信（微盘 + 群聊）** 的组织知识，可控地迁入
**飞书**知识库，并把「分类 / 权限 / 持续治理」一起跑起来。

- **交付形态**：Python 工具集（连接器 + 处理管线）+ 轻量 Web 控制台。
- **自动化**：AI 辅助 + 人工确认（关键动作由知识 owner 复核后入库）。
- **AI 能力**：Claude 结构化分类 / 元数据抽取（enum 约束目录，杜绝编造）。

> 设计与执行方案详见 plan 文档；治理方法与模板见 `docs/`。

## 架构

```
Web 控制台(FastAPI) ── 任务下发 / 进度看板 / 去重·分类人工确认队列
        │
处理管线(可断点续跑, 每阶段落台账)
  抽取 → 文本提取 → 去重 → AI分类·元数据 → 人工确认 → 写飞书
        │
连接器: 本地文件夹 / SharePoint(Graph) / 微盘(WeDrive) / 群聊(会话存档)
        │
迁移台账(SQLite) ← 幂等·追溯·断点恢复的唯一权威
```

## 快速开始

```bash
pip install -r requirements.txt
cp .env.example .env         # 填入凭证（凭证只从环境读取，不写死）

# 盘点+抽取：四类源都汇入同一台账、走同一后续管线
python cli.py scan-local  D:/知识资料     # 本地文件夹：盘点 + 抽取 + 精确去重(SHA256)
python cli.py scan-sharepoint             # SharePoint(MS Graph)；--site 关键字或 MS_SITE_FILTER 收窄站点
python cli.py scan-wedrive 空间ID1,空间ID2  # 企业微信微盘(仅普通文件；服务器 IP 须在可信白名单)
python cli.py migrate-chat <群聊ID> --name "群名"  # 群聊会话存档→按天聚合为会话片段进管线；在线时顺带下载群文件(需存档 SDK + RSA 私钥)

python cli.py dedup                       # 近似去重(MinHashLSH，中文字符 4-gram)
python cli.py semantic                     # 语义去重(第三层，标疑似重复进人工队列；需可选重依赖，缺则自动跳过)
python cli.py classify                    # AI 分类(无 API Key 时走离线启发式)
python cli.py stats                       # 各阶段条目数 + 沉淀比例
python cli.py runs                        # 最近管线执行批次、状态与统计
python cli.py source-changes              # 查看源端内容/属性变化审计队列（不自动覆盖已入库副本）
python cli.py apply-source-changes         # 为待处理变更派生 #revN 新版本
python cli.py finalize-source-change <变更ID> --commit  # 新版本入库后归档旧版本
python cli.py missing-sources              # 查看本地源端已删除条目
python cli.py resolve-missing "<key>" --action keep     # 保留知识副本并关闭缺失事件
python cli.py resolve-missing "<key>" --action archive --commit  # 归档源端已删知识
python cli.py failures                    # 查看失败项、失败阶段与可重试性
python cli.py permissions "sharepoint:<drive>/<item>"  # 查看最近一次采集到的来源权限
python cli.py governance                  # 查看复审到期、归档到期、待整理、无 owner 队列
python cli.py complete-review "<key>" --actor "<复审人>"  # 完成复审并按 taxonomy 顺延
python cli.py archive                     # 预览保留期到期的归档动作
python cli.py archive --commit            # 真实移入 99 归档（Drive/Wiki；Wiki 需 OAuth）
python cli.py insights                    # 查看人工分类反馈与待整理主题信号
python cli.py review                      # 人工确认队列
python cli.py confirm "<key>" "01 制度与流程"

# 阶段1：在飞书建目标结构 + 回填「分类->token」映射，再真实写入
python cli.py bootstrap                    # 建云空间文件夹树(仅需应用凭证，无需 OAuth)
python cli.py targets                       # 查看已回填的映射
python cli.py load --dry-run                # 预览写入计划(不真实上传)
python cli.py load --commit                 # 真实写入（需已 bootstrap；不加 --commit 仅预览）
python cli.py retry-failed                  # 重试写飞书失败项(load 阶段；已上传的不重传)
python cli.py retry-failed --stage fetch    # 重排下载失败项，再运行对应 scan 命令
python cli.py retry-failed --stage extract  # 重排抽取失败项，再运行对应 scan 命令
python cli.py retry-failed --stage dedup    # 重排后重新运行 dedup
python cli.py retry-failed --stage wiki     # 重排后重新运行 push-to-wiki

# 阶段5(Wiki 模式)：把已 load 的文件挂进 Wiki 分类节点(需先 bootstrap --wiki + OAuth)
python cli.py push-to-wiki --dry-run        # 预览：待挂入数 + Wiki 节点分类数
python cli.py push-to-wiki --commit         # 以【用户身份】重传并挂入 Wiki，清理租户旧副本

# 群聊治理（写飞书之后）：成员→协作者映射 + 群名打标「原群名[已备份]」
python cli.py govern-chat <群聊ID> --url "<飞书入口链接>"           # 默认 dry-run：预览将加的协作者 + 打标动作
python cli.py govern-chat <群聊ID> --url "<飞书入口链接>" --commit  # 真写：逐个群文档加协作者 + 尽力改群名(失败降级发通知/记人工)

# Web 控制台（单页可视化，所有基本操作一页完成）
uvicorn kb_migrator.web.app:app --host 127.0.0.1 --port 8000
```

#### 一键起停：`console.py`（推荐）

控制台需**常驻运行**（供你在浏览器里持续操作），用随附的 `console.py` 管理，服务以**后台分离进程**运行，关掉终端窗口也不会停：

```bash
python console.py start      # 启动：版本握手+就绪检查通过后自动开浏览器
python console.py stop       # 关闭
python console.py restart    # 重启
python console.py status     # 查看运行状态 + /api/status
# 可选：python console.py start --no-browser  # 启动但不自动开浏览器
```

Windows 也可**双击** `start-console.bat` / `stop-console.bat`（内部就是调 `console.py`）。

- **绑定 `127.0.0.1:8000`（勿暴露公网）**；PID 记到 `data/webconsole.pid`，日志写 `data/uvicorn.log`；仅当已运行实例通过版本握手和就绪检查时才直接复用。
- 用 `sys.executable -m uvicorn` 启动，拿到的 **PID 就是真正的服务进程**（避开 Windows `python` 启动器派生子进程、PID 杀不准的坑）；关闭优先按 PID，装了 `psutil` 时再按监听端口兜底扫一遍。
- 修改或更新源码后执行 `python console.py restart`，不要只刷新浏览器。页面启动时会校验
  `/api/meta`（接口协议和能力）及 `/api/health/ready`（SQLite、taxonomy、运行路径）；
  动态页面和 API 均返回 `Cache-Control: no-store`，避免旧页面缓存。
- 排障：先运行 `python console.py status`；起不来看 `data/uvicorn.log` 末尾。若提示端口上的
  服务是旧版本，执行 `python console.py restart` 后在页面点击「重新检查」。

### 统一 Web 控制台

一个页面七个标签，无需记命令、无需手改 `.env`：

| 标签 | 能做什么 |
|---|---|
| **概览** | 各阶段条目数、**沉淀比例**、知识健康度、凭证/授权/目标结构状态、最近任务 |
| **配置** | 表单填写并保存 飞书 / Claude / 企业微信(微盘·群聊) / SharePoint 凭证 + 阈值（写入 `.env` 并热生效；密钥脱敏、留空=不改；群聊 RSA 私钥只填**路径**） |
| **授权** | 一键飞书 OAuth（建 Wiki 空间用）；飞书 / 企业微信**连接测试** |
| **迁移** | 本地/SharePoint/微盘/群聊迁移，近似与语义去重、AI 分类、群聊治理预览/执行及实时日志 |
| **确认队列** | 逐项确认归类 / 判为重复 |
| **治理** | 失败、复审、归档、无责任人、待整理队列，完成复审、分类阈值建议、待整理知识簇 |
| **结构工作台** | 自动生成并人工编辑规划目录；与 Drive/Wiki 现状三栏对比；保存草稿、校验、差异预览、最终确认和版本化发布 |

#### 结构工作台推荐流程

1. 打开「结构工作台」，系统首次把 taxonomy 和已有 `feishu_targets.json` 导入为版本化结构。
2. 在左侧规划树新增、重命名、删除、排序或拖拽目录；可生成只读建议，也可从历史版本
   恢复到当前草稿。右侧飞书结构始终只读，支持逐层或全部展开/收起；搜索会自动展开
   命中路径，并可执行“采纳、映射、外部管理、待合并、待归档”等本地规划操作。
3. 点击「保存草稿」后刷新飞书现状，再生成差异计划。`CREATE/MAP/MOVE/CONFLICT`
   会分别显示，飞书侧独有目录只标记为 `REMOTE_ONLY`，不会自动删除。
4. 差异无阻断冲突后点击「最终确认」冻结版本。可指定 1 人或多人审批；多人模式下，
   首次审批后版本进入 `reviewing` 并禁止继续编辑，达到所需人数才转为 `approved`。
   确认本身不写飞书；随后审批持久化发布计划，先「预览发布」，再显式点击
   「执行并激活」。
5. 仅当所有规划节点创建或绑定成功后，新版本才成为迁移路由版本。迁移条目记录
   `structure_version_id + target_node_id`；后续改名不再破坏历史分类关系。

结构保存采用 revision 乐观锁，避免多人后保存覆盖先保存。已激活版本不可原地修改；
迁移期间调整会产生新草稿，当前任务继续使用启动时已绑定的版本。

- **发布范围**：默认“仅未迁移文件”，重命名、移动和合并只切换后续路由，旧目录及历史
  内容原样保留；“包含失败与重试文件”还会让待重试项采用新路由。只有显式选择并审批
  “允许调整历史内容”时，激活后才会生成独立历史重定位计划；结构发布器本身永不搬动
  既有文件。
- **目录合并**：勾选两个或多个规划节点，选择保留目标后生成 `MERGE + RETIRE`。
  发布只改变后续路由。历史文件进入独立计划，并在执行前完成内容和权限预检；旧目录
  始终保留，不自动删除。
- **规则拆分**：勾选一个分类目录后可创建两个或更多子目录，并按文件名、来源路径、
  文档类型、年份或 `metadata.*` 字段配置等于、包含、前缀、正则和列表规则。规则按优先级
  执行且同级必须有且最多一个兜底目录；未配置时自动增加“90 待整理”，避免未命中内容
  静默留在原目录。
- **历史重定位**：结构激活后可生成逐文件候选计划，人工取消不应搬迁的条目并保存选择。
  “远程冲突预检”读取目标目录和来源/目标公开权限：同名不同内容、疑似近似内容或目标
  权限更开放时，会在移动第一项前整体阻断；同名且 SHA-256 相同则按精确重复处理，不覆盖。
  审批后执行采用逐项持久化状态，中断可续跑；只有本计划实际移动且记录了原父目录的条目
  允许受限回滚。
- **安全重命名**：在目标位置创建替代节点并切换新写入绑定，旧节点和历史内容保持不变；
  历史文件只能通过独立重定位计划移动。
- **治理策略**：合并后使用更短的复审周期和更长的保留期，owner/steward 冲突保留为
  阻断条件，用户必须明确选择合并后的值。节点右侧“⚙”可以继续调整治理属性。
- **审计**：`GET /api/structures/{version_id}/audit` 返回结构事件、合并意图、目录重定位及
  历史文件重定位计划和逐项结果。
- **健康检查**：`GET /api/structures/{version_id}/health` 汇总层级深度、远程绑定率、
  owner/steward 覆盖率、空叶目录、规则命中情况和历史重定位候选。

> **安全与可靠性**：控制台涉及凭证操作，**务必绑定 `127.0.0.1`**（本机运维工具，勿暴露公网）；
> GET 接口不回明文密钥；OAuth state 使用一次性服务端记录并绑定 HttpOnly Cookie。任务快照持久化到
> `KBM_JOBS_DB`，重启后的未完成任务会标为 `interrupted`，并发上限由
> `KBM_MAX_CONCURRENT_JOBS` 控制。写入、重试、Wiki 挂载和归档 API 缺省均为 dry-run；
> 页面上的真实写操作另有确认提示。

### 飞书接入所需凭证（阶段1）

1. **应用凭证**（云空间文件夹树 + 上传，tenant token，**无需 OAuth**）：
   在 `.env` 填 `FEISHU_APP_ID` / `FEISHU_APP_SECRET`（飞书开放平台「自建应用」→凭证与基础信息），
   并在应用权限里开通 `drive:drive`（云空间读写）。→ 直接 `python cli.py bootstrap` 即可。
2. **建 Wiki 知识空间**（需 **user_access_token**，走一次 OAuth）：
   - `.env` 设 `FEISHU_REDIRECT_URI=http://localhost:8000/feishu/oauth/callback`
     （并在应用「安全设置」里登记同一重定向 URL），应用开通 `wiki:wiki` 权限；
   - 启动控制台后浏览器访问 **`/feishu/oauth/login`** → 飞书授权 → 回调自动把
     `user_access_token` 落盘到 `data/feishu_user_token.json`；
   - 再运行 `python cli.py bootstrap --wiki`（自动读取该 token）建空间 + 分类节点。
3. **把文件挂进 Wiki**（`push-to-wiki`）：先跑完 `load`（文件先落云空间），再
   `python cli.py push-to-wiki --commit` 把每份文件挂到对应分类节点下。
   > **所有权约束（实测）**：Wiki 空间由 OAuth 用户创建、**归该用户所有**；飞书只允许把
   > **用户本人拥有的文档** `move_docs_to_wiki` 挂进去。`load` 阶段的文件是**应用(tenant)
   > 上传、应用拥有**的——即便把用户加为 `full_access` 协作者、甚至 `transfer_owner`，
   > 挂载仍报 `131006 no move permission`。故 `push-to-wiki` 会**以用户身份从本地副本重新
   > 上传**（文件归用户）再挂载，成功后把应用上传的旧副本删入回收站（去重、可恢复）。
   > 因此 `push-to-wiki --commit` 依赖本地 `work_dir` 仍保留原始文件副本。

## 模块

| 目录 | 说明 |
|---|---|
| `kb_migrator/connectors/` | 源系统连接器，统一输出 `SourceItem` |
| `kb_migrator/pipeline/` | 文本提取 / 去重 / Claude 分类 / 编排器 |
| `kb_migrator/feishu/` | 飞书认证 / 限流客户端 / 高层写入 |
| `kb_migrator/structure/` | 目录版本、稳定节点、远程快照、差异计划与安全发布 |
| `kb_migrator/ledger.py` | 迁移台账（幂等 / 版本 / 管线批次 / 状态事件 / 断点追溯） |
| `kb_migrator/web/` | 单页 Web 控制台（`app.py` API + `static/index.html` 前端 + `jobs.py` 任务运行器 + `settings_io.py` 凭证读写） |
| `config/taxonomy.yaml` | 目录树 + 分类枚举 + 治理元数据 |
| `docs/` | 操作 SOP · 治理模板 · 迁移记录模板 |

## 依赖分层

- **MVP 必需（纯离线可跑）**：pydantic / PyYAML / python-docx / openpyxl / charset-normalizer / datasketch。
- **按需重依赖**：sentence-transformers + faiss-cpu（语义去重，默认注释）；旧格式 .doc/.ppt/.xls 转换——**Windows 装了 Office 时自动走 Office COM(pywin32，免安装)**，否则回退 LibreOffice；扫描件 OCR——pymupdf + pytesseract + 系统 Tesseract 引擎(含 chi_sim 语言包，路径用 `KBM_TESSERACT` 指定)。
- **联外**：anthropic（Claude）/ httpx / msal（SharePoint）/ pycryptodome（群聊解密）。

## 平台边界（重要）

1. 企业微信**原生微文档/智能表格无法 API 批量导出**，仅微盘里的文件（Word/Excel/PDF）可迁。
2. 企业微信**群聊历史**须开通「会话内容存档」且成员逐个授权，消息保留期有限。
3. **群名打标**：企业微信只允许应用改名/发消息到**该应用自建的服务群**；对用户自建群
   `govern-chat` 会尽力改名→降级发通知→再不行记 manual（回写台账 `tag_status`，交群主手动改名）。
4. **成员→协作者映射**需人工维护一份本地 JSON（`WECOM_FEISHU_USER_MAP` 指向
   `{企业微信userid: 飞书open_id}`）；未命中的成员进 `unmapped` 人工清单，不阻断整批。
5. **来源文件权限同步**：`KBM_IDENTITY_MAP_FILE` 指向 `{来源邮箱或用户ID: 飞书open_id}`。
   SharePoint 成功采集的来源权限以及 taxonomy 的 owner/steward 会在 `load --commit` 和
   `push-to-wiki --commit` 时映射为协作者。角色变化会更新；源端撤权时只撤销本工具曾授予的
   协作者，不触碰飞书侧人工添加成员。未映射主体仅记审计与统计，不扩大默认访问范围。

## 测试

```bash
python -m pytest -q     # 台账 / 去重 / 命名 / 本地连接器 / 编排器端到端
```
