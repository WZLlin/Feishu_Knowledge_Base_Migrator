# 迁移操作 SOP（标准作业流程）

供各组织 AI100 复用。按阶段执行，每步以**迁移台账**为准，可随时中断续跑。

> **两种操作方式，任选其一**：
> - **单页 Web 控制台（推荐，零命令）**：`uvicorn kb_migrator.web.app:app --host 127.0.0.1 --port 8000`
>   打开 → 「配置」填存凭证 → 「授权」测试连接/飞书 OAuth → 「迁移」填路径/空间ID一键触发看实时进度 →
>   「确认队列」复核 → 「目标结构」bootstrap + 写入。**务必绑定 127.0.0.1**（涉及凭证，勿暴露公网）。
> - **CLI**：见下文各阶段命令，适合脚本化/批处理。二者共用同一台账与 `.env`，可混用。

## 阶段 0 · 准备与摸底

1. **盘点范围**：列出本组织 VDI 外可迁移知识（制度/流程、项目资料、会议结论、模板、参考），
   标优先级（先制度/模板/高频复用）。用「盘点清单」登记来源系统、粗略数量、负责人。
2. **申请凭证**（`.env`）：
   - 飞书自建应用 `app_id`/`app_secret`，在开发者后台「权限管理」申请：
     `wiki:wiki`、`drive:drive`、`docx:document`；管理员发布。
   - SharePoint：Entra ID 应用（`client_credentials`），应用权限 `Sites.Read.All` +
     `Files.Read.All`，**管理员同意**（或用 `Sites.Selected` 收窄）。
   - 微盘：企业微信应用 `corpsecret`，把**服务器 IP 加入可信 IP 白名单**。
   - 群聊：确认是否开通「会话内容存档」、候选群成员授权覆盖率；托管 RSA 私钥。
3. **敲定治理规则**：填 `config/taxonomy.yaml` 的 owner/steward/复审节奏/保留期；
   参照 `docs/知识治理模板.md`。

## 阶段 1 · 建飞书知识库骨架

1. 用 **user_access_token** 建知识空间（`create_wiki_space` 需用户身份）。
2. 建一级目录（`config/taxonomy.yaml` 的 `all_folder_paths()`，含 `90 待整理`/`99 归档`）。
3. 配置空间成员与默认权限（管理员=owner、编辑=steward、成员=只读）。
4. 记录「分类路径 → 飞书 folder_token」映射，回填给 `load` 步骤的 `folder_map`。

## 阶段 2 · 跑迁移管线（四类源汇入同一台账、共用后续管线）

```
python cli.py scan-local <路径>              # 本地文件夹：盘点+抽取+精确去重
python cli.py scan-sharepoint                # SharePoint(MS Graph)；--site 关键字或 MS_SITE_FILTER 收窄站点
python cli.py scan-wedrive <空间ID1,空间ID2>  # 企业微信微盘（仅普通文件；服务器 IP 须在可信白名单）
python cli.py dedup                          # 近似去重（MinHashLSH）
python cli.py semantic                       # 语义去重（第三层，可选）
python cli.py classify                       # AI 分类
```
- **语义去重（第三层）**：`semantic` 用嵌入 + FAISS 找 cos≥阈值（默认 0.90，`--threshold` 调）的疑似
  重复对，把其中一方标 `dedup_verdict=semantic_candidate` **进人工队列**（不改 stage、不自动删），
  由 owner 定夺。**需可选重依赖** sentence-transformers + faiss-cpu；未装则 `available=False` 优雅跳过
  不阻断。Web 控制台入口 `POST /api/jobs/semantic`。
- **SharePoint**：需 `MS_TENANT_ID`/`MS_CLIENT_ID`/`MS_CLIENT_SECRET`（应用权限，管理员同意）；
  站点多时用 `--site 关键字` 或 `.env` 的 `MS_SITE_FILTER` 收窄，避免全量递归。
- **微盘**：仅普通文件（Word/Excel/PDF/PPT）可迁，原生微文档跳过并记 note；应用须为每个微盘空间成员。
- 群聊会话正文走**阶段 5** 的 `migrate-chat`（同样汇入本管线）；群里的**文件**用 `scan-wedrive` 迁。
- 观察 `python cli.py stats` 的**沉淀比例**与**失败/重复**计数。
- 失败项看 `error_detail`：旧格式需装 LibreOffice；扫描件需 OCR；锁文件自动跳过。
- **AI 分类默认走 Batch**（token 5 折，`KBM_CLAUDE_USE_BATCH=true`）；待分类数≥8 且在线时启用，
  中转网关不代理 batch 端点时**自动回退逐条**（带 prompt cache，功能不打折，仅不省那 5 折）。

## 阶段 3 · 人工确认（Web 控制台）

1. `uvicorn kb_migrator.web.app:app` 打开控制台 → 「人工确认队列」。
2. 逐项**确认归类**或**判为重复**；近似重复项默认落队列，由 owner 定夺保留哪份。
3. 高置信项（≥阈值且模型未标 needs_review）已自动确认，无需人工介入。

## 阶段 4 · 写入飞书

```
python cli.py load               # 默认 dry-run 预览（不上传）
python cli.py load --commit       # 真实写入（需 .env 配好飞书凭证 + 已 bootstrap 的 folder_map）
```
- **默认 dry-run**：不加 `--commit` 只预览写入计划，避免误上传；确认无误再加 `--commit` 真写。
- 每份文件上传到对应分类文件夹，按需 `import` 转飞书原生文档，写入即**收紧对外分享**。
- 敏感内容单独节点 + `only_full_access`（禁复制/下载/打印）。
- 全程回写台账 `feishu_token`/`feishu_url`，可追溯。
- **重试失败项**：`python cli.py retry-failed`（默认 dry-run 预览，`--commit` 才真写）——只回捞写飞书失败
  （`error_detail` 前缀 `load: `）的条目重排回 CONFIRMED 再跑；已上传成功（有 `feishu_token`）
  的条目**跳过重传**只重收紧，避免重复文件。Web 控制台亦有对应重试入口（`POST /api/jobs/retry`）。

## 阶段 4.5 · 挂入 Wiki（Wiki 目标态，需 OAuth）

若目标态是 **Wiki 知识空间**（`bootstrap --wiki`）而非云空间文件夹，`load` 之后再跑一步把文件挂进节点：

```
python cli.py push-to-wiki --dry-run    # 预览：待挂入数 + Wiki 节点分类数
python cli.py push-to-wiki --commit      # 真实挂入（需已完成 OAuth，读 data/feishu_user_token.json）
```

- **所有权约束（务必理解，实测踩坑）**：Wiki 空间由 OAuth 用户创建、**归该用户所有**；飞书只允许把
  **用户本人拥有的文档**挂进去。`load` 的文件是**应用(tenant)上传、应用拥有**的，即便把用户加
  `full_access` 协作者、甚至 `transfer_owner`，`move_docs_to_wiki` 仍报 `131006 no move permission`。
- **本工具的解法**：`push-to-wiki` **以用户身份从本地副本(`work_dir`)重新上传**（文件即归用户）再挂载，
  成功后把应用上传的旧副本**删入回收站**（去重、可恢复）。故此步依赖 `work_dir` 仍有原始文件副本。
- **幂等**：台账 `wiki_node_token` 是否存在即标记，已挂入的重跑自动跳过；挂载失败会回滚刚上传的用户副本
  （不留孤儿），记 `error_detail="wiki: ..."`，重跑自动重试。stage 保持 `LOADED` 不回退。
- Web 控制台亦有对应入口（`POST /api/jobs/push-to-wiki`）。

## 阶段 5 · 群聊迁移（POC，前提：已开通会话存档）

```
python cli.py migrate-chat <群聊ID> --name "群名" --limit 1000
```
1. **前置**：`.env` 配 `WECOM_CORP_ID`/`WECOM_CHAT_ARCHIVE_SECRET`、`WECOM_CHAT_PRIVATE_KEY_FILE`
   （RSA 私钥**只填路径**、独立隔离托管，不粘贴内容）、`WECOM_CHAT_SDK_LIB`（原生 WeWorkFinanceSdk 库路径）。
2. 选「成员少、已授权」试点群，跑 `migrate-chat`。命令内部：抽取（`seq` 增量 + 私钥解密）→
   **按自然日聚合成「会话片段」**，每片段渲染为 `.md` 作为 `SourceItem(WECOM_CHAT)` 进标准管线（置 EXTRACTED）。
   **在线时顺带下载群文件**：消息里的文件按 `sdkfileid` 拉媒体字节落盘，登记为独立 `SourceItem`
   （键 `wecom_chat:<群>:file:<sdkfileid>`），与会话片段一样进管线（`stats["files"]` 计数；下载失败记
   `error_detail="fetch: 群文件下载失败"` 不阻断）。离线假连接器无 `fetch_media` 时跳过、向后兼容。
3. 随后照常 `dedup` → `classify` → 确认 → `load`/`push-to-wiki`，与文件走同一后续流程。
4. **增量**：台账 `last_message_seq` 作游标，重跑只拉新消息；已推进到后续阶段（已分类/确认/入库）的
   旧片段跳过、不回退。`member_snapshot`（群成员快照）回写台账，供权限映射。
5. **未就绪降级**：未开通存档 / 无原生 SDK / 无私钥时 `online=False`，命令**不报错**，打印就绪指引并
   提示降级为「仅迁群文件」——群文件用 `scan-wedrive` 迁即可。Web 控制台「迁移」页做就绪性检测。

### 阶段 5.5 · 群聊治理（写飞书之后）：协作者映射 + 群名打标

```
python cli.py govern-chat <群聊ID> --url "<飞书入口链接>"           # 默认 dry-run 预览
python cli.py govern-chat <群聊ID> --url "<飞书入口链接>" --commit  # 真写
```
1. **成员→协作者映射**：人工维护一份本地 JSON（`.env` 的 `WECOM_FEISHU_USER_MAP` 指向
   `{企业微信userid: 飞书open_id}`）。`govern-chat` 读群成员快照，**给该群产出的每个飞书文档**逐个
   已映射成员加协作者（默认 `view`；wiki 节点用 `wiki_node_token`、云文件用 `feishu_token`）。
   未命中映射的成员进 `unmapped` 人工清单，不阻断。
2. **群名打标「原群名[已备份]」**：企业微信**只允许应用改名/发消息到该应用自建的服务群**。故按
   「尽力而为 + 降级」：先 `appchat/update` 改名 → 失败则 `appchat/send` 发含飞书入口的通知 →
   再失败记 `manual`（回写台账 `tag_status`，交群主手动改名）。需 `.env` 配 `WECOM_APP_SECRET`。
3. **默认 dry-run**：不加 `--commit` 只预览「将加的协作者数 + 打标动作」，确认无误再真写。
   Web 控制台入口 `POST /api/jobs/govern-chat`（同样 `dry_run` 缺省 True）。

## 断点续跑与幂等

- 任一命令可重复执行：已完成阶段的条目自动跳过。
- 幂等键：文件级 `source_type:source_id`；群聊级 `wecom_chat_id`。
- **台账是权威**：群名标识、文件存在与否只是辅助信号。

---

## 附：接入实操经验（试运行沉淀，供复用）

### A. Claude 分类接入（支持中转网关）
- `.env` 配 `ANTHROPIC_API_KEY`；若走**中转/自建网关**（key 形如 `sk-xxx` 而非官方
  `sk-ant-`），额外配 `ANTHROPIC_BASE_URL`（如公司 LLM 网关）。
- 鉴权自动判别：官方长 key 用 `x-api-key`，中转短 key 用 `Bearer`（`KBM_CLAUDE_AUTH_STYLE=auto`）。
- 验证：`python cli.py classify` 打印 `classifier online=True` 即接通；离线无 key 时自动
  降级关键词启发式（一律进人工队列），保证管线可跑。
- **阈值**：`KBM_CONFIDENCE_THRESHOLD`，0.7=召回优先（自动确认更多）、0.85=保守。
  注意 `needs_human_review=true` 会**覆盖**阈值强制进人工——双因子门禁。

### B. 旧格式 `.doc/.ppt/.xls` 提取（Windows 免安装方案）
- **本机装了 Office 时优先走 Office COM**（`pywin32`，无需管理员、无需装 LibreOffice），
  保真度高；无 Office 才回退 LibreOffice（`KBM_SOFFICE` 指定 soffice 路径）。
- 企业机常无 admin/包管理器（winget/choco），Office COM 是最省事的落地路径。

### C. 扫描件 OCR
- 代码路径：PyMuPDF 渲染每页 → `pytesseract`（`chi_sim+eng`）。
- 需另装**系统 Tesseract 引擎 + 中文语言包**（`chi_sim.traineddata` 放入 `tessdata/`），
  用户目录安装即可（NSIS 安装器 `/S /D=<用户目录>`），在 `.env` 设 `KBM_TESSERACT`
  指向 `tesseract.exe`。缺引擎时该项优雅降级并在 `error_detail` 标注。

### D. Windows 控制台中文乱码
- CLI 已强制 stdout/stderr 走 UTF-8；若仍乱码，运行前 `set PYTHONUTF8=1`。

### E. 阶段1 建目标结构（bootstrap）
- **最快真实写入**（无需 OAuth）：`python cli.py bootstrap` 建云空间文件夹树，仅需
  应用凭证（`drive:drive`）；`分类→token` 映射持久化到 `data/feishu_targets.json`，幂等。
- **建 Wiki 空间**（阶段1目标态，需 OAuth）：启动控制台 → 浏览器访问
  `/feishu/oauth/login` 授权 → `user_access_token` 自动落盘 →
  `python cli.py bootstrap --wiki`；随后 `load` → `push-to-wiki`（见阶段 4.5）把文件挂进节点。
- `python cli.py targets` 随时查看已回填映射；`python cli.py load` 真实写入。
