"""飞书高层写入操作。

覆盖迁移落地所需动作，均对齐官方接口与限额：
- 云空间：建文件夹、上传文件(≤20MB / 分片)、导入外部文档转飞书原生文档并轮询；
- 知识库：建知识空间(需 user_access_token)、建/挂节点、把云文档挂入 wiki；
- 权限：加协作者、收紧对外/链接分享。

大文件分片、import 轮询、权限收紧等细节封装在这里，编排器只调高层方法。
"""
from __future__ import annotations

import os
import time
from dataclasses import dataclass
from typing import Optional

from .client import FeishuClient

_UPLOAD_LIMIT = 20 * 1024 * 1024   # 单次上传 20MB 上限


@dataclass
class ImportResult:
    job_status: int          # 0=成功
    token: Optional[str]     # 结果文档 token
    url: Optional[str]
    note: str = ""


class FeishuWriter:
    def __init__(self, client: FeishuClient):
        self.c = client

    # ── 云空间：文件夹 ────────────────────────────────────

    def create_folder(self, name: str, parent_token: str = "") -> str:
        body = self.c.call(
            "POST", "/drive/v1/files/create_folder",
            bucket="drive_folder",
            json={"name": name, "folder_token": parent_token},
        )
        return body["data"]["token"]

    # ── 云空间：上传文件 ──────────────────────────────────

    def upload_file(self, local_path: str, parent_folder_token: str,
                    file_name: str | None = None, user_token: str = "") -> str:
        """上传文件到云空间文件夹，返回 file_token。>20MB 自动走分片。

        user_token 非空时以【用户身份】上传（文件归该用户所有）——挂入用户拥有的
        Wiki 空间必须如此，否则租户上传的文件无移动权限（131006）。
        """
        size = os.path.getsize(local_path)
        name = file_name or os.path.basename(local_path)
        if size <= _UPLOAD_LIMIT:
            return self._upload_all(local_path, parent_folder_token, name, size, user_token)
        return self._upload_chunked(local_path, parent_folder_token, name, size, user_token)

    def _upload_all(self, local_path: str, parent: str, name: str, size: int,
                    user_token: str = "") -> str:
        with open(local_path, "rb") as f:
            files = {"file": (name, f)}
            data = {
                "file_name": name, "parent_type": "explorer",
                "parent_node": parent, "size": str(size),
            }
            body = self.c.call(
                "POST", "/drive/v1/files/upload_all",
                bucket="drive_upload", data=data, files=files,
                user_token=user_token or None,
            )
        return body["data"]["file_token"]

    def _upload_chunked(self, local_path: str, parent: str, name: str, size: int,
                        user_token: str = "") -> str:
        # upload_prepare -> upload_part(多次) -> upload_finish
        prep = self.c.call(
            "POST", "/drive/v1/files/upload_prepare",
            bucket="drive_upload",
            json={"file_name": name, "parent_type": "explorer",
                  "parent_node": parent, "size": size},
            user_token=user_token or None,
        )["data"]
        upload_id = prep["upload_id"]
        block_size = int(prep.get("block_size", 4 * 1024 * 1024))
        block_num = int(prep.get("block_num", (size + block_size - 1) // block_size))
        with open(local_path, "rb") as f:
            for seq in range(block_num):
                chunk = f.read(block_size)
                self.c.call(
                    "POST", "/drive/v1/files/upload_part",
                    bucket="drive_upload",
                    data={"upload_id": upload_id, "seq": str(seq), "size": str(len(chunk))},
                    files={"file": ("part", chunk)},
                    user_token=user_token or None,
                )
        body = self.c.call(
            "POST", "/drive/v1/files/upload_finish",
            bucket="drive_upload",
            json={"upload_id": upload_id, "block_num": block_num},
            user_token=user_token or None,
        )
        return body["data"]["file_token"]

    # ── 云空间：导入为飞书原生文档 ────────────────────────

    def import_as_doc(self, file_token: str, file_extension: str,
                      target_type: str = "docx", mount_folder_token: str = "",
                      file_name: str = "", poll_timeout: float = 120.0) -> ImportResult:
        """把已上传文件导入转为飞书原生文档并轮询结果。

        注意：file_token 须 5 分钟内消费；file_extension 须与真实后缀严格一致。
        """
        create = self.c.call(
            "POST", "/drive/v1/import_tasks",
            bucket="import_task",
            json={
                "file_extension": file_extension.lstrip("."),
                "file_token": file_token,
                "type": target_type,
                "file_name": file_name or None,
                "point": {"mount_type": 1, "mount_key": mount_folder_token},
            },
        )
        ticket = create["data"]["ticket"]
        deadline = time.time() + poll_timeout
        while time.time() < deadline:
            res = self.c.call(
                "GET", f"/drive/v1/import_tasks/{ticket}", bucket="import_task",
            )["data"]["result"]
            status = res.get("job_status", 1)
            if status == 0:
                return ImportResult(0, res.get("token"), res.get("url"),
                                    note=self._import_warnings(res))
            if status in (1, 2):    # init / processing
                time.sleep(2)
                continue
            # 其余为失败态（100 加密 / 110 无权限 / 115 过大 / 7000-7002 超限…）
            return ImportResult(status, None, None,
                                note=f"import 失败 job_status={status}")
        return ImportResult(-1, None, None, note="import 轮询超时")

    @staticmethod
    def _import_warnings(res: dict) -> str:
        extra = res.get("extra") or []
        return f"截断/警告码: {extra}" if extra else ""

    # ── 知识库：空间与节点 ────────────────────────────────

    def create_wiki_space(self, name: str, description: str = "",
                          user_token: str = "") -> str:
        """建知识空间。**必须** user_access_token。返回 space_id。"""
        if not user_token:
            raise ValueError("create_wiki_space 需要 user_access_token")
        body = self.c.call(
            "POST", "/wiki/v2/spaces",
            bucket="wiki_space_create",
            json={"name": name, "description": description},
            user_token=user_token,
        )
        return body["data"]["space"]["space_id"]

    def create_wiki_node(self, space_id: str, title: str,
                         parent_node_token: str = "", obj_type: str = "docx",
                         user_token: str = "") -> dict:
        """在知识空间建节点（默认 docx）。返回 {node_token, obj_token}。

        空间由 user_access_token 创建时归该用户所有，租户(应用)在空间内无权限；
        故建节点须带同一 user_token（否则 131006 tenant needs edit permission）。
        """
        body = self.c.call(
            "POST", f"/wiki/v2/spaces/{space_id}/nodes",
            bucket="wiki_node",
            json={"obj_type": obj_type, "node_type": "origin",
                  "parent_node_token": parent_node_token, "title": title},
            user_token=user_token or None,
        )
        node = body["data"]["node"]
        return {"node_token": node["node_token"], "obj_token": node["obj_token"]}

    def move_doc_to_wiki(self, space_id: str, obj_type: str, obj_token: str,
                         parent_wiki_token: str = "", apply: bool = True,
                         user_token: str = "") -> dict:
        """把云空间已有文档挂入 wiki（异步，可能返回 task_id 需轮询）。

        user_token 同 create_wiki_node：用户拥有的空间须以用户身份写入。
        """
        body = self.c.call(
            "POST", f"/wiki/v2/spaces/{space_id}/nodes/move_docs_to_wiki",
            bucket="wiki_node",
            json={"obj_type": obj_type, "obj_token": obj_token,
                  "parent_wiki_token": parent_wiki_token, "apply": apply},
            user_token=user_token or None,
        )
        return body["data"]

    def mount_doc_to_wiki(self, space_id: str, obj_type: str, obj_token: str,
                          parent_wiki_token: str = "", poll_timeout: float = 60.0,
                          user_token: str = "") -> dict:
        """把云空间已有文档挂入 wiki 并拿到最终 wiki 节点 token（封装同步/异步两种返回）。

        move_docs_to_wiki 可能：
        - 同步返回 wiki_token（单文件常见）→ 直接返回；
        - 异步返回 task_id → 轮询 /wiki/v2/tasks/{id}?task_type=move 至完成再取 wiki_token。
        返回 {"wiki_token": <新节点 token 或 None>, "applied": bool}。
        """
        data = self.move_doc_to_wiki(space_id, obj_type, obj_token, parent_wiki_token,
                                     apply=True, user_token=user_token)
        wiki_token = data.get("wiki_token")
        if wiki_token:
            return {"wiki_token": wiki_token, "applied": bool(data.get("applied", True))}
        task_id = data.get("task_id")
        if not task_id:
            # 既无 wiki_token 又无 task_id：无法确认挂载结果
            return {"wiki_token": None, "applied": bool(data.get("applied", False))}
        return {"wiki_token": self._poll_move_task(task_id, user_token, poll_timeout),
                "applied": True}

    def _poll_move_task(self, task_id: str, user_token: str = "",
                        poll_timeout: float = 60.0) -> str:
        """轮询 move_docs_to_wiki 异步任务，返回结果节点 wiki_token（失败/超时抛异常）。"""
        deadline = time.time() + poll_timeout
        while time.time() < deadline:
            body = self.c.call(
                "GET", f"/wiki/v2/tasks/{task_id}",
                bucket="wiki_node", params={"task_type": "move"},
                user_token=user_token or None,
            )
            task = (body.get("data") or {}).get("task") or {}
            results = task.get("move_result") or []
            if results:
                first = results[0]
                node = first.get("node") or {}
                wt = node.get("wiki_token") or node.get("node_token")
                if wt:
                    return wt
                status = first.get("status")
                if status not in (None, 0):
                    raise RuntimeError(
                        f"move_docs_to_wiki 任务失败 status={status} {first.get('status_msg') or ''}")
            time.sleep(2)
        raise RuntimeError("move_docs_to_wiki 轮询超时")

    def upload_as_user_and_mount(self, space_id: str, local_path: str, name: str,
                                 parent_wiki_token: str = "", user_token: str = "",
                                 sensitive: bool = False,
                                 poll_timeout: float = 60.0) -> dict:
        """把本地文件以【用户身份】上传，再以用户身份挂进其 Wiki 空间。返回 {wiki_token, obj_token}。

        为什么必须这样：Wiki 空间由用户 OAuth 创建、归用户所有；实测只有【用户本人
        拥有的文档】能 move_docs_to_wiki 挂进去。租户(应用)上传的文件即便把用户
        加为 full_access 协作者、甚至 transfer_owner，move 仍报 131006 no move
        permission。故此处用 user_token 重新上传，使文件归用户所有再挂载。

        失败时回滚（删除刚上传的用户副本），避免重试反复累积孤儿文件。
        """
        if not user_token:
            raise ValueError("upload_as_user_and_mount 需要 user_access_token")
        file_token = self.upload_file(local_path, "", name, user_token=user_token)
        try:
            # 挂入前先收紧对外分享（以用户身份，对用户拥有的文件才有权限）；
            # 失败不阻断（Wiki 空间默认租户内可见，外泄风险低），仅内部记录。
            try:
                self.lock_down_external(file_token, "file", sensitive=sensitive,
                                        user_token=user_token)
            except Exception:  # noqa: BLE001
                pass
            data = self.move_doc_to_wiki(space_id, "file", file_token, parent_wiki_token,
                                         apply=False, user_token=user_token)
            wiki_token = data.get("wiki_token")
            if not wiki_token:
                task_id = data.get("task_id")
                if not task_id:
                    raise RuntimeError(f"move_docs_to_wiki 未返回 wiki_token/task_id: {data}")
                wiki_token = self._poll_move_task(task_id, user_token, poll_timeout)
            return {"wiki_token": wiki_token, "obj_token": file_token}
        except Exception:
            # 回滚：把刚上传的用户副本删除（进回收站，可恢复），保持重试幂等
            try:
                self.delete_drive_file(file_token, "file", user_token=user_token)
            except Exception:  # noqa: BLE001
                pass
            raise

    def delete_drive_file(self, token: str, obj_type: str = "file",
                          user_token: str = "") -> None:
        """删除云空间文件（进回收站，可恢复）。user_token 非空时以用户身份删除。"""
        self.c.call(
            "DELETE", f"/drive/v1/files/{token}",
            bucket="drive_folder", params={"type": obj_type},
            user_token=user_token or None,
        )

    def add_wiki_member(self, space_id: str, member_id: str,
                        member_type: str = "userid", role: str = "member") -> None:
        self.c.call(
            "POST", f"/wiki/v2/spaces/{space_id}/members",
            bucket="wiki_node",
            json={"member_type": member_type, "member_id": member_id,
                  "member_role": role},
        )

    # ── 权限 ──────────────────────────────────────────────

    def add_collaborator(self, token: str, obj_type: str, member_id: str,
                         perm: str = "view", member_type: str = "openid") -> None:
        """加协作者。perm: view/edit/full_access。"""
        self.c.call(
            "POST", f"/drive/v1/permissions/{token}/members",
            bucket="permission",
            params={"type": obj_type},
            json={"member_type": member_type, "member_id": member_id, "perm": perm},
        )

    def lock_down_external(self, token: str, obj_type: str,
                           sensitive: bool = False, user_token: str = "") -> None:
        """收紧对外/链接分享。sensitive=True 时进一步禁复制/下载/打印。

        user_token 非空时以用户身份收紧（用户拥有的文件租户无权限，需用户 token）。
        """
        payload = {
            "external_access": False,
            "link_share_entity": "closed" if sensitive else "tenant_readable",
            "invite_external": False,
            "share_entity": "same_tenant",
        }
        if sensitive:
            payload["security_entity"] = "only_full_access"
        self.c.call(
            "PATCH", f"/drive/v1/permissions/{token}/public",
            bucket="permission", params={"type": obj_type}, json=payload,
            user_token=user_token or None,
        )
