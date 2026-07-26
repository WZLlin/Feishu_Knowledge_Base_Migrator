"""源系统连接器。每个连接器统一 yield `SourceItem`，管线对源无感知。"""
from .base import BaseConnector
from .local_folder import LocalFolderConnector

__all__ = ["BaseConnector", "LocalFolderConnector"]
