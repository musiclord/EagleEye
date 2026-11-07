"""Process-lifetime OS lock on a data directory: keeps a single DuckDB writer per directory."""
import os
from pathlib import Path

class DataDirectoryLease:
    """OS lock, not a PID file. A crash releases it without stale-PID guessing."""
    def __init__(self,directory):
        self.directory=Path(directory).resolve()
        self.file=None
    def acquire(self):
        if self.file is not None:return self
        self.directory.mkdir(parents=True,exist_ok=True)
        file=(self.directory/".eagleeye-writer.lock").open("a+b")
        file.seek(0,os.SEEK_END)
        if file.tell()==0:file.write(b"\\0");file.flush()
        file.seek(0)
        try:
            if os.name=="nt":
                import msvcrt
                msvcrt.locking(file.fileno(),msvcrt.LK_NBLCK,1)
            else:
                import fcntl
                fcntl.flock(file.fileno(),fcntl.LOCK_EX|fcntl.LOCK_NB)
        except (OSError,BlockingIOError) as exc:
            file.close()
            raise RuntimeError("此資料目錄已有鷹眼執行個體使用；請先完整退出該實例，避免兩個程序寫入同一資料庫。") from exc
        self.file=file
        return self
    def close(self):
        file,self.file=self.file,None
        if file is None:return
        try:
            file.seek(0)
            if os.name=="nt":
                import msvcrt
                msvcrt.locking(file.fileno(),msvcrt.LK_UNLCK,1)
            else:
                import fcntl
                fcntl.flock(file.fileno(),fcntl.LOCK_UN)
        finally:file.close()
