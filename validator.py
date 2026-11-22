"""动态批处理结果排序验证器。

离线消费事件流（到达顺序、批次组成、token 流、完成、取消、失败、deadline），
计算每个请求的实际输出、状态与资源释放点，并报告顺序/迟到 token 等违规。

仅消费离线事件，不与任何在线推理服务交互。
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional


class Status:
    PENDING = "pending"                # 已到达，尚未产出 token
    STREAMING = "streaming"            # 正在流式输出
    COMPLETED = "completed"            # 正常完成
    CANCELLED = "cancelled"            # 客户端取消
    FAILED_RETRYABLE = "failed_retryable"   # 批次失败，未产生任何输出，可重试
    FAILED_PARTIAL = "failed_partial"       # 批次失败，已产生部分输出，不可直接重试
    TIMEOUT = "timeout"                # 超过 deadline


TERMINAL_STATUSES = frozenset([
    Status.COMPLETED,
    Status.CANCELLED,
    Status.FAILED_RETRYABLE,
    Status.FAILED_PARTIAL,
    Status.TIMEOUT,
])


@dataclass
class RequestRecord:
    request_id: str
    arrival_time: float
    deadline: Optional[float] = None
    batch_id: Optional[str] = None
    status: str = Status.PENDING
    expected_seq: int = 0
    delivered_tokens: List[str] = field(default_factory=list)
    finish_time: Optional[float] = None
    release_time: Optional[float] = None


@dataclass
class BatchRecord:
    batch_id: str
    request_ids: List[str]
    formed_time: float
    failed: bool = False
    release_time: Optional[float] = None


@dataclass
class Violation:
    time: float
    kind: str
    request_id: Optional[str]
    detail: str


class OrderingValidator:
    """按时间顺序消费事件，维护每请求/每批次状态机。"""

    def __init__(self):
        self.requests = {}  # type: Dict[str, RequestRecord]
        self.batches = {}   # type: Dict[str, BatchRecord]
        self.violations = []  # type: List[Violation]
        self._now = 0.0

    # ------------------------------------------------------------------ API

    def process(self, events):
        """消费完整事件流并返回报告。事件必须按 time 非递减排列。"""
        for event in events:
            self.apply(event)
        return self.report()

    def apply(self, event):
        etype = event["type"]
        t = event["time"]
        if t < self._now:
            raise ValueError("events must be ordered by non-decreasing time")
        self._advance_time(t)

        handler = {
            "arrival": self._on_arrival,
            "batch_formed": self._on_batch_formed,
            "token": self._on_token,
            "complete": self._on_complete,
            "cancel": self._on_cancel,
            "batch_failed": self._on_batch_failed,
        }.get(etype)
        if handler is None:
            raise ValueError("unknown event type: %r" % etype)
        handler(event)

    def finalize(self, end_time):
        """在观察窗口结束时触发所有到期 deadline。"""
        self._advance_time(end_time)
        return self.report()

    def report(self):
        return {
            "requests": {
                rid: {
                    "status": rec.status,
                    "delivered_tokens": list(rec.delivered_tokens),
                    "batch_id": rec.batch_id,
                    "finish_time": rec.finish_time,
                    "release_time": rec.release_time,
                }
                for rid, rec in self.requests.items()
            },
            "batches": {
                bid: {
                    "request_ids": list(brec.request_ids),
                    "failed": brec.failed,
                    "release_time": self._batch_release_time(brec),
                }
                for bid, brec in self.batches.items()
            },
            "violations": [
                {
                    "time": v.time,
                    "kind": v.kind,
                    "request_id": v.request_id,
                    "detail": v.detail,
                }
                for v in self.violations
            ],
        }

    # ------------------------------------------------------------- internal

    def _advance_time(self, t):
        self._now = t
        for rec in self.requests.values():
            if (
                rec.deadline is not None
                and rec.status not in TERMINAL_STATUSES
                and rec.deadline <= t
            ):
                self._terminate(rec, Status.TIMEOUT, rec.deadline)

    def _terminate(self, rec, status, t):
        rec.status = status
        rec.finish_time = t
        rec.release_time = t  # 请求槽位在终态时刻释放

    def _batch_release_time(self, brec):
        """批次资源在所有成员都到达终态后才释放，取最大终态时刻。"""
        times = []
        for rid in brec.request_ids:
            rec = self.requests.get(rid)
            if rec is None or rec.finish_time is None:
                return None
            times.append(rec.finish_time)
        return max(times) if times else brec.formed_time

    def _record_violation(self, t, kind, request_id, detail):
        self.violations.append(Violation(t, kind, request_id, detail))

    # ------------------------------------------------------------- handlers

    def _on_arrival(self, event):
        rid = event["request_id"]
        if rid in self.requests:
            self._record_violation(event["time"], "duplicate_arrival", rid,
                                   "request arrived twice")
            return
        self.requests[rid] = RequestRecord(
            request_id=rid,
            arrival_time=event["time"],
            deadline=event.get("deadline"),
        )

    def _on_batch_formed(self, event):
        bid = event["batch_id"]
        if bid in self.batches:
            self._record_violation(event["time"], "duplicate_batch", None,
                                   "batch %s formed twice" % bid)
            return
        self.batches[bid] = BatchRecord(
            batch_id=bid,
            request_ids=list(event["request_ids"]),
            formed_time=event["time"],
        )
        for rid in event["request_ids"]:
            rec = self.requests.get(rid)
            if rec is None:
                self._record_violation(event["time"], "unknown_request", rid,
                                       "batched before arrival")
                continue
            if rec.batch_id is not None:
                self._record_violation(event["time"], "double_batching", rid,
                                       "request assigned to two batches")
                continue
            rec.batch_id = bid

    def _on_token(self, event):
        rid = event["request_id"]
        t = event["time"]
        rec = self.requests.get(rid)
        if rec is None:
            self._record_violation(t, "unknown_request", rid,
                                   "token for unknown request")
            return
        if rec.batch_id != event.get("batch_id"):
            self._record_violation(t, "batch_mismatch", rid,
                                   "token emitted under wrong batch")
            return
        if rec.status in TERMINAL_STATUSES:
            # 取消/完成/失败后到达的迟到 token：不得交付给客户端
            self._record_violation(t, "late_token", rid,
                                   "token arrived after request reached %s" % rec.status)
            return
        seq = event["seq"]
        if seq != rec.expected_seq:
            self._record_violation(
                t, "out_of_order", rid,
                "expected seq %d, got %d" % (rec.expected_seq, seq))
            return
        rec.delivered_tokens.append(event["token"])
        rec.expected_seq += 1
        rec.status = Status.STREAMING

    def _on_complete(self, event):
        rid = event["request_id"]
        rec = self.requests.get(rid)
        if rec is None:
            self._record_violation(event["time"], "unknown_request", rid,
                                   "complete for unknown request")
            return
        if rec.status in TERMINAL_STATUSES:
            self._record_violation(event["time"], "complete_after_terminal", rid,
                                   "already %s" % rec.status)
            return
        self._terminate(rec, Status.COMPLETED, event["time"])

    def _on_cancel(self, event):
        rid = event["request_id"]
        rec = self.requests.get(rid)
        if rec is None:
            self._record_violation(event["time"], "unknown_request", rid,
                                   "cancel for unknown request")
            return
        if rec.status in TERMINAL_STATUSES:
            self._record_violation(event["time"], "cancel_after_terminal", rid,
                                   "already %s" % rec.status)
            return
        self._terminate(rec, Status.CANCELLED, event["time"])

    def _on_batch_failed(self, event):
        bid = event["batch_id"]
        t = event["time"]
        brec = self.batches.get(bid)
        if brec is None:
            self._record_violation(t, "unknown_batch", None,
                                   "failure for unknown batch %s" % bid)
            return
        brec.failed = True
        for rid in brec.request_ids:
            rec = self.requests.get(rid)
            if rec is None or rec.status in TERMINAL_STATUSES:
                continue
            if rec.delivered_tokens:
                # 已向客户端交付部分输出，不能简单重试
                self._terminate(rec, Status.FAILED_PARTIAL, t)
            else:
                self._terminate(rec, Status.FAILED_RETRYABLE, t)
