import unittest

from validator import OrderingValidator, Status


def arrival(t, rid, deadline=None):
    event = {"type": "arrival", "time": t, "request_id": rid}
    if deadline is not None:
        event["deadline"] = deadline
    return event


def batch(t, bid, rids):
    return {"type": "batch_formed", "time": t, "batch_id": bid, "request_ids": rids}


def token(t, bid, rid, seq, text):
    return {"type": "token", "time": t, "batch_id": bid,
            "request_id": rid, "seq": seq, "token": text}


def complete(t, rid):
    return {"type": "complete", "time": t, "request_id": rid}


def cancel(t, rid):
    return {"type": "cancel", "time": t, "request_id": rid}


def failed(t, bid):
    return {"type": "batch_failed", "time": t, "batch_id": bid}


class TestInterleavedOutput(unittest.TestCase):
    def test_interleaved_tokens_stay_ordered_per_request(self):
        events = [
            arrival(0.0, "r1"),
            arrival(0.1, "r2"),
            batch(0.2, "b1", ["r1", "r2"]),
            token(0.3, "b1", "r1", 0, "a"),
            token(0.3, "b1", "r2", 0, "x"),
            token(0.4, "b1", "r2", 1, "y"),
            token(0.4, "b1", "r1", 1, "b"),
            token(0.5, "b1", "r1", 2, "c"),
            token(0.5, "b1", "r2", 2, "z"),
            complete(0.6, "r1"),
            complete(0.7, "r2"),
        ]
        report = OrderingValidator().process(events)

        self.assertEqual(report["requests"]["r1"]["delivered_tokens"], ["a", "b", "c"])
        self.assertEqual(report["requests"]["r2"]["delivered_tokens"], ["x", "y", "z"])
        self.assertEqual(report["requests"]["r1"]["status"], Status.COMPLETED)
        self.assertEqual(report["requests"]["r2"]["status"], Status.COMPLETED)
        self.assertEqual(report["violations"], [])
        # 批次在两个请求都结束后才释放
        self.assertEqual(report["batches"]["b1"]["release_time"], 0.7)
        self.assertEqual(report["requests"]["r1"]["release_time"], 0.6)
        self.assertEqual(report["requests"]["r2"]["release_time"], 0.7)

    def test_out_of_order_token_flagged_and_not_delivered(self):
        events = [
            arrival(0.0, "r1"),
            batch(0.1, "b1", ["r1"]),
            token(0.2, "b1", "r1", 0, "a"),
            token(0.3, "b1", "r1", 2, "c"),  # 跳号
            token(0.4, "b1", "r1", 1, "b"),
            complete(0.5, "r1"),
        ]
        report = OrderingValidator().process(events)

        kinds = [v["kind"] for v in report["violations"]]
        self.assertEqual(kinds, ["out_of_order"])
        # 乱序 token 不交付；seq=1 补上后恢复
        self.assertEqual(report["requests"]["r1"]["delivered_tokens"], ["a", "b"])


class TestCancellation(unittest.TestCase):
    def test_cancel_drops_late_tokens_without_shifting_others(self):
        events = [
            arrival(0.0, "r1"),
            arrival(0.0, "r2"),
            batch(0.1, "b1", ["r1", "r2"]),
            token(0.2, "b1", "r1", 0, "a"),
            token(0.2, "b1", "r2", 0, "x"),
            cancel(0.3, "r1"),
            token(0.4, "b1", "r1", 1, "LATE"),  # 取消后的迟到 token
            token(0.4, "b1", "r2", 1, "y"),
            token(0.5, "b1", "r2", 2, "z"),
            complete(0.6, "r2"),
        ]
        report = OrderingValidator().process(events)

        r1 = report["requests"]["r1"]
        r2 = report["requests"]["r2"]
        self.assertEqual(r1["status"], Status.CANCELLED)
        self.assertEqual(r1["delivered_tokens"], ["a"])  # 迟到 token 未交付
        self.assertEqual(r1["release_time"], 0.3)
        # 同批次其他请求不受影响、不错位
        self.assertEqual(r2["status"], Status.COMPLETED)
        self.assertEqual(r2["delivered_tokens"], ["x", "y", "z"])

        late = [v for v in report["violations"] if v["kind"] == "late_token"]
        self.assertEqual(len(late), 1)
        self.assertEqual(late[0]["request_id"], "r1")
        # 批次释放等待最迟结束的成员
        self.assertEqual(report["batches"]["b1"]["release_time"], 0.6)


class TestBatchPartialFailure(unittest.TestCase):
    def test_failure_distinguishes_retryable_and_partial(self):
        events = [
            arrival(0.0, "r1"),
            arrival(0.0, "r2"),
            arrival(0.0, "r3"),
            batch(0.1, "b1", ["r1", "r2", "r3"]),
            token(0.2, "b1", "r1", 0, "a"),
            token(0.3, "b1", "r1", 1, "b"),
            cancel(0.35, "r3"),
            failed(0.4, "b1"),
        ]
        report = OrderingValidator().process(events)

        # 已产生部分输出 -> 不可直接重试
        self.assertEqual(report["requests"]["r1"]["status"], Status.FAILED_PARTIAL)
        self.assertEqual(report["requests"]["r1"]["delivered_tokens"], ["a", "b"])
        # 无任何输出 -> 可重试
        self.assertEqual(report["requests"]["r2"]["status"], Status.FAILED_RETRYABLE)
        self.assertEqual(report["requests"]["r2"]["delivered_tokens"], [])
        # 失败前已取消的保持取消状态
        self.assertEqual(report["requests"]["r3"]["status"], Status.CANCELLED)

        self.assertTrue(report["batches"]["b1"]["failed"])
        # 批次在失败时刻释放（所有成员均已终态，最晚为 0.4）
        self.assertEqual(report["batches"]["b1"]["release_time"], 0.4)
        self.assertEqual(report["requests"]["r1"]["release_time"], 0.4)
        self.assertEqual(report["requests"]["r2"]["release_time"], 0.4)


class TestTimeout(unittest.TestCase):
    def test_deadline_marks_timeout_and_releases_slot(self):
        events = [
            arrival(0.0, "r1", deadline=1.0),
            arrival(0.0, "r2"),
            batch(0.1, "b1", ["r1", "r2"]),
            token(0.2, "b1", "r1", 0, "a"),
            token(0.5, "b1", "r2", 0, "x"),
            token(1.5, "b1", "r2", 1, "y"),  # 触发 r1 的 deadline 检查
            complete(2.0, "r2"),
        ]
        report = OrderingValidator().process(events)

        r1 = report["requests"]["r1"]
        self.assertEqual(r1["status"], Status.TIMEOUT)
        self.assertEqual(r1["delivered_tokens"], ["a"])
        self.assertEqual(r1["finish_time"], 1.0)
        self.assertEqual(r1["release_time"], 1.0)
        self.assertEqual(report["requests"]["r2"]["status"], Status.COMPLETED)
        self.assertEqual(report["batches"]["b1"]["release_time"], 2.0)

    def test_finalize_triggers_pending_deadlines(self):
        validator = OrderingValidator()
        validator.process([
            arrival(0.0, "r1", deadline=1.0),
            batch(0.1, "b1", ["r1"]),
        ])
        report = validator.finalize(5.0)
        self.assertEqual(report["requests"]["r1"]["status"], Status.TIMEOUT)
        self.assertEqual(report["batches"]["b1"]["release_time"], 1.0)


class TestEventOrdering(unittest.TestCase):
    def test_rejects_out_of_order_events(self):
        validator = OrderingValidator()
        validator.apply(arrival(1.0, "r1"))
        with self.assertRaises(ValueError):
            validator.apply(arrival(0.5, "r2"))


if __name__ == "__main__":
    unittest.main()
