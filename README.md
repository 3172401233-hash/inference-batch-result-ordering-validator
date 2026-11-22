# inference-batch-result-ordering-validator
验证动态批处理中的输出顺序、流式结果和失败隔离

## 概述

`validator.py` 实现了一个**离线**结果排序验证器（Python 3.8.2+）。它只消费预先
录制的事件流，不与任何在线推理服务交互：不重放请求、不连接模型、不产生新的
推理流量。给定请求到达顺序、批次组成、token 流、完成事件、取消、批次失败和
deadline，验证器计算每个请求的实际输出、最终状态和资源释放点，并报告违规。

## 事件模型

事件为 dict，必须按 `time` 非递减排列：

| `type`         | 字段                                             | 含义               |
|----------------|--------------------------------------------------|--------------------|
| `arrival`      | `request_id`, `deadline?`                        | 请求到达           |
| `batch_formed` | `batch_id`, `request_ids`                        | 批次组成           |
| `token`        | `batch_id`, `request_id`, `seq`, `token`         | 流式 token         |
| `complete`     | `request_id`                                     | 请求正常完成       |
| `cancel`       | `request_id`                                     | 客户端取消         |
| `batch_failed` | `batch_id`                                       | 整个批次失败       |

## 验证规则

- 同一请求的 token 必须按 `seq` 连续递增交付；乱序 token 记为
  `out_of_order` 违规且不交付。
- 请求到达终态（取消/完成/失败/超时）后到达的 token 记为 `late_token`
  违规，绝不交付给客户端；同批次其他请求的输出不错位。
- 批次失败时区分成员：未产生任何输出的请求为 `failed_retryable`
  （可重试），已交付部分输出的请求为 `failed_partial`（不可直接重试）。
- 超过 `deadline` 仍未终态的请求标记为 `timeout`，终态时刻取 deadline。

## 资源释放

- 请求槽位在到达终态的时刻释放（`release_time == finish_time`）。
- 批次资源在其所有成员都到达终态后才释放，释放点为各成员终态时刻的
  最大值；存在未终态成员时释放点为 `None`。

## 输出

`OrderingValidator().process(events)` 返回：

- `requests`：每个请求的 `status`、`delivered_tokens`、`finish_time`、
  `release_time`。
- `batches`：每个批次的成员、`failed` 标志和 `release_time`。
- `violations`：所有违规（`out_of_order`、`late_token`、`batch_mismatch`
  等）。

## 运行测试

```sh
python -m unittest
```

测试覆盖交错输出、乱序 token、单请求取消、批次部分失败和超时。
