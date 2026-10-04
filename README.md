# go-reservation-waitlist

本项目用于 GSB 评测，实现带"名额保留期限"的预约候补晋级队列。

## 模型

- `Waitlist` 持有固定数量的名额槽位（`slot-N`），候补申请按 FIFO 排队。
- `Promote` 为队首申请保留一个空闲槽位并生成晋级记录 `Promotion`，记录
  名额（`SlotID`）、候补版本（`Version`）、确认截止时间（`Deadline`）和
  通知编号（`NotificationID`）。
- 申请人需在截止时间前 `Confirm`（含边界，`now <= Deadline` 有效），也可
  `Decline` 主动放弃；`Scan(now)` 负责把超时未确认的晋级置为过期并释放名额。

## 并发与一致性

- 所有状态变更在单把互斥锁下完成。确认、放弃、超时扫描对同一晋级只能产生
  一个终态（confirmed / declined / expired），名额最多被占用或释放一次，
  每次变动都写入槽位台账 `SlotEvent`（动作 + 原因）。
- 候补条件变化（加入、退出、晋级、放弃、过期）会推进候补版本号；旧版本
  的晋级再确认时返回 `ErrStaleVersion`。
- 超时或放弃释放名额后，`Scan` 按当前队列快照依次晋级下一位；已明确放弃
  或过期的申请已离开队列，不会被同一次或后续扫描再次晋级。

## 幂等

- `Promote` 以 `PromoteRequest.ID` 为幂等键：相同晋级号、相同内容（通知
  编号、名额、截止时间）重放返回原记录；名额或截止时间变化返回
  `ErrConflict`。
- `Confirm` / `Decline` 对已达同一终态的晋级重放返回原结果；跨终态操作
  返回 `ErrClosed`。

## 查询

`View()` 返回一致快照：候补顺序（`Queue`）、每次晋级及其确认结果
（`Promotions`）、槽位占用（`Slots`）和名额变动原因（`Events`）。

## 测试

```sh
go test -race ./...
```

覆盖确认边界（含截止时间恰等/超过）、版本过期拒绝、超时释放并晋级下一
位、放弃者不被重复晋级、重复扫描空操作、确认/放弃/扫描并发争抢唯一终
态，以及幂等重放与冲突。
