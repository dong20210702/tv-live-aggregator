# tv-live-aggregator

自动抓取公开 IPTV 列表、测速、分组，并每 6 小时生成一份固定地址的直播源（M3U / TXT）。
逻辑还原自「影视仓接口助手 V3.0」的直播源模块，实现细节见 [ANALYSIS.md](ANALYSIS.md)。

## 部署（约 3 分钟）

1. 在 GitHub 新建一个仓库（例如 `tv-live`），可以设为 Public（Raw 链接需要公开才能直接订阅）。
2. 把本目录所有文件上传进去（保持目录结构：`scripts/`、`sources/`、`output/`、`.github/`）。
3. 进入仓库 **Actions** 页，若提示启用工作流，点击启用。
4. 在 **update-live** 工作流里点 **Run workflow** 手动跑一次，完成后 `output/` 下会出现文件。
5. 之后每 6 小时自动运行一次，有变化才提交。

## 订阅地址（固定不变）

把 `<用户名>` 和 `<仓库名>` 换成你的：

| 格式 | 地址 |
|---|---|
| M3U（大多数播放器） | `https://raw.githubusercontent.com/<用户名>/<仓库名>/main/output/live.m3u` |
| TXT（TVBox 等，分组格式） | `https://raw.githubusercontent.com/<用户名>/<仓库名>/main/output/live.txt` |
| JSON（含清晰度、首包时间、评分） | `https://raw.githubusercontent.com/<用户名>/<仓库名>/main/output/live.json` |
| 备用 CDN（有缓存） | `https://cdn.jsdelivr.net/gh/<用户名>/<仓库名>@main/output/live.m3u` |

地址永远不变，内容由定时任务刷新。

## 本地运行

```bash
python3 scripts/build_live.py --seeds sources/seeds.json --out output
```

仅用标准库，Python 3.10+ 即可。失败（抓不到任何条目或可用条目少于 `--min-ok`）时以退出码 1 结束，并且**不覆盖**已有输出。

## 常用调整

- 增删源：编辑 `sources/seeds.json`。每一项是一组镜像地址，按顺序尝试。
- 每频道保留条数：`--per-channel 5`（默认 5）。
- 探测并发：环境变量 `PROBE_WORKERS`（默认 128）。
- 探测总时长：`--probe-budget 900`（秒）。

## 注意事项

- **探测位置很重要。** GitHub 托管的 Runner 在海外。部分只对国内开放或国内 CDN 的直播源，从海外测速会被误判为不可用，结果会比你在国内手机上看到的少。若需要国内视角，可以把工作流改为跑在你自己的国内机器上（self-hosted runner）。
- **上游会失效。** 列表来自第三方公开聚合，源站随时可能下线或改地址。脚本每次都会重新抓取，所以地址不变，但可用条目数会随上游变化。
- **版权与用途。** 这些列表中的流多为未经授权的转播。请自行判断合规风险，建议仅个人使用，不要公开传播或商用。
- 定时任务可能有几分钟到十几分钟的延迟，这是 GitHub Actions 的正常行为。
- 仓库长期没有任何提交时，GitHub 会停用定时任务。本工作流每次有变化都会提交，一般不会触发；若被停用，在 Actions 页手动运行一次即可恢复。
