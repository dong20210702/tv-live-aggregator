# 影视仓接口助手 V3.0：直播源逻辑分析

分析对象：`184960c7-________3.0.apk`。
方法：解包后，用一个自写的 dex 解析/反汇编脚本读取 `com.ysc.lineverify` 包下的 `LiveEngine`、`Live`、`Http` 等类的字节码，并与字符串常量、界面文本对照。

## 1. 架构

- **界面层**：`assets/index.html`，WebView 页面。它只负责展示，并通过 `window.YscAndroid` 调用原生桥：`startLive()`、`getLive()`、`cancelLive()`、`exportM3u()`。原生结果通过 `window.__yscLive(type, payload)` 回传。
- **原生层**：`com.ysc.lineverify`，真正的抓取、探测、打分、分组都在这里。
  - `LiveEngine`：主流程（`run`、`fetchList`、`probeAll`、`probe`、`score`、`build`）
  - `Live`：M3U/TXT 解析、名称清理、清晰度判定、频道归类
  - `Http`：网络请求、分片探测、防 SSRF 检查、URL 规范化
  - `MainActivity$Bridge`：暴露给 JS 的接口
- 与直播无关的部分：`ConfigDecoder`（AES-CBC 解密，用于「全网搜寻」里的 TVBox 配置类源）、`assets/akLtIVPdkx`（一个独立的 `cloudinject` 注入模块，内含自己的 dex 和布局资源）。我没有在直播路径中找到它们的调用。

## 2. 主流程（`LiveEngine.run`）

1. **准备种子**：`collectSeeds` 合并内置 `SEEDS`（15 组）和用户保存的 `liveseeds.json`。没有种子则报错退出。
2. **并发抓取列表**：线程池 160。每组 seed 依次尝试镜像地址：
   - 每个地址最多尝试 2 次，超时 35 秒；
   - 返回 404 则跳过；
   - 解析成功且非空即返回。
3. **解析**：有 `#EXTINF` 或以 `#EXTM3U` 开头按 M3U 解析，否则按 TXT（`分组,#genre#` / `名称,地址`）解析。
4. **去重**：按 URL 去重，总量上限 10,000 条。
5. **探测**（`probeAll`，线程池 160，总预算约 900 秒）：见第 3 节。
6. **打分、归类、排序**：`fillQualityByName` → `build`。
7. **输出**：`saveLive` 持久化，界面展示「可用频道 / 分组 / 央视可用」，导出为 M3U。

## 3. 探测（`LiveEngine.probe`）

| 步骤 | 动作 | 参数 |
|---|---|---|
| a | 非 http(s) 地址直接判失败（如 rtmp 等） | 「非 http 流」 |
| b | GET 播放列表，记录首包时间 TTFB | 超时 **2 s** |
| c | 内容必须含 `#EXTM3U` 或 `#EXTINF` | 否则「不是直播流」 |
| d | 取第一个非 `.m3u8` 的媒体行，解析为绝对地址（`firstSegmentUrl`） | |
| e | 若有分片：`Range: bytes=0-1023` 请求；HTTP≥400、返回 HTML/JSON（`<`/`{` 开头或 Content-Type 含 html）、读取失败均判死 | 超时 **1.5–4 s**，原值 **3 s** |
| f | 若没有分片但是主列表（含 `#EXT-X-STREAM-INF`）：取 `BANDWIDTH` 最大的变体，得到码率（kbps）与高度 | |
| g | 无分片且非主列表 → 「播放列表无分片」 | |
| h | 清晰度 `quality` 由高度/名称/码率决定（见第 5 节） | |

UA：分片探测使用 `okhttp/4.12.0`。列表抓取的 UA 我没有确认，脚本里统一用同一个。

## 4. 打分（`LiveEngine.score`，已确认）

```
若 ok == false：score = 0
speed        = 1 - clamp(ttfb_ms, 0, 3000) / 3000
bitrate_term = min(kbps / 12000, 1)           （kbps 为 0 时取 0）
height       = resH > 0 ? resH : heightOf(quality)
res_term     = min(height / 1920, 1)          （height 为 0 时取 0）
q            = 0.6 * bitrate_term + 0.4 * res_term
score        = round(25 + 40 * speed + 35 * q)
```

## 5. 清晰度（`Live.qualityOf`，已确认）

1. 实测高度 ≥2000 → 4K；≥1000 → 1080P；≥700 → 720P；≥400 → 480P。
2. 否则看名称：含 `4k`/`2160` → 4K；含 `1080`/`fhd` → 1080P；含 `720` → 720P；含 `480` → 480P。
3. 否则看码率：≥4000 → 1080P；≥2000 → 720P；≥800 → 480P。
4. 否则名称含 `hd`/`高清` → 高清；都不满足则为空。

排序档位（`rank`）：4K 5、1080P 4、720P 3、高清 2、480P 1、空 0。

## 6. 频道名与分组（`Live.classify`、`LiveEngine.baseName`）

**名称清理（`baseName`）**：
- 删除括号注释、清晰度词（HD/FHD/4K/1080P…）、「高清/超清/蓝光/标清」；
- 合并空白，去掉末尾分隔符；
- CCTV 统一为 `CCTV-N`（如 `CCTV5+` → `CCTV-5+`）。

**分组优先级**：
1. 名称精确命中 `STD_CCTV`（CCTV-1…17、CCTV-5+、CGTN）→ **央视频道**；命中 `STD_WSTV`（37 个卫视名）→ **卫视频道**；
2. 分组标题关键字：含 `央视/cctv/卫视` → 按名称再判；含 `地方/城市/本地` → **地方频道**；含 `电影/影视/剧场/影院` → **影视频道**；含 `体育/赛事` → **体育频道**；含 `少儿/儿童/卡通/动画` → **少儿频道**；含 `港澳/港台/台湾/香港/澳门` → **港澳台**；
3. 兜底按名称：含「央视/CCTV」→ 央视；含「卫视」→ 卫视；含 LOCAL_WORDS（31 省 + 约 80 个城市）→ 地方；其余 → **其他**。

界面中的 8 个分组即：央视频道、卫视频道、地方频道、影视频道、体育频道、少儿频道、港澳台、其他。

## 7. 与界面的对应（推断，未逐项验证）

- 「抓取 10000 条 · 源 15/15 成功」很可能对应：原始条目达到上限 10000，15 个种子全部成功。
- 「1172 可用频道」很可能是探测通过（ok）的条目数。
- 每频道行按「清晰度 → 评分」排序，「98ms」很可能是首包时间（TTFB）。
- 「央视可用 87」很可能是央视分组下的可用条目数。

## 8. 未完全确认的部分

- 每个频道保留的条数：界面中单频道最多显示 5 条，我按 5 实现。字节码里还有一个分支会给 3，条件未完全理清。
- 同 URL 去重：我确认了 `LinkedHashMap`/`HashSet` 的使用，但没有完整读完 `run` 的去重分支，按「URL 相同只保留一条」实现。
- `fillQualityByName`、`spotCheck` 在主流程中没有看到调用，可能是界面上的其他按钮。脚本中未实现。
- `Http.stripProxy` 的调用位置未理清。脚本只保留了「双 scheme 修正」，没有去掉 gh-proxy 前缀。
- 导出 M3U（`buildM3u`）的逐行格式未还原。脚本输出的是通用写法。
- 「全网搜寻」（TVBox 配置、AES 解密）不在直播路径中，未做分析。

## 9. 脚本与原版的差异

- 原版是 Android 原生实现。`scripts/build_live.py` 是按上述逻辑重新实现的纯 Python 版本，数值和判定规则一致，但不是字节码的逐行翻译。
- 脚本额外做了两处增强（代码里有注释）：
  - 名称里含「香港/台湾/澳门」等字样时归入港澳台（原版只看分组标题）；
  - 结果为空或可用条目过少时不覆盖旧输出。
- 脚本**不会**去掉 gh-proxy 前缀，直接探测种子给出的地址。
