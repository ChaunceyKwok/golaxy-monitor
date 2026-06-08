#!/usr/bin/env python3
"""Golaxy 腾讯金融舆情监测 — GitHub Actions 版本
================================================
每 5 分钟由 GitHub Actions 触发。状态通过 Actions cache 持久化。
API Key 和 Webhook 从环境变量读取 (GitHub Secrets)。
"""

import json, os, re, ssl, time, urllib.request, csv
from datetime import datetime, timedelta

# ---- 配置 (敏感信息从环境变量读取) ----
GOLAXY_URL  = "https://mindhub.golaxy.cn:40443/golaxy/data-service/api/v1/contents/search"
API_KEY     = os.environ["GOLAXY_API_KEY"]
WEBHOOK     = os.environ["WECOM_WEBHOOK"]
STATE_FILE  = ".golaxy_seen.json"
EXCLUDE_FILE = "exclude_words.txt"
BLOCK_MEDIA_FILE = "block_media.txt"

# ---- 监控关键词 (API text 参数 ES match) ----
MONITOR_TEXT = "微信支付 财付通 腾讯理财 零钱通 微粒贷 微众银行 微保 微信分付 微信月付 微信提现 腾讯金融 腾安基金 微企付"
COMPOUND_CHECK = ["微信支付","财付通","腾讯理财","零钱通","微粒贷","微众银行",
                  "微保","微信分付","微信月付","微信提现","腾讯金融","腾安基金","微企付"]

# 仅监测媒体报道的品牌(社媒UGC pass)
MEDIA_ONLY_BRANDS = ["微众银行", "微粒贷", "微保"]
SOCIAL_MEDIA_SITES = ["微博","小红书","抖音","快手","微头条","贴吧","豆瓣","知乎",
                      "懂车帝","有驾","汽车之家","易车","卡农","论坛","黑猫投诉",
                      "百度贴吧","b站","哔哩哔哩","qq空间"]

# ---- 参数 ----
WINDOW_MINUTES = 60
LOOKBACK_MINUTES = 10
PAGE_SIZE = 100; MAX_PAGES = 2; TIMEOUT = 10
MAX_PUSH = 20
SENTI_MAP = {"positive":"正面","neutral":"中性","negative":"负面"}

SSL_CTX = ssl.create_default_context()
SSL_CTX.check_hostname = False; SSL_CTX.verify_mode = ssl.CERT_NONE


def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def http_post(url, body, headers=None):
    h = {"Content-Type":"application/json"}
    if headers: h.update(headers)
    data = json.dumps(body).encode()
    req = urllib.request.Request(url, data=data, headers=h)
    with urllib.request.urlopen(req, timeout=TIMEOUT, context=SSL_CTX) as r:
        return json.loads(r.read())


def load_lines(path):
    if not os.path.exists(path): return []
    return [l.strip().lower() for l in open(path) if l.strip() and not l.startswith("#")]


def load_excludes():
    words = []
    raw = open(EXCLUDE_FILE, encoding="utf-8").read() if os.path.exists(EXCLUDE_FILE) else ""
    for w in raw.replace("｜","|").replace("\n","|").split("|"):
        w = w.strip().lower()
        if w: words.append(w)
    return words


def load_block_media():
    return load_lines(BLOCK_MEDIA_FILE)


def load_state():
    if os.path.exists(STATE_FILE):
        try: return json.load(open(STATE_FILE))
        except: pass
    return {"seen": [], "scan": None}


def save_state(st):
    st["seen"] = st["seen"][-8000:]
    json.dump(st, open(STATE_FILE, "w"), ensure_ascii=False)


def fetch_docs(begin, end):
    docs = []
    for p in range(MAX_PAGES):
        body = {"page":p,"size":PAGE_SIZE,"sortBy":"captureTime","sortOrder":"desc",
                "captureTimeFrom":begin,"captureTimeTo":end,"text":MONITOR_TEXT}
        try:
            res = http_post(GOLAXY_URL, body, {"X-API-Key":API_KEY})
        except Exception as e:
            log(f"  API page={p} fail: {e}")
            break
        contents = res.get("data",{}).get("contents",[])
        if not contents: break
        docs.extend(contents)
        if not res.get("data",{}).get("hasNext"): break
    return docs


def make_summary(title, text, max_len=160):
    text = re.sub(r"\s+"," ",(text or "")).strip()
    if not text: return (title or "")[:max_len]
    sents = re.split(r"(?<=[。！？!?；;])", text)
    sents = [s.strip() for s in sents if len(s.strip())>=6]
    if not sents: return text[:max_len]
    summary = ""; idx = 0
    while idx < len(sents) and len(summary)+len(sents[idx]) <= max_len:
        summary += sents[idx]; idx += 1
    return summary or text[:max_len]


def push_card(item):
    text = item["text"]
    if len(text) > 300: text = text[:300] + "…"
    lines = [
        f"**数据标签**：{item['tag']}",
        f"**作者**：{item['author']}",
        f"**标题**：{item['title']}",
        f"**发布时间**：{item['publishTime']}",
        f"**发布平台**：{item.get('platform','-')}",
        f"**内容摘要**：{text}",
    ]
    if item["url"]:
        lines.append(f"**原文链接**：[{item['url']}]({item['url']})")
    content = "\n".join(lines)
    if len(content.encode()) > 4000:
        content = "\n".join(lines[:4]+[f"**内容摘要**：{text[:200]}…"]+lines[5:])
    res = http_post(WEBHOOK, {"msgtype":"markdown","markdown":{"content":content}})
    return res.get("errcode") == 0


def main():
    log("===== 开始 =====")
    state = load_state()
    seen_set = set(state.get("seen",[]))
    excludes = load_excludes()
    block_media = load_block_media()
    now = datetime.now()

    # 时间窗口
    last_scan = state.get("scan")
    if last_scan:
        last_t = datetime.fromisoformat(last_scan)
        begin = (last_t - timedelta(minutes=LOOKBACK_MINUTES)).strftime("%Y-%m-%d %H:%M:%S")
    else:
        begin = (now - timedelta(minutes=WINDOW_MINUTES)).strftime("%Y-%m-%d %H:%M:%S")
    end = now.strftime("%Y-%m-%d %H:%M:%S")
    log(f"窗口: {begin} ~ {end}")

    # 拉取
    docs = fetch_docs(begin, end)
    log(f"拉取 {len(docs)} 条")

    if not docs:
        state["scan"] = now.isoformat()
        save_state(state)
        log("===== 完成 (无数据) =====")
        return

    # 过滤
    matched = []; dup = exc = blk = 0
    for src in docs:
        cid = src.get("contentId","")
        title = (src.get("title") or "").strip()
        norm = re.sub(r"\s+","",title)[:80]
        fp = f"fp:{norm}" if norm else f"fp:{cid}"

        if (cid and cid in seen_set) or fp in seen_set:
            dup += 1; continue
        sw = src.get("sourceWebsite","")
        if sw and any(b in sw for b in block_media):
            blk += 1; continue
        combined = (title + " " + (src.get("text") or "")).lower()
        hit_brands = [c for c in COMPOUND_CHECK if c.lower() in combined]
        if not hit_brands:
            continue
        if any(w in combined for w in excludes):
            exc += 1; continue

        # 媒体报道专属品牌: 社媒UGC pass
        cap_web = (src.get("captureWebsite") or "").lower()
        if not [b for b in hit_brands if b not in MEDIA_ONLY_BRANDS]:
            if any(s in cap_web for s in SOCIAL_MEDIA_SITES):
                exc += 1; continue

        ce = src.get("contentExt") or {}
        emotion = ce.get("emotion","neutral")
        sentiment = SENTI_MAP.get(emotion,"中性")
        api_tag = " / ".join(p for p in [src.get("tag") or "", src.get("subtag") or ""] if p) or "舆情"
        summary = (src.get("summary") or "").strip()
        if not summary or len(summary) < 20:
            summary = make_summary(title, src.get("text") or "")

        item = {
            "tag": f"{api_tag}｜{sentiment}",
            "author": src.get("author") or "-",
            "title": title or "(无标题)",
            "publishTime": src.get("publishTime") or "-",
            "text": summary,
            "url": src.get("url") or "",
            "platform": src.get("captureWebsite") or src.get("sourceWebsite") or "-",
        }
        matched.append(item)
        seen_set.add(fp)

    log(f"过滤: 命中{len(matched)} 排除{exc} 屏蔽{blk} 重复{dup}")

    # 推送
    pushed = 0
    for it in matched[:MAX_PUSH]:
        if push_card(it):
            pushed += 1
            log(f"  推送: {it['title'][:50]}")
            time.sleep(0.3)
        if pushed >= MAX_PUSH: break

    # 保存状态
    state["seen"] = list(seen_set)
    state["scan"] = now.isoformat()
    save_state(state)
    log(f"===== 完成 (推送{pushed}条) =====")


if __name__ == "__main__":
    main()
