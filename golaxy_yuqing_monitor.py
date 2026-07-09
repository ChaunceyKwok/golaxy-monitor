#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
中科天玑 Golaxy 舆情监测 + 企业微信推送
===========================================

数据源: 中科天玑数据服务 (Golaxy) — POST /api/v1/contents/search
推送:   企业微信群机器人 webhook

核心流程:
  1. 按时间窗口拉取 Golaxy 打标数据 (publishTime 窗口 + 无关键词过滤, 全量)
  2. 本地布尔表达式关键词过滤
  3. 排除词过滤 + 站点黑名单
  4. 内容指纹(title)去重
  5. 格式化为 Markdown 详情卡推送到企微群
  6. 负面舆情补发 text 消息 @ 指定成员

运行方式:
  python3 golaxy_yuqing_monitor.py              # 单次运行
  python3 golaxy_yuqing_monitor.py --daemon     # 常驻轮询 (60s/轮)
  bash restart_golaxy.sh                        # 一键重启
"""

from __future__ import annotations

import os
import sys
import json
import time
import re
import ssl
import urllib.request
import csv
from datetime import datetime, timedelta
from typing import Optional

# SSL context (禁用证书验证, Golaxy 内网证书)
SSL_CTX = ssl.create_default_context()
SSL_CTX.check_hostname = False
SSL_CTX.verify_mode = ssl.CERT_NONE

# ==================== 配置区 ====================
BASE = os.path.dirname(os.path.abspath(__file__))

# Golaxy API
GOLAXY_BASE_URL = "https://mindhub.golaxy.cn:40443/golaxy/data-service"
GOLAXY_API_KEY  = os.environ.get("GOLAXY_API_KEY", "")

# 企微 Webhook
WEBHOOK_URL = os.environ.get("WECOM_WEBHOOK", "")

# 关键词文件
INCLUDE_FILE     = f"{BASE}/golaxy_keywords_include.txt"
EXCLUDE_FILE     = f"{BASE}/golaxy_keywords_exclude.txt"
BLOCK_MEDIA_FILE = f"{BASE}/golaxy_keywords_block_media.txt"
BLOCK_URL_FILE   = f"{BASE}/golaxy_keywords_block_url.txt"

# 轮询参数
POLL_INTERVAL = 60       # 常驻模式每轮间隔(秒)
# 时间窗口: 接口(EsQueryRequest)仅支持 publishTimeFrom/To 作过滤字段;
# 之前误用 captureTimeFrom/To(接口不认、被静默忽略 → 窗口从未生效, 等于每轮全量靠去重凑合)。
# 改用 publishTime 窗口后, 因"发布早但入库晚"的滞后, 回看窗口需放大以防漏报。
WINDOW_MINUTES = 360     # 首次/回扫拉取窗口(分钟, publishTime)
LOOKBACK_MINUTES = 360   # 增量回看(分钟, publishTime): 覆盖发布→入库滞后, 防漏报; 去重保证不重推
PAGE_SIZE = 200           # 每页条数(API 上限 1000)
MAX_PAGES = 15            # 每轮最多翻页数 (无条数上限, 窗口内拉满取全, 消除"第201条漏报")
REQ_TIMEOUT = 10          # HTTP 请求超时(秒)
MAX_RETRY = 1             # 不重试: API 抖动时快速失败, 靠兜底回扫补数据
API_FAIL_RESTART = 3      # 连续 N 次 API 不可达 → 重启进程刷新网络连接(自愈)

# 状态文件
STATE_FILE = f"{BASE}/.golaxy_yuqing_seen.json"
STOP_FILE  = f"{BASE}/.golaxy_yuqing_stop"

# 推送配置
MAX_PUSH = 20             # 单次推送最多条数
NEGATIVE_ALERT = True     # 负面舆情 @ 提醒
ALERT_AT_ALL = False      # True=@所有人; False=@手机号列表
ALERT_MOBILES = ["13140197825"]  # ulrichguo
# 移动话费充值事件相关舆情 → 额外 @ 群里的 anderschen(陈恩达), 用企业微信 userid
MOBILE_EVENT_AT_USERIDS = ["anderschen"]  # 陈恩达
# 理财通/零钱通 负面舆情 → 额外 @ 群里的 yalinlei / minazeng, 用企业微信 userid
LICAI_ALERT_USERIDS = ["yalinlei", "minazeng"]
# 命中这些业务标签的负面内容视为"理财业务负面", 触发 LICAI_ALERT_USERIDS @提醒
LICAI_ALERT_BIZ = ["理财通", "零钱通", "腾讯理财通", "微信理财", "腾安基金"]
# 微信分付 负面 → @ anderschen(与移动事件同一负责人), 不@ulrichguo
FENFU_ALERT_BIZ = ["分付", "微信分付"]

# ==================== 领导人特别提示 ====================
# 涉及腾讯集团/金科高管的舆情, 命中后单独发一条"👑涉领导人"高亮提醒(不论正负面)。
# 名单可随时增删。用全名精确匹配, 避免短名误命中(如"马化腾"不会因"腾"字误触)。
VIP_PERSONS = [
    # 腾讯集团高管
    "马化腾", "刘炽平", "任宇昕", "张小龙", "汤道生", "郭凯天", "网大为", "James Mitchell", "米切尔",
    # 腾讯金融科技高管
    "林海峰", "郑浩剑", "邱跃鹏",
]
# 领导人提示 @ 谁: ulrichguo 用手机号(见 ALERT_MOBILES), 其余可加 userid
VIP_ALERT_MOBILES = ["13140197825"]   # ulrichguo
VIP_ALERT_USERIDS = []                # 如需额外@他人, 填企业微信 userid

# 推送记录
PUSH_LOG_ENABLE = True
PUSH_LOG_DIR = f"{BASE}/push_logs"

# 深处偶发降噪: 长正文中命中词只在靠后处出现 → 判软文噪音
LONGTEXT_LEN = 300
FRONT_LEN = 200

# 强关系近邻参数
PROXIMITY_ENABLE = True    # 启用强关系兜底
PROXIMITY_WINDOW = 80      # 命中词近邻窗口(字符), 窗口内共现才算强关系

SENTI_MAP = {"正面": "正面", "中性": "中性", "负面": "负面",
             "positive": "正面", "neutral": "中性", "negative": "负面"}

# 情感词集合(用于从业务标签中剥离, 以及综合判定)
_SENTI_TOKENS = ["负面", "正面", "中性", "negative", "positive", "neutral"]


def resolve_sentiment(src):
    """统一情感判定。Golaxy 的 contentExt.emotion 常为 null, 真实情感多写在 tag 字段
    (且可能是'正面中性'这类多值)。规则: 负面优先 —— 只要任一来源出现负面, 即判负面,
    不再叠加中性/正面; 其次正面; 都没有才中性。
    返回 ('负面'|'正面'|'中性', senti_int)。"""
    ce = src.get("contentExt") or {}
    blob = f"{ce.get('emotion') or ''} {src.get('tag') or ''}".lower()
    if "负面" in blob or "negative" in blob:
        return "负面", -1
    if "正面" in blob or "positive" in blob:
        return "正面", 1
    return "中性", 0


def strip_senti_tokens(tag_part):
    """从业务标签片段中剔除情感词(负面/正面/中性等), 避免业务标签里混入情感造成'负面…｜中性'矛盾。"""
    s = tag_part
    for t in _SENTI_TOKENS:
        s = s.replace(t, "")
    return s.strip(" /、，,")


def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


# ==================== HTTP 工具 ====================

def http_post(url, payload, extra_headers=None):
    data = json.dumps(payload).encode("utf-8")
    headers = {"Content-Type": "application/json"}
    if extra_headers:
        headers.update(extra_headers)
    req = urllib.request.Request(url, data=data, headers=headers)
    last_err = None
    for attempt in range(1, MAX_RETRY + 1):
        try:
            with urllib.request.urlopen(req, timeout=REQ_TIMEOUT, context=SSL_CTX) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except Exception as e:
            last_err = e
            if attempt < MAX_RETRY:
                wait = 5 * attempt
                log(f"   请求失败(第{attempt}次): {e}, {wait}s后重试")
                time.sleep(wait)
            else:
                log(f"   请求失败(第{attempt}次): {e}")
    raise RuntimeError(f"重试{MAX_RETRY}次失败: {last_err}")


# ==================== 布尔表达式解析器 ====================

class BoolParser:
    """expr = or_term ('|' or_term)* ; or_term = and_term ('&' and_term)* ;
       and_term = '(' expr ')' | LEAF"""
    def __init__(self, s):
        self.s = s; self.i = 0; self.n = len(s)
    def peek(self):
        return self.s[self.i] if self.i < self.n else ""
    def parse(self):
        return self._parse_or()
    def _parse_or(self):
        nodes = [self._parse_and()]
        while self.peek() == "|":
            self.i += 1; nodes.append(self._parse_and())
        return nodes[0] if len(nodes) == 1 else ("or", nodes)
    def _parse_and(self):
        nodes = [self._parse_atom()]
        while self.peek() == "&":
            self.i += 1; nodes.append(self._parse_atom())
        return nodes[0] if len(nodes) == 1 else ("and", nodes)
    def _parse_atom(self):
        if self.peek() == "(":
            self.i += 1; node = self._parse_or()
            if self.peek() == ")": self.i += 1
            return node
        start = self.i
        while self.i < self.n and self.s[self.i] not in "&|()":
            self.i += 1
        return ("leaf", self.s[start:self.i].strip())


def eval_node(node, text_lower):
    if node[0] == "leaf":
        return node[1].lower() in text_lower
    if node[0] == "and":
        return all(eval_node(c, text_lower) for c in node[1])
    if node[0] == "or":
        return any(eval_node(c, text_lower) for c in node[1])
    return False


def matched_leaves(node, text_lower, out):
    if node[0] == "leaf":
        if node[1] and node[1].lower() in text_lower:
            out.append(node[1])
    else:
        for c in node[1]:
            matched_leaves(c, text_lower, out)


def _min_window_single(ast, text_lower):
    """单组命中词在文本中的最小覆盖窗口宽度"""
    lv = []
    matched_leaves(ast, text_lower, lv)
    lv = list(dict.fromkeys([w.lower() for w in lv if w]))
    if len(lv) <= 1:
        return 0
    positions = []
    for w in lv:
        idxs = []; start = 0
        while True:
            i = text_lower.find(w, start)
            if i < 0: break
            idxs.append((i, i + len(w))); start = i + 1
        if not idxs: return 10**9
        positions.append(idxs)
    flat = []
    for wid, idxs in enumerate(positions):
        for s, e in idxs:
            flat.append((s, e, wid))
    flat.sort()
    from collections import defaultdict
    cnt = defaultdict(int); have = 0; l = 0; best = 10**9
    for r in range(len(flat)):
        cnt[flat[r][2]] += 1
        if cnt[flat[r][2]] == 1: have += 1
        while have == len(lv):
            best = min(best, flat[r][1] - flat[l][0])
            cnt[flat[l][2]] -= 1
            if cnt[flat[l][2]] == 0: have -= 1
            l += 1
    return best


# ==================== 强关系判定 ====================

_STRONG_ANCHORS = [a.lower() for a in [
    "微信支付", "财付通", "理财通", "零钱通", "微信零钱通", "微信理财", "微众", "微保", "微粒贷",
    "tenpay", "tenpay go", "wepay", "wechat pay", "微企付", "微证券", "腾讯自选股",
    "微信钱包", "微信零钱", "微信红包", "微信账单", "微信立减金", "微信支付分",
    "腾讯金融", "腾讯金科", "腾讯金控", "腾讯区块链", "微企链", "qq钱包",
    "智汇鹅", "腾安基金", "腾富金融", "腾诺保险", "微民保险", "微汇款",
    "恒智信", "微恒科技", "刷掌支付", "亲属卡", "腾讯稳定币", "金腾科技",
    "微信提现", "微信转账", "微信收款码", "微信付款码",
    "paypal", "visa", "mastercard", "万事达", "数字人民币", "支付宝",
    "拉卡拉", "连连支付", "pingpong", "网联", "银联", "富途证券",
]]


def has_strong_anchor(text_lower):
    for a in _STRONG_ANCHORS:
        if a in text_lower:
            return True
    return False


def passes_strong_relation(text_lower, asts):
    if not PROXIMITY_ENABLE:
        return True
    if has_strong_anchor(text_lower):
        return True
    for _raw, ast in asts:
        if eval_node(ast, text_lower):
            if _min_window_single(ast, text_lower) <= PROXIMITY_WINDOW:
                return True
    return False


# ==================== 排除逻辑 ====================

EXCLUDE_GUARD = {
    "洗钱": ["反洗钱"],
}

GUARD_IF_PRESENT = {
    "跨境理财通": ["腾讯理财通", "微信理财通", "腾讯", "微信", "财付通"],
    "理财通道":   ["腾讯理财通", "微信理财通", "腾讯", "微信", "财付通"],
    # AI/大模型类排除词: 同篇含腾讯系金融主体时豁免(微信支付AI专属卡等真舆情常蹭"大模型"/"Kimi")
    "大模型": ["腾讯", "微信支付", "微信零钱", "理财通", "零钱通", "财付通",
               "微信ai", "ai专属卡", "微企付", "微粒贷", "微众"],
    # 反诈/防骗类排除词本为拦"反诈宣传软文", 但真实"支付被冻→拿解控文件→申诉复核被驳回"
    # 的受害维权叙事会提到"反诈中心解控文件", 属高价值负面舆情, 不应被误杀。
    # 同篇含支付受限维权信号词时豁免。(修复: 网易号《非得逼得人走投无路吗？！》被"反诈中心"误杀)
    "反诈中心": ["解控", "冻结", "解冻", "申诉", "复核", "限制", "驳回", "腾讯客服", "微信支付"],
    "防骗提醒": ["解控", "冻结", "解冻", "申诉", "复核", "驳回", "腾讯客服"],
    "谨防诈骗": ["解控", "冻结", "解冻", "申诉", "复核", "驳回", "腾讯客服"],
    # 以下为高频普通词做子串排除词, 易误伤真实维权/投诉叙事, 加腾讯系+维权信号豁免:
    #   得到  → 拦"得到App"知识付费, 误伤"得到解决/得到妥善解决"
    #   手机号→ 拦手机号黑产, 误伤"实名手机号求助/换绑"
    #   纳税  → 拦税务代办软文, 误伤"依法纳税/税务热线"投诉叙述
    # (修复2026-07-09: 分付/微信申诉/微信解封绑定/电诉宝投诉 等真负面被这些词误杀)
    "得到": ["微信支付", "微信解封", "微信申诉", "分付", "微粒贷", "腾讯客服", "限制", "冻结", "封号", "被限"],
    "手机号": ["微信支付", "微信解封", "微信申诉", "解封", "封号", "腾讯客服", "冻结", "被限", "绑定异常"],
    "手机卡": ["微信支付", "微信解封", "微信申诉", "解封", "封号", "腾讯客服", "冻结", "被限", "绑定异常"],
    "纳税": ["微信支付", "财付通", "投诉", "退款", "虚假宣传", "诱导消费", "腾讯客服"],
}


def is_excluded(text_lower, excludes):
    for w in excludes:
        wl = w.lower()
        if wl not in text_lower:
            continue
        # 豁免检查
        guards = EXCLUDE_GUARD.get(w)
        if guards:
            tmp = text_lower
            for g in guards:
                tmp = tmp.replace(g.lower(), "")
            if wl not in tmp:
                continue
        present_guards = GUARD_IF_PRESENT.get(w)
        if present_guards and any(g.lower() in text_lower for g in present_guards):
            continue
        return True, w
    return False, None


# ==================== 词表加载 ====================

def load_include_exprs():
    if not os.path.exists(INCLUDE_FILE):
        return []
    raw = open(INCLUDE_FILE, "r", encoding="utf-8").read()
    raw = "\n".join(l for l in raw.splitlines() if l.strip() and not l.strip().startswith("#"))
    raw = raw.strip().replace("\n", "")
    exprs = []; depth = 0; buf = ""
    for ch in raw:
        if ch == "(": depth += 1; buf += ch
        elif ch == ")": depth -= 1; buf += ch
        elif ch == "|" and depth == 0:
            if buf.strip(): exprs.append(buf.strip())
            buf = ""
        else: buf += ch
    if buf.strip(): exprs.append(buf.strip())
    asts = []
    for e in exprs:
        try:
            asts.append((e, BoolParser(e).parse()))
        except Exception as ex:
            log(f"!! 表达式编译失败: {e[:40]} -> {ex}")
    return asts


def load_exclude_words():
    if not os.path.exists(EXCLUDE_FILE):
        return []
    raw = open(EXCLUDE_FILE, "r", encoding="utf-8").read()
    raw = "\n".join(l for l in raw.splitlines() if l.strip() and not l.strip().startswith("#"))
    raw = raw.replace("｜", "|").replace("\n", "|")
    words = [w.strip() for w in raw.split("|") if w.strip()]
    seen, out = set(), []
    for w in words:
        if w.lower() not in seen:
            seen.add(w.lower()); out.append(w)
    return out


def load_block_media():
    if not os.path.exists(BLOCK_MEDIA_FILE):
        return []
    raw = open(BLOCK_MEDIA_FILE, "r", encoding="utf-8").read()
    raw = "\n".join(l for l in raw.splitlines() if l.strip() and not l.strip().startswith("#"))
    raw = raw.replace("｜", "|").replace("\n", "|")
    words = [w.strip() for w in raw.split("|") if w.strip()]
    seen, out = set(), []
    for w in words:
        if w.lower() not in seen:
            seen.add(w.lower()); out.append(w)
    return out


def load_block_url():
    """加载 URL 黑名单: 每行一条规则, 命中 url 字符串包含即拦截"""
    if not os.path.exists(BLOCK_URL_FILE):
        return []
    out = []
    for line in open(BLOCK_URL_FILE, "r", encoding="utf-8").read().splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        out.append(s.lower())
    return out


def is_blocked_by_url(url: str, block_url_list: list[str]) -> str | None:
    """URL 黑名单匹配. 返回首个命中的关键词, 否则 None"""
    if not url or not block_url_list:
        return None
    u = url.lower()
    for kw in block_url_list:
        if kw in u:
            return kw
    return None


# ==================== 去重状态 ====================

def load_seen():
    if os.path.exists(STATE_FILE):
        try:
            d = json.load(open(STATE_FILE, "r", encoding="utf-8"))
            return set(d.get("seen", [])), d.get("seen", [])
        except Exception:
            pass
    return set(), []


def save_seen(seen_list):
    seen_list = seen_list[-8000:]
    try:
        json.dump({"seen": seen_list, "updated": datetime.now().isoformat()},
                  open(STATE_FILE, "w", encoding="utf-8"), ensure_ascii=False)
    except PermissionError:
        log(f"!! 无法写入状态文件 {STATE_FILE}")


# ==================== Golaxy API 调用 ====================

FETCH_DEADLINE = 40    # fetch_golaxy 总耗时上限(秒): 窗口内拉满取全, 适当放宽

# 按 API 文档(EsQueryRequest): 用 title 字段做 ES match 检索。
# 改 title(标题命中)而非 text(正文命中)的原因: 标题短, "微信支付"分词凑词(微信+支付分散)
# 误命中概率远低于长正文; 实测 title 检索完整词占比 31/50, text 仅 15/50, 精度翻倍。
# (本地仍保留 COMPOUND_CHECK 复合词校验作冗余精筛)
MONITOR_TEXT = "腾讯金融科技 微信支付 腾讯支付 财付通 腾讯理财通 零钱通 微信零钱通 腾安基金 腾讯自选股 微信分付 微企付 微信零钱 微信提现 微信话费 微信充值 移动话费 充值话费"

# ES match 分词后 ("微信支付"→"微信"+"支付") 太宽泛，本地加复合词校验
# "腾讯支付": 用户口语对微信支付/财付通支付体系的通称(如"腾讯支付被冻")。
#   缺此词曾导致「玉人er 腾讯支付被冻申诉无人工通道」高价值负面漏判(COMPOUND_CHECK命中空→丢弃)。修复于2026-07-09。
COMPOUND_CHECK = ["腾讯金融科技", "微信支付", "腾讯支付", "财付通", "腾讯理财通", "理财通", "微信理财", "零钱通", "微信零钱通",
                  "腾安基金", "腾讯自选股", "微信分付", "微企付",
                  "微信零钱", "微信提现"]

# 业务主体裁定: COMPOUND_CHECK 只判"是否命中完整词", 但一篇文章可能顺带提到多个产品词,
# 机械取"第一个命中词"会误标(如通篇讲微信支付付款码/扣款、文末才提一句零钱通 → 误标"零钱通")。
# resolve_business_tag 按各业务在正文的加权得分裁定真正主体, 让【业务标签】反映文章核心。
# 场景词: 命中即给对应业务加分(解决"微信支付"被分词拆散、正文无连续"微信支付"却明显是支付话题)。
BUSINESS_SCENE_KW = {
    "微信支付": ["付款码", "支付逻辑", "支付入口", "免密支付", "自动扣款",
                 "补扣", "刷掌", "刷脸", "亲属卡", "分账", "收单",
                 "支付方式", "支付顺序", "扣费", "免密", "代扣", "腾讯支付"],
    "零钱通": ["零钱通", "货币基金", "七日年化"],
    "理财通": ["理财通", "腾讯理财通", "微信理财", "基金", "净值", "赎回"],
    "微信零钱": ["微信零钱", "零钱余额", "零钱提现", "提现", "提现免手续费",
                 "提现手续费", "提现额度", "免费额度", "收款码",
                 "经营账户", "经营收款码", "个人收款码", "银行卡"],
    "微粒贷": ["微粒贷", "借款额度"],
    "分付": ["微信分付", "分付", "先用后付"],
}


def resolve_business_tag(title, text, hit_brands):
    """裁定文章真正的业务主体标签。
    策略: 对每个候选业务, 按 (命中完整品牌词×3) + (命中场景词次数×1) + (标题出现×2) 计分, 取最高分业务。
    平局或全 0 时回落到 hit_brands 原顺序。"""
    blob = f"{title or ''} {text or ''}"
    title_s = title or ""
    scores = {}
    # 候选集: hit_brands 命中的 + 场景词表里的业务
    candidates = set(hit_brands) | set(BUSINESS_SCENE_KW.keys())
    for biz in candidates:
        s = 0
        # 完整品牌词命中(强信号)
        if biz in blob:
            s += 3
            if biz in title_s:
                s += 2
        # 场景词命中次数
        for kw in BUSINESS_SCENE_KW.get(biz, []):
            c = blob.count(kw)
            if c:
                s += c
                if kw in title_s:
                    s += 1
        if s > 0:
            scores[biz] = s
    if not scores:
        return " / ".join(hit_brands) if hit_brands else "舆情"
    top = max(scores.values())
    winners = [b for b, v in scores.items() if v == top]
    # 平局时优先 hit_brands 里出现的, 保持稳定
    if len(winners) > 1:
        for hb in hit_brands:
            if hb in winners:
                return hb
    return winners[0]

# 核心理财产品强主体词: 含任一即视为腾讯理财业务的实质讨论(吐槽/体验/疑问皆是核心舆情)。
# 用于 UGC 负面/短问句关的豁免——理财通/零钱通的任何吐槽都是用户明确要的核心舆情, 不应因缺硬投诉词被误杀。
LICAI_CORE_KW = ["理财通", "零钱通", "微信理财", "腾讯理财通", "腾安基金"]

# 理财收益异动/吐槽信号词: 与 LICAI_CORE_KW 叠加用于 SPAM 关豁免。
# 目的: 让"零钱通收益太低/年化下降/转去余额宝"这类真实理财舆情绕过 SPAM(备用金/秒到账等贷款广告词),
# 同时不放进"零钱通实用功能/如何转入"等纯科普教程(它们不含收益异动语义)。
LICAI_SENTIMENT_SIGNALS = [
    "七日年化", "年化", "收益率", "收益", "利率", "利息", "回报",
    "转出", "转到", "转去", "赎回", "撤出", "搬到", "挪到", "提现",
    "太低", "越来越低", "不足", "缩水", "下降", "降到", "跌到", "不如",
    "亏", "划算", "不划算", "高一些", "略高", "余额宝", "存哪",
]

# ==================== 危机事件专项监测: 移动话费充值 ====================
# 事件: "微信充不了移动话费" / "移动下线/关闭微信充值入口" / "移动中止与腾讯合作"
# 特点: 主体词是"移动+微信/腾讯", 不含腾讯系完整品牌词, 会被 COMPOUND_CHECK 漏掉,
#       故单独建事件层, 命中即优先放行(绕过复合词关), 并按中性/负面分类打标。
MOBILE_EVENT_ENABLED = True
# 腾讯/微信一侧主体 (二选一须命中)
_ME_TENCENT = ["微信", "腾讯", "财付通", "微信支付", "tenpay", "wepay"]
# 运营商一侧主体 (须命中, 区分"中国移动"与无关的"移动支付/移动互联网")
_ME_CARRIER = ["中国移动", "移动公司", "移动话费", "移动充值", "移动号码", "移动用户",
               "移动营业厅", "移动手机", "移动卡", "运营商", "三大运营商", "移动联通电信"]
# 事件动作/场景词 (须命中至少一个, 锚定到"充值/合作/下线"语境, 排除泛"移动"噪音)
_ME_ACTION = ["话费", "充值", "充话费", "充不了", "充不上", "缴费", "充值入口",
              "下线", "关闭", "停止", "中止", "终止", "暂停", "解除合作", "停止合作",
              "合作", "通道", "接口", "充值渠道", "充值失败", "无法充值", "充不进"]

# 负面甩锅信号: 把责任导向腾讯/微信、带失实或负面情绪 (用于事件内的中性/负面细分)
_ME_BLAME = ["腾讯下线", "微信下线", "微信关闭", "腾讯关闭", "微信不让", "腾讯不让",
             "微信封", "腾讯封", "微信的锅", "腾讯的锅", "怪微信", "怪腾讯",
             "微信不作为", "腾讯不作为", "微信太坑", "腾讯太坑", "微信垃圾", "腾讯垃圾",
             "微信故意", "腾讯故意", "微信霸权", "腾讯霸权", "微信吃相", "腾讯吃相",
             "微信店大欺客", "腾讯店大欺客", "都怪微信", "都怪腾讯", "微信不负责", "腾讯不负责",
             "微信背锅", "腾讯背锅", "鄙视腾讯", "鄙视微信", "抵制微信", "抵制腾讯"]
# 我方口径/官方公告信号 (用于标注"含我方口径"的中性报道, 重点关注)
_ME_OFFICIAL = ["回应", "声明", "公告", "澄清", "辟谣", "官方表示", "腾讯回应", "微信回应",
                "财付通回应", "腾讯方面", "微信方面", "官方回应", "据了解", "记者从", "通报"]


def match_mobile_event(title, text):
    """移动话费充值事件专项判定。
    返回: (hit:bool, category:str, blame:bool)
      category: 'official'(含我方口径/公告) / 'blame'(甩锅腾讯微信) / 'neutral'(其他相关)
    判定: 腾讯系主体 AND 运营商主体 AND 事件动作词, 三者俱全才算命中(防泛'移动'噪音)。
    """
    if not MOBILE_EVENT_ENABLED:
        return (False, "", False)
    blob = f"{title or ''} {text or ''}"
    bl = blob.lower()
    has_tx = any(w.lower() in bl for w in _ME_TENCENT)
    has_carrier = any(w in blob for w in _ME_CARRIER)
    has_action = any(w in blob for w in _ME_ACTION)
    if not (has_tx and has_carrier and has_action):
        return (False, "", False)
    is_blame = any(w in blob for w in _ME_BLAME)
    has_official = any(w in blob for w in _ME_OFFICIAL)
    if is_blame:
        cat = "blame"
    elif has_official:
        cat = "official"
    else:
        cat = "neutral"
    return (True, cat, is_blame)


# 投诉模板骨架字段词: 黑猫投诉等平台抓回的"空壳"内容只有这些字段标签、无实质叙述,
# 如"退款 投诉编号 投诉对象 财付通 投诉问题 退款不及时 投诉要求 还我钱"。
# 命中 >=2 个字段词 视为模板骨架, 整条丢弃(无舆情价值, 用户明确不要这种格式)。
COMPLAINT_TEMPLATE_FIELDS = ["投诉编号", "投诉对象", "投诉问题", "投诉要求", "投诉进度"]


def is_complaint_template(combined):
    """判断是否为投诉模板骨架(只有字段标签、无实质描述)。命中>=2字段即判定。"""
    return sum(1 for f in COMPLAINT_TEMPLATE_FIELDS if f in combined) >= 2

# 仅监测媒体报道的品牌(社媒UGC信息pass): 若文章只关联这几个品牌且来源是社媒, 则跳过
MEDIA_ONLY_BRANDS = ["微众银行", "微粒贷", "微保"]

# Golaxy 数据库标记的「权重媒体」subtag: 命中即跳过所有过滤, 直接推送
# 这是数据库层面的人工标注权威媒体(如新华社转发、央行发布、监管通报等)
WEIGHTED_MEDIA_SUBTAGS = ["官方及央媒资讯", "官方及央媒", "央媒资讯", "权重媒体"]

# Golaxy 数据库标记的「产品知识/营销教程」subtag: 命中即拦截(非真实舆情)
# 这些是 zeroqiantong 开通入口、使用指南、提现攻略等运营/营销内容
# 例外: PRODUCT_FRIENDLY_KW 命中(亲情账户/境外支付等) 或 非社媒平台+内容有实质价值 → 放行
PRODUCT_KNOWLEDGE_SUBTAGS = ["产品知识及活动"]

# 产品知识 subtag 但来自正规媒体/今日头条的有价值内容应放行
# 判断: captureWebsite 是媒体型(不是微信公众号/社媒) + 内容含新闻价值词
PRODUCT_KW_MEDIA_OVERRIDE = ["内测", "测试", "官宣", "首批", "新物种", "AI支付", "工具箱",
                              "盗刷", "安全", "风险", "违规", "扣款", "冻结", "警惕", "删掉"]

# 官方账号作者(author 精确/包含匹配): 腾讯金融科技矩阵官方号发布的内容, 即使被标"产品知识及活动"
# subtag, 也应放行(重大产品动态/升级公告本就该推, 不是运营软文)。
# (修复: 官方号《腾讯自选股接入混元Hy3模型》被"产品知识"subtag黑名单误杀)
OFFICIAL_ACCOUNTS = ["腾讯金融科技", "腾讯自选股财经", "腾讯自选股微信版", "腾讯自选股",
                     "腾讯理财通", "微信支付", "腾讯微证券", "微证券"]

# 产品重大动态信号词: 命中即视为有舆情价值的官方/媒体产品动态(而非运营教程), 放行 subtag 黑名单
# (混元/大模型接入、功能上线发布、版本升级、战略合作等)
PRODUCT_DYNAMIC_KW = ["混元", "大模型", "接入", "升级", "上线", "发布", "推出", "新增",
                      "战略合作", "合作", "全面接入", "全场景", "全链路", "AI服务",
                      "智能问答", "问元宝", "新功能", "重磅", "正式"]

# ⚡ 竞品替代种草 强制放行白名单 (用户指定 2026-07-07):
# 「余额宝攒着 + 零钱通」类内容 = 竞品(阿里系余额宝)对零钱通的隐性替代压制舆情, 高价值必推。
# 触发: 同时含"余额宝" + "零钱通" + 攒/存/放/转类资金动作词 → 无条件放行(跳过所有黑名单/排除词)。
# 叠加资金动作词是为排除纯政策科普(如"9·30新规"并列提及但无种草迁移语义)。
COMPETITOR_SEED_YEB = "余额宝"
COMPETITOR_SEED_LQT = "零钱通"
COMPETITOR_SEED_ACTION = ["攒", "存", "放", "转", "挪", "搬", "囤", "吃利息", "吃收益",
                          "收益", "利息", "年化", "划算", "香", "首选", "保底", "躺"]

# 产品功能/活动友好关键词: 命中后放行 PRODUCT_KNOWLEDGE_SUBTAGS / UGC_PERSONAL 拦截
# 这些词代表腾讯金融产品的实际功能或活动(如亲情账户、收益对比、福利领取)
# 用户的#理财通亲情账户#等 UGC 笔记应被推送, 而非被当作营销拦截
PRODUCT_FRIENDLY_KW = [
    # 理财通 亲情账户/亲情卡 (产品功能)
    "亲情账户", "亲情卡", "亲情号", "亲情", "家庭账户", "家人共享",
    # 收益/对比类 (用户实际使用反馈)
    "收益对比", "收益怎么样", "收益如何", "收益吐槽", "一天收益", "理财收益",
    "收益才", "收益才多少", "收益好低", "收益好少",
    # 福利/红包/活动
    "理财通福利", "零钱通福利", "理财福利", "理财红包",
    "微信支付红包", "零钱通红包", "理财活动", "福利活动",
    # 注意: 易方达/基金公司 不加入白名单 (腾讯自选股App的ETF行情播报常含"易方达", 会误放行)
    # 用户使用体验关键词
    "用了理财通", "用理财通", "微信理财通收益", "我的理财",
]

# 腾讯自选股 App 行情域名: 这些 URL 是机器生成的 ETF/股票/卖空播报, 全部拦截
TXZQ_APP_DOMAINS = ["gu.qq.com", "guqq.com"]
# 社媒/UGC/自媒体 平台(captureWebsite 包含匹配): 个人发帖、社区、问答、公众号软文, 非正规媒体报道
# 注意: 微信公众号(captureWebsite="微信")属自媒体, 对"仅媒体"品牌按社媒处理
SOCIAL_MEDIA_SITES = ["微信", "微博", "小红书", "抖音", "快手", "微头条", "贴吧", "豆瓣",
                      "知乎", "懂车帝", "有驾", "汽车之家", "易车", "卡农", "论坛",
                      "黑猫投诉", "消费保", "百度贴吧", "b站", "哔哩哔哩", "qq空间", "百家号",
                      "搜狐号", "网易号", "一点资讯", "快资讯", "大鱼号", "企鹅号"]
# "仅媒体"品牌专属引流软文排除词(贷款产品推广): 命中即剔除
MEDIA_ONLY_PROMO = ["取用", "怎么取", "三步", "3步", "几步", "避坑指南", "额度最高",
                    "日利率", "随借随还", "按日计息", "开通入口", "申请入口", "借款入口",
                    "怎么开通", "如何开通", "教你", "教程", "攻略", "下款", "借钱攻略",
                    "小鹅花钱"]

AUTO_LOW_VALUE = ["懂车帝", "有驾", "汽车之家", "易车"]

# 法律咨询类平台: 非负面不推送 (正面/中性多为普法/引流软文, 只有投诉举报才值得关注)
LEGAL_LOW_VALUE = ["法律快车", "法律问答", "华律网", "找法网", "律师", "法帮"]

# 完全屏蔽平台: 消费者投诉/维权平台, 个体纠纷无舆情价值, 全部pass
# 注: 消费保改为白名单放行(见下 XFB_WHITELIST_KW), 不再一刀切屏蔽; 此处保留其他需全屏的平台
BLOCKED_PLATFORMS = []
# 消费保(xfb315)白名单: 命中核心腾讯系支付/理财业务才放行(财付通/微信支付等真实负面投诉),
# 其余个体消费纠纷仍屏蔽。比黑猫白名单宽(黑猫上微信支付投诉噪音过多故只留理财通类)。
XFB_WHITELIST_KW = ["财付通", "微信支付", "微信零钱", "微信提现", "零钱通",
                    "理财通", "腾讯理财通", "微信理财", "腾安基金",
                    "微粒贷", "微众", "微保", "分付", "微企付", "tenpay"]
# ===== 黑猫投诉规则（固定，禁止扩展）=====
# 黑猫投诉是个人用户投诉平台，仅当以下词出现时才具备金融科技舆情价值
# ⚠️ 严禁将"财付通"、"微信支付"等宽泛词加入此列表
# ⚠️ 加入宽泛词会放行大量个人催收/退款/游戏投诉（易付宝/安鑫花/小蚕/游戏充值等），造成噪音泛滥
# 规则: 黑猫投诉平台只有明确涉及「腾讯理财通」或「腾安基金」产品的投诉才推送
HEIMAOTOU_WHITELIST_KW = ["腾讯理财通", "腾安基金", "理财通"]

# UGC个人平台 (微博/小红书/微头条/微信公众号/知乎/51kanong/贴吧 等):
# 负面短文多是段子/玩笑/个人提问, 须含明确投诉关键词才算有效舆情
UGC_PERSONAL = ["新浪微博", "微博", "小红书", "微头条", "今日头条微头条",
                "微信", "知乎", "51kanong", "卡农", "贴吧", "豆瓣",
                "百度贴吧", "b站", "哔哩哔哩", "qq空间", "百家号", "搜狐号", "网易号", "大鱼号"]
# 有效投诉关键词 (UGC 负面内容必须命中其一才算真投诉)
# 注: "法院"、"诉讼" 等司法词是个人被执行人求助帖的高频词, 不能算有效舆情, 移除
VALID_COMPLAINT_KW = ["投诉", "举报", "维权", "曝光", "律师咨询",
                     "12315", "12321", "工信部", "银保监", "央行", "金融监管",
                     "消保委", "市场监督", "黑猫投诉", "消费保",
                     # 微信支付功能限制类 (用户真实体验反馈, 值得监控)
                     "被限制", "限制了", "限额", "限制支付", "无法支付", "支付失败",
                     "不能转账", "转账限制", "转账失败", "不能付款", "付款失败",
                     "转钱不行", "不让转", "转账不行", "转不了", "转不了钱", "不行啊",
                     "转不了", "转钱都不行", "转账都不行",
                     "被封", "账号限制", "功能受限", "风控限制", "触发风控",
                     "换绑", "实名认证", "身份证换绑", "支付密码",
                     "账户异常", "冻结账户", "账号异常",
                     # 境外/跨境微信支付 (产品功能体验)
                     "境外支付", "境外微信", "跨境支付", "海外使用", "外国人微信",
                     "微信支付海外", "境外汇率", "微信境外",
                     # 微信支付功能受限/解除限制
                     "解除限制", "解除封禁", "解封微信",
                     # 风控/交易受阻 (用户真实遇到微信支付风控、无法支付的体验)
                     "交易风险", "风险提示", "触发风险", "交易失败", "支付被拒",
                     "不让我支付", "无法完成支付",
                     # 微信支付提现/补贴（正面产品动态）
                     "提现免费", "提现手续费", "零钱提现", "微信提现券",]

# 腾讯生态关键事件触发词: 自选股作为参与方/接入方被提及的报道 → 关联报道
# 特征: 文章主体是腾讯生态动作, 自选股只是参与方之一
# 典型: "微信官宣开放AI生态接入...京东等已首批接入" (文中含自选股)
TENCENT_ECO_EVENTS = [
    "ai 生态", "ai 接入", "ai 能力", "ai 应用", "ai 落地", "ai 内测",
    "ai能力", "ai应用", "ai落地", "ai内测",
    "agent 生态", "agent 能力", "agent能力", "agent生态",
    "skill 文档", "skill文档", "skill 生态", "skill生态",
    "数字人", "智能体", "混元", "hunyuan",
    "首批接入", "首批合作", "首批用户", "首批内测", "首批入驻",
    "开放生态", "能力开放", "生态开放", "生态合作",
    "小程序接入", "小程序生态", "微信生态", "腾讯生态",
    "接入指引", "开发者接入", "ai 助手", "ai助手",
    "京东", "美团", "拼多多", "滴滴", "贝壳",
    "微信ai", "小程序ai", "腾讯ai", "腾讯云ai", "腾讯大模型",
]
# 外部引流/广告软文拦截词 (自选股开户/投资类广告)
PROMO_KW = [
    "点击开户", "立即开户", "无需下载", "轻松投资", "免费开户",
    "立即下载", "马上开户", "1分钟开户", "扫码开户", "在线开户",
    "开户即可", "开户送", "0元开户", "开户即享", "首次开户",
    "股票开户", "证券开户", "一键开户", "立即注册", "马上注册",
    "立即领取", "限时领取", "扫码下载", "点击下载",
]
# 自选股 App 内行情播报词 (机器生成, 非舆情)
APP_MARKET_KW = [
    "异动", "走高", "走低", "涨超", "跌超", "涨逾", "跌逾",
    "etf", "涨幅", "跌幅", "开盘", "收盘", "板块", "个股",
    "涨停", "跌停", "成交", "换手", "大盘", "指数",
]

# 垃圾内容特征词 (教程/软文/广告/生活技巧): PR Channel所有命中词, 文章若含这些特征且非负面, 则跳过
# 注意: 这些词本身不应该加到 exclude_words.txt 因为它们会误杀真正的负面投诉
SPAM_PATTERNS = [
    # 教程/操作指引
    "怎么导出", "操作步骤", "详细教程", "手把手", "几步搞定", "一键生成",
    "快点收藏", "建议收藏", "先收藏", "还不赶紧", "抓紧收藏",
    # 生活技巧/省钱
    "省不少钱", "能省钱", "不吃亏", "真相让人", "才知道",
    "还不知道", "原来可以", "超实用", "才知道", "搞明白了",
    # 广告/推广
    "优惠券", "立减", "满减", "折扣", "店庆", "周年庆", "狂欢",
    "大放送", "宠粉福利", "击穿底价", "剁手",
    # 自媒体政策/教程解读软文 (标题含「速看/政策解读/扶持计划/自动生效」, 非正规媒体报道)
    "速看", "重磅福利", "扶持计划", "小商家速看", "自动生效",
    "核心政策解读", "年收款.*免", "薅到", "这波羊毛",
    # 贷款/借钱推广
    "备用金", "资金周转", "轻松借", "秒到账", "急用钱",
    # 教程/开通指引
    "开通即用", "只需几步", "超简单", "最新流程", "最新）", "开通入口",
    "最新版", "怎么开通", "如何开通", "教你", "教程", "攻略",
    # 信用卡/银行推广 (非微信支付相关, 精确匹配套现推广)
    # 注: "信用卡" 本身不拦截 (银盛诱骗等案例中「信用卡」是受害者陈述的背景词, 不是推广词)
    "刷信用卡套现", "信用卡套现", "摇优惠", "积分兑换", "刷卡金",
    # 开通步骤特征
    "按步骤", "激活开通", "终于来了", "只讲一件事", "很多人找不到入口",
    # 个人用户问题 (封号/解封/支付方式变化等, 是个体问题非行业舆情)
    "被封号", "封号了", "解封失败", "解封了", "突然被封", "怎么解封",
    "封了一天", "封了两天", "被封了", "异常行为",
    # 个人被司法冻结 (执行案件被执行人求助帖, 非金融科技行业舆情)
    "被执行人", "执行案件", "执行程序", "迟延履行", "强制执行",
    "被冻结给生活", "账户与微信支付被冻结", "解除银行账户",
    "司法冻结", "执行法院", "执行单位",
    # 法院冻结零钱 段子/科普类 (非行业舆情)
    "冻结？这波", "冻结#微信", "#微信支付冻结", "解冻#微信",
    # 零钱通/微信支付/分付 开通指南/操作教程/使用攻略类 (用户运营内容, 非舆情)
    "开通指南", "开通流程", "开通方法", "开通步骤", "开通方式", "开通教学",
    "如何使用", "使用指南", "使用教程", "使用方法", "使用攻略",
    "使用技巧", "操作指南", "操作教程", "操作方法", "操作流程", "操作步骤",
    "提现指南", "提现教程", "提现方法", "提现流程", "提现步骤",
    "转入指南", "转出指南", "转入教程", "转出教程",
    "开通零钱通", "零钱通入口", "微信零钱通入口", "怎么开通零钱通",
    "如何开通零钱通", "零钱通怎么", "零钱通如何",
    "找回指南", "联系方法", "联系方式", "客服热线", "客服电话",
    "零钱通开通", "开通入口最新", "入口最新",
    # 理财通: 仅拦截引流教程类, 不拦截产品功能/活动关键词 (亲情账户/收益对比等)
    "理财通怎么", "如何开通理财通", "理财通入口", "开通理财通",
    "理财通开户", "理财通收益怎么", "理财通怎么用",
    # ETF/股票 行情自动播报 (腾讯自选股App机器生成, 非舆情)
    "异动走高", "异动走低", "涨超", "跌超", "涨逾", "跌逾",
    "etf博时", "etf富国", "etf华夏", "etf南方", "etf招商", "etf嘉实",
    "etf易方达", "etf华泰柏瑞", "etf国泰", "etf广发", "etf银华", "etf天弘",
    # 法律问答 SEO 类教程 (常见于自媒体: "财付通扣我银行卡怎么追回" 等)
    "怎么追回", "如何追回",
]

# 个人被骗/受害叙事标志词: UGC平台+中性+命中→直接拦截, 不走SPAM_GUARD
# 这些是纯个人生活故事 (被骗嫁妆/彩礼/养老钱), 微信支付仅作为转账工具被顺带提及
# 与VALID_COMPLAINT_KW不同: 后者拦截"对微信支付产品的投诉", 这里是"骗子骗了我的钱, 付款用了微信支付"
PERSONAL_SCAM_MARKERS = ["嫁妆", "彩礼被骗", "养老钱被骗", "救命钱被骗"]

# 防误杀关键词: 匹配 SPAM_PATTERNS 但同时涉及投诉举报 → 不拦截
SPAM_GUARD = [
    "投诉", "举报", "维权", "纠纷", "诈骗", "欺诈", "虚假",
    "侵权", "盗刷", "被盗", "异常扣款", "未经授权", "擅自",
    "退款难", "不退", "客服不", "人工客服", "无法联系",
    "投诉无门", "曝光", "避雷", "踩坑", "避坑", "受害者",
    "坑人", "骗局", "陷阱", "圈套", "黑心", "霸王",
    "诱骗", "套现", "被骗", "骗走", "服务费", "受害",
    "风险", "恳求", "求助", "无路", "走投无路",
]



def fetch_golaxy(publishTimeFrom, publishTimeTo):
    """按发布时间窗口 + title(ES match) 拉取数据。
    重要: 接口(EsQueryRequest)仅支持 publishTimeFrom/To 作时间过滤;
    旧代码用 captureTimeFrom/To 是接口不认的字段, 被静默忽略 → 等于每轮全量。
    现改用合法的 publishTime 窗口, 并在窗口内翻页拉满取全(无条数上限), 消除"第201条漏报"。
    返回 (docs, connected): connected 表示至少成功请求了一次 API"""
    docs = []
    connected = False
    t0 = time.time()
    for page_num in range(MAX_PAGES):
        if time.time() - t0 > FETCH_DEADLINE:
            log(f"   fetch_golaxy 超时 {FETCH_DEADLINE}s 上限, 停止翻页")
            connected = connected or bool(docs)
            break
        payload = {
            "page": page_num,
            "size": PAGE_SIZE,
            "sortBy": "publishTime",
            "sortOrder": "desc",
            "publishTimeFrom": publishTimeFrom,
            "publishTimeTo": publishTimeTo,
            "title": MONITOR_TEXT,
        }
        try:
            res = http_post(
                f"{GOLAXY_BASE_URL}/api/v1/contents/search",
                payload,
                extra_headers={"X-API-Key": GOLAXY_API_KEY},
            )
            connected = True  # 成功连上 API
        except Exception as e:
            log(f"   Golaxy page={page_num} 失败: {e}")
            break
        if res.get("code") != 200:
            log(f"   Golaxy page={page_num} 返回 code={res.get('code')}: {res.get('message')}")
            break
        data = res.get("data", {})
        contents = data.get("contents", [])
        if not contents:
            break
        docs.extend(contents)
        if not data.get("hasNext"):
            break
    return docs, connected


# ==================== 格式化 & 推送 ====================

# 无信息量的开场白/寒暄句式 — 即使是首句也不优先, 命中则降权
_FILLER_PAT = re.compile(
    r"^(大家好|哈喽|哈啰|hello|hi |今天|最近|近日|话说|说起|不知道|你们|"
    r"姐妹们|宝子们|家人们|各位|朋友们|前阵子|前段时间|我是|有钱了|"
    r"这个问题|这问题|讲真|说实话|不得不说|分享一下|今儿|昨天|刚刚)"
)
# 舆情价值信号词: 事件/情感/投诉/产品动作, 句子含之则更值得入摘要
_SIGNAL_WORDS = (
    "投诉", "举报", "维权", "扣", "划扣", "封", "冻结", "限制", "失败", "不到账",
    "收费", "手续费", "风控", "异常", "漏洞", "退款", "客服", "赎回", "收益",
    "额度", "故障", "亏", "套牢", "被骗", "罚", "处罚", "回应", "整改", "下架",
    "升级", "上线", "发布", "推出", "新增", "调整", "改版", "开通", "试点",
    "合作", "接入", "降", "涨", "暴露", "曝光", "争议", "质疑", "提现",
)
# 摘要锚定的腾讯系核心主体词
_SUMMARY_CORE = (
    "微信支付", "微信理财通", "微信理财", "微信零钱", "微信红包", "微信提现",
    "财付通", "理财通", "零钱通", "微粒贷", "微众", "微保", "分付", "微企付",
    "腾讯", "腾安基金", "微信", "wepay", "tenpay",
)

# 行情播报体压缩: 自选股/ETF 播报正文冗长(换手率/成交额/溢折率/自选哥注...),
# 用户只关心"标的 + 涨跌 + 幅度"。命中则直接产出精简摘要, 不进后续抽句。
_MARKET_PAT = re.compile(
    r"(.{2,30}?)\s*[（(]?\s*(\d{6}\.[A-Z]{2})?\s*[）)]?\s*"
    r"(异动(?:走高|下跌|拉升|走低)?)[，,]\s*(现[涨跌]超?[\d.]+%|[涨跌][\d.]+%)"
)
# 排版噪音: 句首的"一、/1./①/✅/⚠️/【x】/（日期）"等结构标记, 抽句前剥离
_LAYOUT_PAT = re.compile(
    r"^\s*(?:[一二三四五六七八九十]{1,3}[、.．]|\d{1,2}[、.．)]|"
    r"[①②③④⑤⑥⑦⑧⑨⑩]|[✅⚠️❗️🔴🟢▶️•·➤►]|【[^】]{1,12}】|"
    r"（\s*\d{4}[.\-年].*?生效?\s*）|\(\s*\d{4}[.\-].*?\)|"
    r"第[一二三四五六七八九十\d]+[章节部分条])+\s*"
)


def _compress_market(text):
    """行情播报体 → '标的（代码）异动下跌，现跌超X%'。非播报体返回 None。"""
    m = _MARKET_PAT.search(text)
    if not m:
        return None
    name = re.sub(r"\s+", "", m.group(1)).strip("，,、。 ")
    code = m.group(2) or ""
    action = m.group(3) or "异动"
    change = m.group(4) or ""
    # 标的名太短(可能误匹配)则放弃
    if len(name) < 3:
        return None
    head = f"{name}（{code}）" if code else name
    return f"{head}{action}，{change}".strip("，,")


def make_summary(title, text, hit_words, max_len=80):
    """规则式摘要(零API): 先压缩行情播报体; 否则剥离排版噪音后按'信息密度+舆情信号+主体'
    打分抽句, 丢弃寒暄/过渡句, 按原文顺序拼到 max_len。"""
    text = re.sub(r"\s+", " ", (text or "")).strip()
    # 去掉结尾成串话题标签 (#xxx #yyy)
    cleaned = re.sub(r"(#[^#\s]+\s*)+$", "", text).strip() or text
    if not cleaned:
        return (title or "(无正文)")[:max_len]

    # ① 行情播报体专项压缩
    mk = _compress_market(cleaned)
    if mk:
        return mk[:max_len]

    # ② 通用抽句
    sentences = re.split(r"(?<=[。！？!?；;\n])", cleaned)
    clean_sents = []
    for s in sentences:
        s = _LAYOUT_PAT.sub("", s).strip(" ，,、；;：:")   # 剥离句首排版标记
        s = re.sub(r"\s+", " ", s)
        if len(s) >= 6:
            clean_sents.append(s)
    if not clean_sents:
        return _LAYOUT_PAT.sub("", cleaned).strip()[:max_len] or cleaned[:max_len]

    core = [w for w in dict.fromkeys(list(hit_words) + list(_SUMMARY_CORE)) if w]
    scored = []
    seen = set()
    for i, s in enumerate(clean_sents):
        if s in seen:      # 去重复句
            continue
        seen.add(s)
        sl = s.lower()
        sc = 0
        if any(w.lower() in sl for w in core):
            sc += 3
        sig = sum(1 for g in _SIGNAL_WORDS if g in s)
        sc += min(sig, 2) * 2                       # 舆情信号词(封顶)
        if re.search(r"\d", s):                     # 含数字=有事实密度
            sc += 1
        if re.search(r"[%％元万亿倍]", s):          # 含量化单位再+1
            sc += 1
        if _FILLER_PAT.match(s):                    # 寒暄/引子降权
            sc -= 3
        scored.append((sc, i, s))

    picked = [x for x in scored if x[0] > 0]
    if not picked:
        picked = [min(scored, key=lambda x: x[1])]  # 全低分: 取最靠前的一句
    # 先按分数选, 再按原文顺序拼接(保证可读)
    picked.sort(key=lambda x: (-x[0], x[1]))
    chosen = picked[:4]
    chosen.sort(key=lambda x: x[1])
    summary = ""
    for _, _, s in chosen:
        sep = "" if not summary else " "
        if len(summary) + len(sep) + len(s) > max_len:
            if not summary:
                summary = s[:max_len] + "…"
            break
        summary += sep + s
    if len(summary) < 12 and len(cleaned) > len(summary):
        base = _LAYOUT_PAT.sub("", cleaned).strip()
        summary = base[:max_len] + ("…" if len(base) > max_len else "")
    return summary or cleaned[:max_len]


def build_item(src, tag_words, asts):
    """旧版构建 (保留兼容, 但不再使用)"""
    title = (src.get("title") or "").strip()
    text = (src.get("text") or "").strip()
    text = re.sub(r"\s+", " ", text)
    ce = src.get("contentExt") or {}
    sentiment_raw = ce.get("emotion", "中性")
    sentiment = SENTI_MAP.get(sentiment_raw, "中性")

    tags = list(dict.fromkeys(tag_words))[:6]
    tag_str = "/".join(tags) if tags else "-"
    tag_full = f"{tag_str}｜{sentiment}"

    summary = make_summary(title, text, tags)

    return {
        "tag": tag_full,
        "author": src.get("author") or "-",
        "title": title or "(无标题)",
        "publishTime": src.get("publishTime") or "-",
        "text": summary,
        "matched_kw": "/".join(tags) if tags else "-",
        "url": src.get("url") or "",
        "sourceWebsite": src.get("sourceWebsite") or "-",
        "platform": src.get("captureWebsite") or src.get("sourceWebsite") or "-",
        "_senti": -1 if sentiment == "负面" else (1 if sentiment == "正面" else 0),
        "_contentId": src.get("contentId") or "",
        "_interact": (ce.get("commentNum") or 0) + (ce.get("forwardNum") or 0)
                     + (ce.get("viewNum") or 0) + (ce.get("praiseNum") or 0),
    }


def build_item_v2(src):
    """按 API 文档返回字段构建推送项 (使用 API 原生 tag/subtag/emotion)"""
    title = (src.get("title") or "").strip()
    # 标题规整: 部分 UGC 平台(今日头条/微博/小红书/抖音等)把整段正文塞进 title 字段,
    # 导致标题过长。统一处理: 标题超长则取第一句(按 。！？!?\n 分隔); 第一句仍超长则硬截断。
    title = re.sub(r"\s+", " ", title).strip()
    TITLE_MAX = 50
    if len(title) > TITLE_MAX:
        first = re.split(r"[。！？!?\n]", title, maxsplit=1)[0].strip()
        # 去掉话题标签/@提及尾巴, 避免第一句里混入 #xxx# @xxx
        first = re.sub(r"[#＃].*$", "", first).strip()
        if first and 0 < len(first) <= TITLE_MAX:
            title = first
        else:
            # 第一句也过长或被清空 → 硬截断到上限
            base = first if first else title
            title = base[:TITLE_MAX].rstrip() + "…"
    text = (src.get("text") or "").strip()
    ce = src.get("contentExt") or {}
    # 统一情感判定: 负面优先(有负面不叠加中性), tag/emotion 综合
    sentiment, senti_int = resolve_sentiment(src)

    # 使用 API 返回的原生 tag / subtag (剔除其中混入的情感词, 避免'负面…｜中性'矛盾)
    api_tag = strip_senti_tokens(src.get("tag") or "")
    api_subtag = strip_senti_tokens(src.get("subtag") or "")
    second_trades = ce.get("secondTrades") or ""
    tag_parts = [p.strip() for p in [api_tag, api_subtag, second_trades] if p.strip()]
    tag_str = " / ".join(tag_parts[:3]) if tag_parts else "舆情监控"
    tag_full = f"{tag_str}｜{sentiment}"

    # 摘要: 规则式提炼(围绕命中标签+腾讯系核心句, 丢开场白, 短≤80字)
    # 命中词取自标签(tag_parts), 让摘要锚定到真正相关的句子;
    # 正文优先, 正文为空(如纯标题贴/视频号)再退回标题做摘要源
    hit_words = [w for p in tag_parts for w in re.split(r"[ /、]", p) if w]
    summary = make_summary(title, text or title, hit_words, 80)

    interact = (ce.get("commentNum") or 0) + (ce.get("forwardNum") or 0) + (ce.get("viewNum") or 0)

    # 领导人特别提示: 用原始 title+text 全文检测(不用截断后的 title, 防长文人名丢失)
    _vip_blob = f"{src.get('title') or ''} {src.get('text') or ''}"
    vip_hits = [p for p in VIP_PERSONS if p in _vip_blob]

    return {
        "tag": tag_full,
        "author": src.get("author") or "-",
        "title": title or "(无标题)",
        "publishTime": src.get("publishTime") or "-",
        "text": summary,
        "matched_kw": tag_str,
        "url": src.get("url") or "",
        "sourceWebsite": src.get("sourceWebsite") or "-",
        "platform": src.get("captureWebsite") or src.get("sourceWebsite") or "-",
        "_senti": senti_int,
        "_contentId": src.get("contentId") or "",
        "_interact": interact,
        "_vip_hits": vip_hits,
    }


def format_card(it):
    """格式锁定: 纯文本 `【标签】值` 逐行格式，禁止 Markdown、禁止表格"""
    text = it["text"]
    if len(text) > 300:
        text = text[:300] + "…"
    senti = it["tag"].split("｜")[1] if "｜" in it["tag"] else "中性"
    senti_icon = {"负面": "🔴", "中性": "⚪", "正面": "🟢"}.get(senti, "⚪")
    matched_kw = it.get("matched_kw") or it.get("_matched_kw") or ""
    if not matched_kw and "｜" in it["tag"]:
        matched_kw = it["tag"].split("｜")[0]
    lines = [
        f"【舆情类别】：{senti_icon}{senti}",
        f"【业务标签】{matched_kw or '舆情'}",
        f"【作者】{it.get('author', '-')}",
        f"【发布平台】{it.get('platform', '-')}",
        f"【发布时间】{it.get('publishTime', '-')}",
        f"【标题】{it.get('title', '-')}",
        f"【内容摘要】{text}",
    ]
    if it.get("url"):
        lines.append(f"【原文链接】{it['url']}")
    return "\n".join(lines)


def log_push_to_csv(it):
    if not PUSH_LOG_ENABLE:
        return
    try:
        os.makedirs(PUSH_LOG_DIR, exist_ok=True)
        date_str = datetime.now().strftime("%Y%m%d")
        filepath = os.path.join(PUSH_LOG_DIR, f"push_{date_str}.csv")
        row = {
            "数据标签": str(it.get("tag", "")),
            "作者": str(it.get("author", "")),
            "标题": str(it.get("title", "")),
            "发布时间": str(it.get("publishTime", "")),
            "内容摘要": str(it.get("text", ""))[:300],
            "来源网站": str(it.get("sourceWebsite", "")),
            "原文链接": str(it.get("url", "")),
        }
        fields = list(row.keys())
        file_exists = os.path.exists(filepath) and os.path.getsize(filepath) > 10
        with open(filepath, "a", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=fields)
            if not file_exists:
                writer.writeheader()
            writer.writerow(row)
    except Exception as e:
        log(f"    CSV记录失败: {e}")


def push_one(it):
    content = format_card(it)
    if len(content.encode("utf-8")) > 4000:
        it2 = dict(it); it2["text"] = it["text"][:200] + "…"
        content = format_card(it2)
    # 格式锁定: msgtype 固定为 text，禁止使用 markdown
    res = http_post(WEBHOOK_URL, {"msgtype": "text", "text": {"content": content}})
    ok = res.get("errcode") == 0
    if ok:
        log_push_to_csv(it)
    # 👑 领导人特别提示: 命中腾讯集团/金科高管 → 卡片后单独发一条高亮提醒(不论正负面)
    vip_hits = it.get("_vip_hits") or []
    if ok and vip_hits:
        senti_txt = it["tag"].split("｜")[1] if "｜" in it.get("tag", "") else "中性"
        vip_text = (f"👑 涉领导人舆情提示｜{('、'.join(vip_hits))}\n"
                    f"舆情类别：{senti_txt}｜业务：{it.get('matched_kw','-')}\n"
                    f"{it.get('title','')[:40]}\n{it.get('url','')}")
        vip_payload = {"msgtype": "text", "text": {"content": vip_text}}
        if VIP_ALERT_MOBILES:
            vip_payload["text"]["mentioned_mobile_list"] = list(VIP_ALERT_MOBILES)
        if VIP_ALERT_USERIDS:
            vip_payload["text"]["mentioned_list"] = list(VIP_ALERT_USERIDS)
        try:
            time.sleep(0.5)
            http_post(WEBHOOK_URL, vip_payload)
        except Exception as e:
            log(f"   领导人提示发送失败: {e}")
    is_mobile_event = bool(it.get("_mobile_event"))
    # 业务归属判定: 理财通/零钱通 → yalinlei/minazeng; 移动事件/微信分付 → anderschen; 其余 → ulrichguo
    _biz = it.get("matched_kw", "") or ""
    is_licai_biz = any(b in _biz for b in LICAI_ALERT_BIZ)
    is_fenfu_biz = any(b in _biz for b in FENFU_ALERT_BIZ)
    # 负面 @ 提醒 (按业务归属, 只@对应负责人, 不再统一兜底@ulrichguo)
    if ok and NEGATIVE_ALERT and it.get("_senti") == -1:
        at_userids = []      # 用 userid @ (yalinlei/minazeng/anderschen)
        at_mobiles = []      # 用手机号 @ (ulrichguo)
        at_names = []        # 文案里展示的 @昵称
        if is_licai_biz:
            at_userids += list(LICAI_ALERT_USERIDS)
            at_names += ["@yalinlei", "@minazeng"]
        elif is_mobile_event or is_fenfu_biz:
            at_userids += list(MOBILE_EVENT_AT_USERIDS)
            at_names += ["@anderschen"]
        else:
            at_mobiles += list(ALERT_MOBILES)
            at_names += ["@ulrichguo"]
        _at_tail = " ".join(at_names) + " 请及时关注处理。"
        at_text = (f"⚠️ 负面舆情预警｜{it.get('matched_kw','-')}\n"
                   f"{it.get('title','')[:40]}\n{_at_tail}")
        payload = {"msgtype": "text", "text": {"content": at_text}}
        if ALERT_AT_ALL:
            payload["text"]["mentioned_list"] = ["@all"]
        else:
            if at_mobiles:
                payload["text"]["mentioned_mobile_list"] = at_mobiles
            if at_userids:
                payload["text"]["mentioned_list"] = at_userids
        try:
            time.sleep(0.5)
            http_post(WEBHOOK_URL, payload)
        except Exception as e:
            log(f"   负面@提醒失败: {e}")
    # 移动话费充值事件(非负面: 中性/我方口径) → 单独 @ anderschen(陈恩达)
    elif ok and is_mobile_event and MOBILE_EVENT_AT_USERIDS:
        at_text = (f"📵 移动话费充值事件相关舆情｜{it.get('matched_kw','-')}\n"
                   f"{it.get('title','')[:40]}\n@anderschen 请关注。")
        payload = {"msgtype": "text", "text": {"content": at_text,
                                               "mentioned_list": list(MOBILE_EVENT_AT_USERIDS)}}
        try:
            time.sleep(0.5)
            http_post(WEBHOOK_URL, payload)
        except Exception as e:
            log(f"   移动事件@提醒失败: {e}")
    return ok, res


def push_items(items):
    if not items:
        log("本轮无命中新舆情, 静默不推送")
        return True, []
    # 防御: 缺失 _senti 字段时补 0 (中性), 防止 KeyError
    for x in items:
        if "_senti" not in x:
            x["_senti"] = 0
        if "_interact" not in x:
            x["_interact"] = 0
        if "_contentId" not in x:
            x["_contentId"] = ""
    items.sort(key=lambda x: (x.get("_senti") == -1, x.get("_interact", 0)), reverse=True)
    pushed_contentIds = []
    ok_cnt = 0
    for idx, it in enumerate(items[:MAX_PUSH], 1):
        ok, res = push_one(it)
        if ok:
            ok_cnt += 1
            pushed_contentIds.append(it["_contentId"])
        else:
            log(f"   第{idx}条推送失败: {res}")
        time.sleep(0.5)
    log(f"✅ 成功推送 {ok_cnt}/{min(len(items), MAX_PUSH)} 条到企业微信")
    return ok_cnt > 0, pushed_contentIds


# ==================== 主流程 ====================

def run_once(asts, excludes, block_media, block_url):
    seen_set, seen_list = load_seen()
    now = datetime.now()

    # 时间窗口: 接口仅支持 publishTime 过滤(EsQueryRequest)。
    # 发布时间可能早于入库, 故回看窗口放大(LOOKBACK_MINUTES=360), 靠去重保证不重推。
    last_scan = next((x for x in reversed(seen_list) if x.startswith("scan:")), None)
    if last_scan:
        last_t = datetime.fromisoformat(last_scan[5:])
        begin = (last_t - timedelta(minutes=LOOKBACK_MINUTES)).strftime("%Y-%m-%d %H:%M:%S")
        log(f"增量扫描: 从扫描点 {last_scan[5:]} 回看{LOOKBACK_MINUTES}min (publishTime)")
    else:
        begin = (now - timedelta(minutes=WINDOW_MINUTES)).strftime("%Y-%m-%d %H:%M:%S")
        log(f"首次/回扫: 窗口 {WINDOW_MINUTES} 分钟 (publishTime)")
    end = now.strftime("%Y-%m-%d %H:%M:%S")

    # 1. 拉取 (API title 参数做 ES 关键词搜索 + publishTime 窗口, 窗口内拉满取全)
    log(f"拉取 Golaxy 数据 publishTime: {begin} ~ {end} + title=ES match")
    docs, api_ok = fetch_golaxy(begin, end)
    if not api_ok:
        log(f"⚠️ API 不可达, 本轮跳过 (扫描点不推进, 下次重试)")
        return -1  # -1 表示 API 不可达 (与"命中0"区分, 用于自愈计数)
    log(f"共拉取 {len(docs)} 条, 开始本地过滤...")

    # 2. 简化过滤: 去重 + 站点黑名单 + 排除词 (API 已做关键词初筛)
    matched = []
    dup = excluded = blocked = 0
    batch_fps = set()

    for src in docs:
        cid = src.get("contentId") or ""
        title_raw = src.get("title") or ""
        norm_title = re.sub(r"\s+", "", title_raw)[:80]
        fp = "fp:" + norm_title if norm_title else "fp:" + cid
        sh = src.get("simhash")

        # 去重: contentId + 标题指纹 + simhash
        if (cid and cid in seen_set) or fp in seen_set or fp in batch_fps:
            dup += 1; continue
        if sh and f"sh:{sh}" in seen_set:
            dup += 1; continue

        # ⚡ 权重媒体 subtag 白名单: 数据库标记的官方及央媒资讯
        # 例外: 必须命中 COMPOUND_CHECK 监测关键词 (过滤掉腾讯公益等非金融科技业务)
        _subtag = (src.get("subtag") or "").strip()
        if _subtag in WEIGHTED_MEDIA_SUBTAGS:
            text_s = src.get("text") or ""
            combined_for_kw = f"{title_raw} {text_s}"
            hit_brand = any(c.lower() in combined_for_kw.lower() for c in COMPOUND_CHECK)
            if hit_brand:
                matched.append(build_item_v2(src))
                batch_fps.add(fp)
                continue
            # subtag 是央媒资讯但内容无关金融科技 → 走正常过滤流程 (大概率被排除词拦住)

        # ⚡ 竞品替代种草 强制放行 (用户指定 2026-07-07):
        #   「余额宝 + 零钱通 + 资金动作词(攒/存/放/转/收益…)」→ 竞品替代压制舆情, 无条件放行,
        #   跳过 subtag黑名单/URL黑名单/排除词/SPAM/UGC 等所有关卡。
        #   叠加资金动作词以排除纯政策科普(9·30新规等仅并列提及、无种草迁移语义的)。
        _text_seed = src.get("text") or ""
        _combined_seed = f"{title_raw} {_text_seed}"
        if (COMPETITOR_SEED_YEB in _combined_seed and COMPETITOR_SEED_LQT in _combined_seed
                and any(a in _combined_seed for a in COMPETITOR_SEED_ACTION)):
            item = build_item_v2(src)
            item["matched_kw"] = "竞品替代种草(余额宝vs零钱通)"
            matched.append(item)
            batch_fps.add(fp)
            continue

        # 🚫 产品知识/营销教程 subtag 黑名单: 零钱通开通入口/使用指南/提现攻略等运营内容
        # 例外1: 命中产品功能/活动关键词 (亲情账户/收益对比/境外支付等) → 放行
        # 例外2: 非社媒平台（今日头条/媒体网站）+ 命中新闻价值词 (内测/安全/盗刷等) → 放行
        text_s = src.get("text") or ""
        combined_for_kw = f"{title_raw} {text_s}"
        has_friendly_kw = any(fk in combined_for_kw for fk in PRODUCT_FRIENDLY_KW)
        cap_web_raw = (src.get("captureWebsite") or "")
        is_social_media = any(s in cap_web_raw.lower() for s in ["微信", "小红书", "微博", "微头条", "百家号", "搜狐号", "网易号"])
        has_media_kw = any(kw in combined_for_kw for kw in PRODUCT_KW_MEDIA_OVERRIDE)

        # ⚡ 官方号重大动态 快速放行通道 (优先于所有为噪音设计的关卡):
        #   官方账号作者(腾讯金融科技/腾讯自选股财经/微证券等) + 命中产品动态词(混元/大模型/接入/升级/发布)
        #   → 直接放行, 跳过 URL黑名单/排除词/自选股App规则等 (这些是拦机器播报/软文的, 不该套在官方公告上)
        #   防误放: ETF机器播报作者是"市场透视"(不在OFFICIAL_ACCOUNTS), 不会命中此通道。
        #   (修复: 官方号《腾讯自选股接入混元Hy3模型》被 subtag黑名单 + gu.qq.com URL黑名单
        #    + 排除词"配股"(误命中"适配股票") 三重误杀 → 全网副本几乎全漏)
        _author = (src.get("author") or "").strip()
        is_official = any(oa in _author for oa in OFFICIAL_ACCOUNTS)
        has_dynamic_kw = any(dk in combined_for_kw for dk in PRODUCT_DYNAMIC_KW)
        if is_official and has_dynamic_kw:
            item = build_item_v2(src)
            item["matched_kw"] = resolve_business_tag(title_raw, text_s, [c for c in COMPOUND_CHECK if c.lower() in combined_for_kw.lower()])
            matched.append(item)
            batch_fps.add(fp)
            continue

        if _subtag in PRODUCT_KNOWLEDGE_SUBTAGS and not has_friendly_kw:
            if is_social_media or not has_media_kw:
                blocked += 1; continue

        # URL 黑名单（先于站点名匹配，URL 是更稳定的拦截维度）
        url_s = src.get("url") or ""
        hit_url_kw = is_blocked_by_url(url_s, block_url)
        if hit_url_kw:
            blocked += 1; continue

        # 站点黑名单（sourceWebsite 字段匹配）
        source_web = src.get("sourceWebsite") or ""
        if source_web and any(bm in source_web for bm in block_media):
            blocked += 1; continue

        # 排除词
        text_s = src.get("text") or ""
        combined = f"{title_raw} {text_s}".lower()
        exc, _w = is_excluded(combined, excludes)
        if exc:
            excluded += 1; continue

        # 投诉模板骨架过滤: 只有"投诉编号/投诉对象/投诉问题/投诉要求"等字段标签、无实质叙述的空壳投诉
        # (如标题"退款"+正文全是字段词), 无舆情价值, 整条丢弃
        if is_complaint_template(f"{title_raw} {text_s}"):
            excluded += 1; continue

        # ⚡ 危机事件专项: 移动话费充值事件 (移动+微信/腾讯+话费/充值/合作/下线)
        # 事件主体不含腾讯系完整品牌词, 会被下方 COMPOUND_CHECK 漏掉, 故在此优先放行。
        # 已过站点黑名单/URL黑名单/排除词/模板骨架, 噪音已大幅过滤。
        ev_hit, ev_cat, ev_blame = match_mobile_event(title_raw, text_s)
        if ev_hit:
            item = build_item_v2(src)
            # 事件标记: 标签前置"📵移动话费事件", 负面甩锅强制标负面+@提醒
            cat_label = {"official": "我方口径/公告", "blame": "甩锅腾讯微信", "neutral": "事件相关"}.get(ev_cat, "事件相关")
            item["tag"] = f"📵移动话费充值事件·{cat_label}｜" + item["tag"].split("｜")[-1]
            item["matched_kw"] = f"移动话费充值事件/{cat_label}"
            item["_mobile_event"] = True  # 标记移动事件 → 推送时 @anderschen(陈恩达)
            if ev_blame:
                item["_senti"] = -1  # 甩锅腾讯/微信的失实负面 → 强制负面, 触发@提醒
            matched.append(item)
            batch_fps.add(fp)
            continue

        # 复合词校验: ES match 分词后太宽泛(如"微信支付"→"微信"+"支付"), 必须含完整词
        hit_brands = [c for c in COMPOUND_CHECK if c.lower() in combined]
        if not hit_brands:
            continue

        # 媒体报道专属品牌(微众银行/微粒贷/微保): 社媒UGC信息pass, 只留媒体报道
        cap_web = (src.get("captureWebsite") or "").lower()
        url_domain = (src.get("url") or "").lower()
        # 负面判定: 统一走 resolve_sentiment (负面优先, tag/emotion 综合)
        _sentiment, _senti_int = resolve_sentiment(src)
        is_negative = _senti_int == -1

        # 若命中了仅媒体品牌 → 社媒来源一律pass (不管是否同时命中其他品牌)
        media_hits = [b for b in hit_brands if b in MEDIA_ONLY_BRANDS]
        if media_hits:
            is_social = any(s in cap_web for s in SOCIAL_MEDIA_SITES)
            is_social = is_social or any(s in url_domain for s in SOCIAL_MEDIA_SITES)
            if is_social:
                excluded += 1; continue
            if any(p.lower() in combined for p in MEDIA_ONLY_PROMO):
                excluded += 1; continue

        # 个人被骗/受害叙事: UGC平台 + 中性 + 命中标志词 → 直接拦截
        # 不走SPAM_GUARD (SPAM_GUARD里的"被骗"会误保护这类纯个人故事)
        is_ugc_personal = any(u in cap_web for u in UGC_PERSONAL)
        if not is_negative and is_ugc_personal:
            if any(m in combined for m in PERSONAL_SCAM_MARKERS):
                excluded += 1; continue

        # 垃圾内容过滤: 教程/软文/广告特征词 + 非负面 → pass
        if not is_negative:
            is_spam = any(p in combined for p in SPAM_PATTERNS)
            if is_spam:
                # 防误杀: 含投诉举报关键词的不拦截
                is_complaint = any(g in combined for g in SPAM_GUARD)
                # 防误杀2: 核心理财产品词(理财通/零钱通…) + 收益异动/吐槽信号词 → 理财舆情, 放行
                #   仅 LICAI_CORE 太宽(会放进"零钱通实用功能/转入教程"等科普), 故须叠加信号词;
                #   (修复: "零钱通七日年化率太低我转去余额宝, 备用金放哪" 因"备用金"命中SPAM被误杀)
                has_licai_core = any(c in combined for c in LICAI_CORE_KW)
                has_licai_signal = any(s in combined for s in LICAI_SENTIMENT_SIGNALS)
                if not is_complaint and not (has_licai_core and has_licai_signal):
                    excluded += 1; continue

        # 懂车帝/汽车垂类: 非负面不推送
        if any(a in cap_web for a in AUTO_LOW_VALUE):
            if not is_negative:
                excluded += 1; continue

        # 法律咨询类平台: 非负面不推送
        if any(l in cap_web for l in LEGAL_LOW_VALUE):
            if not is_negative:
                excluded += 1; continue

        # 完全屏蔽平台 (BLOCKED_PLATFORMS 现为空, 保留扩展位)
        if BLOCKED_PLATFORMS and any(b in cap_web for b in BLOCKED_PLATFORMS):
            excluded += 1; continue
        # 消费保(xfb315): 仅命中核心腾讯系业务的投诉才放行, 其余个体消费纠纷屏蔽
        if "消费保" in cap_web or "xfb315" in url_domain:
            if not any(kw in combined for kw in XFB_WHITELIST_KW):
                excluded += 1; continue
        # 黑猫投诉平台: 仅腾讯理财通/腾安基金相关内容推送
        if "黑猫投诉" in cap_web or "tousu.sina.com.cn" in url_domain:
            has_heimaotou_kw = any(kw in combined for kw in HEIMAOTOU_WHITELIST_KW)
            if not has_heimaotou_kw:
                excluded += 1; continue

        # UGC个人平台 (微博/小红书/微头条/微信公众号/知乎/51kanong/贴吧 等):
        # 负面短文多是段子/玩笑/个人提问, 须含明确投诉关键词才算有效舆情
        # 例外1: 含产品体验友好词(我的理财/收益吐槽/用了理财通等) → 是真实产品体验吐槽, 放行
        # 例外2: 含核心理财产品词(理财通/零钱通/微信理财等) → 理财业务吐槽是核心舆情, 放行
        #   (修复: 之前负面分支只认硬投诉词, 导致理财通/零钱通赎回/收费/利息吐槽被误杀)
        if is_negative and any(u in cap_web for u in UGC_PERSONAL):
            is_valid = any(kw in combined for kw in VALID_COMPLAINT_KW)
            has_product_exp = any(fk in combined for fk in PRODUCT_FRIENDLY_KW)
            has_licai_core = any(c in combined for c in LICAI_CORE_KW)
            if not is_valid and not has_product_exp and not has_licai_core:
                excluded += 1; continue

        # UGC个人平台 + 短问句 (我.../怎么.../如何.../能不能...) → 个人提问, 非舆情
        # 解决: "我进行了微信支付投诉，钱能否退回" "微信支付账户被限制交易一年，该怎么解决"
        # 例外: 命中 VALID_COMPLAINT_KW (限额/限制/实名认证等产品功能问题) → 放行
        if any(u in cap_web for u in UGC_PERSONAL) and len(combined) < 300:
            has_friendly = any(fk in combined for fk in PRODUCT_FRIENDLY_KW)
            has_complaint_kw = any(kw in combined for kw in VALID_COMPLAINT_KW)
            has_licai_core = any(c in combined for c in LICAI_CORE_KW)
            if not has_friendly and not has_complaint_kw and not has_licai_core:
                is_personal_ask = bool(re.match(r"^(我|怎么|如何|能不能|可不可以用|求|请问|请教|想|该|微信支付|微信|#微信)", combined))
                if is_personal_ask and not is_negative:
                    excluded += 1; continue

        # 腾讯自选股 监测规则:
        # 1. 外部媒体(微信/搜狐/新浪/头条/媒体网站/客户端):
        #    a) 标题含"自选股" → 主体报道, 推送
        #    b) 标题不含"自选股"但文中含, 且涉及腾讯生态关键事件 → 关联报道, 推送
        #    c) 引流软文 → 拦截
        # 2. 腾讯自选股 App 平台:
        #    a) ETF/股票 行情播报 → 拦截
        #    b) 含其他监测关键词 (微信支付/微粒贷/微众/微保/零钱通/微信分付 等) → 推送
        if "腾讯自选股" in hit_brands:
            is_app = "自选股" in cap_web or "自选股" in url_domain
            if is_app:
                # App 行情播报 → 拦截
                if any(p in combined for p in APP_MARKET_KW):
                    excluded += 1; continue
                # App 内: 必须含其他监测关键词才推送
                other_brand = [b for b in hit_brands if b != "腾讯自选股"]
                if not other_brand:
                    excluded += 1; continue
                # 含其他金融关键词 → 继续走完整过滤链
            else:
                # 外部媒体引流/广告拦截 (优先于其他规则)
                if any(p in combined for p in PROMO_KW):
                    excluded += 1; continue
                # 外部媒体: 标题含"自选股" → 主体报道
                # 标题不含"自选股"但文中含, 且涉及腾讯生态关键事件 → 关联报道
                if "自选股" not in title_raw:
                    is_eco_event = any(e in combined for e in TENCENT_ECO_EVENTS)
                    if not is_eco_event:
                        excluded += 1; continue

        # 通过 → 构建推送项
        item = build_item_v2(src)
        # 覆盖 matched_kw: 裁定文章真正业务主体(防"通篇微信支付、文末提一句零钱通→误标零钱通")
        item["matched_kw"] = resolve_business_tag(title_raw, text_s, hit_brands)
        matched.append(item)
        batch_fps.add(fp)

    log(f"过滤结果: 命中{len(matched)} | 排除{excluded} | 站点屏蔽{blocked} | 重复{dup}")

    # 3. 推送
    ok, pushed_ids = push_items(matched)

    # 4. 更新去重状态 (含扫描点, 用于增量窗口)
    pushed_set = set(pushed_ids)
    new_ids = [f"cid:{c}" for c in pushed_ids]
    pushed_fps = [fp for item, fp in zip(matched, batch_fps)
                  if item["_contentId"] in pushed_set]
    new_ids += pushed_fps
    # 标记本次扫描时间(用于增量窗口), 清理旧扫描点只保留最新
    seen_list = [x for x in seen_list if not x.startswith("scan:")]
    seen_list.append(f"scan:{now.isoformat()}")
    if new_ids:
        seen_list.extend(new_ids)
    save_seen(seen_list)
    return len(matched)


def main():
    daemon = "--daemon" in sys.argv or "-d" in sys.argv
    watchdog = "--watchdog" in sys.argv or "-w" in sys.argv
    if watchdog:
        daemon = True  # watchdog 模式自带 daemon

    # 按 API 文档: 不再需要本地布尔表达式 (API text 参数做 ES 搜索)
    # 保留 asts 参数占位兼容, 但实际不再使用
    asts = []  # 不再需要
    excludes = load_exclude_words()
    block_media = load_block_media()
    block_url = load_block_url()
    log(f"排除词: {len(excludes)} | 站点黑名单: {len(block_media)} | URL黑名单: {len(block_url)} | 模式: {'看门狗常驻' if watchdog else ('常驻轮询' if daemon else '单次')}")
    log(f"API text 搜索词: {MONITOR_TEXT}")

    if not daemon:
        run_once(asts, excludes, block_media, block_url)
        log("完成.")
        return

    def daemon_loop():
        """单次 daemon 循环: 持续轮询直到收到停止信号或异常退出"""
        # 信号处理: SIGTERM → 优雅退出
        sig_received = [False]
        import signal as _signal
        def _handle(_sig, _frame):
            sig_received[0] = True
        _signal.signal(_signal.SIGTERM, _handle)

        log(f"进入常驻模式, 每 {POLL_INTERVAL}s 一轮, 首轮回扫 {WINDOW_MINUTES} 分钟, 后续增量窗口 {LOOKBACK_MINUTES} 分钟")
        log(f"停止方法: touch {STOP_FILE}")
        consecutive_api_fail = 0   # 连续 API 不可达计数, 用于自愈
        while True:
            if os.path.exists(STOP_FILE) or sig_received[0]:
                log(f"检测到停止信号, 优雅退出")
                try: os.remove(STOP_FILE)
                except Exception: pass
                return
            t0 = time.time()
            try:
                l_excludes = load_exclude_words()
                l_block_media = load_block_media()
                l_block_url = load_block_url()
                ret = run_once([], l_excludes, l_block_media, l_block_url)
                # 自愈: 连续 API 不可达 → 退出 daemon_loop, 由 watchdog 拉起全新进程(刷新网络连接)
                if ret == -1:
                    consecutive_api_fail += 1
                    if consecutive_api_fail >= API_FAIL_RESTART:
                        log(f"!! 连续 {consecutive_api_fail} 次 API 不可达, 重启进程刷新连接")
                        raise RuntimeError("api_unreachable_self_heal")
                else:
                    consecutive_api_fail = 0  # 成功一次即清零
            except RuntimeError:
                raise  # 自愈信号, 抛给 watchdog
            except Exception as e:
                log(f"!! 本轮异常: {type(e).__name__} {e}")
            elapsed = time.time() - t0
            sleep_s = max(5, POLL_INTERVAL - elapsed)
            log(f"本轮耗时 {elapsed:.0f}s, 休眠 {sleep_s:.0f}s\n")
            # 分段睡眠, 每 5s 检查一次停止信号
            slept = 0
            while slept < sleep_s:
                chunk = min(5, sleep_s - slept)
                time.sleep(chunk)
                slept += chunk
                if os.path.exists(STOP_FILE) or sig_received[0]:
                    log(f"休眠中收到停止信号, 退出")
                    try: os.remove(STOP_FILE)
                    except Exception: pass
                    return

    # 看门狗主循环: daemon_loop 异常退出 → os.execv 真重启进程(刷新网络连接)
    if not watchdog:
        daemon_loop()
        return

    restart_count = 0
    while True:
        try:
            daemon_loop()
        except RuntimeError as e:
            if "api_unreachable_self_heal" in str(e):
                log(f"!! 看门狗: API 不可达自愈, 5s 后用 os.execv 真重启进程")
                time.sleep(5)
                os.execv(sys.executable, [sys.executable] + sys.argv)
        except Exception as e:
            restart_count += 1
            log(f"!! 看门狗: daemon 退出/异常 (#{restart_count}): {type(e).__name__} {e}")
            if restart_count > 10:
                log(f"!! 看门狗: 重启过于频繁 (10次+), 暂停 5 分钟")
                time.sleep(300)
                restart_count = 0
        # daemon_loop 正常返回 (STOP_FILE 或 SIGTERM)
        if os.path.exists(STOP_FILE):
            log("看门狗: 检测到停止信号, 退出")
            break
        log(f"看门狗: 5s 后自动重启 daemon...")
        time.sleep(5)


if __name__ == "__main__":
    main()
