#!/usr/bin/env python3
"""Golaxy 腾讯金融舆情监测 — GitHub Actions 适配器(瘦封装)
================================================================
本文件不再自带任何过滤逻辑, 只做三件事:
  1. 从环境变量(GitHub Secrets)注入 API Key / Webhook;
  2. 复用与本地完全一致的规则引擎 golaxy_yuqing_monitor.py(同目录、逐字镜像);
  3. 单次运行(run_once), 状态存仓库内 .golaxy_yuqing_seen.json(由 Actions cache 持久化)。

→ 规则、过滤管道、推送格式 全部 = 本地生产脚本, 永不再漂移。
   以后本地脚本/词库有更新, 把 golaxy_yuqing_monitor.py + 4个词库 同步进本仓库即可。
"""
import os, sys

# 必须先有 secrets 才能跑(模块在 import 时即读取这两个环境变量)
if not os.environ.get("GOLAXY_API_KEY") or not os.environ.get("WECOM_WEBHOOK"):
    print("[FATAL] 缺少 GOLAXY_API_KEY / WECOM_WEBHOOK 环境变量(请在 GitHub Secrets 配置)")
    sys.exit(1)

BASE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE)

import golaxy_yuqing_monitor as gm  # noqa: E402  规则引擎(本地镜像)

# 状态文件落在仓库目录(workflow 用 actions/cache 持久化, 文件名须与 workflow 的 cache path 一致)
gm.STATE_FILE = os.path.join(BASE, ".golaxy_seen.json")

# 关闭进程级自愈/重启(Actions 是单次执行, 不需要 watchdog)
gm.STOP_FILE = os.path.join(BASE, ".golaxy_yuqing_stop")


def run_single():
    excludes    = gm.load_exclude_words()
    block_media = gm.load_block_media()
    block_url   = gm.load_block_url()
    gm.log(f"[Actions] 排除词:{len(excludes)} | 站点黑名单:{len(block_media)} | "
           f"URL黑名单:{len(block_url)} | 单次运行")
    gm.run_once([], excludes, block_media, block_url)
    gm.log("[Actions] 完成.")


if __name__ == "__main__":
    run_single()
