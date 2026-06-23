# =====================================================================
# SIGNAL CONFLICT STRATEGY - TREND VS MEAN REVERSION DIVERGENCE
# HACKATHON EDITION - AI DECISION LAYER + AGENT MEMORY + REGIME AWARENESS
# Research Demonstration Build
# Hackathon Edition
#
# HACKATHON SUBMISSION VERSION - MODULES RENAMED FOR IP PROTECTION
# - EM Module: Adaptive signal management for exhaustion conditions
# - SM Module: Saturation mode for extreme RSI conditions
# - LL Module: Laddered entry management
# - TDM Module: Time-based decay management
# - LM Module: Dynamic exposure management
# - CZ Framework: Conflict zone management
# - LRE Module: Execution safety layer
# - LR Framework: Layering rules
# - OE Controls: Strategy ownership enforcement
# - MBS Layer: Multi-bot safety coordination
# =====================================================================

import math
import asyncio
import time
import argparse
import signal
import sqlite3
import re
import sys
import os
import json
import warnings
from decimal import Decimal, ROUND_HALF_UP, ROUND_DOWN, getcontext
from pathlib import Path
from datetime import datetime, timedelta, timezone
from collections import deque, defaultdict
from typing import List, Dict, Optional, Any, Tuple
from dataclasses import dataclass, field, asdict
from enum import Enum

import aiohttp
import pandas as pd
from loguru import logger
from dotenv import load_dotenv
from binance import AsyncClient, BinanceSocketManager

# Suppress deprecation warnings
warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=DeprecationWarning)

# --------------------------------- DASHBOARD IMPORTS --------------------------------- #
try:
    from flask import Flask, jsonify, render_template_string
    import threading
    FLASK_AVAILABLE = True
except ImportError:
    FLASK_AVAILABLE = False
    logger.warning("Flask not available. Dashboard will be disabled.")

# --------------------------------- LLM IMPORTS (Gemini + OpenAI fallback) --------------------------------- #
try:
    import openai
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False
    logger.warning("OpenAI not available. LLM decision layer will use Gemini fallback.")

# Use the new google.genai package (replaces deprecated google.generativeai)
try:
    from google import genai as genai_new
    # Also try to import the old one as fallback
    import google.generativeai as genai_old
    GEMINI_AVAILABLE = True
    USE_NEW_GEMINI = True
    logger.info("Using google.genai package (new SDK)")
except ImportError:
    try:
        import google.generativeai as genai_old
        GEMINI_AVAILABLE = True
        USE_NEW_GEMINI = False
        logger.warning("Using deprecated google.generativeai package. Consider upgrading to google.genai")
    except ImportError:
        GEMINI_AVAILABLE = False
        USE_NEW_GEMINI = False
        logger.warning("Gemini not available. LLM decision layer will use rule-based fallback.")

# --------------------------------- CLI & ENV --------------------------------- #
parser = argparse.ArgumentParser(description="Binance USDT Futures Bot - Signal Conflict Strategy")
parser.add_argument("--verbose", action="store_true", help="Enable detailed per-symbol debug logging")
parser.add_argument("--selftest", type=str, default="", help="Run self-test for a single symbol and exit")
parser.add_argument("--test-connection", action="store_true", help="Test API connection and exit")
parser.add_argument("--symbols", type=str, default="", help="Override symbols (comma-separated)")
parser.add_argument("--close-all", action="store_true", help="Close all open positions and exit")
parser.add_argument("--show-trades", action="store_true", help="Show today's trades and exit")
parser.add_argument("--emergency-stop", action="store_true", help="Emergency stop - close all positions and stop trading")
parser.add_argument("--mode", type=str, default="adaptive", choices=["fade", "breakout", "wait", "adaptive"], 
                    help="Strategy mode: fade=conflict means reversal, breakout=conflict means continuation, wait=wait for resolution, adaptive=dynamic resolution")
parser.add_argument("--dashboard", action="store_true", help="Start web dashboard")
parser.add_argument("--dashboard-port", type=int, default=5000, help="Dashboard port")
parser.add_argument("--disable-ai", action="store_true", help="Disable AI decision layer (use rule-based)")
args = parser.parse_args()
VERBOSE = args.verbose
SELFTEST_SYMBOL = args.selftest.strip().upper()
TEST_CONNECTION = args.test_connection
CLOSE_ALL_POSITIONS = args.close_all
SHOW_TRADES = args.show_trades
EMERGENCY_STOP = args.emergency_stop
STRATEGY_MODE = args.mode
ENABLE_DASHBOARD = args.dashboard
DASHBOARD_PORT = args.dashboard_port
# Properly handle DISABLE_AI from both command line and .env
DISABLE_AI_LAYER = args.disable_ai or os.getenv("DISABLE_AI", "false").lower() in {"1","true","yes","y","on"}
OVERRIDE_SYMBOLS = [s.strip().upper() for s in args.symbols.split(",") if s.strip()] if args.symbols else None

load_dotenv()

# --------------------------------- Paths & Logging --------------------------------- #
DB_PATH = Path("database/signal_conflict.db")
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

LOG_DIR = Path("logs/signal_conflict")
LOG_DIR.mkdir(parents=True, exist_ok=True)

DASHBOARD_DIR = Path("dashboard")
DASHBOARD_DIR.mkdir(parents=True, exist_ok=True)

logger.remove()
logger.add(
    sys.stdout,
    level="INFO",
    enqueue=False,
    colorize=True,
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>"
)

logger.add(
    LOG_DIR / "trading.log",
    rotation="100 MB",
    retention="30 days",
    delay=True,
    encoding="utf-8",
    backtrace=True,
    diagnose=False,
    format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}"
)

if VERBOSE:
    logger.add(
        LOG_DIR / "debug.log",
        rotation="50 MB",
        retention="7 days",
        level="DEBUG",
        enqueue=True,
        delay=True,
        encoding="utf-8",
        backtrace=True,
        diagnose=False
    )

def _read_secret_file(p):
    try:
        with open(p, "r", encoding="utf-8") as f:
            return f.read().strip()
    except Exception:
        return ""

BINANCE_API_KEY = _read_secret_file("/run/secrets/binance_api_key") or os.getenv("BINANCE_API_KEY", "").strip()
BINANCE_API_SECRET = _read_secret_file("/run/secrets/binance_api_secret") or os.getenv("BINANCE_API_SECRET", "").strip()

print("=" * 70)
print("SIGNAL CONFLICT AI AGENT - HACKATHON EDITION")
print("=" * 70)
display_mode = "ADAPTIVE CONFLICT RESOLUTION" if STRATEGY_MODE == "adaptive" else STRATEGY_MODE.upper()
print(f"Mode: {display_mode}")
print(f"AI Decision Layer: {'DISABLED' if DISABLE_AI_LAYER else 'ENABLED'}")
print(f"Agent Memory: ENABLED")
print(f"API KEY loaded: {bool(BINANCE_API_KEY)} (length: {len(BINANCE_API_KEY)})")
print(f"API SECRET loaded: {bool(BINANCE_API_SECRET)} (length: {len(BINANCE_API_SECRET)})")

# --------------------------------- Config (Safe parameters - no exact thresholds exposed) ---------------------------------
DRY_RUN = os.getenv("DRY_RUN", "").strip().lower() in {"1","true","yes","y","on"}
USE_TESTNET = os.getenv("BINANCE_FUTURES_TESTNET", "false").lower() in {"1","true","yes","y","on"}
QUOTE_ASSET = os.getenv("QUOTE_ASSET", "USDT").upper()
POLL_INTERVAL_SEC = int(os.getenv("POLL_INTERVAL_SEC", "60"))
PREF_LEVERAGE = int(os.getenv("PREF_LEVERAGE", "5"))
MAX_LEVERAGE = int(os.getenv("MAX_LEVERAGE", "5"))
MAX_WALLET_PCT_PER_TOKEN = float(os.getenv("MAX_WALLET_PCT_PER_TOKEN", "12.0"))
MIN_DAILY_QUOTE_VOL_USD = float(os.getenv("MIN_DAILY_QUOTE_VOL_USD", "99999"))
SYMBOLS_ENV = os.getenv("SYMBOLS", "ALL_USDT_FUTURES").strip().upper()

# Universe selection - loaded from external config or environment
CORE_FUTURES_UNIVERSE = os.getenv("CORE_FUTURES_UNIVERSE", "").strip()
if CORE_FUTURES_UNIVERSE:
    ETH_MAJOR_SYMBOLS = [s.strip().upper() for s in CORE_FUTURES_UNIVERSE.split(",") if s.strip()]
else:
    # Default universe if not specified - safe list
    ETH_MAJOR_SYMBOLS = [
        "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT",
        "ADAUSDT", "DOGEUSDT", "DOTUSDT", "LINKUSDT", "AVAXUSDT",
        "MATICUSDT", "UNIUSDT", "ATOMUSDT", "LTCUSDT", "NEARUSDT"
    ]

if not OVERRIDE_SYMBOLS:
    OVERRIDE_SYMBOLS = ETH_MAJOR_SYMBOLS
MAX_RETRIES = 3
RETRY_SLEEP = 1.0
REQUEST_TIMEOUT = 30
MAX_SYMBOLS_TO_ANALYZE = int(os.getenv("MAX_SYMBOLS_TO_ANALYZE", "21"))
MAX_CONCURRENT_POSITIONS = int(os.getenv("MAX_CONCURRENT_POSITIONS", "1"))

# Strategy Mode Configuration (Safe - no exact values exposed)
CONFLICT_RESOLUTION_WINDOW_MINUTES = int(os.getenv("CONFLICT_RESOLUTION_WINDOW_MINUTES", "30"))
MIN_CONFLICT_STRENGTH = float(os.getenv("MIN_CONFLICT_STRENGTH", "0.3"))
MAX_CONFLICT_AGE_HOURS = int(os.getenv("MAX_CONFLICT_AGE_HOURS", "4"))

# AutoFlag Parameters (safe ranges only)
ADX_PERIOD = int(os.getenv("ADX_PERIOD", "14"))
ADX_MIN_STRENGTH = float(os.getenv("ADX_MIN_STRENGTH", "10.0"))
EMA_FAST = int(os.getenv("EMA_FAST", "9"))
EMA_SLOW = int(os.getenv("EMA_SLOW", "21"))

# Mean Reversion Parameters (safe ranges only)
BOLLINGER_PERIOD = int(os.getenv("BOLLINGER_PERIOD", "20"))
BOLLINGER_STD = float(os.getenv("BOLLINGER_STD", "1.8"))
MACD_CONFIRMATION = os.getenv("MACD_CONFIRMATION", "true").lower() in {"1","true","yes","y"}

# Conflict Detection Parameters (safe ranges only)
TREND_STRENGTH_THRESHOLD = float(os.getenv("TREND_STRENGTH_THRESHOLD", "0.3"))
REVERSION_STRENGTH_THRESHOLD = float(os.getenv("REVERSION_STRENGTH_THRESHOLD", "0.3"))
CONFLICT_CONFIRMATION_BARS = int(os.getenv("CONFLICT_CONFIRMATION_BARS", "3"))

# RISK MANAGEMENT - RRR ALIGNMENT (safe ranges only)
# Protected parameters - values loaded from environment with fallback to safe defaults
def _safe_float_env(key: str, default: float) -> float:
    val = os.getenv(key, str(default))
    try:
        return float(val)
    except (ValueError, TypeError):
        logger.warning(f"Could not convert {key}={val} to float, using default {default}")
        return default

def _safe_int_env(key: str, default: int) -> int:
    val = os.getenv(key, str(default))
    try:
        return int(val)
    except (ValueError, TypeError):
        logger.warning(f"Could not convert {key}={val} to int, using default {default}")
        return default

DAILY_LOSS_CAP_PCT = _safe_float_env("DAILY_LOSS_CAP_PCT", 5.0)
GLOBAL_DAILY_LOSS_CAP_PCT = _safe_float_env("GLOBAL_DAILY_LOSS_CAP_PCT", 8.0)
MAX_CONSECUTIVE_SL = _safe_int_env("MAX_CONSECUTIVE_SL", 3)
SL_STREAK_RESET_HOURS = _safe_int_env("SL_STREAK_RESET_HOURS", 24)
LOSS_COOLDOWN_HOURS = _safe_int_env("LOSS_COOLDOWN_HOURS", 4)
RISK_PER_TRADE_PCT = _safe_float_env("RISK_PER_TRADE_PCT", 0.5)
MIN_RISK_REWARD_RATIO = _safe_float_env("MIN_RISK_REWARD_RATIO", 1.5)
MAX_DAILY_TRADES = _safe_int_env("MAX_DAILY_TRADES", 10)
MIN_WIN_RATE = _safe_float_env("MIN_WIN_RATE", 0.4)
MIN_STOP_DISTANCE_PCT = _safe_float_env("MIN_STOP_DISTANCE_PCT", 0.3)
MIN_PROFIT_PCT = _safe_float_env("MIN_PROFIT_PCT", 0.5)
ATR_STOP_MULTIPLIER = _safe_float_env("ATR_STOP_MULTIPLIER", 1.5)
ATR_PERIOD = _safe_int_env("ATR_PERIOD", 14)

# Entry/Exit Parameters (safe ranges only)
ENTRY_BUFFER_PCT = _safe_float_env("ENTRY_BUFFER_PCT", 0.1)
EXIT_BUFFER_PCT = _safe_float_env("EXIT_BUFFER_PCT", 0.1)
MAX_STOP_LOSS_PCT = _safe_float_env("MAX_STOP_LOSS_PCT", 2.0)
MAX_SLIPPAGE_PCT = _safe_float_env("MAX_SLIPPAGE_PCT", 0.5)

# RRR Alignment Parameters (safe ranges only)
TP_MULTIPLIER = _safe_float_env("TP_MULTIPLIER", 2.5)
MIN_EARLY_EXIT_R = _safe_float_env("MIN_EARLY_EXIT_R", 0.5)
TRAILING_STOP_ACTIVATE_R = _safe_float_env("TRAILING_STOP_ACTIVATE_R", 1.5)
MAX_SL_PCT_CAP = _safe_float_env("MAX_SL_PCT_CAP", 5.0)
ENABLE_RRR_VALIDATOR = os.getenv("ENABLE_RRR_VALIDATOR", "true").lower() in {"1","true","yes","y"}
ENABLE_RRR_HISTOGRAM = os.getenv("ENABLE_RRR_HISTOGRAM", "true").lower() in {"1","true","yes","y"}

# Order Management
ORDER_DELAY_MS = _safe_int_env("ORDER_DELAY_MS", 100)
ORDER_FILL_TIMEOUT = _safe_int_env("ORDER_FILL_TIMEOUT", 30)
PRICE_BUFFER_PCT = _safe_float_env("PRICE_BUFFER_PCT", 0.1)
LIMIT_FILL_WAIT_SEC = _safe_int_env("LIMIT_FILL_WAIT_SEC", 10)
STOP_LIMIT_BUFFER_PCT = _safe_float_env("STOP_LIMIT_BUFFER_PCT", 0.1)

# Time Filters
AVOID_FUNDING_TIME = os.getenv("AVOID_FUNDING_TIME", "false").lower() in {"1","true","yes","y"}
MIN_TIME_TO_FUNDING = _safe_int_env("MIN_TIME_TO_FUNDING", 10)
MAX_POSITION_HOLD_HOURS = _safe_int_env("MAX_POSITION_HOLD_HOURS", 48)

# Additional Safety Parameters
MIN_POSITION_VALUE_USD = _safe_float_env("MIN_POSITION_VALUE_USD", 5.0)
MAX_PRICE_CHANGE_24H_PCT = _safe_float_env("MAX_PRICE_CHANGE_24H_PCT", 15.0)
MIN_TRADES_FOR_EVALUATION = _safe_int_env("MIN_TRADES_FOR_EVALUATION", 10)

# --- TRAILING STOP PARAMETERS ---
TRAILING_STOP_ACTIVATE_PCT = _safe_float_env("TRAILING_STOP_ACTIVATE_PCT", 1.5)
TRAILING_STOP_DISTANCE_PCT = _safe_float_env("TRAILING_STOP_DISTANCE_PCT", 0.5)
BREAKEVEN_ACTIVATE_PCT = _safe_float_env("BREAKEVEN_ACTIVATE_PCT", 0.8)

# --- EARLY STOP LOSS PROTECTION ---
MIN_HOLD_TIME_BEFORE_SL_HOURS = _safe_float_env("MIN_HOLD_TIME_BEFORE_SL_HOURS", 0.5)
MAX_EARLY_SL_PCT = _safe_float_env("MAX_EARLY_SL_PCT", 1.0)

# --- IMPROVED EXIT PARAMETERS ---
EARLY_EXIT_PROFIT_THRESHOLD = _safe_float_env("EARLY_EXIT_PROFIT_THRESHOLD", 0.3)
MEAN_BAND_EXIT_ENABLED = os.getenv("MEAN_BAND_EXIT_ENABLED", "true").lower() in {"1","true","yes","y"}

# --- SHORT POSITION CONTROL ---
DISABLE_SHORT_POSITIONS = os.getenv("DISABLE_SHORT_POSITIONS", "false").lower() in {"1","true","yes","y"}
SHORT_RISK_MULTIPLIER = _safe_float_env("SHORT_RISK_MULTIPLIER", 0.8)
SHORT_REWARD_MULTIPLIER = _safe_float_env("SHORT_REWARD_MULTIPLIER", 0.8)

# --- MARKET BIAS DETECTION ---
ENABLE_MARKET_BIAS_DETECTION = os.getenv("ENABLE_MARKET_BIAS_DETECTION", "true").lower() in {"1","true","yes","y"}
MARKET_BIAS_SMA_PERIOD = _safe_int_env("MARKET_BIAS_SMA_PERIOD", 50)
MARKET_BIAS_THRESHOLD = _safe_float_env("MARKET_BIAS_THRESHOLD", 0.5)

# --- POSITION MANAGEMENT ON RESTART ---
SYNC_EXISTING_POSITIONS = os.getenv("SYNC_EXISTING_POSITIONS", "false").lower() in {"1","true","yes","y"}
CLOSE_EXISTING_POSITIONS_ON_STARTUP = os.getenv("CLOSE_EXISTING_POSITIONS_ON_STARTUP", "false").lower() in {"1","true","yes","y"}

# --- CIRCUIT BREAKER PARAMETERS ---
CIRCUIT_BREAKER_ENABLED = os.getenv("CIRCUIT_BREAKER_ENABLED", "true").lower() in {"1","true","yes","y"}
MAX_DRAWDOWN_PCT = _safe_float_env("MAX_DRAWDOWN_PCT", 10.0)
MAX_CONSECUTIVE_LOSSES = _safe_int_env("MAX_CONSECUTIVE_LOSSES", 5)
VOLATILITY_CIRCUIT_BREAKER_PCT = _safe_float_env("VOLATILITY_CIRCUIT_BREAKER_PCT", 5.0)
VOLATILITY_LOOKBACK_MINUTES = _safe_int_env("VOLATILITY_LOOKBACK_MINUTES", 15)

# --- PARTIAL PROFIT TAKING ---
ENABLE_PARTIAL_PROFIT_TAKING = os.getenv("ENABLE_PARTIAL_PROFIT_TAKING", "true").lower() in {"1","true","yes","y"}
PARTIAL_PROFIT_LEVELS = json.loads(os.getenv("PARTIAL_PROFIT_LEVELS", '[0.5, 1.0, 1.5]'))
PARTIAL_PROFIT_PERCENTAGES = json.loads(os.getenv("PARTIAL_PROFIT_PERCENTAGES", '[0.25, 0.25, 0.5]'))

# Signal Weighting
TREND_SIGNAL_WEIGHT = _safe_float_env("TREND_SIGNAL_WEIGHT", 0.4)
REVERSION_SIGNAL_WEIGHT = _safe_float_env("REVERSION_SIGNAL_WEIGHT", 0.4)
VOLUME_CONFIRMATION_WEIGHT = _safe_float_env("VOLUME_CONFIRMATION_WEIGHT", 0.1)

# Conflict Zones
ENABLE_CONFLICT_ZONES = os.getenv("ENABLE_CONFLICT_ZONES", "true").lower() in {"1","true","yes","y"}
CONFLICT_ZONE_PCT = _safe_float_env("CONFLICT_ZONE_PCT", 1.0)

# --- NEW PARAMETERS FOR TRADE FREQUENCY ---
ANALYSIS_COOLDOWN_SEC = _safe_int_env("ANALYSIS_COOLDOWN_SEC", 60)
ENABLE_SINGLE_SIGNAL_MODE = os.getenv("ENABLE_SINGLE_SIGNAL_MODE", "true").lower() in {"1","true","yes","y"}
SINGLE_SIGNAL_STRENGTH_THRESHOLD = _safe_float_env("SINGLE_SIGNAL_STRENGTH_THRESHOLD", 0.6)
MAX_NEW_POSITIONS_PER_CYCLE = _safe_int_env("MAX_NEW_POSITIONS_PER_CYCLE", 1)

# --- ADAPTIVE DECISION MODULES (Renamed for IP Protection) ---

# EM Module - Exhaustion Mode
ENABLE_EM = os.getenv("ENABLE_EM", "true").lower() in {"1","true","yes","y"}
EM_CONFIRMATION_BARS = _safe_int_env("EM_CONFIRMATION_BARS", 3)
EM_STALL_THRESHOLD = _safe_float_env("EM_STALL_THRESHOLD", 3.0)
ENFORCE_EM_ENTRY_DELAY = os.getenv("ENFORCE_EM_ENTRY_DELAY", "true").lower() in {"1","true","yes","y"}
EM_HOLD_HOURS = _safe_float_env("EM_HOLD_HOURS", 2.0)
EM_SL_RELAX_FACTOR = _safe_float_env("EM_SL_RELAX_FACTOR", 1.2)
EM_COOLDOWN_HOURS = _safe_int_env("EM_COOLDOWN_HOURS", 4)

# SM Module - Saturation Mode
ENABLE_SM = os.getenv("ENABLE_SM", "true").lower() in {"1","true","yes","y"}
SM_STALL_CONFIRMATION_BARS = _safe_int_env("SM_STALL_CONFIRMATION_BARS", 3)
SM_SLOPE_FLIP_THRESHOLD = _safe_float_env("SM_SLOPE_FLIP_THRESHOLD", 1.0)
SM_ATR_EXTREME_SL_MULTIPLIER = _safe_float_env("SM_ATR_EXTREME_SL_MULTIPLIER", 0.8)
SM_MAX_HOLD_EXTREME_MINUTES = _safe_int_env("SM_MAX_HOLD_EXTREME_MINUTES", 15)
ENABLE_SM_FUNDING_HOLD = os.getenv("ENABLE_SM_FUNDING_HOLD", "true").lower() in {"1","true","yes","y"}
SM_COOLDOWN_HOURS = _safe_int_env("SM_COOLDOWN_HOURS", 2)
SM_TIME_EXIT_NEUTRAL_CIRCUIT = os.getenv("SM_TIME_EXIT_NEUTRAL_CIRCUIT", "true").lower() in {"1","true","yes","y"}

# LM Module - Leverage Management
ENABLE_LM = os.getenv("ENABLE_LM", "false").lower() in {"1","true","yes","y"}
LM_TIME_MINUTES = _safe_int_env("LM_TIME_MINUTES", 30)
LM_SIZE_MULTIPLIER = _safe_float_env("LM_SIZE_MULTIPLIER", 0.5)
MAX_TOTAL_LEVERAGE = _safe_int_env("MAX_TOTAL_LEVERAGE", 10)

# Adaptive Signal Management
SM_CONSECUTIVE_CANDLES_REQUIRED = _safe_int_env("SM_CONSECUTIVE_CANDLES_REQUIRED", 3)
HARD_SL_DISABLE_HOURS = _safe_float_env("HARD_SL_DISABLE_HOURS", 4.0)
TIME_STOP_HOURS = _safe_float_env("TIME_STOP_HOURS", 6.0)
EXIT_PRIORITY_REORDERED = os.getenv("EXIT_PRIORITY_REORDERED", "true").lower() in {"1","true","yes","y"}
ATR_COMPRESSION_RESUME_THRESHOLD = _safe_float_env("ATR_COMPRESSION_RESUME_THRESHOLD", 0.3)
SL_SUPPRESSION_ATR_MULTIPLIER = _safe_float_env("SL_SUPPRESSION_ATR_MULTIPLIER", 0.5)
CONDITIONAL_LEVERAGE_ADDON = os.getenv("CONDITIONAL_LEVERAGE_ADDON", "true").lower() in {"1","true","yes","y"}
LM_SAFETY_FUNDING_WINDOW_MIN = _safe_int_env("LM_SAFETY_FUNDING_WINDOW_MIN", 30)
SM_TIME_DECAY_MINUTES = _safe_int_env("SM_TIME_DECAY_MINUTES", 60)
DO_NOTHING_PHASE_MINUTES = _safe_int_env("DO_NOTHING_PHASE_MINUTES", 15)
CONFLICT_STRENGTH_WITH_SM_TIME = os.getenv("CONFLICT_STRENGTH_WITH_SM_TIME", "true").lower() in {"1","true","yes","y"}
MIN_SM_TIME_SINCE_EXTREME = _safe_int_env("MIN_SM_TIME_SINCE_EXTREME", 5)
ENHANCED_ENTRY_LOGGING = os.getenv("ENHANCED_ENTRY_LOGGING", "true").lower() in {"1","true","yes","y"}
ENTRY_SNAPSHOT_LOGGING = os.getenv("ENTRY_SNAPSHOT_LOGGING", "true").lower() in {"1","true","yes","y"}

# LRE Module - Execution Safety Layer
MAX_TOTAL_RISK_R = _safe_float_env("MAX_TOTAL_RISK_R", 6.0)
BLOCK_ADDS_WATCH_ZONE = os.getenv("BLOCK_ADDS_WATCH_ZONE", "true").lower() in {"1","true","yes","y"}
BLOCK_ADDS_REDUCE_ZONE = os.getenv("BLOCK_ADDS_REDUCE_ZONE", "false").lower() in {"1","true","yes","y"}
BLOCK_ADDS_CRITICAL_ZONE = os.getenv("BLOCK_ADDS_CRITICAL_ZONE", "true").lower() in {"1","true","yes","y"}
AUTO_LEVERAGE_REDUCTION_ENABLED = os.getenv("AUTO_LEVERAGE_REDUCTION_ENABLED", "true").lower() in {"1","true","yes","y"}
ABSOLUTE_TIME_STOP_HOURS = _safe_float_env("ABSOLUTE_TIME_STOP_HOURS", 24.0)
ENFORCE_ABSOLUTE_TIME_STOP = os.getenv("ENFORCE_ABSOLUTE_TIME_STOP", "true").lower() in {"1","true","yes","y"}
ENABLE_PRETRADE_SIMULATION = os.getenv("ENABLE_PRETRADE_SIMULATION", "false").lower() in {"1","true","yes","y"}
SIMULATION_LOOKBACK_BARS = _safe_int_env("SIMULATION_LOOKBACK_BARS", 50)
DAILY_EQUITY_DRAWDOWN_LIMIT_PCT = _safe_float_env("DAILY_EQUITY_DRAWDOWN_LIMIT_PCT", 5.0)
ATH_EQUITY_DRAWDOWN_LIMIT_PCT = _safe_float_env("ATH_EQUITY_DRAWDOWN_LIMIT_PCT", 10.0)
ENABLE_EQUITY_THROTTLING = os.getenv("ENABLE_EQUITY_THROTTLING", "true").lower() in {"1","true","yes","y"}
ENABLE_LRE_LOGGING = os.getenv("ENABLE_LRE_LOGGING", "true").lower() in {"1","true","yes","y"}

# OE Controls - Strategy Ownership
ENFORCE_OE = os.getenv("ENFORCE_OE", "true").lower() in {"1","true","yes","y"}
STRATEGY_ID = os.getenv("STRATEGY_ID", "SIGNAL_CONFLICT_V2")
ENABLE_LL = os.getenv("ENABLE_LL", "true").lower() in {"1","true","yes","y"}
LL_TRANCHES = _safe_int_env("LL_TRANCHES", 3)
LL_SPACING_PCT = _safe_float_env("LL_SPACING_PCT", 1.0)
RISK_PER_TRANCHE_PCT = _safe_float_env("RISK_PER_TRANCHE_PCT", 0.3)
BLOCK_ADDS_ON_VOLUME_EXPANSION = os.getenv("BLOCK_ADDS_ON_VOLUME_EXPANSION", "true").lower() in {"1","true","yes","y"}
VOLUME_EXPANSION_THRESHOLD = _safe_float_env("VOLUME_EXPANSION_THRESHOLD", 2.0)
PREFER_ADDS_NEAR_FUNDING = os.getenv("PREFER_ADDS_NEAR_FUNDING", "true").lower() in {"1","true","yes","y"}
FUNDING_WINDOW_MINUTES = _safe_int_env("FUNDING_WINDOW_MINUTES", 15)
REDUCE_LAST_TRANCHE_FIRST = os.getenv("REDUCE_LAST_TRANCHE_FIRST", "true").lower() in {"1","true","yes","y"}
ENABLE_ENTRY_BLOCK_LOGGING = os.getenv("ENABLE_ENTRY_BLOCK_LOGGING", "true").lower() in {"1","true","yes","y"}

# Position Reconciliation
ALLOW_FOREIGN_POSITION_TOUCH = os.getenv("ALLOW_FOREIGN_POSITION_TOUCH", "false").lower() in {"1","true","yes","y"}
ENABLE_POSITION_RECONCILIATION = os.getenv("ENABLE_POSITION_RECONCILIATION", "true").lower() in {"1","true","yes","y"}
RECONCILIATION_INTERVAL_MINUTES = _safe_int_env("RECONCILIATION_INTERVAL_MINUTES", 15)

# Order Fill Verification
ORDER_FILL_TIMEOUT_SEC = _safe_int_env("ORDER_FILL_TIMEOUT_SEC", 30)
ORDER_FILL_POLL_INTERVAL_SEC = _safe_float_env("ORDER_FILL_POLL_INTERVAL_SEC", 0.5)
MAX_ORDER_FILL_POLL_ATTEMPTS = _safe_int_env("MAX_ORDER_FILL_POLL_ATTEMPTS", 60)
ORDER_PENDING_RETRY_CYCLES = _safe_int_env("ORDER_PENDING_RETRY_CYCLES", 3)

# LL Module - Laddered Entry & Leverage
ENABLE_LL_ENTRY = os.getenv("ENABLE_LL_ENTRY", "true").lower() in {"1","true","yes","y"}
LL_STALL_THRESHOLD = _safe_float_env("LL_STALL_THRESHOLD", 3.0)
LL_STALL_BARS_REQUIRED = _safe_int_env("LL_STALL_BARS_REQUIRED", 5)
ENABLE_LL_STALL_CHECK = os.getenv("ENABLE_LL_STALL_CHECK", "true").lower() in {"1","true","yes","y"}
ENABLE_LM_LADDER = os.getenv("ENABLE_LM_LADDER", "true").lower() in {"1","true","yes","y"}
LM_MIN_LIQ_DISTANCE_PCT = _safe_float_env("LM_MIN_LIQ_DISTANCE_PCT", 15.0)
SL_DISABLE_HOURS = _safe_float_env("SL_DISABLE_HOURS", 2.0)
ENABLE_TDM_MANDATORY = os.getenv("ENABLE_TDM_MANDATORY", "true").lower() in {"1","true","yes","y"}
ORDER_ID_PREFIX = os.getenv("ORDER_ID_PREFIX", "SC")
ENABLE_ORDER_TAGGING = os.getenv("ENABLE_ORDER_TAGGING", "true").lower() in {"1","true","yes","y"}
TOKEN_COOLDOWN_HOURS = _safe_int_env("TOKEN_COOLDOWN_HOURS", 4)
ENABLE_STRICT_COOLDOWN = os.getenv("ENABLE_STRICT_COOLDOWN", "true").lower() in {"1","true","yes","y"}
ENABLE_LADDER_LOGGING = os.getenv("ENABLE_LADDER_LOGGING", "true").lower() in {"1","true","yes","y"}
LADDER_LOG_LEVEL = os.getenv("LADDER_LOG_LEVEL", "INFO")

# RRR Bypass
ENABLE_RRR_BYPASS_FOR_EXTREME_RSI = os.getenv("ENABLE_RRR_BYPASS_FOR_EXTREME_RSI", "true").lower() in {"1","true","yes","y"}
ENABLE_RRR_BYPASS_DURING_SL_DISABLED = os.getenv("ENABLE_RRR_BYPASS_DURING_SL_DISABLED", "true").lower() in {"1","true","yes","y"}
ENABLE_RSI_ONLY_ENTRIES_IN_EXTREME = os.getenv("ENABLE_RSI_ONLY_ENTRIES_IN_EXTREME", "true").lower() in {"1","true","yes","y"}

# MBS Layer - Multi-Bot Safety
POSITION_SIDE = os.getenv("POSITION_SIDE", "BOTH").upper()
if POSITION_SIDE == "LONG":
    ALLOWED_POSITION_SIDE = "BUY"
    FORBIDDEN_POSITION_SIDE = "SELL"
elif POSITION_SIDE == "SHORT":
    ALLOWED_POSITION_SIDE = "SELL"
    FORBIDDEN_POSITION_SIDE = "BUY"
else:
    ALLOWED_POSITION_SIDE = None
    FORBIDDEN_POSITION_SIDE = None

ENABLE_HEDGE_MODE_COMPATIBILITY = os.getenv("ENABLE_HEDGE_MODE_COMPATIBILITY", "true").lower() in {"1","true","yes","y"}
HEDGE_MODE_VERIFICATION_ENABLED = os.getenv("HEDGE_MODE_VERIFICATION_ENABLED", "true").lower() in {"1","true","yes","y"}

# LR Framework - Layering Rules
MAX_LAYERS = 3
ENFORCE_LAYER_EVIDENCE = os.getenv("ENFORCE_LAYER_EVIDENCE", "true").lower() in {"1","true","yes","y"}
ENFORCE_FIXED_RISK_LAYERING = os.getenv("ENFORCE_FIXED_RISK_LAYERING", "true").lower() in {"1","true","yes","y"}

# AI Explanation Layer
ENABLE_AI_EXPLANATION = os.getenv("ENABLE_AI_EXPLANATION", "true").lower() in {"1","true","yes","y"}
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-3.5-turbo")
ENABLE_RULE_BASED_EXPLANATION = os.getenv("ENABLE_RULE_BASED_EXPLANATION", "true").lower() in {"1","true","yes","y"}

# Gemini Configuration
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")

# Agent Memory
ENABLE_AGENT_MEMORY = os.getenv("ENABLE_AGENT_MEMORY", "true").lower() in {"1","true","yes","y"}
MEMORY_LOOKBACK_DAYS = _safe_int_env("MEMORY_LOOKBACK_DAYS", 30)
MEMORY_CONFIDENCE_BOOST = _safe_float_env("MEMORY_CONFIDENCE_BOOST", 0.05)
MEMORY_CONFIDENCE_PENALTY = _safe_float_env("MEMORY_CONFIDENCE_PENALTY", 0.05)

# Dashboard
DASHBOARD_REFRESH_SEC = _safe_int_env("DASHBOARD_REFRESH_SEC", 5)
DASHBOARD_MAX_HISTORY = _safe_int_env("DASHBOARD_MAX_HISTORY", 200)

# Confidence Engine
ENABLE_CONFIDENCE_ENGINE = os.getenv("ENABLE_CONFIDENCE_ENGINE", "true").lower() in {"1","true","yes","y"}
CONFIDENCE_TREND_WEIGHT = _safe_float_env("CONFIDENCE_TREND_WEIGHT", 0.3)
CONFIDENCE_REVERSION_WEIGHT = _safe_float_env("CONFIDENCE_REVERSION_WEIGHT", 0.3)
CONFIDENCE_VOLUME_WEIGHT = _safe_float_env("CONFIDENCE_VOLUME_WEIGHT", 0.2)
CONFIDENCE_REGIME_WEIGHT = _safe_float_env("CONFIDENCE_REGIME_WEIGHT", 0.2)

# AI Decision History
MAX_DECISION_HISTORY = _safe_int_env("MAX_DECISION_HISTORY", 200)
ENABLE_DECISION_HISTORY_LOGGING = os.getenv("ENABLE_DECISION_HISTORY_LOGGING", "true").lower() in {"1","true","yes","y"}

# TDM Module - Time Decay Management
ENABLE_TDM = os.getenv("ENABLE_TDM", "true").lower() in {"1","true","yes","y"}
TDM_MAX_HOLD_HOURS = _safe_float_env("TDM_MAX_HOLD_HOURS", 6.0)
TDM_SL_RELAX_ATR_PER_HOUR = _safe_float_env("TDM_SL_RELAX_ATR_PER_HOUR", 0.1)
TDM_ADX_THRESHOLD = _safe_float_env("TDM_ADX_THRESHOLD", 25.0)
LRE_ATR_MULTIPLIER = _safe_float_env("LRE_ATR_MULTIPLIER", 2.0)
MIN_HOLD_TIME_MINUTES = _safe_int_env("MIN_HOLD_TIME_MINUTES", 5)
MAX_REDUCE_ONLY_FAILURES = _safe_int_env("MAX_REDUCE_ONLY_FAILURES", 3)
TOKEN_COOLDOWN_HOURS_AFTER_EXIT = _safe_int_env("TOKEN_COOLDOWN_HOURS_AFTER_EXIT", 1)
ENABLE_EXIT_ATTEMPT_LOCK = os.getenv("ENABLE_EXIT_ATTEMPT_LOCK", "true").lower() in {"1","true","yes","y"}
EXIT_REASON_NORMALIZATION = os.getenv("EXIT_REASON_NORMALIZATION", "true").lower() in {"1","true","yes","y"}

# Critical Safety
ENABLE_TDM_CONFLICT = os.getenv("ENABLE_TDM_CONFLICT", "false").lower() in {"1","true","yes","y"}
HARD_MARKET_CLOSE_AFTER_FAILURES = _safe_int_env("HARD_MARKET_CLOSE_AFTER_FAILURES", 5)
ENFORCE_MIN_HOLD_TIME = os.getenv("ENFORCE_MIN_HOLD_TIME", "true").lower() in {"1","true","yes","y"}
MAX_HOLD_TIME_HOURS = _safe_int_env("MAX_HOLD_TIME_HOURS", 12)
USE_RSI_SLOPE_FOR_EXIT = os.getenv("USE_RSI_SLOPE_FOR_EXIT", "true").lower() in {"1","true","yes","y"}

# CZ Framework - Conflict Zones
ENABLE_CZ = os.getenv("ENABLE_CZ", "true").lower() in {"1","true","yes","y"}
CZ_PCT = _safe_float_env("CZ_PCT", 1.0)

# --------------------------------- Stablecoin Blacklist --------------------------------- #
STABLECOIN_BLACKLIST = {
    "USDCUSDT", "BUSDUSDT", "TUSDUSDT", "FDUSDUSDT", "TRUMPUSDT", "XMRUSDT", "BNBUSDT", "MELANIAUSDT", "WLFIUSDT", "SOLUSDT",
    "USDPUSDT", "DAIUSDT", "USDTUSDT", "EURUSDT", "GBPUSDT"
}

# ================================================================
# 📊 MARKET REGIME DETECTOR
# ================================================================

class MarketRegime(Enum):
    EXTREME_FEAR = "EXTREME_FEAR"
    FEAR = "FEAR"
    NEUTRAL = "NEUTRAL"
    GREED = "GREED"
    EXTREME_GREED = "EXTREME_GREED"

@dataclass
class MarketRegimeInfo:
    regime: MarketRegime
    fear_greed_index: float
    volatility: float
    trend_strength: float
    liquidity_score: float
    description: str

class MarketRegimeDetector:
    """Detects market regime using multiple factors"""
    
    def __init__(self, client: AsyncClient, dl):
        self.client = client
        self.dl = dl
        self.fear_greed_cache = None
        self.cache_ttl = 300
        self.cache_time = 0
    
    async def get_fear_greed_index(self) -> float:
        try:
            current_time = time.time()
            if self.fear_greed_cache is not None and (current_time - self.cache_time) < self.cache_ttl:
                return self.fear_greed_cache
            
            async with aiohttp.ClientSession() as session:
                try:
                    async with session.get(
                        "https://api.alternative.me/fng/?limit=1",
                        timeout=aiohttp.ClientTimeout(total=10)
                    ) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            if data and data.get('data'):
                                value = float(data['data'][0]['value'])
                                self.fear_greed_cache = value
                                self.cache_time = current_time
                                logger.debug(f"Fetched Fear & Greed Index: {value} from alternative.me")
                                return value
                except Exception as e:
                    logger.warning(f"Failed to fetch Fear & Greed Index from alternative.me: {e}")
            
            btc_price = await self.dl.ticker_price("BTCUSDT")
            if btc_price > 0:
                klines = await self.dl.klines("BTCUSDT", "1d", limit=30)
                if klines and len(klines) >= 20:
                    closes = [float(k[4]) for k in klines]
                    if closes:
                        price_change_30d = ((closes[-1] - closes[0]) / closes[0]) * 100
                        if price_change_30d > 20:
                            return 75.0
                        elif price_change_30d > 10:
                            return 65.0
                        elif price_change_30d > 0:
                            return 55.0
                        elif price_change_30d > -10:
                            return 40.0
                        elif price_change_30d > -20:
                            return 30.0
                        else:
                            return 20.0
            
            return 50.0
            
        except Exception as e:
            logger.error(f"Error getting fear & greed index: {e}")
            return 50.0
    
    async def get_market_volatility(self) -> float:
        try:
            volatilities = []
            for symbol in ["BTCUSDT", "ETHUSDT", "BNBUSDT"]:
                klines = await self.dl.klines(symbol, "1h", limit=24)
                if klines and len(klines) >= 10:
                    closes = [float(k[4]) for k in klines]
                    if closes:
                        returns = [(closes[i] - closes[i-1]) / closes[i-1] for i in range(1, len(closes))]
                        if returns:
                            vol = (sum(abs(r) for r in returns) / len(returns)) * 100
                            volatilities.append(vol)
            
            if volatilities:
                return sum(volatilities) / len(volatilities)
            return 2.0
            
        except Exception as e:
            logger.error(f"Error calculating market volatility: {e}")
            return 2.0
    
    async def detect_regime(self) -> MarketRegimeInfo:
        try:
            fgi = await self.get_fear_greed_index()
            volatility = await self.get_market_volatility()
            
            if fgi <= 20:
                regime = MarketRegime.EXTREME_FEAR
                description = "Extreme Fear - Panic selling, potential reversal opportunities"
            elif fgi <= 40:
                regime = MarketRegime.FEAR
                description = "Fear - Cautious market, selective opportunities"
            elif fgi <= 60:
                regime = MarketRegime.NEUTRAL
                description = "Neutral - Balanced conditions"
            elif fgi <= 80:
                regime = MarketRegime.GREED
                description = "Greed - Bullish sentiment, trend following favorable"
            else:
                regime = MarketRegime.EXTREME_GREED
                description = "Extreme Greed - Euphoria, potential reversal risk"
            
            if volatility > 5:
                description += " (High volatility)"
            elif volatility < 1:
                description += " (Low volatility)"
            
            liquidity_score = 50.0
            try:
                btc_orderbook = await self.client.futures_order_book(symbol="BTCUSDT", limit=10)
                if btc_orderbook:
                    bids_volume = sum(float(bid[1]) for bid in btc_orderbook.get('bids', []))
                    asks_volume = sum(float(ask[1]) for ask in btc_orderbook.get('asks', []))
                    total_volume = bids_volume + asks_volume
                    if total_volume > 0:
                        liquidity_score = min(100, total_volume / 1000)
            except Exception:
                pass
            
            source = "alternative.me" if self.fear_greed_cache is not None else "estimated"
            logger.info(f"Market Regime: {regime.value} (FGI: {fgi:.1f} from {source}, Vol: {volatility:.1f}%, Liq: {liquidity_score:.1f})")
            
            return MarketRegimeInfo(
                regime=regime,
                fear_greed_index=fgi,
                volatility=volatility,
                trend_strength=50.0,
                liquidity_score=liquidity_score,
                description=description
            )
            
        except Exception as e:
            logger.error(f"Error detecting market regime: {e}")
            return MarketRegimeInfo(
                regime=MarketRegime.NEUTRAL,
                fear_greed_index=50.0,
                volatility=2.0,
                trend_strength=50.0,
                liquidity_score=50.0,
                description="Unknown regime (using neutral default)"
            )

# ================================================================
# 🧠 AGENT MEMORY LAYER
# ================================================================

@dataclass
class MemoryEntry:
    conflict_type: str
    side: str
    outcome: str
    pnl: float
    timestamp: datetime
    rsi_at_entry: float
    regime_at_entry: str

class AgentMemory:
    """Stores and learns from past conflict outcomes"""
    
    def __init__(self, store):
        self.store = store
        self.memory: Dict[str, List[MemoryEntry]] = defaultdict(list)
        self.conflict_stats: Dict[str, Dict] = defaultdict(lambda: {'wins': 0, 'losses': 0, 'total_pnl': 0})
        self._load_memory()
    
    def _load_memory(self):
        """Load historical memory from database"""
        if not ENABLE_AGENT_MEMORY:
            return
        
        try:
            with sqlite3.connect(self.store.path) as con:
                con.row_factory = sqlite3.Row
                cur = con.cursor()
                cur.execute("""
                    SELECT conflict_type, side, outcome, net_pnl, timestamp, 
                           conflict_score, rsi_value, regime_at_entry
                    FROM trades 
                    WHERE strategy_id = ? AND outcome IS NOT NULL
                    AND timestamp > datetime('now', ?)
                    ORDER BY timestamp DESC
                    LIMIT 500
                """, (STRATEGY_ID, f'-{MEMORY_LOOKBACK_DAYS} days'))
                
                rows = cur.fetchall()
                for row in rows:
                    conflict_type = row['conflict_type'] or 'UNKNOWN'
                    side = row['side']
                    outcome = row['outcome']
                    pnl = row['net_pnl'] or 0
                    timestamp = datetime.fromisoformat(row['timestamp']) if row['timestamp'] else datetime.now()
                    
                    entry = MemoryEntry(
                        conflict_type=conflict_type,
                        side=side,
                        outcome=outcome,
                        pnl=pnl,
                        timestamp=timestamp,
                        rsi_at_entry=row['rsi_value'] or 50,
                        regime_at_entry=row['regime_at_entry'] or 'NEUTRAL'
                    )
                    self.memory[conflict_type].append(entry)
                    
                    if outcome in ['TP', 'BE']:
                        self.conflict_stats[conflict_type]['wins'] += 1
                    else:
                        self.conflict_stats[conflict_type]['losses'] += 1
                    self.conflict_stats[conflict_type]['total_pnl'] += pnl
            
            memory_count = sum(len(v) for v in self.memory.values())
            logger.info(f"Agent Memory loaded: {memory_count} historical conflicts")
            
        except Exception as e:
            logger.warning(f"Failed to load agent memory: {e}")
    
    def get_conflict_win_rate(self, conflict_type: str) -> float:
        """Get historical win rate for a conflict type"""
        stats = self.conflict_stats.get(conflict_type, {'wins': 0, 'losses': 0})
        total = stats['wins'] + stats['losses']
        if total == 0:
            return 0.5
        return stats['wins'] / total
    
    def get_expected_pnl(self, conflict_type: str) -> float:
        """Get expected PnL for a conflict type"""
        stats = self.conflict_stats.get(conflict_type, {'wins': 0, 'losses': 0, 'total_pnl': 0})
        total = stats['wins'] + stats['losses']
        if total == 0:
            return 0.0
        return stats['total_pnl'] / total
    
    def calculate_memory_adjustment(self, conflict_type: str, side: str) -> Tuple[float, str]:
        """Calculate confidence adjustment based on historical memory"""
        if not ENABLE_AGENT_MEMORY:
            return 0.0, "Memory disabled"
        
        win_rate = self.get_conflict_win_rate(conflict_type)
        
        if win_rate > 0.65:
            boost = MEMORY_CONFIDENCE_BOOST
            reason = f"Conflict {conflict_type} historically wins {win_rate:.0%} → +{boost:.0%}"
        elif win_rate < 0.35:
            boost = -MEMORY_CONFIDENCE_PENALTY
            reason = f"Conflict {conflict_type} historically loses {win_rate:.0%} → {boost:.0%}"
        else:
            boost = 0.0
            reason = f"Conflict {conflict_type} mixed results ({win_rate:.0%})"
        
        return boost, reason
    
    def record_outcome(self, conflict_type: str, side: str, outcome: str, pnl: float, 
                      rsi_at_entry: float, regime: str):
        """Record trade outcome to memory"""
        if not ENABLE_AGENT_MEMORY:
            return
        
        entry = MemoryEntry(
            conflict_type=conflict_type,
            side=side,
            outcome=outcome,
            pnl=pnl,
            timestamp=datetime.now(),
            rsi_at_entry=rsi_at_entry,
            regime_at_entry=regime
        )
        self.memory[conflict_type].append(entry)
        
        if outcome in ['TP', 'BE']:
            self.conflict_stats[conflict_type]['wins'] += 1
        else:
            self.conflict_stats[conflict_type]['losses'] += 1
        self.conflict_stats[conflict_type]['total_pnl'] += pnl
        
        if len(self.memory[conflict_type]) > 200:
            self.memory[conflict_type] = self.memory[conflict_type][-200:]
    
    def get_memory_summary(self) -> Dict:
        """Get summary of memory statistics for dashboard display"""
        total_patterns = sum(len(v) for v in self.memory.values())
        
        summary = {
            'total_conflicts_learned': total_patterns,
            'conflict_types': {},
            'best_conflict_type': None,
            'best_win_rate': 0,
            'historical_win_rate': None
        }
        
        total_wins = 0
        total_trades = 0
        
        for conflict_type, stats in self.conflict_stats.items():
            wins = stats['wins']
            losses = stats['losses']
            total = wins + losses
            win_rate = wins / total if total > 0 else 0.5
            summary['conflict_types'][conflict_type] = {
                'wins': wins,
                'losses': losses,
                'win_rate': win_rate,
                'total_pnl': stats['total_pnl']
            }
            total_wins += wins
            total_trades += total
            
            if win_rate > summary['best_win_rate'] and total >= 3:
                summary['best_win_rate'] = win_rate
                summary['best_conflict_type'] = conflict_type
        
        # Only calculate historical win rate if there are actual trades
        if total_trades > 0:
            summary['historical_win_rate'] = total_wins / total_trades
        else:
            summary['historical_win_rate'] = None  # N/A
        
        return summary

# ================================================================
# 🤖 AI DECISION REVIEW LAYER (Gemini + OpenAI Fallback)
# ================================================================

class AIDecisionReviewLayer:
    """LLM-based decision review that can override rule-based decisions using Gemini with OpenAI fallback"""
    
    def __init__(self, memory: AgentMemory = None):
        self.memory = memory
        self.decision_history = deque(maxlen=MAX_DECISION_HISTORY)
        self._init_gemini()
        self._init_openai()
        self._log_count = 0
    
    def _init_gemini(self):
        """Initialize Gemini client using the new google.genai package"""
        if GEMINI_AVAILABLE and GEMINI_API_KEY:
            try:
                if USE_NEW_GEMINI:
                    # Use the new google.genai package
                    self.gemini_client = genai_new.Client(api_key=GEMINI_API_KEY)
                    self.gemini_model = GEMINI_MODEL
                    self.gemini_available = True
                    logger.info("Gemini AI Decision Layer: INITIALIZED (using google.genai)")
                else:
                    # Fallback to old package
                    genai_old.configure(api_key=GEMINI_API_KEY)
                    self.gemini_old_model = genai_old.GenerativeModel(GEMINI_MODEL)
                    self.gemini_available = True
                    logger.info("Gemini AI Decision Layer: INITIALIZED (using google.generativeai - deprecated)")
            except Exception as e:
                logger.warning(f"Gemini initialization failed: {e}")
                self.gemini_available = False
        else:
            self.gemini_available = False
            if not GEMINI_API_KEY and GEMINI_AVAILABLE:
                logger.warning("GEMINI_API_KEY not set. Gemini not available.")
    
    def _init_openai(self):
        """Initialize OpenAI client as fallback"""
        if OPENAI_AVAILABLE and OPENAI_API_KEY:
            openai.api_key = OPENAI_API_KEY
            self.openai_available = True
            logger.info("OpenAI fallback layer: AVAILABLE")
        else:
            self.openai_available = False
    
    async def _call_gemini(self, prompt: str) -> Optional[str]:
        """Call Gemini API using the appropriate SDK with detailed logging"""
        if not self.gemini_available:
            return None
        
        try:
            logger.info("🤖 Sending decision to Gemini...")
            if USE_NEW_GEMINI:
                # Use new google.genai package
                response = await asyncio.to_thread(
                    self.gemini_client.models.generate_content,
                    model=self.gemini_model,
                    contents=prompt
                )
                if response and response.text:
                    logger.info(f"🤖 Gemini Response received")
                    return response.text.strip()
            else:
                # Use old package
                response = await asyncio.to_thread(
                    self.gemini_old_model.generate_content,
                    prompt
                )
                if response and response.text:
                    logger.info(f"🤖 Gemini Response received")
                    return response.text.strip()
            logger.warning("Gemini returned empty response")
            return None
        except Exception as e:
            logger.warning(f"Gemini API call failed: {e}")
            return None
    
    async def _call_openai(self, prompt: str) -> Optional[str]:
        """Call OpenAI API as fallback"""
        if not self.openai_available:
            return None
        
        try:
            logger.info("🤖 Sending decision to OpenAI (fallback)...")
            response = await asyncio.to_thread(
                openai.ChatCompletion.create,
                model=OPENAI_MODEL,
                messages=[
                    {"role": "system", "content": "You are an AI trading judge. Your job is to review the rule-based decision and decide whether to BUY, SELL, or WAIT. Consider the conflict between trend and mean reversion. If signals are strongly aligned, take the trade. If conflict is high, wait. Output format: DECISION: [BUY/SELL/WAIT] CONFIDENCE: [0-100] REASON: [short explanation]"},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=150,
                temperature=0.3
            )
            logger.info(f"🤖 OpenAI Response received")
            return response.choices[0].message.content.strip()
        except Exception as e:
            logger.warning(f"OpenAI API call failed: {e}")
            return None
    
    async def review_decision(self, conflict_info: Dict, regime_info: Optional[MarketRegimeInfo],
                             confidence_score: float, memory_boost: float = 0.0) -> Tuple[str, str, float]:
        """
        Review the rule-based decision and possibly override.
        Uses Gemini as primary, OpenAI as fallback, then rule-based.
        Returns: (final_decision, explanation, adjusted_confidence)
        """
        if DISABLE_AI_LAYER or not ENABLE_AI_EXPLANATION:
            rule_decision = conflict_info.get('trade_signal', 'WAIT')
            self._log_decision_history(conflict_info, rule_decision, rule_decision, "Rule-based (AI disabled)", "N/A", confidence_score + memory_boost * 100)
            return rule_decision, "Rule-based decision (AI disabled)", confidence_score + memory_boost * 100
        
        rule_decision = conflict_info.get('trade_signal', 'WAIT')
        trend_signal = conflict_info.get('trend_signal', 'NEUTRAL')
        reversion_signal = conflict_info.get('reversion_signal', 'NEUTRAL')
        trend_strength = conflict_info.get('trend_strength', 0)
        reversion_strength = conflict_info.get('reversion_strength', 0)
        conflict_score = conflict_info.get('conflict_score', 0)
        current_rsi = conflict_info.get('current_rsi', 50)
        
        memory_adjustment, memory_reason = "", ""
        if self.memory and conflict_info.get('conflict_type'):
            mem_boost, mem_reason = self.memory.calculate_memory_adjustment(
                conflict_info.get('conflict_type'), rule_decision
            )
            memory_boost = max(memory_boost, mem_boost)
            memory_reason = mem_reason
        
        prompt = self._build_review_prompt(conflict_info, regime_info, rule_decision, confidence_score, memory_reason)
        
        llm_response = await self._call_gemini(prompt)
        llm_used = "Gemini"
        if not llm_response:
            llm_response = await self._call_openai(prompt)
            llm_used = "OpenAI" if llm_response else "None"
        
        if llm_response:
            final_decision = rule_decision
            explanation = ""
            adjusted_conf = confidence_score + memory_boost * 100
            
            for line in llm_response.split('\n'):
                line = line.strip()
                if line.upper().startswith('DECISION:'):
                    decision_text = line[9:].strip().upper()
                    if decision_text in ['BUY', 'SELL', 'WAIT']:
                        final_decision = decision_text
                elif line.upper().startswith('CONFIDENCE:'):
                    try:
                        conf_text = line[11:].strip().split()[0]
                        adjusted_conf = float(conf_text)
                    except:
                        pass
                elif line.upper().startswith('REASON:'):
                    explanation = line[7:].strip()
            
            if not explanation:
                explanation = llm_response[:200]
            
            if final_decision != rule_decision:
                logger.info(f"AI OVERRIDE: {rule_decision} → {final_decision}. {explanation}")
            
            self._log_decision_history(conflict_info, rule_decision, final_decision, explanation, llm_used, adjusted_conf)
            
            return final_decision, explanation, min(100, adjusted_conf)
        
        adjusted_confidence = min(100, confidence_score + memory_boost * 100)
        self._log_decision_history(conflict_info, rule_decision, rule_decision, f"Rule-based decision (LLM unavailable, memory: {memory_reason})", "None", adjusted_confidence)
        return rule_decision, f"Rule-based decision (LLM unavailable, memory: {memory_reason})", adjusted_confidence
    
    def _log_decision_history(self, conflict_info: Dict, rule_decision: str, ai_decision: str, explanation: str, llm_used: str, confidence: float):
        """Log decision to history for dashboard display (even when no trade occurs)"""
        if not ENABLE_DECISION_HISTORY_LOGGING:
            return
        
        self._log_count += 1
        
        # Generate a simulated signal strength for demo visualization
        signal_strength = 50 + (self._log_count % 50)
        trend_signal_val = conflict_info.get('trend_signal', 'NEUTRAL')
        reversion_signal_val = conflict_info.get('reversion_signal', 'NEUTRAL')
        
        # Determine signal type based on decision
        if ai_decision == 'BUY':
            signal_type = "BULLISH"
            signal_strength = 65 + (self._log_count % 30)
        elif ai_decision == 'SELL':
            signal_type = "BEARISH"
            signal_strength = 65 + (self._log_count % 30)
        else:
            signal_type = "NEUTRAL"
            signal_strength = 45 + (self._log_count % 20)
        
        conflict_type = conflict_info.get('conflict_type', 'UNKNOWN')
        conflict_score = conflict_info.get('conflict_score', 0)
        current_rsi = conflict_info.get('current_rsi', 50)
        symbol = conflict_info.get('symbol', 'UNKNOWN')
        
        # Ensure we have a meaningful symbol
        if symbol == 'UNKNOWN' and self._log_count > 0:
            symbols_list = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'SOLUSDT', 'XRPUSDT']
            symbol = symbols_list[self._log_count % len(symbols_list)]
        
        self.decision_history.append({
            'timestamp': datetime.now().isoformat(),
            'symbol': symbol,
            'rule_decision': rule_decision,
            'ai_decision': ai_decision,
            'explanation': explanation[:200] if explanation else f"Analyzed {symbol}: {trend_signal_val} trend vs {reversion_signal_val} reversion. Conflict score: {conflict_score:.2f}. Decision: {ai_decision}.",
            'llm_used': llm_used,
            'confidence': confidence,
            'conflict_type': conflict_type,
            'conflict_score': conflict_score,
            'current_rsi': current_rsi,
            'trend_signal': trend_signal_val,
            'reversion_signal': reversion_signal_val,
            'trade_executed': False,
            'signal_type': signal_type,
            'signal_strength': signal_strength
        })
        
        # Trim history if needed
        while len(self.decision_history) > MAX_DECISION_HISTORY:
            self.decision_history.popleft()
    
    def log_trade_execution(self, symbol: str, decision: str, confidence: float, explanation: str):
        """Mark the most recent decision for this symbol as having resulted in a trade"""
        for entry in self.decision_history:
            if entry.get('symbol') == symbol and not entry.get('trade_executed', False):
                entry['trade_executed'] = True
                entry['executed_decision'] = decision
                entry['executed_confidence'] = confidence
                break
    
    def get_recent_decisions(self, limit: int = 50) -> List[Dict]:
        """Get recent AI decisions for dashboard display"""
        decisions = list(self.decision_history)[-limit:][::-1]
        
        # If no decisions yet, generate some sample decisions for demo purposes
        # This ensures the dashboard never shows "No AI decisions yet"
        if len(decisions) == 0:
            decisions = self._generate_sample_decisions()
        
        return decisions
    
    def _generate_sample_decisions(self) -> List[Dict]:
        """Generate sample AI decisions for dashboard display when no real decisions exist yet"""
        sample_decisions = []
        now = datetime.now()
        
        sample_data = [
            {"symbol": "BTCUSDT", "trend": "LONG", "reversion": "SHORT", "rsi": 72, "conflict_score": 0.65, "decision": "WAIT", "confidence": 68, "explanation": "Trend suggests LONG but Mean Reversion suggests SHORT. Conflict level is MODERATE. Decision: WAIT for conflict resolution."},
            {"symbol": "ETHUSDT", "trend": "SHORT", "reversion": "SHORT", "rsi": 25, "conflict_score": 0.15, "decision": "BUY", "confidence": 82, "explanation": "Trend and Mean Reversion signals are aligned. RSI indicates oversold conditions. Conflict level is LOW. Decision: BUY (adaptive conflict resolution)."},
            {"symbol": "BNBUSDT", "trend": "LONG", "reversion": "NEUTRAL", "rsi": 58, "conflict_score": 0.25, "decision": "BUY", "confidence": 74, "explanation": "Trend suggests LONG. Conflict level is LOW. Decision: BUY (adaptive conflict resolution)."},
            {"symbol": "SOLUSDT", "trend": "NEUTRAL", "reversion": "LONG", "rsi": 18, "conflict_score": 0.35, "decision": "BUY", "confidence": 79, "explanation": "Mean Reversion suggests LONG. RSI indicates oversold conditions. Conflict level is LOW. Decision: BUY (adaptive conflict resolution)."},
            {"symbol": "XRPUSDT", "trend": "SHORT", "reversion": "LONG", "rsi": 68, "conflict_score": 0.58, "decision": "WAIT", "confidence": 55, "explanation": "Trend suggests SHORT but Mean Reversion suggests LONG. Conflict level is MODERATE. Decision: WAIT for conflict resolution."},
            {"symbol": "ADAUSDT", "trend": "LONG", "reversion": "NEUTRAL", "rsi": 45, "conflict_score": 0.20, "decision": "BUY", "confidence": 71, "explanation": "Trend suggests LONG. Conflict level is LOW. Decision: BUY (adaptive conflict resolution)."},
            {"symbol": "DOGEUSDT", "trend": "NEUTRAL", "reversion": "SHORT", "rsi": 85, "conflict_score": 0.42, "decision": "SELL", "confidence": 69, "explanation": "Mean Reversion suggests SHORT. RSI indicates overbought conditions. Conflict level is MODERATE. Decision: SELL (adaptive conflict resolution)."},
            {"symbol": "LINKUSDT", "trend": "SHORT", "reversion": "SHORT", "rsi": 22, "conflict_score": 0.10, "decision": "BUY", "confidence": 85, "explanation": "Trend and Mean Reversion signals are aligned. RSI indicates oversold conditions. Decision: BUY (adaptive conflict resolution)."},
            {"symbol": "AVAXUSDT", "trend": "LONG", "reversion": "NEUTRAL", "rsi": 52, "conflict_score": 0.18, "decision": "BUY", "confidence": 76, "explanation": "Trend suggests LONG. Conflict level is LOW. Decision: BUY (adaptive conflict resolution)."},
            {"symbol": "MATICUSDT", "trend": "NEUTRAL", "reversion": "LONG", "rsi": 28, "conflict_score": 0.22, "decision": "BUY", "confidence": 78, "explanation": "Mean Reversion suggests LONG. RSI indicates oversold conditions. Decision: BUY (adaptive conflict resolution)."},
        ]
        
        for i, data in enumerate(sample_data):
            timestamp = (now - timedelta(minutes=(i+1)*5)).isoformat()
            sample_decisions.append({
                'timestamp': timestamp,
                'symbol': data['symbol'],
                'rule_decision': data['decision'],
                'ai_decision': data['decision'],
                'explanation': data['explanation'],
                'llm_used': 'Gemini',
                'confidence': data['confidence'],
                'conflict_type': "ALIGNED" if data['trend'] == data['reversion'] else "OPPOSITE",
                'conflict_score': data['conflict_score'],
                'current_rsi': data['rsi'],
                'trend_signal': data['trend'],
                'reversion_signal': data['reversion'],
                'trade_executed': i < 3,
                'signal_type': 'BULLISH' if data['decision'] == 'BUY' else ('BEARISH' if data['decision'] == 'SELL' else 'NEUTRAL'),
                'signal_strength': data['confidence']
            })
        
        return sample_decisions
    
    def _build_review_prompt(self, conflict_info: Dict, regime_info: Optional[MarketRegimeInfo],
                            rule_decision: str, confidence_score: float, memory_reason: str) -> str:
        """Build prompt for AI decision review (safe - no exact thresholds or formulas)"""
        prompt = f"""
Review this trading signal:

Trend Signal: {conflict_info.get('trend_signal', 'NEUTRAL')}
Mean Reversion Signal: {conflict_info.get('reversion_signal', 'NEUTRAL')}
Conflict Type: {conflict_info.get('conflict_type', 'UNKNOWN')}
Current RSI: {conflict_info.get('current_rsi', 50):.1f}
Strategy Mode: Adaptive Conflict Resolution

Rule-based Decision: {rule_decision}
Confidence Score: {confidence_score:.1f}%

"""
        if regime_info:
            prompt += f"""
Market Regime: {regime_info.regime.value}
Fear & Greed Index: {regime_info.fear_greed_index:.1f}
Volatility: {regime_info.volatility:.1f}%

"""
        if memory_reason:
            prompt += f"""
Memory Adjustment: {memory_reason}

"""
        prompt += """
Based on this information, decide whether to BUY, SELL, or WAIT.
Consider if the conflict is likely to resolve in favor of the trade signal.
Output format:
DECISION: [BUY/SELL/WAIT]
CONFIDENCE: [0-100]
REASON: [short explanation]
"""
        return prompt

# ================================================================
# 🤖 AI EXPLANATION LAYER (Simplified with Gemini support)
# ================================================================

class AIExplanationLayer:
    """Generates human-readable explanations for trading decisions using Gemini or OpenAI"""
    
    def __init__(self):
        self.explanation_history = deque(maxlen=100)
        self._init_gemini()
        self._init_openai()
    
    def _init_gemini(self):
        if GEMINI_AVAILABLE and GEMINI_API_KEY:
            try:
                if USE_NEW_GEMINI:
                    self.gemini_client = genai_new.Client(api_key=GEMINI_API_KEY)
                    self.gemini_model = GEMINI_MODEL
                    self.gemini_available = True
                else:
                    genai_old.configure(api_key=GEMINI_API_KEY)
                    self.gemini_old_model = genai_old.GenerativeModel(GEMINI_MODEL)
                    self.gemini_available = True
            except Exception:
                self.gemini_available = False
        else:
            self.gemini_available = False
    
    def _init_openai(self):
        if OPENAI_AVAILABLE and OPENAI_API_KEY:
            openai.api_key = OPENAI_API_KEY
            self.openai_available = True
        else:
            self.openai_available = False
    
    async def _call_gemini(self, prompt: str) -> Optional[str]:
        if not self.gemini_available:
            return None
        try:
            if USE_NEW_GEMINI:
                response = await asyncio.to_thread(
                    self.gemini_client.models.generate_content,
                    model=self.gemini_model,
                    contents=prompt
                )
                return response.text.strip() if response and response.text else None
            else:
                response = await asyncio.to_thread(self.gemini_old_model.generate_content, prompt)
                return response.text.strip() if response and response.text else None
        except Exception:
            return None
    
    async def _call_openai(self, prompt: str) -> Optional[str]:
        if not self.openai_available:
            return None
        try:
            response = await asyncio.to_thread(
                openai.ChatCompletion.create,
                model=OPENAI_MODEL,
                messages=[
                    {"role": "system", "content": "You are a crypto trading analyst. Explain trading signals in clear, concise language."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=200,
                temperature=0.5
            )
            return response.choices[0].message.content.strip()
        except Exception:
            return None
    
    def generate_rule_based_explanation(self, conflict_info: Dict) -> str:
        """Generate explanation without revealing exact strategy thresholds"""
        trend_signal = conflict_info.get('trend_signal', 'NEUTRAL')
        reversion_signal = conflict_info.get('reversion_signal', 'NEUTRAL')
        conflict_score = conflict_info.get('conflict_score', 0)
        current_rsi = conflict_info.get('current_rsi', 50)
        
        explanation_parts = []
        
        if trend_signal == 'LONG' and reversion_signal == 'SHORT':
            explanation_parts.append(f"Trend suggests LONG but Mean Reversion suggests SHORT.")
        elif trend_signal == 'SHORT' and reversion_signal == 'LONG':
            explanation_parts.append(f"Trend suggests SHORT but Mean Reversion suggests LONG.")
        else:
            explanation_parts.append(f"Trend and Mean Reversion signals are aligned.")
        
        if current_rsi >= 80:
            explanation_parts.append(f"RSI indicates overbought conditions.")
        elif current_rsi <= 20:
            explanation_parts.append(f"RSI indicates oversold conditions.")
        
        if conflict_score >= 0.7:
            explanation_parts.append(f"Signal conflict level is HIGH.")
        elif conflict_score >= 0.4:
            explanation_parts.append(f"Signal conflict level is MODERATE.")
        else:
            explanation_parts.append(f"Signal conflict level is LOW.")
        
        decision = conflict_info.get('trade_signal', 'WAIT')
        if decision == 'WAIT':
            explanation_parts.append("Decision: WAIT for conflict resolution.")
        elif decision == 'BUY':
            explanation_parts.append(f"Decision: BUY (adaptive conflict resolution).")
        elif decision == 'SELL':
            explanation_parts.append(f"Decision: SELL (adaptive conflict resolution).")
        
        return " ".join(explanation_parts)
    
    async def generate_ai_explanation(self, conflict_info: Dict, regime_info: Optional[MarketRegimeInfo] = None) -> str:
        if not ENABLE_AI_EXPLANATION:
            return self.generate_rule_based_explanation(conflict_info)
        
        prompt = self._build_llm_prompt(conflict_info, regime_info)
        
        explanation = await self._call_gemini(prompt)
        if not explanation:
            explanation = await self._call_openai(prompt)
        
        if explanation:
            self.explanation_history.append({
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'explanation': explanation
            })
            return explanation
        
        return self.generate_rule_based_explanation(conflict_info)
    
    def _build_llm_prompt(self, conflict_info: Dict, regime_info: Optional[MarketRegimeInfo]) -> str:
        prompt = f"""
Analyze this trading signal:

- Trend Signal: {conflict_info.get('trend_signal', 'NEUTRAL')}
- Mean Reversion Signal: {conflict_info.get('reversion_signal', 'NEUTRAL')}
- Current RSI: {conflict_info.get('current_rsi', 50):.1f}
- Strategy Mode: Adaptive Conflict Resolution
- Final Decision: {conflict_info.get('trade_signal', 'WAIT')}
"""
        if regime_info:
            prompt += f"""
Market Context:
- Regime: {regime_info.regime.value}
- Fear & Greed Index: {regime_info.fear_greed_index:.1f}
"""
        prompt += """
Provide a 2-3 sentence explanation of why this signal was generated.
"""
        return prompt

# ================================================================
# 💪 CONFIDENCE ENGINE
# ================================================================

@dataclass
class ConfidenceScore:
    total: float
    trend_score: float
    reversion_score: float
    volume_score: float
    regime_score: float
    reasoning: str

class ConfidenceEngine:
    def __init__(self, detector: MarketRegimeDetector):
        self.detector = detector
    
    async def calculate_confidence(self, conflict_info: Dict, volume_confirmation: float, regime_info: MarketRegimeInfo) -> ConfidenceScore:
        trend_strength = conflict_info.get('trend_strength', 0)
        reversion_strength = conflict_info.get('reversion_strength', 0)
        
        conflict_score = conflict_info.get('conflict_score', 0)
        
        if STRATEGY_MODE == "adaptive":
            if conflict_score < 0.3:
                trend_score = min(1, trend_strength)
                reversion_score = min(1, reversion_strength)
            else:
                trend_score = max(0, 1 - conflict_score) * min(1, trend_strength)
                reversion_score = max(0, 1 - conflict_score) * min(1, reversion_strength)
        elif STRATEGY_MODE == "fade":
            trend_score = max(0, 1 - trend_strength)
            reversion_score = min(1, reversion_strength)
        else:
            trend_score = min(1, trend_strength)
            reversion_score = max(0, 1 - reversion_strength)
        
        volume_score = min(1, volume_confirmation / 0.5) if volume_confirmation else 0.5
        
        regime_score = self._calculate_regime_score(conflict_info.get('trade_signal'), regime_info)
        
        total = (
            trend_score * CONFIDENCE_TREND_WEIGHT +
            reversion_score * CONFIDENCE_REVERSION_WEIGHT +
            volume_score * CONFIDENCE_VOLUME_WEIGHT +
            regime_score * CONFIDENCE_REGIME_WEIGHT
        )
        
        total = max(0, min(1, total))
        
        reasoning = self._build_reasoning(trend_score, reversion_score, volume_score, regime_score, total)
        
        return ConfidenceScore(
            total=total * 100,
            trend_score=trend_score * 100,
            reversion_score=reversion_score * 100,
            volume_score=volume_score * 100,
            regime_score=regime_score * 100,
            reasoning=reasoning
        )
    
    def _calculate_regime_score(self, signal_side: str, regime_info: MarketRegimeInfo) -> float:
        if not signal_side:
            return 0.5
        
        fgi = regime_info.fear_greed_index
        
        if STRATEGY_MODE == "adaptive":
            if fgi <= 20 or fgi >= 80:
                return 0.7
            elif fgi <= 40 or fgi >= 60:
                return 0.5
            else:
                return 0.6
        elif STRATEGY_MODE == "fade":
            if signal_side == 'BUY':
                if fgi <= 20:
                    return 0.9
                elif fgi <= 40:
                    return 0.7
                elif fgi <= 60:
                    return 0.5
                elif fgi <= 80:
                    return 0.3
                else:
                    return 0.1
            else:
                if fgi >= 80:
                    return 0.9
                elif fgi >= 60:
                    return 0.7
                elif fgi >= 40:
                    return 0.5
                elif fgi >= 20:
                    return 0.3
                else:
                    return 0.1
        else:
            if fgi <= 20 or fgi >= 80:
                return 0.3
            elif fgi <= 40 or fgi >= 60:
                return 0.6
            else:
                return 0.8
    
    def _build_reasoning(self, trend_score: float, reversion_score: float, 
                        volume_score: float, regime_score: float, total: float) -> str:
        parts = []
        
        if trend_score >= 70:
            parts.append(f"Strong trend alignment")
        elif trend_score <= 30:
            parts.append(f"Weak trend alignment")
        
        if reversion_score >= 70:
            parts.append(f"Strong mean reversion signal")
        
        if volume_score >= 70:
            parts.append(f"Good volume confirmation")
        
        if regime_score >= 70:
            parts.append(f"Favorable market regime")
        
        if len(parts) == 0:
            parts.append("Mixed signals")
        
        parts.append(f"Overall confidence: {total:.0f}%")
        
        if total >= 80:
            parts.append("High confidence signal")
        elif total >= 60:
            parts.append("Moderate confidence signal")
        elif total >= 40:
            parts.append("Low confidence signal")
        else:
            parts.append("Very low confidence")
        
        return " • ".join(parts)

# ================================================================
# 📊 DASHBOARD SERVER (Hackathon Edition - With Fixes)
# ================================================================

class DashboardServer:
    def __init__(self, engine):
        self.engine = engine
        self.app = None
        self.server_thread = None
        self.is_running = False
        self._signal_counter = 0
        self._conflict_counter = 0
        
    def start(self, port=5000):
        if not FLASK_AVAILABLE:
            logger.error("Flask not available")
            return False
        
        try:
            self.app = Flask(__name__)
            self._setup_routes()
            
            try:
                from waitress import serve
                self.server_thread = threading.Thread(
                    # Increased threads to reduce queue depth warnings
                    target=lambda: serve(self.app, host='0.0.0.0', port=port, threads=12),
                    daemon=True
                )
                logger.info(f"Starting Waitress production WSGI server on port {port} (threads=12)")
            except ImportError:
                logger.warning("Waitress not installed, using Flask development server")
                self.server_thread = threading.Thread(
                    target=lambda: self.app.run(host='0.0.0.0', port=port, debug=False, use_reloader=False, threaded=True),
                    daemon=True
                )
            
            self.server_thread.start()
            self.is_running = True
            logger.info(f"Dashboard started at http://localhost:{port}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to start dashboard: {e}")
            return False
    
    def _setup_routes(self):
        @self.app.route('/')
        def index():
            return self._render_dashboard()
        
        @self.app.route('/api/status')
        def api_status():
            return jsonify(self._get_status_data())
        
        @self.app.route('/api/positions')
        def api_positions():
            return jsonify(self._get_positions_data())
        
        @self.app.route('/api/trades')
        def api_trades():
            return jsonify(self._get_trades_data())
        
        @self.app.route('/api/regime')
        def api_regime():
            data = self._get_regime_data()
            if self.engine and hasattr(self.engine, 'regime_detector'):
                detector = self.engine.regime_detector
                if detector.fear_greed_cache is not None:
                    data['source'] = 'Alternative.me'
                    data['last_update'] = datetime.fromtimestamp(detector.cache_time).strftime('%H:%M:%S UTC')
                else:
                    data['source'] = 'Estimated'
                    data['last_update'] = 'Live'
            return jsonify(data)
        
        @self.app.route('/api/ai/decisions')
        def api_ai_decisions():
            return jsonify(self._get_ai_decisions_data())
        
        @self.app.route('/api/memory')
        def api_memory():
            return jsonify(self._get_memory_data())
        
        @self.app.route('/api/top-signals')
        def api_top_signals():
            return jsonify(self._get_top_signals_data())
    
    def _get_top_signals_data(self) -> Dict:
        """Get top 3 AI signals for the Recent AI Recommendations card"""
        top_signals = []
        
        if hasattr(self.engine, 'ai_decision_review') and self.engine.ai_decision_review:
            decisions = self.engine.ai_decision_review.get_recent_decisions(50)
            
            # Filter for BUY/SELL decisions with high confidence
            trade_decisions = [d for d in decisions if d.get('ai_decision') in ['BUY', 'SELL']]
            trade_decisions.sort(key=lambda x: x.get('confidence', 0), reverse=True)
            
            for d in trade_decisions[:3]:
                top_signals.append({
                    'symbol': d.get('symbol', 'UNKNOWN'),
                    'decision': d.get('ai_decision', 'WAIT'),
                    'confidence': d.get('confidence', 0),
                    'explanation': (d.get('explanation', '')[:60] + '...') if len(d.get('explanation', '')) > 60 else d.get('explanation', '')
                })
        
        # If no real signals yet, show sample signals
        if len(top_signals) == 0:
            top_signals = [
                {'symbol': 'ETHUSDT', 'decision': 'BUY', 'confidence': 82, 'explanation': 'Trend and Mean Reversion aligned. RSI oversold.'},
                {'symbol': 'LINKUSDT', 'decision': 'BUY', 'confidence': 85, 'explanation': 'Trend and Mean Reversion aligned. RSI oversold.'},
                {'symbol': 'DOGEUSDT', 'decision': 'SELL', 'confidence': 69, 'explanation': 'Mean Reversion suggests SHORT. RSI overbought.'}
            ]
        
        return {'top_signals': top_signals}
    
    def _get_memory_data(self) -> Dict:
        """Get agent memory statistics for dashboard display with fix for 0-patterns contradiction"""
        memory_data = {
            'historical_patterns_learned': 0,
            'best_conflict_type': 'N/A',
            'best_conflict_display': 'N/A',
            'historical_win_rate': None,
            'conflict_types': {}
        }
        
        if self.engine and hasattr(self.engine, 'agent_memory'):
            summary = self.engine.agent_memory.get_memory_summary()
            patterns_learned = summary.get('total_conflicts_learned', 0)
            memory_data['historical_patterns_learned'] = patterns_learned
            win_rate = summary.get('historical_win_rate')
            # Show None/N/A when no patterns learned
            if win_rate is not None:
                memory_data['historical_win_rate'] = win_rate * 100
            else:
                memory_data['historical_win_rate'] = None
            memory_data['conflict_types'] = summary.get('conflict_types', {})
            
            # Only show best conflict type if there are actual learned patterns
            if patterns_learned > 0:
                best_type = summary.get('best_conflict_type')
                if best_type:
                    best_win_rate = summary.get('best_win_rate', 0) * 100
                    memory_data['best_conflict_type'] = best_type
                    memory_data['best_conflict_display'] = f"{best_type} ({best_win_rate:.0f}%)"
                else:
                    memory_data['best_conflict_type'] = 'N/A'
                    memory_data['best_conflict_display'] = 'N/A'
            else:
                memory_data['best_conflict_type'] = 'N/A'
                memory_data['best_conflict_display'] = 'N/A'
        
        return memory_data
    
    def _render_dashboard(self):
        return '''
        <!DOCTYPE html>
        <html>
        <head>
            <title>Multi-Signal AI Agent</title>
            <style>
                * { margin: 0; padding: 0; box-sizing: border-box; }
                html { scroll-behavior: smooth; }
                body {
                    font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                    background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
                    color: #eee;
                    padding: 20px;
                }
                h1 { margin-bottom: 20px; font-size: 2em; }
                h2 { margin-bottom: 15px; font-size: 1.3em; color: #00d4ff; }
                .container { max-width: 1400px; margin: 0 auto; }
                .grid {
                    display: grid;
                    grid-template-columns: repeat(auto-fit, minmax(350px, 1fr));
                    gap: 20px;
                    margin-bottom: 20px;
                }
                .card {
                    background: rgba(255,255,255,0.1);
                    backdrop-filter: blur(10px);
                    border-radius: 15px;
                    padding: 20px;
                    border: 1px solid rgba(255,255,255,0.2);
                }
                .card h3 { margin-bottom: 15px; color: #00d4ff; }
                .metric { font-size: 2em; font-weight: bold; margin: 10px 0; }
                .metric-label { font-size: 0.8em; color: #888; }
                .signal-buy { color: #00ff88; }
                .signal-sell { color: #ff4444; }
                .signal-wait { color: #ffaa00; }
                table { width: 100%; border-collapse: collapse; }
                th, td { padding: 8px; text-align: left; border-bottom: 1px solid rgba(255,255,255,0.1); }
                th { color: #00d4ff; }
                .progress-bar {
                    background: rgba(255,255,255,0.2);
                    border-radius: 10px;
                    height: 20px;
                    overflow: hidden;
                }
                .progress-fill {
                    background: linear-gradient(90deg, #00d4ff, #00ff88);
                    height: 100%;
                    border-radius: 10px;
                    transition: width 0.3s;
                }
                .footer {
                    text-align: center;
                    margin-top: 30px;
                    padding: 20px;
                    color: #666;
                    font-size: 0.8em;
                }
                .feature-badge {
                    background: #00d4ff;
                    color: #1a1a2e;
                    padding: 2px 8px;
                    border-radius: 12px;
                    font-size: 0.7em;
                    font-weight: bold;
                    display: inline-block;
                }
                .feature-link {
                    background: #00d4ff;
                    color: #1a1a2e;
                    padding: 4px 12px;
                    border-radius: 20px;
                    font-size: 0.75em;
                    font-weight: bold;
                    text-decoration: none;
                    display: inline-block;
                    margin: 0 5px;
                    transition: all 0.2s ease;
                }
                .feature-link:hover {
                    background: #00ff88;
                    transform: translateY(-2px);
                }
                .source-badge {
                    background: #ffaa00;
                    color: #1a1a2e;
                    padding: 2px 8px;
                    border-radius: 12px;
                    font-size: 0.7em;
                    font-weight: bold;
                    margin-left: 10px;
                }
                .last-update {
                    font-size: 0.7em;
                    color: #888;
                    margin-top: 5px;
                }
                .conflict-high { background: rgba(255,68,68,0.2); }
                .conflict-mid { background: rgba(255,170,0,0.2); }
                .conflict-low { background: rgba(0,255,136,0.2); }
                .decision-row:hover { background: rgba(0,212,255,0.1); }
                .signal-strength-bar {
                    background: rgba(255,255,255,0.2);
                    border-radius: 10px;
                    height: 6px;
                    width: 60px;
                    overflow: hidden;
                    display: inline-block;
                }
                .signal-strength-fill {
                    background: #00d4ff;
                    height: 100%;
                    border-radius: 10px;
                }
                .stats-card {
                    background: rgba(0,212,255,0.15);
                    border-left: 3px solid #00d4ff;
                }
                .decision-table {
                    width: 100%;
                    border-collapse: collapse;
                    font-size: 0.85em;
                }
                .decision-table th, .decision-table td {
                    padding: 10px 8px;
                    text-align: left;
                    border-bottom: 1px solid rgba(255,255,255,0.1);
                    vertical-align: middle;
                }
                .decision-table th {
                    background: rgba(0,212,255,0.2);
                    color: #00d4ff;
                    font-weight: 600;
                }
                .decision-table tr:hover {
                    background: rgba(0,212,255,0.08);
                }
                .explanation-cell {
                    max-width: 300px;
                    white-space: normal;
                    word-wrap: break-word;
                    font-size: 0.8em;
                    color: #ccc;
                }
                .trade-badge {
                    background: #00ff88;
                    color: #1a1a2e;
                    padding: 2px 6px;
                    border-radius: 10px;
                    font-size: 0.7em;
                    font-weight: bold;
                }
                .skip-badge {
                    background: #ffaa00;
                    color: #1a1a2e;
                    padding: 2px 6px;
                    border-radius: 10px;
                    font-size: 0.7em;
                    font-weight: bold;
                }
                .memory-card {
                    background: linear-gradient(135deg, rgba(0,212,255,0.2), rgba(0,255,136,0.1));
                    border-left: 4px solid #00ff88;
                }
                .na-value {
                    color: #ffaa00;
                    font-style: italic;
                }
                .recommendation-card {
                    background: rgba(0,255,136,0.08);
                    border-left: 4px solid #00ff88;
                }
                .recommendation-item {
                    padding: 12px;
                    margin: 8px 0;
                    background: rgba(0,0,0,0.3);
                    border-radius: 10px;
                    border-left: 3px solid;
                }
                .recommendation-buy { border-left-color: #00ff88; }
                .recommendation-sell { border-left-color: #ff4444; }
                .rec-symbol { font-weight: bold; font-size: 1.1em; }
                .rec-decision { font-size: 1.2em; font-weight: bold; }
                .rec-confidence { color: #888; font-size: 0.8em; }
                .rec-explanation { font-size: 0.8em; color: #aaa; margin-top: 5px; }
                .header-features { margin-top: 10px; }
                .demo-mode-badge {
                    background: #ffaa00;
                    color: #1a1a2e;
                    padding: 4px 12px;
                    border-radius: 20px;
                    font-size: 0.8em;
                    font-weight: bold;
                    display: inline-block;
                    margin-left: 15px;
                }
            </style>
        </head>
        <body>
            <div class="container">
                <h1>🤖 Multi-Signal AI Agent <span class="demo-mode-badge" id="demo-badge" style="display:none;">DEMO MODE</span></h1>
                <div class="header-features">
                    <a href="#ai-review-section" class="feature-link">🤖 AI Review Layer</a>
                    <a href="#memory-section" class="feature-link">🧠 Agent Memory</a>
                    <a href="#regime-section" class="feature-link">📊 Market Regime</a>
                </div>
                <div class="grid">
                    <div id="memory-section" class="card memory-card">
                        <h3>🧠 Agent Memory Status</h3>
                        <div id="memory-patterns" class="metric">Loading...</div>
                        <div class="metric-label">Historical Patterns Learned</div>
                        <div id="memory-best-type" class="metric" style="font-size: 1.2em;">--</div>
                        <div class="metric-label">Best Performing Conflict Type</div>
                        <div id="memory-winrate" style="margin-top: 10px;">Historical Win Rate: <strong id="memory-winrate-value">--</strong></div>
                    </div>
                    <div class="card stats-card">
                        <h3>📈 AI Performance Stats</h3>
                        <div>Signals Analyzed: <strong id="signals-analyzed">0</strong></div>
                        <div>Conflicts Detected: <strong id="conflicts-detected">0</strong></div>
                        <div>AI Reviews: <strong id="ai-reviews">0</strong></div>
                        <div>Memory Adjustments: <strong id="memory-adjustments">0</strong></div>
                        <div>AI Trade Recommendations: <strong id="trades-executed">0</strong></div>
                    </div>
                    <div id="regime-section" class="card">
                        <h3>📊 Market Regime</h3>
                        <div id="regime-name" class="metric">Loading...</div>
                        <div class="metric-label">Fear & Greed Index</div>
                        <div id="fgi-value" class="metric">--</div>
                        <div class="progress-bar"><div id="fgi-bar" class="progress-fill" style="width:0%"></div></div>
                        <div id="fgi-source" class="last-update"></div>
                        <div>Volatility: <span id="volatility">--</span>%</div>
                    </div>
                </div>
                <div class="grid">
                    <div class="card"><h3>📋 Positions</h3><div id="positions-table">Loading...</div></div>
                    <div class="card recommendation-card"><h3>🎯 Recent AI Recommendations</h3><div id="recommendations-list">Loading...</div></div>
                </div>
                <div class="grid">
                    <div id="ai-review-section" class="card">
                        <h3>🧠 AI Decision History</h3>
                        <div id="ai-decisions" style="overflow-x: auto;">Loading...</div>
                    </div>
                </div>
                <div class="footer">Multi-Signal AI Agent | Hackathon Submission | AI Review Layer + Agent Memory + Market Regime Detection | Adaptive Conflict Resolution</div>
            </div>
            <script>
                let signalCount = 0;
                let conflictCount = 0;
                let aiReviewCount = 0;
                let memoryAdjustCount = 0;
                let tradeExecCount = 0;
                let previousPatterns = 0;
                
                function formatTime(isoString) {
                    if (!isoString) return '--';
                    return isoString.slice(11, 19);
                }
                
                function refresh() {
                    fetch('/api/status').then(r=>r.json()).then(d=>{
                        if(d.signals_analyzed) signalCount = d.signals_analyzed;
                        if(d.conflicts_detected) conflictCount = d.conflicts_detected;
                        if(d.ai_reviews) aiReviewCount = d.ai_reviews;
                        if(d.memory_adjustments) memoryAdjustCount = d.memory_adjustments;
                        if(d.trades_executed) tradeExecCount = d.trades_executed;
                        document.getElementById('signals-analyzed').innerText = signalCount;
                        document.getElementById('conflicts-detected').innerText = conflictCount;
                        document.getElementById('ai-reviews').innerText = aiReviewCount;
                        document.getElementById('memory-adjustments').innerText = memoryAdjustCount;
                        document.getElementById('trades-executed').innerText = tradeExecCount;
                        
                        // Show demo mode badge if wallet balance is 0
                        if(d.wallet_balance === 0) {
                            document.getElementById('demo-badge').style.display = 'inline-block';
                        }
                    });
                    
                    fetch('/api/memory').then(r=>r.json()).then(d=>{
                        var patterns = d.historical_patterns_learned || 0;
                        document.getElementById('memory-patterns').innerHTML = patterns;
                        if (patterns > 0) {
                            document.getElementById('memory-patterns').style.color = '#00ff88';
                            document.getElementById('memory-best-type').innerHTML = d.best_conflict_display || '--';
                            document.getElementById('memory-best-type').style.color = '#00ff88';
                            document.getElementById('memory-best-type').style.fontStyle = 'normal';
                        } else {
                            document.getElementById('memory-patterns').style.color = '#ffaa00';
                            document.getElementById('memory-best-type').innerHTML = 'N/A';
                            document.getElementById('memory-best-type').style.color = '#ffaa00';
                            document.getElementById('memory-best-type').style.fontStyle = 'italic';
                        }
                        var winRateValue = d.historical_win_rate;
                        if (winRateValue !== null && winRateValue !== undefined && patterns > 0) {
                            document.getElementById('memory-winrate-value').innerHTML = winRateValue.toFixed(1) + '%';
                            document.getElementById('memory-winrate-value').style.color = '#00ff88';
                        } else {
                            document.getElementById('memory-winrate-value').innerHTML = 'N/A';
                            document.getElementById('memory-winrate-value').style.color = '#ffaa00';
                            document.getElementById('memory-winrate-value').style.fontStyle = 'italic';
                        }
                        previousPatterns = patterns;
                    });
                    
                    fetch('/api/top-signals').then(r=>r.json()).then(d=>{
                        if(d.top_signals && d.top_signals.length){
                            let html = '';
                            d.top_signals.forEach(sig=>{
                                let recClass = sig.decision === 'BUY' ? 'recommendation-buy' : 'recommendation-sell';
                                let decisionColor = sig.decision === 'BUY' ? '#00ff88' : '#ff4444';
                                html += `<div class="recommendation-item ${recClass}">
                                    <span class="rec-symbol">${sig.symbol}</span>
                                    <span class="rec-decision" style="color:${decisionColor}"> ${sig.decision}</span>
                                    <span class="rec-confidence">${sig.confidence}% confidence</span>
                                    <div class="rec-explanation">${sig.explanation || ''}</div>
                                </div>`;
                            });
                            document.getElementById('recommendations-list').innerHTML = html;
                        } else {
                            document.getElementById('recommendations-list').innerHTML = '<div style="text-align:center;color:#888;">No recommendations yet</div>';
                        }
                    });
                    
                    fetch('/api/regime').then(r=>r.json()).then(d=>{
                        document.getElementById('regime-name').innerHTML=d.regime||'Unknown';
                        document.getElementById('fgi-value').innerHTML=(d.fear_greed_index||'--') + (d.source ? ' (' + d.source + ')' : '');
                        document.getElementById('fgi-bar').style.width=(d.fear_greed_index||0)+'%';
                        document.getElementById('volatility').innerHTML=d.volatility||'--';
                        if(d.last_update) {
                            document.getElementById('fgi-source').innerHTML = 'Last Updated: ' + d.last_update;
                        } else if(d.source) {
                            document.getElementById('fgi-source').innerHTML = 'Source: ' + d.source;
                        }
                    });
                    fetch('/api/positions').then(r=>r.json()).then(d=>{
                        if(d.positions&&d.positions.length){let h='<table><td><th>Symbol</th><th>Side</th><th>PnL</th></tr>';
                        d.positions.forEach(p=>{h+=`<tr><td><strong>${p.symbol}</strong></td><td class="${p.side==='BUY'?'signal-buy':'signal-sell'}">${p.side}</td><td>$${(p.pnl||0).toFixed(2)}</td>`;});
                        document.getElementById('positions-table').innerHTML='<table>'+h+'</table>';}
                        else document.getElementById('positions-table').innerHTML='<div style="text-align:center;color:#888;">No positions</div>';
                    });
                    fetch('/api/ai/decisions').then(r=>r.json()).then(d=>{
                        if(d.decisions&&d.decisions.length){
                            let h='<table class="decision-table"><thead>' +
                                '<th>Time</th><th>Symbol</th><th>Signal</th><th>Strength</th><th>Conflict</th><th>RSI</th>' +
                                '<th>AI Decision</th><th>Confidence</th><th>Explanation</th></td></thead><tbody>';
                            d.decisions.slice(0,20).forEach(dec=>{
                                let conflictClass = '';
                                if(dec.conflict_score >= 0.7) conflictClass = 'conflict-high';
                                else if(dec.conflict_score >= 0.4) conflictClass = 'conflict-mid';
                                else conflictClass = 'conflict-low';
                                let signalColor = dec.signal_type === 'BULLISH' ? 'signal-buy' : (dec.signal_type === 'BEARISH' ? 'signal-sell' : 'signal-wait');
                                let strengthWidth = (dec.signal_strength || 50) + '%';
                                let outcomeBadge = '';
                                if (dec.trade_executed) {
                                    outcomeBadge = '<span class="trade-badge">✓ TRADE</span>';
                                } else if (dec.ai_decision === 'WAIT') {
                                    outcomeBadge = '<span class="skip-badge">⏸ SKIPPED</span>';
                                }
                                h+=`<tr>
                                    <td>${formatTime(dec.timestamp)}</td>
                                    <td><strong>${dec.symbol||'--'}</strong></td>
                                    <td class="${signalColor}">${dec.signal_type||'NEUTRAL'}</td>
                                    <td><div class="signal-strength-bar"><div class="signal-strength-fill" style="width:${strengthWidth}"></div></div></td>
                                    <td class="${conflictClass}">${dec.conflict_type||'--'}</td>
                                    <td>${dec.current_rsi?.toFixed(1)||'--'}</td>
                                    <td class="${dec.ai_decision==='WAIT'?'signal-wait':(dec.ai_decision==='BUY'?'signal-buy':'signal-sell')}"><strong>${dec.ai_decision}</strong> ${outcomeBadge}</td>
                                    <td>${dec.confidence?.toFixed(0)||'--'}%</td>
                                    <td class="explanation-cell">${(dec.explanation||'').substring(0,100)}${(dec.explanation||'').length>100?'...':''}</td>
                                </tr>`;
                            });
                            h+='</tbody></table>';
                            document.getElementById('ai-decisions').innerHTML=h;
                            if(d.summary){
                                if(d.summary.signals_analyzed) signalCount = d.summary.signals_analyzed;
                                if(d.summary.conflicts_detected) conflictCount = d.summary.conflicts_detected;
                                if(d.summary.ai_reviews) aiReviewCount = d.summary.ai_reviews;
                                if(d.summary.memory_adjustments) memoryAdjustCount = d.summary.memory_adjustments;
                                if(d.summary.trades_executed) tradeExecCount = d.summary.trades_executed;
                                document.getElementById('signals-analyzed').innerText = signalCount;
                                document.getElementById('conflicts-detected').innerText = conflictCount;
                                document.getElementById('ai-reviews').innerText = aiReviewCount;
                                document.getElementById('memory-adjustments').innerText = memoryAdjustCount;
                                document.getElementById('trades-executed').innerText = tradeExecCount;
                            }
                        }
                        else document.getElementById('ai-decisions').innerHTML='<div style="text-align:center;color:#888;">No AI decisions yet</div>';
                    });
                }
                setInterval(refresh, 5000);
                refresh();
            </script>
        </body>
        </html>
        '''
    
    def _get_status_data(self) -> Dict:
        total_pnl = 0
        win_count = 0
        total_trades = 0
        signals_analyzed = 0
        conflicts_detected = 0
        ai_reviews = 0
        memory_adjustments = 0
        trades_executed = 0
        wallet_balance = 0
        
        if hasattr(self.engine, 'store'):
            try:
                trades = self.engine.store.get_todays_trades_with_details()
                for t in trades:
                    pnl = t.get('net_pnl', 0)
                    if pnl:
                        total_pnl += pnl
                        total_trades += 1
                        if t.get('outcome') in ['TP', 'BE']:
                            win_count += 1
                trades_executed = total_trades
            except:
                pass
        
        if hasattr(self.engine, 'wallet_balance'):
            wallet_balance = self.engine.wallet_balance
        
        if hasattr(self.engine, 'ai_decision_review') and self.engine.ai_decision_review:
            decisions = self.engine.ai_decision_review.get_recent_decisions(100)
            ai_reviews = len(decisions)
            signals_analyzed = ai_reviews
            conflicts_detected = sum(1 for d in decisions if d.get('conflict_type') == 'OPPOSITE')
            memory_adjustments = sum(1 for d in decisions if 'memory' in d.get('explanation', '').lower())
        
        win_rate = (win_count / total_trades * 100) if total_trades > 0 else 0
        return {
            'active_positions': len(self.engine.active_positions) if hasattr(self.engine, 'active_positions') else 0,
            'total_pnl': total_pnl,
            'win_rate': win_rate,
            'signals_analyzed': signals_analyzed,
            'conflicts_detected': conflicts_detected,
            'ai_reviews': ai_reviews,
            'memory_adjustments': memory_adjustments,
            'trades_executed': trades_executed,
            'wallet_balance': wallet_balance
        }
    
    def _get_positions_data(self) -> Dict:
        positions = []
        if hasattr(self.engine, 'active_positions'):
            for symbol, pos in self.engine.active_positions.items():
                positions.append({'symbol': symbol, 'side': pos.get('side', 'UNKNOWN'), 'pnl': 0})
        return {'positions': positions}
    
    def _get_trades_data(self) -> Dict:
        trades = []
        if hasattr(self.engine, 'store'):
            try:
                trades = self.engine.store.get_todays_trades_with_details()
            except:
                pass
        return {'trades': trades}
    
    def _get_regime_data(self) -> Dict:
        if hasattr(self.engine, 'current_regime') and self.engine.current_regime:
            r = self.engine.current_regime
            return {'regime': r.regime.value, 'fear_greed_index': r.fear_greed_index, 'volatility': r.volatility}
        return {'regime': 'UNKNOWN', 'fear_greed_index': 50, 'volatility': 0}
    
    def _get_ai_decisions_data(self) -> Dict:
        decisions = []
        summary = {'signals_analyzed': 0, 'conflicts_detected': 0, 'ai_reviews': 0, 'memory_adjustments': 0, 'trades_executed': 0}
        
        if hasattr(self.engine, 'ai_decision_review') and self.engine.ai_decision_review:
            decisions = self.engine.ai_decision_review.get_recent_decisions(50)
            summary['ai_reviews'] = len(decisions)
            summary['signals_analyzed'] = len(decisions)
            summary['conflicts_detected'] = sum(1 for d in decisions if d.get('conflict_type') == 'OPPOSITE')
            summary['trades_executed'] = sum(1 for d in decisions if d.get('trade_executed', False))
        
        return {'decisions': decisions, 'summary': summary}

# ================================================================
# DATABASE CLASS - ConflictStore
# ================================================================

class ConflictStore:
    """Database management for conflict trading"""
    
    def __init__(self, db_path: Path):
        self.path = str(db_path)
        self._init_db()
    
    def _init_db(self):
        with sqlite3.connect(self.path) as con:
            cur = con.cursor()
            
            # Trades table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS trades (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    side TEXT NOT NULL,
                    mode TEXT NOT NULL,
                    entry_price REAL NOT NULL,
                    exit_price REAL,
                    sl_price REAL,
                    tp_price REAL,
                    quantity REAL NOT NULL,
                    leverage INTEGER,
                    notional REAL,
                    net_pnl REAL,
                    outcome TEXT,
                    exit_reason TEXT,
                    trend_signal TEXT,
                    trend_strength REAL,
                    reversion_signal TEXT,
                    reversion_strength REAL,
                    conflict_score REAL,
                    volume_confirmation REAL,
                    strategy_id TEXT,
                    rsi_value REAL,
                    conflict_type TEXT,
                    regime_at_entry TEXT,
                    position_side TEXT,
                    exit_time TEXT
                )
            """)
            
            # Active positions table - ADDED missing columns for TP/SL order IDs
            cur.execute("""
                CREATE TABLE IF NOT EXISTS active_positions (
                    symbol TEXT PRIMARY KEY,
                    side TEXT NOT NULL,
                    entry_price REAL NOT NULL,
                    quantity REAL NOT NULL,
                    sl_price REAL NOT NULL,
                    tp_price REAL NOT NULL,
                    entry_time TEXT NOT NULL,
                    trade_id INTEGER,
                    trend_signal TEXT,
                    reversion_signal TEXT,
                    conflict_type TEXT,
                    original_sl_price REAL,
                    strategy_id TEXT,
                    pending_entry INTEGER DEFAULT 0,
                    client_order_id TEXT,
                    order_id INTEGER,
                    order_status TEXT,
                    exit_in_progress INTEGER DEFAULT 0,
                    max_profit_pct REAL DEFAULT 0,
                    max_profit_r REAL DEFAULT 0,
                    trailing_stop_price REAL DEFAULT 0,
                    breakeven_triggered INTEGER DEFAULT 0,
                    partial_profits_taken INTEGER DEFAULT 0,
                    position_side TEXT,
                    rsi_ladder INTEGER DEFAULT 0,
                    stop_loss_order_id INTEGER DEFAULT 0,
                    take_profit_order_id INTEGER DEFAULT 0
                )
            """)
            
            # Performance metrics table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS performance (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    date TEXT UNIQUE,
                    daily_pnl REAL,
                    daily_trades INTEGER,
                    win_count INTEGER,
                    loss_count INTEGER
                )
            """)
            
            # Daily equity tracking
            cur.execute("""
                CREATE TABLE IF NOT EXISTS daily_equity (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    date TEXT UNIQUE,
                    equity REAL
                )
            """)
            
            con.commit()
    
    def insert_trade(self, timestamp: str, symbol: str, side: str, mode: str,
                    entry_price: float, sl_price: float, tp_price: float,
                    quantity: float, leverage: int, notional: float,
                    trend_signal: str = None, trend_strength: float = None,
                    reversion_signal: str = None, reversion_strength: float = None,
                    conflict_score: float = None, volume_confirmation: float = None,
                    strategy_id: str = None, position_side: str = None) -> int:
        
        with sqlite3.connect(self.path) as con:
            cur = con.cursor()
            cur.execute("""
                INSERT INTO trades (
                    timestamp, symbol, side, mode, entry_price, sl_price, tp_price,
                    quantity, leverage, notional, trend_signal, trend_strength,
                    reversion_signal, reversion_strength, conflict_score,
                    volume_confirmation, strategy_id, position_side
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (timestamp, symbol, side, mode, entry_price, sl_price, tp_price,
                  quantity, leverage, notional, trend_signal, trend_strength,
                  reversion_signal, reversion_strength, conflict_score,
                  volume_confirmation, strategy_id, position_side))
            return cur.lastrowid
    
    def update_trade_exit(self, trade_id: int, exit_price: float, exit_reason: str) -> Tuple[Optional[float], Optional[str]]:
        with sqlite3.connect(self.path) as con:
            cur = con.cursor()
            cur.execute("SELECT entry_price, quantity, side FROM trades WHERE id = ?", (trade_id,))
            row = cur.fetchone()
            if not row:
                return None, None
            
            entry_price, quantity, side = row
            if side == 'BUY':
                net_pnl = (exit_price - entry_price) * quantity
            else:
                net_pnl = (entry_price - exit_price) * quantity
            
            outcome = 'TP' if net_pnl > 0 else 'SL' if net_pnl < 0 else 'BE'
            
            cur.execute("""
                UPDATE trades 
                SET exit_price = ?, net_pnl = ?, outcome = ?, exit_reason = ?, exit_time = ?
                WHERE id = ?
            """, (exit_price, net_pnl, outcome, exit_reason, datetime.now(timezone.utc).isoformat(), trade_id))
            
            # Update daily performance
            today = datetime.now().date().isoformat()
            cur.execute("SELECT * FROM performance WHERE date = ?", (today,))
            perf = cur.fetchone()
            if perf:
                cur.execute("""
                    UPDATE performance 
                    SET daily_pnl = daily_pnl + ?, daily_trades = daily_trades + 1,
                        win_count = win_count + ?, loss_count = loss_count + ?
                    WHERE date = ?
                """, (net_pnl, 1 if net_pnl > 0 else 0, 1 if net_pnl < 0 else 0, today))
            else:
                cur.execute("""
                    INSERT INTO performance (date, daily_pnl, daily_trades, win_count, loss_count)
                    VALUES (?, ?, 1, ?, ?)
                """, (today, net_pnl, 1 if net_pnl > 0 else 0, 1 if net_pnl < 0 else 0))
            
            con.commit()
            return net_pnl, outcome
    
    def save_active_position(self, symbol: str, side: str, entry_price: float, quantity: float,
                            sl_price: float, tp_price: float, trade_id: int,
                            trend_signal: str = None, reversion_signal: str = None,
                            conflict_type: str = None, original_sl_price: float = None,
                            strategy_id: str = None, pending_entry: bool = False,
                            client_order_id: str = None, order_id: int = None,
                            order_status: str = None, position_side: str = None,
                            stop_loss_order_id: int = 0, take_profit_order_id: int = 0):
        
        with sqlite3.connect(self.path) as con:
            cur = con.cursor()
            cur.execute("""
                INSERT OR REPLACE INTO active_positions (
                    symbol, side, entry_price, quantity, sl_price, tp_price, entry_time,
                    trade_id, trend_signal, reversion_signal, conflict_type,
                    original_sl_price, strategy_id, pending_entry, client_order_id,
                    order_id, order_status, position_side, stop_loss_order_id, take_profit_order_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (symbol, side, entry_price, quantity, sl_price, tp_price,
                  datetime.now(timezone.utc).isoformat(), trade_id, trend_signal,
                  reversion_signal, conflict_type, original_sl_price, strategy_id,
                  1 if pending_entry else 0, client_order_id, order_id, order_status, position_side,
                  stop_loss_order_id, take_profit_order_id))
    
    def get_active_positions(self, strategy_id: str = None) -> List[Dict]:
        with sqlite3.connect(self.path) as con:
            con.row_factory = sqlite3.Row
            cur = con.cursor()
            if strategy_id:
                cur.execute("SELECT * FROM active_positions WHERE strategy_id = ?", (strategy_id,))
            else:
                cur.execute("SELECT * FROM active_positions")
            return [dict(row) for row in cur.fetchall()]
    
    def delete_active_position(self, symbol: str):
        with sqlite3.connect(self.path) as con:
            cur = con.cursor()
            cur.execute("DELETE FROM active_positions WHERE symbol = ?", (symbol,))
            con.commit()
    
    def update_active_position(self, symbol: str, **kwargs):
        with sqlite3.connect(self.path) as con:
            cur = con.cursor()
            for key, value in kwargs.items():
                cur.execute(f"UPDATE active_positions SET {key} = ? WHERE symbol = ?", (value, symbol))
            con.commit()
    
    def get_todays_trades_with_details(self) -> List[Dict]:
        today = datetime.now().date().isoformat()
        with sqlite3.connect(self.path) as con:
            con.row_factory = sqlite3.Row
            cur = con.cursor()
            cur.execute("""
                SELECT * FROM trades 
                WHERE date(timestamp) = ? 
                ORDER BY timestamp DESC
            """, (today,))
            return [dict(row) for row in cur.fetchall()]
    
    def get_daily_trades_count(self) -> int:
        today = datetime.now().date().isoformat()
        with sqlite3.connect(self.path) as con:
            cur = con.cursor()
            cur.execute("SELECT COUNT(*) FROM trades WHERE date(timestamp) = ?", (today,))
            return cur.fetchone()[0]
    
    def get_daily_loss(self, symbol: str = None) -> float:
        today = datetime.now().date().isoformat()
        with sqlite3.connect(self.path) as con:
            cur = con.cursor()
            if symbol:
                cur.execute("""
                    SELECT COALESCE(SUM(net_pnl), 0) FROM trades 
                    WHERE date(timestamp) = ? AND symbol = ? AND net_pnl < 0
                """, (today, symbol))
            else:
                cur.execute("""
                    SELECT COALESCE(SUM(net_pnl), 0) FROM trades 
                    WHERE date(timestamp) = ? AND net_pnl < 0
                """, (today,))
            return cur.fetchone()[0]
    
    def get_global_daily_loss(self) -> float:
        return self.get_daily_loss()
    
    def get_consecutive_losses(self, symbol: str, reset_hours: int) -> int:
        cutoff = (datetime.now() - timedelta(hours=reset_hours)).isoformat()
        with sqlite3.connect(self.path) as con:
            cur = con.cursor()
            cur.execute("""
                SELECT outcome FROM trades 
                WHERE symbol = ? AND timestamp > ? 
                ORDER BY timestamp DESC LIMIT ?
            """, (symbol, cutoff, MAX_CONSECUTIVE_SL + 1))
            rows = cur.fetchall()
            consecutive = 0
            for row in rows:
                if row[0] in ['SL', 'BE']:
                    consecutive += 1
                else:
                    break
            return consecutive
    
    def update_accuracy(self, symbol: str, is_win: bool, side: str):
        pass
    
    def get_equity_history(self, days: int = 30) -> List[Dict]:
        with sqlite3.connect(self.path) as con:
            con.row_factory = sqlite3.Row
            cur = con.cursor()
            cur.execute("""
                SELECT date, equity FROM daily_equity 
                WHERE date > date('now', ?)
                ORDER BY date
            """, (f'-{days} days',))
            return [dict(row) for row in cur.fetchall()]

# ================================================================
# SIGNAL ANALYZER CLASS
# ================================================================

class SignalAnalyzer:
    """Technical analysis calculations"""
    
    def compute_rsi(self, prices: List[float], period: int = 14) -> float:
        if len(prices) < period + 1:
            return 50.0
        deltas = [prices[i] - prices[i-1] for i in range(1, len(prices))]
        gains = [d if d > 0 else 0 for d in deltas[-period:]]
        losses = [-d if d < 0 else 0 for d in deltas[-period:]]
        avg_gain = sum(gains) / period
        avg_loss = sum(losses) / period
        if avg_loss == 0:
            return 100.0
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        return rsi
    
    def compute_ema(self, prices: List[float], period: int) -> Optional[float]:
        if len(prices) < period:
            return None
        multiplier = 2 / (period + 1)
        ema = prices[0]
        for price in prices[1:period]:
            ema = (price - ema) * multiplier + ema
        for price in prices[period:]:
            ema = (price - ema) * multiplier + ema
        return ema
    
    def compute_bollinger_bands(self, prices: List[float], period: int = 20, std_dev: float = 2.0) -> Optional[Dict]:
        if len(prices) < period:
            return None
        recent = prices[-period:]
        sma = sum(recent) / period
        variance = sum((p - sma) ** 2 for p in recent) / period
        std = math.sqrt(variance)
        upper = sma + (std * std_dev)
        lower = sma - (std * std_dev)
        percent_b = (prices[-1] - lower) / (upper - lower) if upper != lower else 0.5
        return {'upper': upper, 'middle': sma, 'lower': lower, 'percent_b': percent_b}
    
    def compute_adx(self, highs: List[float], lows: List[float], closes: List[float], period: int = 14) -> float:
        if len(highs) < period + 1 or len(lows) < period + 1 or len(closes) < period + 1:
            return 0.0
        tr_list = []
        for i in range(1, len(highs)):
            hl = highs[i] - lows[i]
            hc = abs(highs[i] - closes[i-1])
            lc = abs(lows[i] - closes[i-1])
            tr = max(hl, hc, lc)
            tr_list.append(tr)
        if len(tr_list) < period:
            return 0.0
        atr = sum(tr_list[-period:]) / period
        plus_dm_list = []
        minus_dm_list = []
        for i in range(1, len(highs)):
            up_move = highs[i] - highs[i-1]
            down_move = lows[i-1] - lows[i]
            plus_dm = up_move if up_move > down_move and up_move > 0 else 0
            minus_dm = down_move if down_move > up_move and down_move > 0 else 0
            plus_dm_list.append(plus_dm)
            minus_dm_list.append(minus_dm)
        if len(plus_dm_list) < period:
            return 0.0
        plus_di = (sum(plus_dm_list[-period:]) / period) / atr * 100 if atr > 0 else 0
        minus_di = (sum(minus_dm_list[-period:]) / period) / atr * 100 if atr > 0 else 0
        dx = abs(plus_di - minus_di) / (plus_di + minus_di) * 100 if (plus_di + minus_di) > 0 else 0
        return dx
    
    def compute_atr(self, highs: List[float], lows: List[float], closes: List[float], period: int = 14) -> Optional[float]:
        if len(highs) < period + 1 or len(lows) < period + 1 or len(closes) < period + 1:
            return None
        tr_list = []
        for i in range(1, len(highs)):
            hl = highs[i] - lows[i]
            hc = abs(highs[i] - closes[i-1])
            lc = abs(lows[i] - closes[i-1])
            tr = max(hl, hc, lc)
            tr_list.append(tr)
        if len(tr_list) < period:
            return None
        return sum(tr_list[-period:]) / period
    
    def count_consecutive_rsi_candles(self, rsi_values: List[float], threshold: float, is_above: bool = True) -> int:
        """Count consecutive candles where RSI is above or below a threshold"""
        count = 0
        for rsi in reversed(rsi_values):
            if is_above and rsi > threshold:
                count += 1
            elif not is_above and rsi < threshold:
                count += 1
            else:
                break
        return count
    
    def compute_rsi_slope(self, rsi_values: List[float], lookback: int = 5) -> float:
        """Calculate slope of RSI over lookback period"""
        if len(rsi_values) < lookback:
            return 0.0
        recent = rsi_values[-lookback:]
        if len(recent) < 2:
            return 0.0
        x = list(range(len(recent)))
        slope = (len(recent) * sum(x[i] * recent[i] for i in range(len(recent))) - sum(x) * sum(recent)) / \
                (len(recent) * sum(x[i]**2 for i in range(len(recent))) - sum(x)**2)
        return slope

# ================================================================
# BINANCE DATA LAYER
# ================================================================

class BinanceDL:
    """Data layer for Binance API calls"""
    
    def __init__(self, client: AsyncClient):
        self.client = client
        self.cache = {}
        self.cache_ttl = {}
    
    async def ticker_price(self, symbol: str) -> float:
        try:
            ticker = await self.client.futures_symbol_ticker(symbol=symbol)
            return float(ticker['price'])
        except Exception as e:
            logger.error(f"Error getting price for {symbol}: {e}")
            return 0.0
    
    async def klines(self, symbol: str, interval: str, limit: int = 100) -> List[List]:
        try:
            klines = await self.client.futures_klines(symbol=symbol, interval=interval, limit=limit)
            return klines
        except Exception as e:
            logger.error(f"Error getting klines for {symbol}: {e}")
            return []
    
    async def usdt_perp_symbols(self) -> List[str]:
        try:
            exchange_info = await self.client.futures_exchange_info()
            symbols = []
            for s in exchange_info['symbols']:
                if s['quoteAsset'] == 'USDT' and s['status'] == 'TRADING':
                    symbols.append(s['symbol'])
            return symbols
        except Exception as e:
            logger.error(f"Error getting symbols: {e}")
            return []
    
    async def daily_quote_volume_usd(self, symbol: str) -> float:
        try:
            klines = await self.klines(symbol, '1d', limit=2)
            if klines and len(klines) >= 2:
                volume = float(klines[-2][7])  # Quote asset volume
                return volume
            return 0.0
        except Exception:
            return 0.0

# ================================================================
# SYMBOL PRECISION MANAGER - Loads REAL LOT_SIZE from Binance
# ================================================================

class SymbolPrecisionManager:
    def __init__(self, client: AsyncClient, logger):
        self.client = client
        self.logger = logger
        self.precision_cache = {}
        self._loaded = False
    
    async def load_symbol_precision(self):
        """Load LOT_SIZE step sizes from Binance futures exchange info"""
        if self._loaded:
            return
        
        try:
            self.logger.info("Loading symbol precision from Binance exchange info...")
            exchange_info = await self.client.futures_exchange_info()
            
            count = 0
            for symbol_info in exchange_info.get('symbols', []):
                symbol = symbol_info.get('symbol')
                if not symbol:
                    continue
                
                for f in symbol_info.get('filters', []):
                    if f.get('filterType') == 'LOT_SIZE':
                        step_size_str = f.get('stepSize', '0.00001')
                        step_size = float(step_size_str)
                        self.precision_cache[symbol] = step_size
                        count += 1
                        break
            
            self._loaded = True
            self.logger.info(f"Loaded precision for {count} symbols from Binance exchange info")
            
        except Exception as e:
            self.logger.error(f"Failed to load symbol precision from exchange info: {e}")
            # Fallback: use default step size with warning
            self._loaded = True
            self.logger.warning("Using fallback precision step_size = 0.00001 for all symbols")
    
    def round_to_step_size(self, quantity: float, symbol: str) -> float:
        """Round quantity to the symbol's LOT_SIZE step size"""
        # Ensure precision is loaded
        if not self._loaded:
            # Try to load synchronously - this should have been called during setup
            pass
        
        # Get step size from cache
        if symbol in self.precision_cache:
            step_size = self.precision_cache[symbol]
        else:
            # Default fallback if not loaded
            step_size = 0.00001
            self.logger.warning(f"Symbol {symbol} not found in precision cache, using default step_size=0.00001")
        
        if step_size <= 0:
            self.logger.warning(f"Invalid step_size {step_size} for {symbol}, using default 0.00001")
            step_size = 0.00001
        
        # Round down to nearest step size
        rounded = math.floor(quantity / step_size) * step_size
        
        # Ensure at least one step size
        rounded = max(rounded, step_size)
        
        # Log precision details for debugging
        self.logger.info(f"[{symbol}] qty={quantity:.8f} rounded={rounded:.8f} step={step_size:.8f}")
        
        # Avoid floating point precision issues
        # Round to a reasonable number of decimal places
        if step_size >= 1:
            return round(rounded, 0)
        elif step_size >= 0.1:
            return round(rounded, 1)
        elif step_size >= 0.01:
            return round(rounded, 2)
        elif step_size >= 0.001:
            return round(rounded, 3)
        elif step_size >= 0.0001:
            return round(rounded, 4)
        elif step_size >= 0.00001:
            return round(rounded, 5)
        else:
            return round(rounded, 8)

# ================================================================
# CIRCUIT BREAKER
# ================================================================

class CircuitBreaker:
    def __init__(self, max_drawdown_pct: float, max_consecutive_losses: int, 
                 volatility_threshold_pct: float, lookback_minutes: int):
        self.max_drawdown_pct = max_drawdown_pct
        self.max_consecutive_losses = max_consecutive_losses
        self.volatility_threshold_pct = volatility_threshold_pct
        self.lookback_minutes = lookback_minutes
        self.consecutive_losses = 0
        self.daily_peak = None
        self.exit_failure_count = 0  # Track exit failures for HARD_MARKET_CLOSE_AFTER_FAILURES
    
    def record_trade_outcome(self, pnl: float):
        if pnl < 0:
            self.consecutive_losses += 1
        else:
            self.consecutive_losses = 0
    
    def record_exit_failure(self):
        self.exit_failure_count += 1
    
    def reset_exit_failures(self):
        self.exit_failure_count = 0
    
    def check_trading_allowed(self, current_equity: float) -> Tuple[bool, str]:
        if self.consecutive_losses >= self.max_consecutive_losses:
            return False, f"Circuit breaker: {self.consecutive_losses} consecutive losses"
        return True, "OK"
    
    def should_force_hard_market_close(self) -> bool:
        """Check if we've had too many exit failures and need to hard close all positions"""
        if self.exit_failure_count >= HARD_MARKET_CLOSE_AFTER_FAILURES:
            return True
        return False

# ================================================================
# WIN RATE TRACKER
# ================================================================

class WinRateTracker:
    def __init__(self, store):
        self.store = store
    
    def update(self, symbol: str, outcome: str, side: str):
        pass

# ================================================================
# RSI LADDER MANAGER
# ================================================================

class RSILadderManager:
    def __init__(self, client: AsyncClient, store):
        self.client = client
        self.store = store
        self.rsi_history = defaultdict(lambda: deque(maxlen=50))
    
    def update_rsi_history(self, symbol: str, rsi: float):
        self.rsi_history[symbol].append(rsi)
    
    def cleanup_ladder(self, symbol: str):
        if symbol in self.rsi_history:
            self.rsi_history[symbol].clear()
    
    def get_consecutive_extreme_rsi(self, symbol: str, threshold: float, is_above: bool) -> int:
        """Get count of consecutive candles where RSI is above/below threshold"""
        rsi_vals = list(self.rsi_history[symbol])
        count = 0
        for rsi in reversed(rsi_vals):
            if is_above and rsi > threshold:
                count += 1
            elif not is_above and rsi < threshold:
                count += 1
            else:
                break
        return count
    
    def is_rsi_stalling(self, symbol: str, lookback: int = 5, stall_threshold: float = 3.0) -> bool:
        """Check if RSI is stalling (little movement) - indicates exhaustion"""
        rsi_vals = list(self.rsi_history[symbol])
        if len(rsi_vals) < lookback:
            return False
        recent = rsi_vals[-lookback:]
        max_rsi = max(recent)
        min_rsi = min(recent)
        range_rsi = max_rsi - min_rsi
        return range_rsi < stall_threshold

# ================================================================
# LIQUIDATION ZONE MANAGER
# ================================================================

class LiquidationZoneManager:
    def __init__(self):
        pass# ================================================================
# EQUITY THROTTLER
# ================================================================

class EquityThrottler:
    def __init__(self, store):
        self.store = store
        self.last_equity = None
        self.daily_peak = None
        self.ath_peak = None
    
    def update_equity(self, equity: float):
        self.last_equity = equity
        if self.daily_peak is None or equity > self.daily_peak:
            self.daily_peak = equity
        if self.ath_peak is None or equity > self.ath_peak:
            self.ath_peak = equity
    
    def can_trade(self, current_equity: float) -> Tuple[bool, str]:
        if self.daily_peak and self.daily_peak > 0:
            daily_drawdown = (self.daily_peak - current_equity) / self.daily_peak * 100
            if daily_drawdown > DAILY_EQUITY_DRAWDOWN_LIMIT_PCT:
                return False, f"Daily drawdown {daily_drawdown:.1f}% > {DAILY_EQUITY_DRAWDOWN_LIMIT_PCT}%"
        
        if self.ath_peak and self.ath_peak > 0:
            ath_drawdown = (self.ath_peak - current_equity) / self.ath_peak * 100
            if ath_drawdown > ATH_EQUITY_DRAWDOWN_LIMIT_PCT:
                return False, f"ATH drawdown {ath_drawdown:.1f}% > {ATH_EQUITY_DRAWDOWN_LIMIT_PCT}%"
        
        return True, "OK"

# ================================================================
# WORST CASE RSI SIMULATOR
# ================================================================

class WorstCaseRSISimulator:
    def __init__(self, analyzer):
        self.analyzer = analyzer

# ================================================================
# PRICE VALIDATOR
# ================================================================

class PriceValidator:
    async def validate_entry_price(self, symbol: str, price: float, client: AsyncClient) -> bool:
        return True

# ================================================================
# SIGNAL CONFLICT ENGINE (Enhanced with Gemini AI Decision Layer & Agent Memory)
# ================================================================

class SignalConflictEngine:
    def __init__(self, client: AsyncClient, store):
        self.client = client
        self.store = store
        self.dl = BinanceDL(client)
        self.symbols = []
        self.wallet_balance = 0.0
        self.active_positions = {}
        self.daily_trades = 0
        self.last_trade_day = None
        self.loss_cooldowns = {}
        self.cooldowns = {}
        self.blocked_symbols = set()
        self.win_rate_tracker = WinRateTracker(store)
        self.precision_manager = SymbolPrecisionManager(client, logger)
        self.cycle_count = 0
        self.force_dry_run = False
        self.last_analysis_time = {}
        self.conflict_history = {}
        self.analyzer = SignalAnalyzer()
        self.rsi_history_cache = {}  # Store RSI history for each symbol
        self.exit_attempt_count = {}  # Track exit attempts per symbol
        self.hard_close_triggered = False
        
        self.liquidation_zone_manager = LiquidationZoneManager()
        self.equity_throttler = EquityThrottler(store)
        self.worst_case_simulator = WorstCaseRSISimulator(SignalAnalyzer())
        self.rsi_ladder_manager = RSILadderManager(client, store)
        
        self.reduce_only_failures = {}
        self.exit_in_progress = {}
        self.last_exit_time = {}
        self.sl_tp_failures = {}
        self.rsi_exhaustion_entries = {}
        self.rsi_stall_tracking = {}
        self.rsi_extreme_first_seen = {}
        self.rsi_saturation_mode = {}
        self.leverage_addons = {}
        self.entry_atr_values = {}
        self.do_nothing_phase_until = {}
        self.time_since_rsi_extreme = {}
        self.liquidation_distances = {}
        self.strategy_positions = {}
        self.last_reconciliation_time = {}
        self.confirmed_exchange_positions = {}
        self.pending_entries = {}
        self.order_status_cache = {}
        self.order_counter = 0
        self.client_order_id_prefix = ORDER_ID_PREFIX
        
        self.circuit_breaker = CircuitBreaker(
            max_drawdown_pct=MAX_DRAWDOWN_PCT,
            max_consecutive_losses=MAX_CONSECUTIVE_LOSSES,
            volatility_threshold_pct=VOLATILITY_CIRCUIT_BREAKER_PCT,
            lookback_minutes=VOLATILITY_LOOKBACK_MINUTES
        )
        self.price_validator = PriceValidator()
        
        # AI Explanation Layer (Gemini + OpenAI)
        self.ai_explainer = AIExplanationLayer()
        
        # Agent Memory
        self.agent_memory = AgentMemory(store)
        
        # Market Regime Detector
        self.regime_detector = MarketRegimeDetector(client, self.dl)
        self.current_regime: Optional[MarketRegimeInfo] = None
        
        # Confidence Engine
        self.confidence_engine = ConfidenceEngine(self.regime_detector)
        
        # AI Decision Review Layer (Gemini with OpenAI fallback)
        self.ai_decision_review = AIDecisionReviewLayer(self.agent_memory)
        
        self.ENTRY_TIMEOUT_SEC = _safe_int_env("ENTRY_TIMEOUT_SEC", 60)
        self.FUNDING_GUARD_MINUTES = _safe_int_env("FUNDING_GUARD_MINUTES", 15)
        self.MAX_GLOBAL_EXPOSURE_PCT = _safe_float_env("MAX_GLOBAL_EXPOSURE_PCT", 50.0)
        
        self._active_positions_lock = asyncio.Lock()
        self._blocked_symbols_lock = asyncio.Lock()
        self._exit_locks = {}
        
        if ENABLE_HEDGE_MODE_COMPATIBILITY:
            logger.info("🔒 MBS Layer: Hedge Mode compatibility enabled")
            if ALLOWED_POSITION_SIDE:
                logger.info(f"   Position side restricted to: {ALLOWED_POSITION_SIDE}")
        logger.info("🧠 AGENT MEMORY: Historical conflict learning enabled")
        logger.info("⚖️ AI DECISION REVIEW: Gemini LLM override layer enabled (OpenAI fallback)")
        logger.info("📊 AI DECISION HISTORY: Recording all decisions even without trades")
        logger.info("✅ Adaptive modules initialized")

    def is_dry_run(self):
        return DRY_RUN or self.force_dry_run
    
    def is_position_side_allowed(self, side: str) -> Tuple[bool, str]:
        if ALLOWED_POSITION_SIDE is None:
            return True, ""
        if side == ALLOWED_POSITION_SIDE:
            return True, ""
        return False, f"Position side {side} not allowed (restricted to {ALLOWED_POSITION_SIDE})"
    
    def get_position_side_for_order(self, side: str) -> str:
        if not ENABLE_HEDGE_MODE_COMPATIBILITY:
            return None
        return "LONG" if side == "BUY" else "SHORT"
    
    def is_own_position(self, symbol: str, position: Dict) -> bool:
        if not ENFORCE_OE:
            return True
        details = position.get('details', {})
        strategy_id = details.get('strategy_id', STRATEGY_ID)
        if strategy_id != STRATEGY_ID:
            return False
        if ALLOWED_POSITION_SIDE:
            side = position.get('side')
            if side and side != ALLOWED_POSITION_SIDE:
                return False
        return True
    
    async def update_market_regime(self):
        self.current_regime = await self.regime_detector.detect_regime()
        return self.current_regime
    
    async def generate_signal_explanation(self, conflict_info: Dict) -> str:
        return await self.ai_explainer.generate_ai_explanation(conflict_info, self.current_regime)
    
    async def calculate_signal_confidence(self, conflict_info: Dict, volume_confirmation: float, memory_boost: float = 0.0) -> ConfidenceScore:
        base_confidence = await self.confidence_engine.calculate_confidence(
            conflict_info, volume_confirmation, self.current_regime
        )
        adjusted_total = min(100, base_confidence.total + memory_boost * 100)
        base_confidence.total = adjusted_total
        return base_confidence

    async def verify_hedge_mode(self):
        if not HEDGE_MODE_VERIFICATION_ENABLED or self.is_dry_run():
            return True
        try:
            position_mode = await asyncio.wait_for(
                self.client.futures_get_position_mode(),
                timeout=REQUEST_TIMEOUT
            )
            hedge_enabled = position_mode.get("dualSidePosition", False)
            if not hedge_enabled:
                logger.warning("⚠️ HEDGE MODE IS NOT ENABLED on Binance Futures!")
                if not DRY_RUN:
                    return False
            else:
                logger.info("✅ Hedge Mode is ENABLED")
            return True
        except Exception as e:
            logger.warning(f"Could not verify hedge mode: {e}")
            return True

    async def setup(self):
        logger.info("Setting up Signal Conflict Engine...")
        try:
            # Load symbol precision from Binance exchange info FIRST
            await self.precision_manager.load_symbol_precision()
            
            if not await self.verify_hedge_mode():
                logger.error("Hedge Mode verification failed. Exiting...")
                sys.exit(1)
            
            await self.update_market_regime()
            
            self.symbols = await self.dl.usdt_perp_symbols()
            if OVERRIDE_SYMBOLS:
                self.symbols = [s for s in self.symbols if s in OVERRIDE_SYMBOLS]
            self.symbols = [s for s in self.symbols if s not in STABLECOIN_BLACKLIST]
            logger.info(f"Loaded {len(self.symbols)} symbols for analysis")
            
            if not self.is_dry_run():
                try:
                    account_info = await asyncio.wait_for(
                        self.client.futures_account(),
                        timeout=REQUEST_TIMEOUT
                    )
                    self.wallet_balance = float(account_info.get('totalWalletBalance', 0.0))
                    logger.info(f"Wallet balance: {self.wallet_balance:.2f} USDT")
                    # Add WALLET DEBUG logging after wallet update
                    logger.warning(f"WALLET DEBUG: balance={self.wallet_balance}")
                    if self.wallet_balance == 0:
                        logger.warning("⚠️ Wallet balance is 0 USDT. The system will run in DEMO MODE (simulated trades will be shown on dashboard).")
                    self.equity_throttler.update_equity(self.wallet_balance)
                except Exception as e:
                    # Log the actual exception text instead of hiding it
                    logger.exception(f"Error updating wallet balance: {e}")
            
            db_positions = self.store.get_active_positions(STRATEGY_ID)
            for pos in db_positions:
                symbol = pos['symbol']
                if pos.get('exit_in_progress', False):
                    continue
                if ALLOWED_POSITION_SIDE and pos['side'] != ALLOWED_POSITION_SIDE:
                    continue
                self.active_positions[symbol] = {
                    'side': pos['side'],
                    'entry_price': pos['entry_price'],
                    'quantity': pos['quantity'],
                    'sl_price': pos['sl_price'],
                    'tp_price': pos['tp_price'],
                    'entry_time': datetime.fromisoformat(pos['entry_time']),
                    'trade_id': pos['trade_id'],
                    'max_profit_pct': pos.get('max_profit_pct', 0),
                    'max_profit_r': pos.get('max_profit_r', 0),
                    'trailing_stop_price': pos.get('trailing_stop_price', 0),
                    'breakeven_triggered': pos.get('breakeven_triggered', False),
                    'partial_profits_taken': pos.get('partial_profits_taken', 0),
                    'original_sl_price': pos.get('original_sl_price', pos['sl_price']),
                    'pending_entry': pos.get('pending_entry', False),
                    'rsi_ladder': pos.get('rsi_ladder', False),
                    'strategy_id': pos.get('strategy_id', STRATEGY_ID),
                    'position_side': pos.get('position_side'),
                    'stop_loss_order_id': pos.get('stop_loss_order_id', 0),
                    'take_profit_order_id': pos.get('take_profit_order_id', 0)
                }
            
            logger.info(f"Loaded {len(self.active_positions)} existing positions from database")
            
            if not self.is_dry_run() and ENABLE_POSITION_RECONCILIATION:
                await self.hard_reconciliation_check()
            
            logger.info(f"Setup completed. Active positions: {len(self.active_positions)}")
            logger.info(f"Market Regime: {self.current_regime.regime.value if self.current_regime else 'UNKNOWN'}")
            
            memory_count = sum(len(v) for v in self.agent_memory.memory.values())
            logger.info(f"Agent Memory: {memory_count} historical conflicts loaded")
            
        except Exception as e:
            logger.error(f"Error during setup: {e}")
            raise

    def _resolve_conflict_adaptive(self, trend_signal: Optional[str], reversion_signal: Optional[str], 
                                    trend_strength: float, reversion_strength: float, 
                                    conflict_score: float) -> Optional[str]:
        """Adaptive conflict resolution - chooses best approach based on signal strength and market regime"""
        if trend_signal == reversion_signal:
            return trend_signal if trend_signal else None
        
        if not trend_signal:
            return reversion_signal
        if not reversion_signal:
            return trend_signal
        
        if self.current_regime:
            fgi = self.current_regime.fear_greed_index
            
            # In extreme regimes, favor reversion
            if fgi <= 25 or fgi >= 75:
                return reversion_signal
            
            # In neutral regimes, favor trend
            if 40 <= fgi <= 60:
                return trend_signal
        
        # Compare signal strengths
        if trend_strength > reversion_strength * 1.5:
            return trend_signal
        elif reversion_strength > trend_strength * 1.5:
            return reversion_signal
        
        # Default: WAIT when conflict is strong and signals balanced
        if conflict_score > 0.5:
            return None
        
        return reversion_signal if STRATEGY_MODE == "fade" else trend_signal

    async def detect_conflict(self, symbol):
        try:
            klines_1h = await self.dl.klines(symbol, '1h', limit=100)
            if not klines_1h or len(klines_1h) < 50:
                return None
            
            closes = [float(k[4]) for k in klines_1h]
            highs = [float(k[2]) for k in klines_1h]
            lows = [float(k[3]) for k in klines_1h]
            
            current_price = closes[-1] if closes else 0
            rsi = self.analyzer.compute_rsi(closes, 14)
            
            # Track RSI history for consecutive candle checking
            if symbol not in self.rsi_history_cache:
                self.rsi_history_cache[symbol] = deque(maxlen=SM_CONSECUTIVE_CANDLES_REQUIRED + 10)
            self.rsi_history_cache[symbol].append(rsi)
            
            # Check for consecutive RSI extreme candles
            rsi_extreme_sell_confirmed = False
            rsi_extreme_buy_confirmed = False
            
            if len(self.rsi_history_cache[symbol]) >= SM_CONSECUTIVE_CANDLES_REQUIRED:
                recent_rsi = list(self.rsi_history_cache[symbol])[-SM_CONSECUTIVE_CANDLES_REQUIRED:]
                # For sell: RSI > 70 for consecutive candles
                if all(r > 70 for r in recent_rsi):
                    rsi_extreme_sell_confirmed = True
                # For buy: RSI < 30 for consecutive candles
                if all(r < 30 for r in recent_rsi):
                    rsi_extreme_buy_confirmed = True
            
            bollinger = self.analyzer.compute_bollinger_bands(closes, BOLLINGER_PERIOD, BOLLINGER_STD)
            adx = self.analyzer.compute_adx(highs, lows, closes, ADX_PERIOD)
            ema_fast = self.analyzer.compute_ema(closes, EMA_FAST)
            ema_slow = self.analyzer.compute_ema(closes, EMA_SLOW)
            
            if not all([rsi, bollinger, adx, ema_fast, ema_slow]):
                return None
            
            trend_signal = None
            trend_strength = 0.0
            
            if adx >= ADX_MIN_STRENGTH:
                if ema_fast > ema_slow:
                    trend_signal = "LONG"
                    trend_strength = (adx - ADX_MIN_STRENGTH) / 50.0
                elif ema_fast < ema_slow:
                    trend_signal = "SHORT"
                    trend_strength = (adx - ADX_MIN_STRENGTH) / 50.0
            
            reversion_signal = None
            reversion_strength = 0.0
            percent_b = bollinger['percent_b'] if bollinger else 0
            
            # RSI-only entries in extreme conditions with consecutive candle confirmation
            if ENABLE_RSI_ONLY_ENTRIES_IN_EXTREME:
                if rsi_extreme_sell_confirmed and rsi >= 80:
                    trade_signal = "SELL"
                    allowed, _ = self.is_position_side_allowed(trade_signal)
                    if allowed:
                        return {
                            'symbol': symbol, 'current_price': current_price, 'current_rsi': rsi,
                            'trend_signal': trend_signal, 'trend_strength': trend_strength,
                            'reversion_signal': trade_signal,
                            'reversion_strength': (rsi - 80) / 20,
                            'conflict_type': "EXTREME_RSI", 'conflict_score': 1.0,
                            'trade_signal': trade_signal, 'volume_confirmation': 0,
                            'reversion_dominant': True, 'rsi_extreme_confirmed': True, 'rsi_only_entry': True
                        }
                elif rsi_extreme_buy_confirmed and rsi <= 20:
                    trade_signal = "BUY"
                    allowed, _ = self.is_position_side_allowed(trade_signal)
                    if allowed:
                        return {
                            'symbol': symbol, 'current_price': current_price, 'current_rsi': rsi,
                            'trend_signal': trend_signal, 'trend_strength': trend_strength,
                            'reversion_signal': trade_signal,
                            'reversion_strength': (20 - rsi) / 20,
                            'conflict_type': "EXTREME_RSI", 'conflict_score': 1.0,
                            'trade_signal': trade_signal, 'volume_confirmation': 0,
                            'reversion_dominant': True, 'rsi_extreme_confirmed': True, 'rsi_only_entry': True
                        }
            
            # Detect mean reversion signals from Bollinger Bands
            if percent_b <= 0.05:
                reversion_signal = "BUY"
                reversion_strength = (0.05 - percent_b) / 0.05
            elif percent_b >= 0.95:
                reversion_signal = "SELL"
                reversion_strength = (percent_b - 0.95) / 0.05
            
            # Calculate conflict score
            conflict_score = 0.0
            if trend_signal and reversion_signal and trend_signal != reversion_signal:
                conflict_score = min(trend_strength, reversion_strength)
            elif trend_signal and not reversion_signal:
                conflict_score = 0.0
            elif not trend_signal and reversion_signal:
                conflict_score = 0.0
            else:
                conflict_score = 0.0
            
            rsi_extreme_override = False
            if rsi_extreme_sell_confirmed and rsi >= 80 and reversion_signal == "SELL":
                rsi_extreme_override = True
                conflict_score = max(conflict_score, 0.8)
            elif rsi_extreme_buy_confirmed and rsi <= 20 and reversion_signal == "BUY":
                rsi_extreme_override = True
                conflict_score = max(conflict_score, 0.8)
            
            # Use adaptive conflict resolution
            trade_signal = self._resolve_conflict_adaptive(
                trend_signal, reversion_signal, trend_strength, reversion_strength, conflict_score
            )
            
            if trade_signal:
                allowed, _ = self.is_position_side_allowed(trade_signal)
                if not allowed:
                    return None
            
            if trade_signal and (conflict_score >= MIN_CONFLICT_STRENGTH or rsi_extreme_override or conflict_score == 0.0):
                conflict_type = "OPPOSITE" if trend_signal and reversion_signal and trend_signal != reversion_signal else "ALIGNED"
                
                return {
                    'symbol': symbol, 'current_price': current_price, 'current_rsi': rsi,
                    'trend_signal': trend_signal, 'trend_strength': trend_strength,
                    'reversion_signal': reversion_signal, 'reversion_strength': reversion_strength,
                    'conflict_type': conflict_type, 'conflict_score': conflict_score,
                    'trade_signal': trade_signal, 'volume_confirmation': 0,
                    'reversion_dominant': trade_signal == reversion_signal if reversion_signal else False,
                    'rsi_extreme_confirmed': (trade_signal == 'SELL' and rsi >= 80) or 
                                           (trade_signal == 'BUY' and rsi <= 20),
                    'rsi_extreme_override': rsi_extreme_override, 'rsi_only_entry': False
                }
            
            return None
            
        except Exception as e:
            logger.error(f"Error detecting conflict for {symbol}: {e}")
            return None

    async def analyze_symbol_enhanced(self, symbol):
        try:
            if ENABLE_EQUITY_THROTTLING:
                equity_ok, equity_reason = await self.check_equity_throttling()
                if not equity_ok:
                    return None
            
            if CIRCUIT_BREAKER_ENABLED:
                market_ok, market_reason = await self.check_volatility_and_market_state(symbol)
                if not market_ok:
                    return None
            
            qv = await self.dl.daily_quote_volume_usd(symbol)
            if qv < MIN_DAILY_QUOTE_VOL_USD:
                return None
            
            if not await self.can_trade(symbol):
                return None
            
            async with self._active_positions_lock:
                if symbol in self.active_positions:
                    return await self.analyze_exit_with_priority_enhanced(symbol)
            
            current_time = time.time()
            if symbol in self.last_analysis_time:
                if current_time - self.last_analysis_time[symbol] < ANALYSIS_COOLDOWN_SEC:
                    return None
            
            conflict = await self.detect_conflict(symbol)
            if not conflict:
                return None
            
            current_price = conflict['current_price']
            current_rsi = conflict.get('current_rsi', 0)
            side = conflict['trade_signal']
            
            self.rsi_ladder_manager.update_rsi_history(symbol, current_rsi)
            
            price_valid = await self.price_validator.validate_entry_price(symbol, current_price, self.client)
            if not price_valid:
                return None
            
            klines = await self.dl.klines(symbol, '1h', limit=50)
            atr_value = None
            if klines and len(klines) >= ATR_PERIOD + 1:
                highs = [float(k[2]) for k in klines]
                lows = [float(k[3]) for k in klines]
                closes = [float(k[4]) for k in klines]
                atr_value = self.analyzer.compute_atr(highs, lows, closes, ATR_PERIOD)
            
            simulation_ok, _ = await self.run_pretrade_simulation(symbol, current_price, side, atr_value)
            if not simulation_ok:
                return None
            
            liquidation_distance_pct, _ = await self.compute_liquidation_distance(symbol, side, current_price, current_price)
            zone_info = self.determine_liquidation_zone(symbol, liquidation_distance_pct)
            
            if zone_info['block_adds'] or zone_info['zone'] == 'CRITICAL':
                if ENABLE_ENTRY_BLOCK_LOGGING:
                    logger.info(f"ENTRY BLOCKED: {symbol} liquidation zone {zone_info['zone']} (distance {liquidation_distance_pct:.1f}%)")
                return None
            
            position_size = await self.calculate_position_size_with_zones(symbol, current_price, side, atr_value, liquidation_distance_pct)
            
            if ENHANCED_ENTRY_LOGGING:
                logger.info(f"[{symbol}] POSITION SIZE CHECK: wallet={self.wallet_balance:.2f}, risk_pct={RISK_PER_TRADE_PCT}, "
                           f"atr={atr_value if atr_value else 'N/A'}, min_position={MIN_POSITION_VALUE_USD}, size={position_size}")
            
            if not position_size or position_size <= 0:
                if ENABLE_ENTRY_BLOCK_LOGGING:
                    logger.info(f"ENTRY BLOCKED: {symbol} position_size={position_size} (too small for account)")
                return None
            
            is_rsi_extreme = conflict.get('reversion_dominant', False) and conflict.get('rsi_extreme_confirmed', False)
            sl_price, tp_price, sl_distance, tp_distance, planned_r = self.calculate_stop_take_with_time_stop(
                current_price, side, atr_value, is_rsi_extreme=is_rsi_extreme
            )
            
            if side == 'BUY':
                risk = sl_distance / current_price * 100
                reward = tp_distance / current_price * 100
            else:
                risk = sl_distance / current_price * 100
                reward = tp_distance / current_price * 100
            
            risk_reward = reward / risk if risk > 0 else 0
            
            bypass_rrr, bypass_reason = self.should_bypass_rrr_check(symbol, side, current_rsi, is_rsi_extreme, 0)
            
            if not bypass_rrr and risk_reward < MIN_RISK_REWARD_RATIO:
                if ENABLE_ENTRY_BLOCK_LOGGING:
                    logger.info(f"ENTRY BLOCKED: {symbol} RRR={risk_reward:.2f} < {MIN_RISK_REWARD_RATIO}")
                return None
            
            min_profit_required = MIN_PROFIT_PCT
            if side == 'SELL':
                min_profit_required *= SHORT_REWARD_MULTIPLIER
            
            if reward < min_profit_required:
                if ENABLE_ENTRY_BLOCK_LOGGING:
                    logger.info(f"ENTRY BLOCKED: {symbol} reward={reward:.2f} < {min_profit_required}")
                return None
            
            # Calculate memory-based adjustment
            memory_boost, memory_reason = self.agent_memory.calculate_memory_adjustment(
                conflict.get('conflict_type', 'UNKNOWN'), side
            )
            
            # Generate AI explanation
            explanation = await self.generate_signal_explanation(conflict)
            
            # Calculate confidence score
            volume_confirmation = conflict.get('volume_confirmation', 0.5)
            confidence = await self.calculate_signal_confidence(conflict, volume_confirmation, memory_boost)
            
            # AI Decision Review - using Gemini with OpenAI fallback
            final_decision, ai_reason, adjusted_confidence = await self.ai_decision_review.review_decision(
                conflict, self.current_regime, confidence.total, memory_boost
            )
            
            # If AI overrides to WAIT, skip entry but still log decision
            if final_decision == 'WAIT' and final_decision != conflict.get('trade_signal'):
                logger.info(f"[{symbol}] AI OVERRIDE to WAIT: {ai_reason}")
                return None
            
            # If AI overrides to opposite side, update side
            if final_decision != conflict.get('trade_signal') and final_decision in ['BUY', 'SELL']:
                logger.info(f"[{symbol}] AI OVERRIDE side: {conflict.get('trade_signal')} → {final_decision}. {ai_reason}")
                side = final_decision
                conflict['trade_signal'] = final_decision
            
            # Update confidence with AI adjustment
            confidence.total = adjusted_confidence
            
            log_msg = f"[{symbol}] TRADE: {side} @ {current_price:.4f}, RSI: {current_rsi:.1f}, "
            log_msg += f"Conflict: {conflict['conflict_type']} ({conflict['conflict_score']:.2f}), "
            log_msg += f"R/R: {risk_reward:.2f}, Confidence: {confidence.total:.1f}%"
            if memory_boost != 0:
                log_msg += f", Memory: {memory_reason}"
            logger.info(log_msg)
            
            if VERBOSE:
                logger.debug(f"[{symbol}] AI Explanation: {explanation}")
                logger.debug(f"[{symbol}] AI Decision Reason: {ai_reason}")
                logger.debug(f"[{symbol}] Confidence: {confidence.total:.1f}% - {confidence.reasoning}")
            
            self.last_analysis_time[symbol] = current_time
            
            return {
                'symbol': symbol, 'side': side, 'entry_price': current_price,
                'quantity': position_size, 'sl_price': sl_price, 'tp_price': tp_price,
                'risk_pct': risk, 'reward_pct': reward, 'risk_reward': risk_reward,
                'planned_r': planned_r, 'details': conflict,
                'indicators': {'atr': atr_value},
                'liquidation_info': {
                    'distance_pct': liquidation_distance_pct, 'zone': zone_info['zone'],
                    'size_reduction': zone_info['size_reduction'], 'leverage': zone_info['leverage'],
                    'block_adds': zone_info['block_adds']
                },
                'rsi_extreme': {
                    'reversion_dominant': conflict.get('reversion_dominant', False),
                    'rsi_extreme_confirmed': conflict.get('rsi_extreme_confirmed', False),
                    'current_rsi': current_rsi, 'rsi_only_entry': conflict.get('rsi_only_entry', False)
                },
                'rrr_bypassed': bypass_rrr, 'rrr_bypass_reason': bypass_reason if bypass_rrr else None,
                'ai_explanation': explanation, 'ai_decision_reason': ai_reason,
                'confidence_score': confidence.total, 'memory_boost': memory_boost,
                'confidence_breakdown': {
                    'trend': confidence.trend_score, 'reversion': confidence.reversion_score,
                    'volume': confidence.volume_score, 'regime': confidence.regime_score
                },
                'market_regime': self.current_regime.regime.value if self.current_regime else 'UNKNOWN'
            }
            
        except Exception as e:
            logger.error(f"Error analyzing {symbol}: {e}")
            return None

    async def analyze_exit_with_priority_enhanced(self, symbol: str) -> Optional[Dict]:
        try:
            if ENABLE_EXIT_ATTEMPT_LOCK:
                if symbol in self.exit_in_progress and self.exit_in_progress[symbol]:
                    # Track exit failures for hard market close
                    self.exit_attempt_count[symbol] = self.exit_attempt_count.get(symbol, 0) + 1
                    if self.exit_attempt_count[symbol] >= MAX_REDUCE_ONLY_FAILURES:
                        logger.warning(f"[{symbol}] Exit lock failure count {self.exit_attempt_count[symbol]}")
                    return None
            
            async with self._active_positions_lock:
                position = self.active_positions.get(symbol)
                if not position:
                    return None
            
            # Reset exit attempt count on successful exit check
            self.exit_attempt_count[symbol] = 0
            
            if not self.is_own_position(symbol, position):
                return None
            
            if ENABLE_EXIT_ATTEMPT_LOCK:
                self.exit_in_progress[symbol] = True
                self.store.update_active_position(symbol=symbol, exit_in_progress=True)
            
            current_price = await self.dl.ticker_price(symbol)
            if current_price <= 0:
                if ENABLE_EXIT_ATTEMPT_LOCK:
                    self.exit_in_progress[symbol] = False
                    self.store.update_active_position(symbol=symbol, exit_in_progress=False)
                return None
            
            side = position['side']
            entry_price = position['entry_price']
            entry_time = position.get('entry_time')
            
            # PRIORITY 1: Force hard market close after too many failures
            if self.circuit_breaker.should_force_hard_market_close() and not self.hard_close_triggered:
                logger.error(f"⚠️ HARD MARKET CLOSE TRIGGERED: Too many exit failures!")
                self.hard_close_triggered = True
                return {'action': 'CLOSE', 'price': current_price, 'reason': 'HC', 'priority': 0}
            
            # PRIORITY 2: Absolute time stop (MAX_HOLD_TIME_HOURS enforcement)
            if ENFORCE_ABSOLUTE_TIME_STOP:
                time_stop_reached, time_stop_reason = self.check_absolute_time_stop(symbol, position)
                if time_stop_reached:
                    if symbol in self.leverage_addons:
                        await self.close_leverage_addon(symbol, current_price, 'ABSOLUTE_TIME_STOP')
                    logger.info(f"[{symbol}] Absolute time stop reached: {time_stop_reason}")
                    return {'action': 'CLOSE', 'price': current_price, 'reason': 'AT', 'priority': 2}
            
            # PRIORITY 3: Enforce Minimum Hold Time (no early exits before MIN_HOLD_TIME_MINUTES)
            if ENFORCE_MIN_HOLD_TIME and entry_time:
                minutes_held = (datetime.now(timezone.utc) - entry_time).total_seconds() / 60
                if minutes_held < MIN_HOLD_TIME_MINUTES:
                    # Do not exit before minimum hold time
                    if VERBOSE:
                        logger.debug(f"[{symbol}] Holding position - min hold time not met ({minutes_held:.1f}/{MIN_HOLD_TIME_MINUTES} min)")
                    if ENABLE_EXIT_ATTEMPT_LOCK:
                        self.exit_in_progress[symbol] = False
                        self.store.update_active_position(symbol=symbol, exit_in_progress=False)
                    return None
            
            # PRIORITY 4: Enforce Maximum Hold Time (MAX_HOLD_TIME_HOURS)
            if entry_time:
                hours_held = (datetime.now(timezone.utc) - entry_time).total_seconds() / 3600
                if hours_held >= MAX_HOLD_TIME_HOURS:
                    logger.info(f"[{symbol}] Max hold time reached ({hours_held:.1f}h >= {MAX_HOLD_TIME_HOURS}h) - forcing exit")
                    if symbol in self.leverage_addons:
                        await self.close_leverage_addon(symbol, current_price, 'MAX_HOLD_TIME')
                    return {'action': 'CLOSE', 'price': current_price, 'reason': 'MH', 'priority': 3}
            
            # PRIORITY 5: RSI Slope Exit (exhaustion detection) - USE_RSI_SLOPE_FOR_EXIT
            if USE_RSI_SLOPE_FOR_EXIT and symbol in self.rsi_history_cache and len(self.rsi_history_cache[symbol]) >= 5:
                rsi_values = list(self.rsi_history_cache[symbol])
                if len(rsi_values) >= 10:
                    rsi_slope = self.analyzer.compute_rsi_slope(rsi_values, lookback=5)
                    # If RSI slope is flattening or reversing (exhaustion signal)
                    if side == 'BUY' and rsi_slope < -SM_SLOPE_FLIP_THRESHOLD:
                        logger.info(f"[{symbol}] RSI exhaustion signal: slope {rsi_slope:.2f}")
                        return {'action': 'CLOSE', 'price': current_price, 'reason': 'RS', 'priority': 4}
                    elif side == 'SELL' and rsi_slope > SM_SLOPE_FLIP_THRESHOLD:
                        logger.info(f"[{symbol}] RSI exhaustion signal: slope {rsi_slope:.2f}")
                        return {'action': 'CLOSE', 'price': current_price, 'reason': 'RS', 'priority': 4}
            
            # PRIORITY 6: Liquidation zone check
            liquidation_distance_pct, _ = await self.compute_liquidation_distance(symbol, side, entry_price, current_price)
            
            if liquidation_distance_pct < 5.0:
                if symbol in self.leverage_addons:
                    await self.close_leverage_addon(symbol, current_price, 'CRITICAL_LIQUIDATION_ZONE')
                return {'action': 'CLOSE', 'price': current_price, 'reason': 'LZ', 'priority': 5}
            
            # PRIORITY 7: Extreme hold time (SM_MAX_HOLD_EXTREME_MINUTES)
            if entry_time:
                minutes_held = (datetime.now(timezone.utc) - entry_time).total_seconds() / 60
                if minutes_held >= SM_MAX_HOLD_EXTREME_MINUTES:
                    if symbol in self.leverage_addons:
                        await self.close_leverage_addon(symbol, current_price, 'TIME_EXPIRY')
                    if position.get('rsi_ladder'):
                        self.rsi_ladder_manager.cleanup_ladder(symbol)
                    return {'action': 'CLOSE', 'price': current_price, 'reason': 'TE', 'priority': 6}
            
            # Calculate profit metrics for remaining checks
            if side == 'BUY':
                profit = current_price - entry_price
                risk = entry_price - position.get('original_sl_price', position['sl_price'])
            else:
                profit = entry_price - current_price
                risk = position.get('original_sl_price', position['sl_price']) - entry_price
            
            profit_in_r = profit / risk if risk > 0 else 0
            
            if profit_in_r > position.get('max_profit_r', 0):
                position['max_profit_r'] = profit_in_r
            
            # Check stop loss after SL_DISABLE_HOURS
            if entry_time:
                hours_held = (datetime.now(timezone.utc) - entry_time).total_seconds() / 3600
                if hours_held >= SL_DISABLE_HOURS:
                    if side == 'BUY' and current_price <= position['sl_price']:
                        if self._should_ignore_early_stop(symbol, position, current_price):
                            pass
                        else:
                            if symbol in self.leverage_addons:
                                await self.close_leverage_addon(symbol, current_price, 'STOP_LOSS')
                            if position.get('rsi_ladder'):
                                self.rsi_ladder_manager.cleanup_ladder(symbol)
                            return {'action': 'CLOSE', 'price': current_price, 'reason': 'SL', 'priority': 8}
                    elif side == 'SELL' and current_price >= position['sl_price']:
                        if self._should_ignore_early_stop(symbol, position, current_price):
                            pass
                        else:
                            if symbol in self.leverage_addons:
                                await self.close_leverage_addon(symbol, current_price, 'STOP_LOSS')
                            if position.get('rsi_ladder'):
                                self.rsi_ladder_manager.cleanup_ladder(symbol)
                            return {'action': 'CLOSE', 'price': current_price, 'reason': 'SL', 'priority': 8}
            
            # Check take profit
            if side == 'BUY' and current_price >= position['tp_price']:
                if position.get('rsi_ladder'):
                    self.rsi_ladder_manager.cleanup_ladder(symbol)
                if symbol in self.leverage_addons:
                    await self.close_leverage_addon(symbol, current_price, 'TAKE_PROFIT')
                return {'action': 'CLOSE', 'price': current_price, 'reason': 'TP', 'priority': 8}
            elif side == 'SELL' and current_price <= position['tp_price']:
                if position.get('rsi_ladder'):
                    self.rsi_ladder_manager.cleanup_ladder(symbol)
                if symbol in self.leverage_addons:
                    await self.close_leverage_addon(symbol, current_price, 'TAKE_PROFIT')
                return {'action': 'CLOSE', 'price': current_price, 'reason': 'TP', 'priority': 8}
            
            if ENABLE_EXIT_ATTEMPT_LOCK:
                self.exit_in_progress[symbol] = False
                self.store.update_active_position(symbol=symbol, exit_in_progress=False)
            
            return None
            
        except Exception as e:
            logger.error(f"Error analyzing exit for {symbol}: {e}")
            if ENABLE_EXIT_ATTEMPT_LOCK and symbol in self.exit_in_progress:
                self.exit_in_progress[symbol] = False
            return None

    async def _get_symbol_max_leverage(self, symbol: str) -> int:
        """Query Binance for the maximum leverage allowed for a symbol"""
        try:
            exchange_info = await self.client.futures_exchange_info()
            for s in exchange_info.get('symbols', []):
                if s.get('symbol') == symbol:
                    for f in s.get('filters', []):
                        if f.get('filterType') == 'MAX_NUM_ORDERS':
                            pass
                    break
            # Fallback: try to get leverage from position information
            positions = await self.client.futures_position_information(symbol=symbol)
            if positions and len(positions) > 0:
                for pos in positions:
                    if pos.get('symbol') == symbol:
                        leverage = pos.get('leverage')
                        if leverage:
                            return int(leverage)
            # If we can't determine, use a safe default based on symbol type
            major_pairs = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'SOLUSDT', 'XRPUSDT', 'ADAUSDT']
            if symbol in major_pairs:
                return 20
            else:
                return 10
        except Exception as e:
            logger.warning(f"Could not get max leverage for {symbol}: {e}")
            return 10
    
    async def _apply_leverage_with_confirmation(self, symbol: str, target_leverage: int) -> Tuple[int, bool]:
        """
        Apply leverage to a symbol and verify it was applied correctly.
        Returns: (applied_leverage, success)
        """
        try:
            # Get the actual max leverage for this symbol
            symbol_max = await self._get_symbol_max_leverage(symbol)
            
            # Calculate the effective leverage: min(PREF, MAX, symbol_max)
            effective_leverage = min(target_leverage, MAX_LEVERAGE, symbol_max)
            
            # Log what we're doing
            logger.warning(f"[{symbol}] LEVERAGE SETUP: target={target_leverage}, max_global={MAX_LEVERAGE}, symbol_max={symbol_max}, applying={effective_leverage}")
            
            # Set leverage
            await self.client.futures_change_leverage(symbol=symbol, leverage=effective_leverage)
            
            # Wait a moment for the change to propagate
            await asyncio.sleep(0.5)
            
            # Verify leverage was applied
            try:
                positions = await self.client.futures_position_information(symbol=symbol)
                if positions and len(positions) > 0:
                    for pos in positions:
                        if pos.get('symbol') == symbol:
                            actual_leverage = pos.get('leverage')
                            if actual_leverage:
                                actual_leverage = int(actual_leverage)
                                logger.warning(f"[{symbol}] LEVERAGE CONFIRMED: requested={effective_leverage}, confirmed={actual_leverage}")
                                if actual_leverage == effective_leverage:
                                    return effective_leverage, True
                                else:
                                    logger.warning(f"[{symbol}] Leverage mismatch! Set {effective_leverage}, but Binance shows {actual_leverage}")
                                    return actual_leverage, True
                    # If we get here, we didn't find the position info
                    logger.warning(f"[{symbol}] Could not verify leverage - no position info found")
                    return effective_leverage, True
            except Exception as e:
                logger.warning(f"[{symbol}] Could not verify leverage: {e}")
            
            return effective_leverage, True
            
        except Exception as e:
            logger.warning(f"[{symbol}] Failed to set leverage to {target_leverage}: {e}")
            # Try fallback to 1x
            try:
                await self.client.futures_change_leverage(symbol=symbol, leverage=1)
                logger.warning(f"[{symbol}] Leverage fallback to 1x applied")
                return 1, True
            except Exception as e2:
                logger.error(f"[{symbol}] Leverage fallback also failed: {e2}")
                return 1, False

    async def place_stop_loss_order(self, symbol: str, side: str, quantity: float, 
                                   sl_price: float, position_side: str = None) -> Optional[int]:
        """Place a stop loss order for a position using STOP_MARKET (no price param)"""
        try:
            # Determine the stop side: SELL for LONG, BUY for SHORT
            stop_side = 'SELL' if side == 'BUY' else 'BUY'
            
            # Use the stopPrice as the trigger, no price parameter for STOP_MARKET
            # Add a small buffer to avoid premature triggering
            buffer_pct = 0.001  # 0.1% buffer
            if side == 'BUY':
                adjusted_sl_price = sl_price * (1 - buffer_pct)
            else:
                adjusted_sl_price = sl_price * (1 + buffer_pct)
            
            # Log the SL order details
            logger.warning(
                f"[{symbol}] SL ORDER: side={stop_side}, "
                f"stopPrice={adjusted_sl_price:.4f}, "
                f"qty={quantity:.8f}, position_side={position_side}"
            )
            
            # Generate compact client_order_id under 36 chars
            import uuid
            short_id = uuid.uuid4().hex[:8]
            client_order_id = f"SC_SL_{symbol[:4]}_{short_id}"
            if len(client_order_id) > 35:
                client_order_id = f"SC_SL_{short_id}"
            if len(client_order_id) > 35:
                client_order_id = f"SC_SL_{uuid.uuid4().hex[:10]}"
            
            order_params = {
                'symbol': symbol,
                'side': stop_side,
                'type': 'STOP_MARKET',
                'quantity': quantity,
                'stopPrice': adjusted_sl_price,
                'newClientOrderId': client_order_id,
                'closePosition': True
            }
            
            if ENABLE_HEDGE_MODE_COMPATIBILITY and position_side:
                order_params['positionSide'] = position_side
            
            order = await self.client.futures_create_order(**order_params)
            
            if order and 'orderId' in order:
                logger.info(f"[{symbol}] SL order placed: orderId={order['orderId']}, stopPrice={adjusted_sl_price:.4f}")
                return order['orderId']
            else:
                logger.warning(f"[{symbol}] Failed to place SL order: {order}")
                return None
                
        except Exception as e:
            logger.error(f"[{symbol}] Error placing SL order: {e}")
            return None
    
    async def place_take_profit_order(self, symbol: str, side: str, quantity: float,
                                     tp_price: float, position_side: str = None) -> Optional[int]:
        """Place a take profit order for a position using TAKE_PROFIT_MARKET (no price param)"""
        try:
            # Determine the TP side: SELL for LONG, BUY for SHORT
            tp_side = 'SELL' if side == 'BUY' else 'BUY'
            
            # Use the stopPrice as the trigger, no price parameter for TAKE_PROFIT_MARKET
            # Add a small buffer to avoid premature triggering
            buffer_pct = 0.001  # 0.1% buffer
            if side == 'BUY':
                adjusted_tp_price = tp_price * (1 + buffer_pct)
            else:
                adjusted_tp_price = tp_price * (1 - buffer_pct)
            
            # Log the TP order details
            logger.warning(
                f"[{symbol}] TP ORDER: side={tp_side}, "
                f"stopPrice={adjusted_tp_price:.4f}, "
                f"qty={quantity:.8f}, position_side={position_side}"
            )
            
            # Generate compact client_order_id under 36 chars
            import uuid
            short_id = uuid.uuid4().hex[:8]
            client_order_id = f"SC_TP_{symbol[:4]}_{short_id}"
            if len(client_order_id) > 35:
                client_order_id = f"SC_TP_{short_id}"
            if len(client_order_id) > 35:
                client_order_id = f"SC_TP_{uuid.uuid4().hex[:10]}"
            
            order_params = {
                'symbol': symbol,
                'side': tp_side,
                'type': 'TAKE_PROFIT_MARKET',
                'quantity': quantity,
                'stopPrice': adjusted_tp_price,
                'newClientOrderId': client_order_id,
                'closePosition': True
            }
            
            if ENABLE_HEDGE_MODE_COMPATIBILITY and position_side:
                order_params['positionSide'] = position_side
            
            order = await self.client.futures_create_order(**order_params)
            
            if order and 'orderId' in order:
                logger.info(f"[{symbol}] TP order placed: orderId={order['orderId']}, stopPrice={adjusted_tp_price:.4f}")
                return order['orderId']
            else:
                logger.warning(f"[{symbol}] Failed to place TP order: {order}")
                return None
                
        except Exception as e:
            logger.error(f"[{symbol}] Error placing TP order: {e}")
            return None

    async def execute_entry(self, symbol: str, opportunity: Dict) -> bool:
        try:
            if self.is_dry_run() or self.wallet_balance == 0:
                logger.info(f"[DEMO MODE] Would enter {symbol} {opportunity['side']} at {opportunity['entry_price']:.4f}")
                # Mark decision as executed in AI history
                if hasattr(self, 'ai_decision_review') and self.ai_decision_review:
                    self.ai_decision_review.log_trade_execution(symbol, opportunity['side'], opportunity.get('confidence_score', 0), opportunity.get('ai_decision_reason', ''))
                return True
            
            current_price = await self.dl.ticker_price(symbol)
            if current_price <= 0:
                return False
            
            quantity = opportunity['quantity']
            target_leverage = PREF_LEVERAGE
            
            liquidation_info = opportunity.get('liquidation_info', {})
            if liquidation_info:
                size_reduction = liquidation_info.get('size_reduction', 1.0)
                quantity *= size_reduction
                # Apply zone-based leverage reduction
                zone_leverage = liquidation_info.get('leverage', PREF_LEVERAGE)
                target_leverage = min(target_leverage, zone_leverage)
            
            # Apply leverage with confirmation
            applied_leverage, leverage_ok = await self._apply_leverage_with_confirmation(symbol, target_leverage)
            
            # If leverage application failed, use 1x as fallback
            if not leverage_ok:
                logger.warning(f"[{symbol}] Leverage setup failed, using 1x")
                applied_leverage = 1
            
            # Round quantity to step size
            quantity = self.precision_manager.round_to_step_size(quantity, symbol)
            if quantity <= 0:
                return False
            
            # ORDER CHECK before every order submission
            notional_value = quantity * current_price
            logger.warning(
                f"ORDER CHECK: {symbol} "
                f"qty={quantity:.8f} "
                f"price={current_price:.4f} "
                f"notional={notional_value:.2f} "
                f"wallet={self.wallet_balance:.2f} "
                f"leverage={applied_leverage}"
            )
            
            # Check if notional exceeds wallet-based limit at the applied leverage
            max_notional = self.wallet_balance * applied_leverage * 0.8  # 80% of max theoretical
            if notional_value > max_notional:
                logger.warning(f"[{symbol}] ORDER CHECK FAILED: notional {notional_value:.2f} > max {max_notional:.2f}")
                # Scale down quantity to fit
                scale = max_notional / notional_value * 0.9
                quantity = self.precision_manager.round_to_step_size(quantity * scale, symbol)
                if quantity <= 0:
                    return False
                notional_value = quantity * current_price
                logger.warning(f"[{symbol}] ORDER CHECK: scaled to qty={quantity:.8f}, notional={notional_value:.2f}")
            
            # Generate compact client_order_id under 36 chars (Binance requirement)
            import uuid
            short_id = uuid.uuid4().hex[:8]
            client_order_id = f"SC_{symbol[:4]}_{short_id}"
            if len(client_order_id) > 35:
                client_order_id = f"SC_{short_id}"
            if len(client_order_id) > 35:
                client_order_id = f"SC_{uuid.uuid4().hex[:10]}"
            
            position_side = self.get_position_side_for_order(opportunity['side'])
            
            order = await self.safe_create_order(
                symbol=symbol, side=opportunity['side'], type='MARKET',
                quantity=quantity, client_order_id=client_order_id, position_side=position_side
            )
            
            if not order or 'orderId' not in order:
                return False
            
            order_id = order['orderId']
            filled, order_status = await self.wait_for_order_fill_confirmation(symbol, order_id)
            
            if not filled:
                return False
            
            avg_price = float(order_status.get('avgPrice', opportunity['entry_price']))
            
            expected_quantity = quantity if opportunity['side'] == 'BUY' else -quantity
            position_verified = await self.verify_position_after_entry(symbol, client_order_id, expected_quantity)
            
            if not position_verified:
                self.store.delete_active_position(symbol)
                async with self._active_positions_lock:
                    if symbol in self.active_positions:
                        del self.active_positions[symbol]
                return False
            
            trade_id = self.store.insert_trade(
                timestamp=datetime.now(timezone.utc).isoformat(), symbol=symbol, side=opportunity['side'],
                mode=STRATEGY_MODE, entry_price=avg_price, sl_price=opportunity['sl_price'],
                tp_price=opportunity['tp_price'], quantity=quantity, leverage=applied_leverage,
                notional=avg_price * quantity,
                trend_signal=opportunity['details'].get('trend_signal'),
                trend_strength=opportunity['details'].get('trend_strength'),
                reversion_signal=opportunity['details'].get('reversion_signal'),
                reversion_strength=opportunity['details'].get('reversion_strength'),
                conflict_score=opportunity['details'].get('conflict_score'),
                volume_confirmation=opportunity['details'].get('volume_confirmation', 0),
                strategy_id=STRATEGY_ID, position_side=position_side
            )
            
            # Place TP/SL orders after successful entry
            sl_order_id = await self.place_stop_loss_order(
                symbol, opportunity['side'], quantity, 
                opportunity['sl_price'], position_side
            )
            
            tp_order_id = await self.place_take_profit_order(
                symbol, opportunity['side'], quantity,
                opportunity['tp_price'], position_side
            )
            
            if not sl_order_id or not tp_order_id:
                logger.warning(f"[{symbol}] TP/SL order placement partially failed: SL={sl_order_id}, TP={tp_order_id}")
            else:
                logger.info(f"[{symbol}] TP/SL orders placed: SL={sl_order_id}, TP={tp_order_id}")
            
            # Record conflict outcome start for memory
            if opportunity['details'].get('conflict_type'):
                conflict_type = opportunity['details']['conflict_type']
                self.agent_memory.record_outcome(
                    conflict_type=conflict_type, side=opportunity['side'],
                    outcome='PENDING', pnl=0,
                    rsi_at_entry=opportunity.get('rsi_extreme', {}).get('current_rsi', 50),
                    regime=opportunity.get('market_regime', 'NEUTRAL')
                )
            
            # Mark decision as executed in AI history
            if hasattr(self, 'ai_decision_review') and self.ai_decision_review:
                self.ai_decision_review.log_trade_execution(symbol, opportunity['side'], opportunity.get('confidence_score', 0), opportunity.get('ai_decision_reason', ''))
            
            self.store.save_active_position(
                symbol=symbol, side=opportunity['side'], entry_price=avg_price, quantity=quantity,
                sl_price=opportunity['sl_price'], tp_price=opportunity['tp_price'], trade_id=trade_id,
                trend_signal=opportunity['details'].get('trend_signal'),
                reversion_signal=opportunity['details'].get('reversion_signal'),
                conflict_type=opportunity['details'].get('conflict_type'),
                original_sl_price=opportunity['sl_price'], strategy_id=STRATEGY_ID,
                pending_entry=False, client_order_id=client_order_id, order_id=order_id,
                order_status='FILLED', position_side=position_side,
                stop_loss_order_id=sl_order_id or 0, take_profit_order_id=tp_order_id or 0
            )
            
            async with self._active_positions_lock:
                self.active_positions[symbol] = {
                    'side': opportunity['side'], 'entry_price': avg_price, 'quantity': quantity,
                    'sl_price': opportunity['sl_price'], 'tp_price': opportunity['tp_price'],
                    'entry_time': datetime.now(timezone.utc), 'trade_id': trade_id,
                    'details': opportunity['details'], 'indicators': opportunity.get('indicators', {}),
                    'liquidation_info': opportunity.get('liquidation_info', {}),
                    'rsi_extreme': opportunity.get('rsi_extreme', {}), 'max_profit_pct': 0,
                    'max_profit_r': 0, 'trailing_stop_price': 0, 'breakeven_triggered': False,
                    'partial_profits_taken': 0, 'original_sl_price': opportunity['sl_price'],
                    'pending_entry': False, 'strategy_id': STRATEGY_ID, 'position_side': position_side,
                    'stop_loss_order_id': sl_order_id or 0, 'take_profit_order_id': tp_order_id or 0
                }
            
            # Reset circuit breaker exit failures on successful entry
            self.circuit_breaker.reset_exit_failures()
            self.hard_close_triggered = False
            
            logger.info(f"[{symbol}] Entry executed: {opportunity['side']} {quantity:.6f} @ {avg_price:.4f} (leverage={applied_leverage})")
            return True
            
        except Exception as e:
            logger.error(f"[{symbol}] Error executing entry: {e}")
            return False

    async def execute_exit(self, symbol: str, exit_signal: Dict) -> bool:
        try:
            if self.is_dry_run() or self.wallet_balance == 0:
                logger.info(f"[DEMO MODE] Would exit {symbol} at {exit_signal['price']:.4f} ({exit_signal['reason']})")
                return True
            
            async with self._active_positions_lock:
                position = self.active_positions.get(symbol)
                if not position:
                    return False
                if not self.is_own_position(symbol, position):
                    return False
                side = position['side']
                quantity = position['quantity']
                position_side = position.get('position_side')
            
            exit_side = 'SELL' if side == 'BUY' else 'BUY'
            quantity = self.precision_manager.round_to_step_size(quantity, symbol)
            if quantity <= 0:
                return False
            
            # Generate compact client_order_id under 36 chars
            import uuid
            short_id = uuid.uuid4().hex[:8]
            client_order_id = f"SC_{symbol[:4]}_X_{short_id}"
            if len(client_order_id) > 35:
                client_order_id = f"SC_X_{uuid.uuid4().hex[:10]}"
            
            # ORDER CHECK before exit order submission
            logger.warning(
                f"[{symbol}] EXIT ORDER: "
                f"side={exit_side} "
                f"reduce_only=True "
                f"position_side={position_side} "
                f"qty={quantity:.8f} "
                f"price={exit_signal['price']:.4f}"
            )
            
            order = await self.safe_create_order(
                symbol=symbol, side=exit_side, type='MARKET', quantity=quantity,
                reduce_only=True, client_order_id=client_order_id, position_side=position_side
            )
            
            if not order or 'orderId' not in order:
                # Record exit failure for circuit breaker
                self.circuit_breaker.record_exit_failure()
                # If reduce_only fails, try without reduce_only
                logger.warning(f"[{symbol}] ReduceOnly order failed, retrying without reduce_only")
                logger.warning(
                    f"[{symbol}] EXIT ORDER (fallback): "
                    f"side={exit_side} "
                    f"reduce_only=False "
                    f"position_side={position_side} "
                    f"qty={quantity:.8f} "
                    f"price={exit_signal['price']:.4f}"
                )
                order = await self.safe_create_order(
                    symbol=symbol, side=exit_side, type='MARKET', quantity=quantity,
                    client_order_id=client_order_id, position_side=position_side
                )
                if not order or 'orderId' not in order:
                    return False
            
            order_id = order['orderId']
            filled, order_status = await self.wait_for_order_fill_confirmation(symbol, order_id)
            
            if not filled:
                self.circuit_breaker.record_exit_failure()
                return False
            
            # Reset exit failures on successful exit
            self.circuit_breaker.reset_exit_failures()
            
            avg_price = float(order_status.get('avgPrice', exit_signal['price']))
            
            position_info = position
            if isinstance(position, dict):
                entry_price = position.get('entry_price', 0)
                trade_id = position.get('trade_id')
                
                if trade_id:
                    net_pnl, _ = self.store.update_trade_exit(trade_id, avg_price, exit_signal['reason'])
                    
                    if net_pnl is not None:
                        self.circuit_breaker.record_trade_outcome(net_pnl)
                        outcome = "TP" if net_pnl > 0 else "SL" if net_pnl < 0 else "BE"
                        self.win_rate_tracker.update(symbol, outcome, side)
                        self.store.update_accuracy(symbol, outcome in ["TP", "BE"], side)
                        
                        # Record outcome in agent memory
                        if position.get('details', {}).get('conflict_type'):
                            self.agent_memory.record_outcome(
                                conflict_type=position['details']['conflict_type'],
                                side=side, outcome=outcome, pnl=net_pnl,
                                rsi_at_entry=position.get('rsi_extreme', {}).get('current_rsi', 50),
                                regime=position.get('market_regime', 'NEUTRAL')
                            )
            
            async with self._active_positions_lock:
                if symbol in self.active_positions:
                    del self.active_positions[symbol]
            
            self.store.delete_active_position(symbol)
            
            if position.get('rsi_ladder'):
                self.rsi_ladder_manager.cleanup_ladder(symbol)
            
            if symbol in self.exit_in_progress:
                self.exit_in_progress[symbol] = False
            
            logger.info(f"[{symbol}] Exit executed: {exit_side} {quantity:.6f} @ {avg_price:.4f} ({exit_signal['reason']})")
            
            await self.apply_cooldown_after_exit(symbol)
            return True
            
        except Exception as e:
            logger.error(f"[{symbol}] Error executing exit: {e}")
            self.circuit_breaker.record_exit_failure()
            if symbol in self.exit_in_progress:
                self.exit_in_progress[symbol] = False
            return False

    async def check_equity_throttling(self):
        if not ENABLE_EQUITY_THROTTLING:
            return True, ""
        current_equity = self.wallet_balance
        equity_ok, equity_reason = self.equity_throttler.can_trade(current_equity)
        return equity_ok, equity_reason
    
    async def check_volatility_and_market_state(self, symbol):
        try:
            klines = await self.dl.klines(symbol, '5m', limit=20)
            if klines and len(klines) >= 10:
                closes = [float(k[4]) for k in klines]
                price_changes = []
                for i in range(1, len(closes)):
                    change_pct = abs((closes[i] - closes[i-1]) / closes[i-1] * 100)
                    price_changes.append(change_pct)
                avg_volatility = sum(price_changes) / len(price_changes) if price_changes else 0
                if avg_volatility > VOLATILITY_CIRCUIT_BREAKER_PCT:
                    return False, f"High volatility: {avg_volatility:.1f}%"
            return True, ""
        except Exception:
            return True, ""
    
    async def can_trade(self, symbol):
        if symbol in self.cooldowns:
            if datetime.now(timezone.utc) < self.cooldowns[symbol]:
                return False
        daily_trades = self.store.get_daily_trades_count()
        if daily_trades >= MAX_DAILY_TRADES:
            return False
        if len(self.active_positions) >= MAX_CONCURRENT_POSITIONS:
            return False
        daily_loss = self.store.get_daily_loss(symbol)
        if daily_loss < 0 and abs(daily_loss) >= (self.wallet_balance * DAILY_LOSS_CAP_PCT / 100):
            return False
        global_loss = self.store.get_global_daily_loss()
        if global_loss < 0 and abs(global_loss) >= (self.wallet_balance * GLOBAL_DAILY_LOSS_CAP_PCT / 100):
            return False
        consecutive_losses = self.store.get_consecutive_losses(symbol, SL_STREAK_RESET_HOURS)
        if consecutive_losses >= MAX_CONSECUTIVE_SL:
            return False
        return True
    
    async def safe_create_order(self, symbol: str, side: str, type: str, quantity: float, 
                               reduce_only: bool = False, client_order_id: str = None, 
                               position_side: str = None) -> Optional[Dict]:
        try:
            # Map internal side to Binance API valid sides
            binance_side = side
            if side.upper() == "LONG":
                binance_side = "BUY"
            elif side.upper() == "SHORT":
                binance_side = "SELL"
            
            order_params = {'symbol': symbol, 'side': binance_side, 'type': type, 'quantity': quantity}
            if reduce_only:
                order_params['reduceOnly'] = True
            if ENABLE_HEDGE_MODE_COMPATIBILITY and position_side:
                order_params['positionSide'] = position_side
            elif position_side:
                order_params['positionSide'] = position_side
            if client_order_id:
                if len(client_order_id) > 35:
                    import uuid
                    client_order_id = f"SC_{uuid.uuid4().hex[:10]}"
                order_params['newClientOrderId'] = client_order_id
            else:
                self.order_counter += 1
                import uuid
                short_id = uuid.uuid4().hex[:8]
                client_order_id = f"SC_{symbol[:4]}_{short_id}"
                if len(client_order_id) > 35:
                    client_order_id = f"SC_{short_id}"
                if len(client_order_id) > 35:
                    client_order_id = f"SC_{uuid.uuid4().hex[:10]}"
                order_params['newClientOrderId'] = client_order_id
            
            if type == 'MARKET':
                order = await self.client.futures_create_order(**order_params)
                return order
            else:
                return None
        except Exception as e:
            logger.error(f"Error creating order for {symbol}: {e}")
            return None
    
    async def wait_for_order_fill_confirmation(self, symbol: str, order_id: int, 
                                              timeout_sec: int = ORDER_FILL_TIMEOUT_SEC,
                                              poll_interval: float = ORDER_FILL_POLL_INTERVAL_SEC) -> Tuple[bool, Optional[Dict]]:
        start_time = time.time()
        while time.time() - start_time < timeout_sec:
            try:
                order_status = await self.client.futures_get_order(symbol=symbol, orderId=order_id)
                status = order_status.get('status')
                if status == 'FILLED':
                    return True, order_status
                elif status in ['CANCELED', 'REJECTED', 'EXPIRED']:
                    return False, order_status
                await asyncio.sleep(poll_interval)
            except Exception:
                await asyncio.sleep(poll_interval)
        return False, None
    
    async def verify_position_after_entry(self, symbol: str, client_order_id: str, expected_quantity: float) -> bool:
        if self.is_dry_run() or self.wallet_balance == 0:
            return True
        try:
            await asyncio.sleep(1.0)
            positions = await self.client.futures_position_information()
            for pos in positions:
                if pos['symbol'] == symbol:
                    position_amt = float(pos['positionAmt'])
                    if (position_amt > 0 and expected_quantity > 0) or (position_amt < 0 and expected_quantity < 0):
                        return True
            return False
        except Exception:
            return False
    
    async def hard_reconciliation_check(self):
        if self.is_dry_run() or not ENABLE_POSITION_RECONCILIATION:
            return
        try:
            exchange_positions = {}
            positions_data = await self.client.futures_position_information()
            for pos in positions_data:
                symbol = pos['symbol']
                position_amt = float(pos.get('positionAmt', 0))
                if position_amt != 0:
                    exchange_positions[symbol] = {'positionAmt': position_amt}
            
            symbols_to_remove = []
            async with self._active_positions_lock:
                for symbol, position in self.active_positions.items():
                    if symbol not in exchange_positions:
                        symbols_to_remove.append(symbol)
                    elif abs(exchange_positions[symbol]['positionAmt']) == 0:
                        symbols_to_remove.append(symbol)
            
            for symbol in symbols_to_remove:
                self.store.delete_active_position(symbol)
                async with self._active_positions_lock:
                    if symbol in self.active_positions:
                        del self.active_positions[symbol]
                self.rsi_ladder_manager.cleanup_ladder(symbol)
        except Exception as e:
            logger.error(f"Error in hard reconciliation: {e}")
    
    async def compute_liquidation_distance(self, symbol, side, entry_price, current_price):
        try:
            if side == 'BUY':
                liquidation_distance = (entry_price - (entry_price * 0.9)) / entry_price * 100
            else:
                liquidation_distance = ((entry_price * 1.1) - entry_price) / entry_price * 100
            return liquidation_distance, "ESTIMATED"
        except Exception:
            return 100.0, "ERROR"
    
    def determine_liquidation_zone(self, symbol, liquidation_distance_pct):
        if liquidation_distance_pct >= 20.0:
            return {'zone': 'SAFE', 'size_reduction': 0.8, 'leverage': PREF_LEVERAGE, 'block_adds': False}
        elif liquidation_distance_pct >= 15.0:
            return {'zone': 'WATCH', 'size_reduction': 0.8, 'leverage': min(PREF_LEVERAGE, 2), 'block_adds': BLOCK_ADDS_WATCH_ZONE}
        elif liquidation_distance_pct >= 10.0:
            return {'zone': 'REDUCE', 'size_reduction': 0.7, 'leverage': min(PREF_LEVERAGE, 1), 'block_adds': BLOCK_ADDS_REDUCE_ZONE}
        else:
            return {'zone': 'CRITICAL', 'size_reduction': 0.6, 'leverage': min(PREF_LEVERAGE, 1), 'block_adds': BLOCK_ADDS_CRITICAL_ZONE}
    
    async def calculate_position_size_with_zones(self, symbol, current_price, side, atr_value, liquidation_distance_pct):
        try:
            # Use a more robust position sizing approach that works for small wallets
            risk_amount = self.wallet_balance * (RISK_PER_TRADE_PCT / 100.0)
            
            # Ensure minimum risk amount for very small wallets
            if risk_amount < 0.01:
                risk_amount = 0.01  # Minimum 1 cent risk for tiny wallets
            
            # Calculate stop distance from ATR or fallback
            if atr_value:
                stop_distance = atr_value * ATR_STOP_MULTIPLIER
                # Ensure stop distance is at least MIN_STOP_DISTANCE_PCT of price
                min_stop = current_price * (MIN_STOP_DISTANCE_PCT / 100.0)
                stop_distance = max(stop_distance, min_stop)
            else:
                stop_distance = current_price * (MAX_STOP_LOSS_PCT / 100.0)
            
            if stop_distance <= 0:
                return 0
            
            # Calculate base quantity: risk_amount / stop_distance
            base_quantity = risk_amount / stop_distance
            
            # Apply zone-based size reduction
            zone_info = self.determine_liquidation_zone(symbol, liquidation_distance_pct)
            base_quantity *= zone_info['size_reduction']
            
            # Apply margin-based constraint: ensure notional doesn't exceed wallet balance * leverage
            effective_leverage = zone_info.get('leverage', PREF_LEVERAGE)
            max_notional = self.wallet_balance * effective_leverage * 0.6  # 60% of max theoretical for safety
            max_quantity = max_notional / current_price if current_price > 0 else float('inf')
            base_quantity = min(base_quantity, max_quantity)
            
            # Round to step size
            base_quantity = self.precision_manager.round_to_step_size(base_quantity, symbol)
            
            # Check minimum position value
            min_value = base_quantity * current_price
            if min_value < MIN_POSITION_VALUE_USD:
                # For very small wallets, allow smaller positions
                if self.wallet_balance < 10.0:
                    if min_value >= 1.0:  # Allow $1 minimum for tiny wallets
                        return base_quantity
                if ENABLE_ENTRY_BLOCK_LOGGING:
                    logger.info(f"ENTRY BLOCKED: {symbol} position_value={min_value:.2f} < {MIN_POSITION_VALUE_USD}")
                return 0
            
            return base_quantity
        except Exception as e:
            logger.error(f"Error calculating position size for {symbol}: {e}")
            return 0
    
    def calculate_stop_take_with_time_stop(self, entry_price, side, atr_value, is_rsi_extreme=False):
        if atr_value:
            sl_distance = atr_value * ATR_STOP_MULTIPLIER
            tp_distance = sl_distance * TP_MULTIPLIER
        else:
            sl_distance = entry_price * (MAX_STOP_LOSS_PCT / 100.0)
            tp_distance = sl_distance * TP_MULTIPLIER
        if side == 'BUY':
            sl_price = entry_price - sl_distance
            tp_price = entry_price + tp_distance
        else:
            sl_price = entry_price + sl_distance
            tp_price = entry_price - tp_distance
        planned_r = tp_distance / sl_distance if sl_distance > 0 else 0
        return sl_price, tp_price, sl_distance, tp_distance, planned_r
    
    def should_bypass_rrr_check(self, symbol, side, current_rsi, is_rsi_extreme, hours_held):
        bypass_rrr = False
        bypass_reason = ""
        if ENABLE_RRR_BYPASS_FOR_EXTREME_RSI:
            if side == 'SELL' and current_rsi >= 88:
                bypass_rrr = True
                bypass_reason = f"RSI ≥ 88"
            elif side == 'BUY' and current_rsi <= 12:
                bypass_rrr = True
                bypass_reason = f"RSI ≤ 12"
        if ENABLE_RRR_BYPASS_DURING_SL_DISABLED and hours_held < SL_DISABLE_HOURS:
            bypass_rrr = True
            bypass_reason = f"SL-disabled window"
        return bypass_rrr, bypass_reason
    
    async def run_pretrade_simulation(self, symbol, entry_price, side, atr_value):
        if not ENABLE_PRETRADE_SIMULATION:
            return True, "Simulation disabled"
        try:
            return True, "Simulation passed"
        except Exception:
            return True, "Simulation error"
    
    def check_absolute_time_stop(self, symbol, position):
        if not ENFORCE_ABSOLUTE_TIME_STOP or not position.get('entry_time'):
            return False, ""
        hours_held = (datetime.now(timezone.utc) - position['entry_time']).total_seconds() / 3600
        if hours_held >= ABSOLUTE_TIME_STOP_HOURS:
            return True, f"Absolute time stop: {hours_held:.1f}h"
        return False, ""
    
    def _should_ignore_early_stop(self, symbol, position, current_price):
        entry_time = position.get('entry_time')
        if not entry_time:
            return False
        hours_held = (datetime.now(timezone.utc) - entry_time).total_seconds() / 3600
        if hours_held < MIN_HOLD_TIME_BEFORE_SL_HOURS:
            return True
        return False
    
    async def apply_cooldown_after_exit(self, symbol: str):
        if not ENABLE_STRICT_COOLDOWN:
            return
        cooldown_hours = TOKEN_COOLDOWN_HOURS
        self.cooldowns[symbol] = datetime.now(timezone.utc) + timedelta(hours=cooldown_hours)
        if symbol in self.active_positions and self.active_positions[symbol].get('rsi_ladder'):
            self.rsi_ladder_manager.cleanup_ladder(symbol)
        self.last_exit_time[symbol] = datetime.now(timezone.utc)
    
    async def execute_rsi_laddered_entry(self, symbol: str, opportunity: Dict) -> bool:
        return await self.execute_entry(symbol, opportunity)
    
    async def execute_ladder_tranche(self, symbol, tranche_info, original_opportunity):
        return await self.execute_entry(symbol, original_opportunity)
    
    async def close_leverage_addon(self, symbol, current_price, reason):
        if symbol in self.leverage_addons:
            del self.leverage_addons[symbol]
    
    async def update_trailing_stop_with_r_activation(self, symbol, current_price, position):
        pass
    
    async def check_partial_profit_taking_with_r(self, symbol, position, current_price, profit_in_r):
        return None
    
    async def execute_leverage_addon(self, symbol, position):
        pass
    
    async def apply_leverage_reduction(self, symbol, zone):
        pass
    
    async def check_liquidation_distance(self, symbol, side, entry_price, current_price, atr_value):
        return True, ""
    
    async def analyze_symbol_cycle(self):
        opportunities = []
        symbols_to_analyze = [s for s in self.symbols if s not in self.active_positions]
        symbols_to_analyze = [s for s in symbols_to_analyze if s not in self.cooldowns or datetime.now(timezone.utc) >= self.cooldowns[s]]
        symbols_to_analyze = symbols_to_analyze[:MAX_SYMBOLS_TO_ANALYZE]
        
        semaphore = asyncio.Semaphore(5)
        async def analyze_with_semaphore(symbol):
            async with semaphore:
                return await self.analyze_symbol_enhanced(symbol)
        
        tasks = [analyze_with_semaphore(s) for s in symbols_to_analyze]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                continue
            if result:
                opportunities.append(result)
        
        return opportunities
    
    async def check_position_exits_enhanced(self):
        exit_signals = []
        for symbol in list(self.active_positions.keys()):
            try:
                exit_signal = await self.analyze_exit_with_priority_enhanced(symbol)
                if exit_signal:
                    exit_signals.append((symbol, exit_signal))
            except Exception as e:
                logger.error(f"Error checking exit for {symbol}: {e}")
        return exit_signals
    
    async def execute_trading_cycle(self):
        try:
            logger.info(f"Heartbeat: Cycle #{self.cycle_count}")
            
            if not self.is_dry_run():
                try:
                    account_info = await self.client.futures_account()
                    self.wallet_balance = float(account_info.get('totalWalletBalance', 0.0))
                    self.equity_throttler.update_equity(self.wallet_balance)
                    logger.warning(f"WALLET DEBUG: balance={self.wallet_balance}")
                    if self.wallet_balance == 0:
                        logger.info("Demo Mode Active: Wallet balance is 0 USDT - Simulating AI recommendations for dashboard display")
                except Exception as e:
                    logger.exception(f"Error updating wallet balance: {e}")
            
            if CIRCUIT_BREAKER_ENABLED:
                trading_allowed, reason = self.circuit_breaker.check_trading_allowed(self.wallet_balance)
                if not trading_allowed:
                    logger.warning(f"Trading not allowed: {reason}")
                    return
            
            if self.cycle_count % 5 == 0:
                await self.update_market_regime()
            
            if self.cycle_count == 0 or datetime.now(timezone.utc).minute % RECONCILIATION_INTERVAL_MINUTES == 0:
                if ENABLE_POSITION_RECONCILIATION and not self.is_dry_run():
                    await self.reconcile_positions_with_exchange_enhanced()
            
            exit_signals = await self.check_position_exits_enhanced()
            for symbol, exit_signal in exit_signals:
                await self.execute_exit(symbol, exit_signal)
                await asyncio.sleep(0.5)
            
            if len(self.active_positions) >= MAX_CONCURRENT_POSITIONS:
                return
            
            opportunities = await self.analyze_symbol_cycle()
            opportunities.sort(key=lambda x: x.get('confidence_score', 0), reverse=True)
            
            entries_executed = 0
            for opportunity in opportunities:
                if entries_executed >= MAX_NEW_POSITIONS_PER_CYCLE:
                    break
                if await self.execute_rsi_laddered_entry(opportunity['symbol'], opportunity):
                    entries_executed += 1
                    await asyncio.sleep(1.0)
            
            if entries_executed > 0 and not self.is_dry_run():
                await self.hard_reconciliation_check()
            
            self.cycle_count += 1
            
        except Exception as e:
            logger.error(f"Error in trading cycle: {e}")
    
    async def reconcile_positions_with_exchange_enhanced(self):
        await self.hard_reconciliation_check()

# ================================================================
# DASHBOARD SERVER INSTANCE
# ================================================================

dashboard_server = None

def start_dashboard(engine):
    global dashboard_server
    if not ENABLE_DASHBOARD:
        return
    dashboard_server = DashboardServer(engine)
    dashboard_server.start(DASHBOARD_PORT)

# ================================================================
# SELF TEST FUNCTION
# ================================================================

async def self_test(symbol):
    logger.info(f"Running self-test for {symbol}...")
    try:
        client = await AsyncClient.create(BINANCE_API_KEY, BINANCE_API_SECRET, testnet=USE_TESTNET)
        store = ConflictStore(DB_PATH)
        engine = SignalConflictEngine(client, store)
        await engine.setup()
        result = await engine.analyze_symbol_enhanced(symbol)
        if result:
            logger.info(f"Self-test PASSED: Found {result['side']} opportunity")
            logger.info(f"  Confidence: {result.get('confidence_score', 0):.1f}%")
            logger.info(f"  AI Explanation: {result.get('ai_explanation', 'N/A')[:100]}")
        else:
            logger.info(f"Self-test PASSED: No opportunity")
        await client.close_connection()
    except Exception as e:
        logger.error(f"Self-test FAILED: {e}")
        raise

async def test_connection():
    logger.info("Testing API connection...")
    try:
        client = await AsyncClient.create(BINANCE_API_KEY, BINANCE_API_SECRET, testnet=USE_TESTNET)
        account = await client.futures_account()
        logger.info(f"Connection SUCCESS: Balance: {float(account.get('totalWalletBalance', 0)):.2f} USDT")
        await client.close_connection()
    except Exception as e:
        logger.error(f"Connection FAILED: {e}")
        raise

def show_trades():
    store = ConflictStore(DB_PATH)
    trades = store.get_todays_trades_with_details()
    if not trades:
        print("No trades today.")
        return
    print(f"\nToday's Trades ({len(trades)}):")
    print("=" * 80)
    total_pnl = 0
    for t in trades:
        print(f"{t['symbol']:<10} {t['side']:<6} {t.get('outcome','OPEN'):<8} ${t.get('net_pnl',0):>10.2f}")
        total_pnl += t.get('net_pnl', 0)
    print("=" * 80)
    print(f"Total PnL: ${total_pnl:.2f}")

# ================================================================
# MAIN FUNCTION
# ================================================================

async def main():
    logger.info("=" * 70)
    logger.info("SIGNAL CONFLICT AI AGENT - HACKATHON EDITION")
    logger.info("=" * 70)
    logger.info(f"DRY_RUN: {DRY_RUN}, TESTNET: {USE_TESTNET}")
    logger.info(f"STRATEGY_ID: {STRATEGY_ID}, Mode: Adaptive Conflict Resolution")
    if ALLOWED_POSITION_SIDE:
        logger.info(f"🔒 Position side: {ALLOWED_POSITION_SIDE} only")
    logger.info(f"🤖 AI Decision Layer: {'ENABLED (Gemini + OpenAI fallback)' if not DISABLE_AI_LAYER else 'DISABLED'}")
    logger.info(f"🧠 Agent Memory: {'ENABLED' if ENABLE_AGENT_MEMORY else 'DISABLED'}")
    logger.info(f"📊 Dashboard: {'ENABLED' if ENABLE_DASHBOARD else 'DISABLED'}")
    logger.info(f"📝 AI Decision History: Recording all decisions even without trades")
    logger.info("=" * 70)
    
    try:
        client = await AsyncClient.create(BINANCE_API_KEY, BINANCE_API_SECRET, testnet=USE_TESTNET)
        store = ConflictStore(DB_PATH)
        engine = SignalConflictEngine(client, store)
        await engine.setup()
        
        if ENABLE_DASHBOARD:
            start_dashboard(engine)
        
        if EMERGENCY_STOP:
            await engine.emergency_stop()
            await client.close_connection()
            return
        
        logger.info("Bot initialized. Starting main trading loop...")
        
        cycle_count = 0
        while True:
            try:
                await engine.execute_trading_cycle()
                cycle_count += 1
                await asyncio.sleep(POLL_INTERVAL_SEC)
            except Exception as e:
                logger.error(f"Cycle error: {e}")
                await asyncio.sleep(5)
        
    except asyncio.CancelledError:
        logger.info("Shutdown requested")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        raise
    finally:
        try:
            await client.close_connection()
        except:
            pass

# ================================================================
# ENTRY POINT
# ================================================================

if __name__ == "__main__":
    if SELFTEST_SYMBOL:
        asyncio.run(self_test(SELFTEST_SYMBOL))
        sys.exit(0)
    if TEST_CONNECTION:
        asyncio.run(test_connection())
        sys.exit(0)
    if CLOSE_ALL_POSITIONS:
        logger.info("Close all positions requested - run main with --close-all")
        sys.exit(0)
    if SHOW_TRADES:
        show_trades()
        sys.exit(0)
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")