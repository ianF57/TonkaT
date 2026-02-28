"""Terminal progress display for the historical backfill.

Renders a live updating status panel directly to the terminal using only
Python stdlib (ANSI escape codes + sys.stdout).  No third-party packages
are needed.

The display looks like this while a Binance symbol is running:

  ┌─────────────────────────────────────────────────────────────────┐
  │  📦  Historical Data Backfill                                   │
  ├─────────────────────────────────────────────────────────────────┤
  │  Overall   ████████████░░░░░░░░░░░░░░░░░░  5 / 18  (27%)      │
  │                                                                  │
  │  BTCUSDT / 1h    [Binance]                                      │
  │  Progress  ████████████████████░░░░░░░░░░  67%                  │
  │  Cursor    2020-06-14                                            │
  │  Target    2017-08-17                                            │
  │  Windows   412       Candles   412,000                          │
  │  Elapsed   1m 43s    ETA       ~1m 22s                          │
  └─────────────────────────────────────────────────────────────────┘

And for Yahoo (single-shot, no pagination):

  │  EURUSD / 1h    [Yahoo]                                         │
  │  Status    Fetching …                                            │

Design notes
────────────
• The panel occupies a fixed number of lines. Each refresh moves the
  cursor back to the top of the panel with ANSI sequences and rewrites
  all lines in place — so the terminal doesn't scroll.
• Logging is redirected to a side-channel file during the backfill so
  that logger.info() calls from backfill.py don't interfere with the
  panel. After the backfill the original handler is restored.
• Windows terminals (cmd.exe / PowerShell) support ANSI since Windows
  10 1511.  We emit a VT-processing enable call on Windows so colors
  work without any extra setup.
• If stdout is not a TTY (e.g. redirected to a file) we fall back to
  plain line-by-line logging so pipes and CI environments still work.
"""
from __future__ import annotations

import logging
import os
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable

# ── ANSI helpers ──────────────────────────────────────────────────────────────

_RESET  = "\033[0m"
_BOLD   = "\033[1m"
_DIM    = "\033[2m"
_GREEN  = "\033[92m"
_CYAN   = "\033[96m"
_YELLOW = "\033[93m"
_BLUE   = "\033[94m"
_WHITE  = "\033[97m"

_CLEAR_LINE  = "\033[2K"      # erase current line
_CURSOR_UP   = "\033[{}A"     # move cursor up N lines


def _supports_ansi() -> bool:
    """Return True if the terminal supports ANSI escape codes."""
    if not sys.stdout.isatty():
        return False
    if os.name == "nt":
        # Enable VT processing on Windows 10+
        try:
            import ctypes
            kernel = ctypes.windll.kernel32  # type: ignore[attr-defined]
            # ENABLE_VIRTUAL_TERMINAL_PROCESSING = 0x0004
            handle = kernel.GetStdHandle(-11)  # STD_OUTPUT_HANDLE
            mode = ctypes.c_ulong()
            kernel.GetConsoleMode(handle, ctypes.byref(mode))
            kernel.SetConsoleMode(handle, mode.value | 0x0004)
            return True
        except Exception:
            return False
    return True


# ── Progress bar renderer ─────────────────────────────────────────────────────

_BAR_WIDTH = 32  # characters for the filled/empty bar


def _bar(fraction: float, width: int = _BAR_WIDTH) -> str:
    """Return a Unicode block progress bar string."""
    filled = int(fraction * width)
    filled = max(0, min(width, filled))
    empty  = width - filled
    return f"{_GREEN}{'█' * filled}{_DIM}{'░' * empty}{_RESET}"


def _fmt_duration(seconds: float) -> str:
    """Format seconds as Xm Ys or Xs."""
    s = int(seconds)
    if s < 60:
        return f"{s}s"
    return f"{s // 60}m {s % 60:02d}s"


def _fmt_epoch(ep: int) -> str:
    """Format a Unix epoch as YYYY-MM-DD."""
    return datetime.fromtimestamp(ep, tz=timezone.utc).strftime("%Y-%m-%d")


def _fmt_num(n: int) -> str:
    """Format an integer with thousands separators."""
    return f"{n:,}"


# ── State shared between the display and the backfill callbacks ───────────────

@dataclass
class SymbolProgress:
    symbol:        str
    timeframe:     str
    market:        str           # "Binance" | "Yahoo"
    now_epoch:     int           # where we started (now)
    target_epoch:  int           # where we want to reach (launch date)
    # mutable — updated on each window callback
    cursor_epoch:  int = 0       # current position (walking backward)
    windows:       int = 0
    candles:       int = 0
    status:        str = "starting"   # "running" | "complete" | "skipped" | "error"
    start_ts:      float = field(default_factory=time.monotonic)
    end_ts:        float | None = None


@dataclass
class BackfillDisplayState:
    targets_total:   int
    targets_done:    int = 0
    current:         SymbolProgress | None = None
    completed:       list[SymbolProgress] = field(default_factory=list)
    overall_start_ts: float = field(default_factory=time.monotonic)


# ── The display engine ────────────────────────────────────────────────────────

_PANEL_LINES = 13   # total lines the panel occupies (kept constant for clean refresh)


class BackfillDisplay:
    """Manages the live terminal panel."""

    def __init__(self, total_targets: int) -> None:
        self.state    = BackfillDisplayState(targets_total=total_targets)
        self._ansi    = _supports_ansi()
        self._lines_written = 0  # track lines so we can move cursor back
        self._log_handler: logging.FileHandler | None = None
        self._original_handlers: list[logging.Handler] = []

    # ── Logging redirection ───────────────────────────────────────────────────

    def _silence_loggers(self) -> None:
        """Suppress console log output while the panel is displayed.

        All log records are still captured to a NullHandler so the records
        are not lost — they will simply not appear on stdout during the
        backfill.  The application's existing handlers (e.g. a file handler
        configured by the user) are left untouched.
        """
        root = logging.getLogger()
        self._original_handlers = root.handlers[:]
        for h in self._original_handlers:
            if isinstance(h, logging.StreamHandler) and h.stream in (sys.stdout, sys.stderr):
                root.removeHandler(h)
        root.addHandler(logging.NullHandler())

    def _restore_loggers(self) -> None:
        """Restore original console log handlers after backfill completes."""
        root = logging.getLogger()
        # Remove the NullHandler we added
        for h in root.handlers[:]:
            if isinstance(h, logging.NullHandler):
                root.removeHandler(h)
        for h in self._original_handlers:
            root.addHandler(h)

    # ── Panel rendering ───────────────────────────────────────────────────────

    def _w(self, text: str) -> None:
        """Write a line to stdout and track line count."""
        sys.stdout.write(text + "\n")
        self._lines_written += 1

    def _reset_cursor(self) -> None:
        """Move the cursor back to the top of the panel."""
        if self._lines_written > 0:
            sys.stdout.write(_CURSOR_UP.format(self._lines_written))
        self._lines_written = 0

    def _render_panel(self) -> None:
        """Write the full panel to stdout."""
        W = 67  # panel inner width

        def rule(char: str = "─") -> str:
            return char * W

        # ── Header ────────────────────────────────────────────────────────────
        self._w(f"{_BOLD}{_CYAN}┌{rule()}┐{_RESET}")
        self._w(f"{_BOLD}{_CYAN}│{_RESET}  📦  {_BOLD}{_WHITE}Historical Data Backfill{_RESET}"
                + " " * (W - 28) + f"{_BOLD}{_CYAN}│{_RESET}")
        self._w(f"{_BOLD}{_CYAN}├{rule()}┤{_RESET}")

        # ── Overall progress ──────────────────────────────────────────────────
        done  = self.state.targets_done
        total = self.state.targets_total
        frac  = done / total if total else 0.0
        pct   = int(frac * 100)
        bar   = _bar(frac)
        overall_elapsed = _fmt_duration(time.monotonic() - self.state.overall_start_ts)
        # Estimate overall ETA from elapsed + fraction done
        if frac > 0.02:
            overall_eta = _fmt_duration(
                (time.monotonic() - self.state.overall_start_ts) / frac * (1 - frac)
            )
        else:
            overall_eta = "calculating…"

        overall_line = f"  Overall   {bar}  {done}/{total}  ({pct}%)"
        self._w(f"{_BOLD}{_CYAN}│{_RESET}{overall_line:<{W}}{_BOLD}{_CYAN}│{_RESET}")

        elapsed_line = f"  Elapsed {_YELLOW}{overall_elapsed:<10}{_RESET}  ETA {_YELLOW}{overall_eta}{_RESET}"
        self._w(f"{_BOLD}{_CYAN}│{_RESET}{elapsed_line:<{W + 10}}{_BOLD}{_CYAN}│{_RESET}")

        self._w(f"{_BOLD}{_CYAN}├{rule()}┤{_RESET}")

        # ── Current symbol ────────────────────────────────────────────────────
        cur = self.state.current

        if cur is None:
            # Nothing running yet
            for _ in range(6):
                self._w(f"{_BOLD}{_CYAN}│{_RESET}" + " " * W + f"{_BOLD}{_CYAN}│{_RESET}")
        else:
            sym_header = f"  {_BOLD}{_WHITE}{cur.symbol}{_RESET} / {_CYAN}{cur.timeframe}{_RESET}" \
                         f"    [{_DIM}{cur.market}{_RESET}]"
            self._w(f"{_BOLD}{_CYAN}│{_RESET}{sym_header:<{W + 14}}{_BOLD}{_CYAN}│{_RESET}")

            if cur.market == "Yahoo":
                # Single-shot: no progress fraction available
                status_str  = f"  Status    {_YELLOW}{cur.status:<20}{_RESET}"
                candle_str  = f"  Candles   {_GREEN}{_fmt_num(cur.candles)}{_RESET}"
                self._w(f"{_BOLD}{_CYAN}│{_RESET}{status_str:<{W + 9}}{_BOLD}{_CYAN}│{_RESET}")
                self._w(f"{_BOLD}{_CYAN}│{_RESET}{candle_str:<{W + 9}}{_BOLD}{_CYAN}│{_RESET}")
                for _ in range(4):
                    self._w(f"{_BOLD}{_CYAN}│{_RESET}" + " " * W + f"{_BOLD}{_CYAN}│{_RESET}")
            else:
                # Binance: full progress info available
                total_span   = max(1, cur.now_epoch   - cur.target_epoch)
                covered_span = max(0, cur.now_epoch   - max(cur.cursor_epoch, cur.target_epoch))
                sym_frac     = min(1.0, covered_span / total_span)
                sym_pct      = int(sym_frac * 100)
                sym_bar      = _bar(sym_frac)

                progress_line = f"  Progress  {sym_bar}  {sym_pct}%"
                cursor_line   = f"  Cursor    {_YELLOW}{_fmt_epoch(cur.cursor_epoch) if cur.cursor_epoch else '—'}{_RESET}"
                target_line   = f"  Target    {_DIM}{_fmt_epoch(cur.target_epoch)}{_RESET}"
                counts_line   = (
                    f"  Windows   {_GREEN}{_fmt_num(cur.windows):<10}{_RESET}"
                    f"Candles   {_GREEN}{_fmt_num(cur.candles)}{_RESET}"
                )

                # Per-symbol ETA
                elapsed_sym = time.monotonic() - cur.start_ts
                if sym_frac > 0.02:
                    eta_sym = _fmt_duration(elapsed_sym / sym_frac * (1 - sym_frac))
                else:
                    eta_sym = "calculating…"
                timing_line = (
                    f"  Elapsed   {_YELLOW}{_fmt_duration(elapsed_sym):<10}{_RESET}"
                    f"ETA       {_YELLOW}~{eta_sym}{_RESET}"
                )

                self._w(f"{_BOLD}{_CYAN}│{_RESET}{progress_line:<{W + 9}}{_BOLD}{_CYAN}│{_RESET}")
                self._w(f"{_BOLD}{_CYAN}│{_RESET}{cursor_line:<{W + 9}}{_BOLD}{_CYAN}│{_RESET}")
                self._w(f"{_BOLD}{_CYAN}│{_RESET}{target_line:<{W + 6}}{_BOLD}{_CYAN}│{_RESET}")
                self._w(f"{_BOLD}{_CYAN}│{_RESET}{counts_line:<{W + 18}}{_BOLD}{_CYAN}│{_RESET}")
                self._w(f"{_BOLD}{_CYAN}│{_RESET}{timing_line:<{W + 18}}{_BOLD}{_CYAN}│{_RESET}")
                # spacer
                self._w(f"{_BOLD}{_CYAN}│{_RESET}" + " " * W + f"{_BOLD}{_CYAN}│{_RESET}")

        # ── Footer ────────────────────────────────────────────────────────────
        self._w(f"{_BOLD}{_CYAN}└{rule()}┘{_RESET}")

        sys.stdout.flush()

    def _refresh(self) -> None:
        """Clear and redraw the panel."""
        if self._ansi:
            self._reset_cursor()
        self._render_panel()

    # ── Fallback (non-TTY) ────────────────────────────────────────────────────

    def _plain_log(self, msg: str) -> None:
        """Emit a plain-text line when ANSI is not available."""
        sys.stdout.write(msg + "\n")
        sys.stdout.flush()

    # ── Public lifecycle ──────────────────────────────────────────────────────

    def start(self) -> None:
        self._silence_loggers()
        if self._ansi:
            self._render_panel()
        else:
            self._plain_log("Starting historical data backfill…")

    def begin_symbol(
        self,
        symbol:       str,
        timeframe:    str,
        market:       str,
        now_epoch:    int,
        target_epoch: int,
    ) -> None:
        sp = SymbolProgress(
            symbol       = symbol,
            timeframe    = timeframe,
            market       = market,
            now_epoch    = now_epoch,
            target_epoch = target_epoch,
            cursor_epoch = now_epoch,
            status       = "starting…",
        )
        self.state.current = sp
        if self._ansi:
            self._refresh()
        else:
            self._plain_log(
                f"  [{self.state.targets_done + 1}/{self.state.targets_total}]"
                f"  {symbol}/{timeframe}  ({market})"
            )

    def update_window(
        self,
        cursor_epoch: int,
        windows:      int,
        candles:      int,
        status:       str = "running…",
    ) -> None:
        if self.state.current is None:
            return
        sp = self.state.current
        sp.cursor_epoch = cursor_epoch
        sp.windows      = windows
        sp.candles      = candles
        sp.status       = status
        if self._ansi:
            self._refresh()
        else:
            # Throttle plain output: only print every 10 windows to avoid spam
            if windows % 10 == 0 or windows == 1:
                total_span   = max(1, sp.now_epoch - sp.target_epoch)
                covered_span = max(0, sp.now_epoch - max(cursor_epoch, sp.target_epoch))
                pct = int(min(1.0, covered_span / total_span) * 100)
                self._plain_log(
                    f"    {symbol_tf(sp)}  {pct:3d}%  "
                    f"windows={_fmt_num(windows)}  candles={_fmt_num(candles)}"
                    f"  cursor={_fmt_epoch(cursor_epoch)}"
                )

    def finish_symbol(self, status: str = "complete") -> None:
        if self.state.current is None:
            return
        sp = self.state.current
        sp.status = status
        sp.end_ts = time.monotonic()
        self.state.completed.append(sp)
        self.state.current = None
        self.state.targets_done += 1
        if self._ansi:
            self._refresh()
        else:
            elapsed = _fmt_duration((sp.end_ts or time.monotonic()) - sp.start_ts)
            self._plain_log(
                f"    ✔  {symbol_tf(sp)}  {status}"
                f"  candles={_fmt_num(sp.candles)}  ({elapsed})"
            )

    def finish(self) -> None:
        """Called after all targets are done."""
        self._restore_loggers()
        if self._ansi:
            # Do a final render with all done, then print a summary below
            self.state.current = None
            self._reset_cursor()
            self._render_panel()
        total_candles = sum(s.candles for s in self.state.completed)
        elapsed = _fmt_duration(time.monotonic() - self.state.overall_start_ts)
        sys.stdout.write(
            f"\n  {_BOLD}{_GREEN}✔  Backfill complete{_RESET}"
            f"  —  {_fmt_num(total_candles)} candles"
            f" across {len(self.state.completed)} pairs"
            f"  ({elapsed})\n\n"
        )
        sys.stdout.flush()


# ── Utility ───────────────────────────────────────────────────────────────────

def symbol_tf(sp: SymbolProgress) -> str:
    return f"{sp.symbol}/{sp.timeframe}"
