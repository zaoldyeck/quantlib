"""[LEGACY 2026-07-09] 已被 research/trading/execution/ 取代,保存為參考實作。

吸收進 execution/ 的部分:OFI/VPIN/TPO/SMC 訊號(microstructure.py,含修正)、
isTrial 過濾、啟動時對帳防重複買、Bid1 pegging 概念(改為階梯+死線)。
未吸收而被修正的問題:(1) recent_sell_volume 無衰減 → 竭盡條件在首筆主賣後
永久為假(致命,改滾動 90s 窗);(2) 無升級死線 → 開高走高的贏家永遠買不到
(違反本系統動能證據,改死線強制升級);(3) dry-run 掛單即成交(TCA 無意義);
(4) Ctrl+C 不撤在途單;(5) 終態只認 30(漏 40/90);(6) 標的寫死在程式內。
"""
from __future__ import annotations
import argparse
import asyncio
import json
import os
import sys
from datetime import datetime
from enum import Enum
from pathlib import Path
from collections import deque
from research.brokers.fubon import FubonBroker, StockOrderRequest

# Ensure PYTHONPATH includes root
sys.path.append(str(Path(__file__).resolve().parents[2]))

class ExecutionState(Enum):
    OBSERVING = "OBSERVING"       # Watching the market drop, looking for exhaustion
    TRIGGERED = "TRIGGERED"       # Slide stopped, ready to place buy order
    SUBMITTED = "SUBMITTED"       # Order placed at Bid 1, monitoring Bid 1 to modify price
    FILLED = "FILLED"             # Order fully executed
    FAILED = "FAILED"             # Order failed or rejected

class SmartExecutionTracker:
    def __init__(self, symbol: str, quantity: int, max_price: float, initial_last: float, initial_low: float):
        self.symbol = symbol
        self.quantity = quantity
        self.max_price = max_price
        
        self.state = ExecutionState.OBSERVING
        
        # Market tracking variables
        self.session_low = initial_last  # Track stabilization since script started monitoring
        self.last_low_time = datetime.now()
        self.daily_low = initial_low     # Real daily low for display/benchmark
        
        # Rolling sell trade volume for exhaustion detection
        self.recent_sell_volume = 0
        self.peak_sell_volume = 0
        
        # Order booking parameters
        self.active_order_id = None
        self.active_order_price = 0.0
        
        # Book price tracking
        self.current_bid1 = 0.0
        self.current_ask1 = 0.0
        
        # TPO Profile fields
        self.vah = 0.0
        self.val = 0.0
        self.poc = 0.0
        
        # SMC Structure fields
        self.order_blocks = []
        self.fvgs = []
        self.sweep_detected = False
        self.sweep_time = None
        
        # OFI (Order Flow Imbalance) fields
        self.prev_bid_price = 0.0
        self.prev_bid_size = 0
        self.prev_ask_price = 0.0
        self.prev_ask_size = 0
        self.ofi_history = deque(maxlen=15)
        self.current_ofi = 0.0
        self.ofi_sma = 0.0
        
        # VPIN (Volume Toxicity) fields
        self.vpin_bucket_size = 500  # volume bucket threshold
        self.vpin_buy_volume = 0.0
        self.vpin_sell_volume = 0.0
        self.vpin_accumulated_volume = 0.0
        self.prev_trade_price = 0.0
        self.vpin_history = deque(maxlen=10)
        self.current_vpin = 0.0
        
    def update_structures(self, bars: list[dict]):
        """Update TPO Profile and SMC (Order Block, FVG, Liquidity Sweep) structures using 1-minute bars."""
        if not bars:
            return
            
        # --- 1. Compute TPO Profile ---
        lows = [b["low"] for b in bars]
        highs = [b["high"] for b in bars]
        min_p = min(lows)
        max_p = max(highs)
        
        range_p = max_p - min_p
        if range_p <= 0:
            bin_size = 1.0
        else:
            # Create ~50 bins for profiling
            bin_size = max(0.05, range_p / 50.0)
            
        from collections import defaultdict
        tpo_counts = defaultdict(int)
        for b in bars:
            start_bin = int(b["low"] / bin_size)
            end_bin = int(b["high"] / bin_size)
            for bin_idx in range(start_bin, end_bin + 1):
                tpo_counts[bin_idx] += 1
                
        if tpo_counts:
            poc_bin = max(tpo_counts, key=tpo_counts.get)
            self.poc = poc_bin * bin_size + bin_size / 2.0
            
            # Value Area Calculation (70% of TPOs)
            total_tpos = sum(tpo_counts.values())
            target_tpos = int(total_tpos * 0.70)
            
            va_bins = {poc_bin}
            covered_tpos = tpo_counts[poc_bin]
            
            while covered_tpos < target_tpos:
                min_va = min(va_bins)
                max_va = max(va_bins)
                
                left_count = tpo_counts.get(min_va - 1, 0)
                right_count = tpo_counts.get(max_va + 1, 0)
                
                if left_count == 0 and right_count == 0:
                    break
                    
                if left_count >= right_count:
                    va_bins.add(min_va - 1)
                    covered_tpos += left_count
                else:
                    va_bins.add(max_va + 1)
                    covered_tpos += right_count
                    
            self.val = min(va_bins) * bin_size
            self.vah = (max(va_bins) + 1) * bin_size
        else:
            self.poc = min_p
            self.val = min_p
            self.vah = max_p
            
        # --- 2. Compute SMC Structures (FVG, Order Block, Liquidity Sweeps) ---
        new_fvgs = []
        new_obs = []
        
        if len(bars) >= 3:
            for i in range(1, len(bars) - 1):
                b1 = bars[i-1]
                b2 = bars[i]
                b3 = bars[i+1]
                
                # Bullish FVG
                if b3["low"] > b1["high"]:
                    new_fvgs.append({
                        "top": b3["low"],
                        "bottom": b1["high"],
                        "mid": (b3["low"] + b1["high"]) / 2.0,
                        "mitigated": False
                    })
                    
                    # Bullish Order Block
                    if b2["close"] < b2["open"]:
                        new_obs.append({
                            "top": b2["high"],
                            "bottom": b2["low"],
                            "mitigated": False
                        })
                    elif b1["close"] < b1["open"]:
                        new_obs.append({
                            "top": b1["high"],
                            "bottom": b1["low"],
                            "mitigated": False
                        })
                        
            # Liquidity Sweep Detection (using 15 bars swing low lookback)
            if len(bars) > 15:
                latest_bar = bars[-1]
                prev_bars = bars[-16:-1]
                local_low = min(b["low"] for b in prev_bars)
                
                if latest_bar["low"] < local_low and latest_bar["close"] >= local_low:
                    if not self.sweep_detected or (self.sweep_time and (datetime.now() - self.sweep_time).total_seconds() > 60):
                        self.sweep_detected = True
                        self.sweep_time = datetime.now()
                        print(f"[{self.symbol}] 💰 SMC Liquidity Sweep detected! Swept low {local_low} TWD, closed at {latest_bar['close']} TWD")
                    
        self.fvgs = new_fvgs
        self.order_blocks = new_obs
        
    def update_ofi(self, bids: list[tuple[float, int]], asks: list[tuple[float, int]]):
        """Update Order Flow Imbalance (OFI) based on level 1 bid/ask updates."""
        if not bids or not asks:
            return
            
        bid_p, bid_s = bids[0]
        ask_p, ask_s = asks[0]
        
        if self.prev_bid_price == 0.0:
            self.prev_bid_price = bid_p
            self.prev_bid_size = bid_s
            self.prev_ask_price = ask_p
            self.prev_ask_size = ask_s
            return
            
        # Delta Bid
        if bid_p > self.prev_bid_price:
            delta_bid = bid_s
        elif bid_p == self.prev_bid_price:
            delta_bid = bid_s - self.prev_bid_size
        else:
            delta_bid = -self.prev_bid_size
            
        # Delta Ask
        if ask_p < self.prev_ask_price:
            delta_ask = ask_s
        elif ask_p == self.prev_ask_price:
            delta_ask = ask_s - self.prev_ask_size
        else:
            delta_ask = -self.prev_ask_size
            
        ofi = delta_bid - delta_ask
        self.ofi_history.append(ofi)
        self.current_ofi = ofi
        
        # Calculate SMA
        self.ofi_sma = sum(self.ofi_history) / len(self.ofi_history) if self.ofi_history else 0.0
        
        self.prev_bid_price = bid_p
        self.prev_bid_size = bid_s
        self.prev_ask_price = ask_p
        self.prev_ask_size = ask_s

    def update_vpin_trade(self, price: float, size: int):
        """Accumulate trade volume into VPIN bucket using the Tick Rule classification."""
        if self.prev_trade_price == 0.0:
            self.prev_trade_price = price
            return
            
        if price > self.prev_trade_price:
            self.vpin_buy_volume += size
        elif price < self.prev_trade_price:
            self.vpin_sell_volume += size
        else:
            self.vpin_buy_volume += size / 2.0
            self.vpin_sell_volume += size / 2.0
            
        self.vpin_accumulated_volume += size
        self.prev_trade_price = price
        
        if self.vpin_accumulated_volume >= self.vpin_bucket_size:
            imbalance = abs(self.vpin_buy_volume - self.vpin_sell_volume)
            vpin = imbalance / self.vpin_accumulated_volume if self.vpin_accumulated_volume > 0 else 0.0
            self.vpin_history.append(vpin)
            self.current_vpin = vpin
            
            self.vpin_buy_volume = 0.0
            self.vpin_sell_volume = 0.0
            self.vpin_accumulated_volume = 0.0

    def update_trade(self, price: float, size: int, ask_price: float, bid_price: float):
        """Update based on tick trade details to determine price stabilization & sell volume exhaustion"""
        # Always update VPIN bucket to keep metrics fresh
        self.update_vpin_trade(price, size)
        
        if self.state != ExecutionState.OBSERVING:
            return
            
        # Update session lowest price
        if price < self.session_low:
            self.session_low = price
            self.last_low_time = datetime.now()
            print(f"[{self.symbol}] New session low: {price} TWD")
            
        # Update daily lowest price
        if price < self.daily_low:
            self.daily_low = price
            
        # Is this an aggressive sell (trades at or below Bid)?
        is_aggressive_sell = price <= bid_price if bid_price > 0 else False
        if is_aggressive_sell:
            self.recent_sell_volume += size
            if self.recent_sell_volume > self.peak_sell_volume:
                self.peak_sell_volume = self.recent_sell_volume

    def update_book(self, bids: list[tuple[float, int]], asks: list[tuple[float, int]]):
        """Update based on best 5 bids/asks to check support levels and prepare for Bid 1 pegging"""
        if not bids or not asks:
            return
            
        self.current_bid1 = bids[0][0]
        self.current_ask1 = asks[0][0]
        
        # Always update OFI metric
        self.update_ofi(bids, asks)
        
        if self.state == ExecutionState.OBSERVING:
            # Exhaustion check:
            # 1. Price has stabilized: no new low in the last 60 seconds (or 30 seconds if strong buying pressure)
            time_since_last_low = (datetime.now() - self.last_low_time).total_seconds()
            price_stabilized = time_since_last_low > 60.0 or (self.ofi_sma > 3.0 and time_since_last_low > 30.0)
            
            # 2. Book support: buy order volume of Bid 1 to Bid 3 is larger than Ask 1 to Ask 3
            bid_volume_3 = sum(b[1] for b in bids[:3])
            ask_volume_3 = sum(a[1] for a in asks[:3])
            support_present = bid_volume_3 > 2.0 * ask_volume_3
            
            # 3. Aggressive sells slowing down: recent sell volume is less than 30% of peak
            sell_volume_exhausted = self.recent_sell_volume < 0.3 * self.peak_sell_volume if self.peak_sell_volume > 0 else True
            
            # 4. TPO & SMC Value Zone Constraint (Price is cheap / mitigating OB/FVG)
            below_val = self.current_bid1 <= self.val if self.val > 0 else False
            in_ob = any(ob["bottom"] <= self.current_bid1 <= ob["top"] for ob in self.order_blocks) if self.order_blocks else False
            in_fvg = any(fvg["bottom"] <= self.current_bid1 <= fvg["top"] for fvg in self.fvgs) if self.fvgs else False
            near_daily_low = self.current_bid1 <= self.daily_low * 1.015 if self.daily_low > 0 else True
            
            price_in_value_zone = below_val or in_ob or in_fvg or near_daily_low
            
            # 5. Microstructure Flow Confirmation (OFI & VPIN)
            bidding_pressure_ok = self.ofi_sma > 0.0
            
            # VPIN is considered toxic if it's extremely high (> 0.75) and we are not in stabilization
            toxicity_is_safe = self.current_vpin < 0.75
            
            # Trigger conditions
            if price_stabilized and support_present and sell_volume_exhausted and price_in_value_zone and bidding_pressure_ok and toxicity_is_safe:
                self.state = ExecutionState.TRIGGERED
                reasons = []
                if below_val: reasons.append("Below VAL")
                if in_ob: reasons.append("Mitigating OB")
                if in_fvg: reasons.append("Mitigating FVG")
                if near_daily_low: reasons.append("Near Daily Low")
                print(f"[{self.symbol}] ⚡ UMEE Triggered! Price: {self.current_bid1} TWD. Reasons: {', '.join(reasons)}. (OFI: {self.ofi_sma:.1f}, VPIN: {self.current_vpin:.2f}). Triggering buy order...")

class SmartOrderExecutor:
    def __init__(self, dry_run: bool = True):
        self.dry_run = dry_run
        self.broker = None
        self.account = None
        self.trackers: dict[str, SmartExecutionTracker] = {}
        self.loop = None
        
    def setup_trackers(self):
        targets = [
            {"symbol": "6446", "quantity": 1, "max_price": 1500.0},
            {"symbol": "3006", "quantity": 1, "max_price": 220.0},
            {"symbol": "5289", "quantity": 1, "max_price": 1600.0},
            {"symbol": "6223", "quantity": 1, "max_price": 6340.0},
            {"symbol": "2408", "quantity": 1, "max_price": 405.0},
            {"symbol": "8109", "quantity": 1, "max_price": 170.0},
            {"symbol": "3413", "quantity": 1, "max_price": 360.0},
        ]
        
        # Fetch active orders and today's filled history from broker to synchronize state and prevent duplicates
        active_orders = {}
        filled_symbols = set()
        if not self.dry_run:
            try:
                # 1. Fetch active/pending orders
                order_results = self.broker.get_order_results()
                if hasattr(order_results, "data") and order_results.data:
                    for o in order_results.data:
                        stock_no = getattr(o, "stock_no", None)
                        status = int(getattr(o, "status", 99))
                        filled_qty = int(getattr(o, "filled_qty", 0))
                        qty = int(getattr(o, "quantity", 0))
                        buy_sell = getattr(o, "buy_sell", None)
                        
                        is_buy = "Buy" in str(buy_sell)
                        
                        # Active order criteria: status != 30 (not cancelled) and not fully filled
                        if stock_no and is_buy and status != 30 and filled_qty < qty:
                            active_orders[stock_no] = {
                                "seq_no": getattr(o, "seq_no", None),
                                "price": float(getattr(o, "price", 0.0)),
                                "quantity": qty,
                                "filled_qty": filled_qty
                            }
                            print(f"[Sync] Found active Buy order on broker for {stock_no}: Seq={getattr(o, 'seq_no')}, Px={getattr(o, 'price')}, Qty={qty}")
            except Exception as e:
                print(f"[Sync] Error fetching active orders from broker: {e}")
                
            try:
                # 2. Fetch today's filled history
                from datetime import datetime
                today_str = datetime.now().strftime("%Y%m%d")
                filled_history = self.broker.get_filled_history(start_date=today_str, end_date=today_str)
                if hasattr(filled_history, "data") and filled_history.data:
                    for f in filled_history.data:
                        stock_no = getattr(f, "stock_no", None)
                        buy_sell = getattr(f, "buy_sell", None)
                        is_buy = "Buy" in str(buy_sell)
                        if stock_no and is_buy:
                            filled_symbols.add(stock_no)
                            print(f"[Sync] Found already FILLED Buy order today for {stock_no}")
            except Exception as e:
                print(f"[Sync] Error fetching filled history from broker: {e}")
                
        # Fetch initial daily stats/quotes for initialization
        print("Fetching initial daily stats from Fubon REST API...")
        for t in targets:
            symbol = t["symbol"]
            initial_last = float("inf")
            initial_low = float("inf")
            
            if not self.dry_run:
                try:
                    res_quote = self.broker.sdk.marketdata.rest_client.stock.intraday.quote(symbol=symbol)
                    if res_quote:
                        initial_low = res_quote.get("lowPrice") or res_quote.get("previousClose") or res_quote.get("referencePrice") or float("inf")
                        initial_last = res_quote.get("lastPrice") or res_quote.get("previousClose") or res_quote.get("referencePrice") or float("inf")
                        print(f"[{symbol}] Initialized: Daily Low={initial_low} TWD, Session Start={initial_last} TWD")
                except Exception as e:
                    print(f"[{symbol}] Failed to fetch initial quote from API: {e}. Defaulting to fallback estimate.")
            
            # Fallback for dry-run or API failure
            if initial_low == float("inf") or initial_low is None:
                initial_low = t["max_price"] * 0.95
            if initial_last == float("inf") or initial_last is None:
                initial_last = t["max_price"] * 0.96
                
            tracker = SmartExecutionTracker(
                symbol=symbol,
                quantity=t["quantity"],
                max_price=t["max_price"],
                initial_last=initial_last,
                initial_low=initial_low
            )
            
            # Synchronize active order or filled status if found on broker
            if symbol in filled_symbols:
                tracker.state = ExecutionState.FILLED
                print(f"[{symbol}] Synchronized tracker to FILLED state (already bought today)")
            elif symbol in active_orders:
                ord_info = active_orders[symbol]
                tracker.state = ExecutionState.SUBMITTED
                tracker.active_order_id = ord_info["seq_no"]
                tracker.active_order_price = ord_info["price"]
                print(f"[{symbol}] Synchronized tracker to SUBMITTED state with active Order Seq={tracker.active_order_id} @ {tracker.active_order_price} TWD")
                
            self.trackers[symbol] = tracker
            
    def handle_ws_message(self, message: str):
        try:
            data = json.loads(message)
            event = data.get("event")
            if event == "data":
                payload = data.get("data", {})
                symbol = payload.get("symbol")
                tracker = self.trackers.get(symbol)
                if not tracker:
                    return
                    
                channel = data.get("channel")
                if channel == "trades":
                    # Ignore trial matched quotes before open
                    if payload.get("isTrial"):
                        return
                    price = payload.get("price")
                    size = payload.get("size")
                    ask = payload.get("ask", 0.0)
                    bid = payload.get("bid", 0.0)
                    tracker.update_trade(price, size, ask, bid)
                elif channel == "books":
                    # bids and asks are lists of dicts or lists of list depending on Fubon API schema
                    raw_bids = payload.get("bids", [])
                    raw_asks = payload.get("asks", [])
                    
                    # Convert to list of tuples: (price, size)
                    bids = [(float(b.get("price")), int(b.get("size"))) for b in raw_bids if b.get("price")]
                    asks = [(float(a.get("price")), int(a.get("size"))) for a in raw_asks if a.get("price")]
                    
                    tracker.update_book(bids, asks)
                    
                    # Run execution loop step for this stock
                    self.execute_step(tracker)
        except Exception as e:
            # Avoid printing parse errors for heartbeats
            pass

    def execute_step(self, tracker: SmartExecutionTracker):
        """State machine tick execution step"""
        if tracker.state == ExecutionState.TRIGGERED:
            # Submit initial order at Bid 1
            target_price = min(tracker.current_bid1, tracker.max_price)
            if target_price <= 0:
                return
                
            tracker.state = ExecutionState.SUBMITTED
            tracker.active_order_price = target_price
            
            print(f"[{tracker.symbol}] Placing pegged limit order: Buy {tracker.quantity} shares @ {target_price} TWD (Bid 1)...")
            
            if self.dry_run:
                tracker.active_order_id = f"mock-order-{tracker.symbol}"
                print(f"[{tracker.symbol}] (Dry Run) Order successfully placed. GUID: {tracker.active_order_id}")
            else:
                try:
                    req = StockOrderRequest(
                        symbol=tracker.symbol,
                        side="Buy",
                        quantity=tracker.quantity,
                        price=str(target_price),
                        price_type="Limit",
                        market_type="IntradayOdd",
                        time_in_force="ROD",
                        order_type="Stock"
                    )
                    res = self.broker.place_stock_order(req)
                    if hasattr(res, "is_success") and res.is_success:
                        # Extract order sequence or no
                        tracker.active_order_id = getattr(res.data, "seq_no", None) or getattr(res.data, "order_no", None)
                        print(f"[{tracker.symbol}] Order submitted. Seq No: {tracker.active_order_id}")
                    else:
                        tracker.state = ExecutionState.FAILED
                        print(f"[{tracker.symbol}] Order placement failed: {getattr(res, 'message', 'Unknown error')}")
                except Exception as exc:
                    tracker.state = ExecutionState.FAILED
                    print(f"[{tracker.symbol}] Order placement exception: {exc}")
                    
        elif tracker.state == ExecutionState.SUBMITTED:
            # We are monitoring the order.
            # 1. Did Bid 1 move higher?
            target_price = min(tracker.current_bid1, tracker.max_price)
            if target_price > tracker.active_order_price and target_price <= tracker.max_price:
                # Cancel and re-submit order to follow the market Bid 1 price (pegging)
                print(f"[{tracker.symbol}] Bid 1 moved from {tracker.active_order_price} to {target_price}. Re-submitting order...")
                
                if self.dry_run:
                    tracker.active_order_price = target_price
                    print(f"[{tracker.symbol}] (Dry Run) Order price successfully updated to {target_price} TWD.")
                else:
                    try:
                        # For IntradayOdd (odd lots), price modification is not allowed on TWSE.
                        # We must cancel the existing order first, then re-submit.
                        order_results = self.broker.get_order_results()
                        matched_order = None
                        if hasattr(order_results, "data") and order_results.data:
                            for o in order_results.data:
                                if getattr(o, "seq_no", None) == tracker.active_order_id or getattr(o, "order_no", None) == tracker.active_order_id:
                                    matched_order = o
                                    break
                        
                        if matched_order:
                            print(f"[{tracker.symbol}] Cancelling existing order {tracker.active_order_id} first...")
                            res_cancel = self.broker.sdk.stock.cancel_order(self.account, matched_order)
                            if hasattr(res_cancel, "is_success") and res_cancel.is_success:
                                print(f"[{tracker.symbol}] Cancellation successful. Resetting state to re-trigger order at {target_price} TWD.")
                                tracker.state = ExecutionState.TRIGGERED
                                tracker.active_order_id = None
                                tracker.active_order_price = 0.0
                            else:
                                print(f"[{tracker.symbol}] Cancellation failed: {getattr(res_cancel, 'message', 'Unknown error')}. Retrying next step.")
                        else:
                            print(f"[{tracker.symbol}] Active order {tracker.active_order_id} not found in broker order results. May be filled or cancelled already. Resetting state to re-trigger.")
                            tracker.state = ExecutionState.TRIGGERED
                            tracker.active_order_id = None
                            tracker.active_order_price = 0.0
                    except Exception as exc:
                        print(f"[{tracker.symbol}] Cancel-and-replace exception: {exc}")
            
            # Check for fill status (Simulated in dry run, queried via broker API in live)
            if self.dry_run:
                # In dry run, simulate that if Bid 1 has crossed our order price or after a delay, it is filled
                # For demo purposes, we fill it after 15 seconds in submitted state
                tracker.state = ExecutionState.FILLED
                print(f"[{tracker.symbol}] 🎉 (Dry Run) Buy order of {tracker.quantity} shares @ {tracker.active_order_price} TWD is FULLY FILLED!")
            else:
                try:
                    # Query active order details
                    order_results = self.broker.get_order_results()
                    if hasattr(order_results, "data") and order_results.data:
                        for o in order_results.data:
                            if getattr(o, "seq_no", None) == tracker.active_order_id or getattr(o, "order_no", None) == tracker.active_order_id:
                                filled_qty = getattr(o, "filled_qty", 0)
                                if filled_qty >= tracker.quantity:
                                    tracker.state = ExecutionState.FILLED
                                    print(f"[{tracker.symbol}] 🎉 Buy order of {tracker.quantity} shares is FULLY FILLED @ {getattr(o, 'price', tracker.active_order_price)} TWD!")
                                break
                except Exception as exc:
                    print(f"[{tracker.symbol}] Error querying fill status: {exc}")

    async def update_all_structures(self):
        """Query 1-minute candles from REST API and update TPO & SMC structures for each tracker."""
        if self.dry_run:
            # In dry run, generate mock candles for testing
            for symbol, tracker in self.trackers.items():
                if tracker.state in (ExecutionState.FILLED, ExecutionState.FAILED):
                    continue
                import random
                mock_bars = []
                base_px = tracker.daily_low if (tracker.daily_low > 0 and tracker.daily_low != float("inf")) else tracker.max_price * 0.95
                for i in range(100):
                    px = base_px + (i - 50) * 0.1 + random.uniform(-0.5, 0.5)
                    mock_bars.append({
                        "open": px - 0.2,
                        "high": px + 0.4,
                        "low": px - 0.5,
                        "close": px + 0.1,
                        "volume": 100
                    })
                tracker.update_structures(mock_bars)
            return

        for symbol, tracker in self.trackers.items():
            if tracker.state in (ExecutionState.FILLED, ExecutionState.FAILED):
                continue
            try:
                # Query intraday 1-minute candles
                res = self.broker.sdk.marketdata.rest_client.stock.intraday.candles(symbol=symbol)
                if isinstance(res, dict) and "data" in res and res["data"]:
                    bars = res["data"]
                    tracker.update_structures(bars)
                # Keep rate limit clean
                await asyncio.sleep(0.2)
            except Exception as e:
                print(f"[Structure Update] Error updating structures for {symbol}: {e}")
                
            # Initialize VPIN using historical trades once on startup
            if len(tracker.vpin_history) == 0:
                try:
                    res_trades = self.broker.sdk.marketdata.rest_client.stock.intraday.trades(symbol=symbol, limit=500)
                    if isinstance(res_trades, dict) and "data" in res_trades and res_trades["data"]:
                        trades_data = res_trades["data"]
                        # Sort chronologically by time or serial
                        sorted_trades = sorted(trades_data, key=lambda x: x.get("time", 0))
                        for t in sorted_trades:
                            tracker.update_vpin_trade(t.get("price", 0.0), t.get("size", 0))
                        if tracker.vpin_history:
                            tracker.current_vpin = tracker.vpin_history[-1]
                            print(f"[{symbol}] Initialized VPIN from {len(sorted_trades)} historical trades. Current VPIN: {tracker.current_vpin:.2f}")
                    await asyncio.sleep(0.2)
                except Exception as e:
                    print(f"[VPIN Init] Error initializing VPIN for {symbol}: {e}")

    async def background_structure_updater(self):
        """Periodically update TPO & SMC structures every 60 seconds."""
        while not self.all_completed():
            try:
                await self.update_all_structures()
            except Exception as e:
                print(f"[Structure Updater Loop] Exception: {e}")
            await asyncio.sleep(60.0)

    def all_completed(self) -> bool:
        return all(t.state in (ExecutionState.FILLED, ExecutionState.FAILED) for t in self.trackers.values())

    async def run(self):
        print("Initializing Fubon Broker SDK...")
        self.broker = FubonBroker.from_env()
        try:
            self.account = self.broker.login()
            print(f"Logged in successfully. Account: {self.account.account[:4]}***")
        except Exception as exc:
            print(f"Login failed: {exc}")
            return

        # Initialize realtime SDK first so that marketdata attributes are populated for REST queries
        self.broker.sdk.init_realtime()

        self.setup_trackers()

        # Initialize TPO & SMC structures on startup
        print("Calculating initial TPO and SMC structures from 1-minute historical candles...")
        await self.update_all_structures()

        print("Connecting WebSocket streaming data...")
        stock = self.broker.sdk.marketdata.websocket_client.stock
        
        # Set callback
        stock.on("message", self.handle_ws_message)
        stock.connect()
        print("WebSocket connected.")
        
        # Start background TPO/SMC structures updater
        asyncio.create_task(self.background_structure_updater())
        
        # Subscribe to channels for each symbol
        await asyncio.sleep(1)
        for symbol in self.trackers:
            print(f"Subscribing to trades and books for {symbol}...")
            stock.subscribe({"channel": "trades", "symbol": symbol})
            await asyncio.sleep(0.1)
            stock.subscribe({"channel": "books", "symbol": symbol})
            await asyncio.sleep(0.1)

        print("\n" + "="*80)
        print("🚀 Smart Execution Engine is active! Monitoring market drops and exhaustion...")
        print("="*80 + "\n")

        # Keep running until all trackers are completed
        try:
            while not self.all_completed():
                await asyncio.sleep(2)
                
                # Periodically print tracker status summary
                print("\n--- Current Engine Status Summary ---")
                for s, t in self.trackers.items():
                    tpo_info = f"VAL: {t.val:<6.1f} | POC: {t.poc:<6.1f}" if t.val > 0 else "TPO: N/A"
                    flow_info = f"OFI: {t.ofi_sma:<+5.1f} | VPIN: {t.current_vpin:<4.2f}"
                    print(f"Symbol: {s:<5} | State: {t.state.value:<10} | Low: {t.daily_low:<6} | {tpo_info} | {flow_info} | Bid1: {t.current_bid1:<6} | Ask1: {t.current_ask1:<6} | Active Px: {t.active_order_price:<6}")
                print("-" * 50)
                
            print("\n🎉 All smart orders have been successfully executed! Shutting down engine.")
        except KeyboardInterrupt:
            print("\nKeyboardInterrupt received. Shutting down engine...")
        except Exception as e:
            print(f"Engine exception: {e}")

def main():
    parser = argparse.ArgumentParser(description="Smart Execution Algo.")
    parser.add_argument("--live", action="store_true", help="Run live order execution instead of dry run")
    args = parser.parse_args()
    
    # Read FUBON_DRY_RUN env or command line flag
    dry_run = not args.live
    if not dry_run:
        # User requested live, verify if FUBON_DRY_RUN in env is also false
        env_dry = os.environ.get("FUBON_DRY_RUN", "true").lower() in {"1", "true", "yes"}
        if env_dry:
            print("Warning: FUBON_DRY_RUN is still true in .env. Overriding to false for live execution.")
            os.environ["FUBON_DRY_RUN"] = "false"
            
    executor = SmartOrderExecutor(dry_run=dry_run)
    
    # Run asyncio loop
    loop = asyncio.get_event_loop()
    loop.run_until_complete(executor.run())

if __name__ == "__main__":
    main()
