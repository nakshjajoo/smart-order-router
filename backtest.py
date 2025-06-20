import json
import time
import numpy as np
import pandas as pd
from kafka import KafkaConsumer
from collections import namedtuple
from itertools import product
import sys

# a named tuple for venue data for easier access
Venue = namedtuple('Venue', ['ask', 'ask_size', 'fee', 'rebate', 'id'])

""" Allocator and Cost Functions (from pseudocode) """

# Computes the total expected cost for a given allocation split
def compute_cost(split, venues, order_size, lambda_over, lambda_under, theta_queue):
    executed = 0
    cash_spent = 0
    
    for i in range(len(venues)):
        # we can only take what's available at the best ask
        exe = min(split[i], venues[i].ask_size)
        executed += exe
        
        # cost for executed shares (price + fee) (assuming fee is a per-share cost)
        cash_spent += exe * (venues[i].ask + venues[i].fee)
        
        # For this static model, we assume no rebates as we are only sending aggressive orders.
        # maker_rebate = max(split[i] - exe, 0) * venues[i].rebate
        # cash_spent -= maker_rebate

    underfill = max(order_size - executed, 0)
    overfill = max(executed - order_size, 0)
    
    # risk penalty for deviation from target size
    risk_pen = theta_queue * (underfill + overfill)
    
    # cost penalty for under/overfill
    cost_pen = lambda_under * underfill + lambda_over * overfill
    
    return cash_spent + risk_pen + cost_pen

# Finds the optimal allocation of shares across venues for a single snapshot. This is a simplified search inspired by the pseudocode.
def allocate(order_size, venues, lambda_over, lambda_under, theta_queue):
    step = 100  # search in 100-share chunks (as per pseudocode)
    
    # Generating possible allocation splits. This is a brute-force approach. For N venues and an order size S, the number of splits is large.
    # Let's constrain the search space to make it tractable. We will generate allocations for each venue up to a reasonable limit.
    max_alloc_per_venue = int(min(order_size + 1, 1001)) # limit search to 1000 shares per venue
    
    possible_q = list(range(0, max_alloc_per_venue, step))
    
    # generate all combinations of allocations for the venues
    alloc_combinations = product(possible_q, repeat=len(venues))

    best_cost = float('inf')
    best_split = [0] * len(venues)

    for alloc in alloc_combinations:
        # we are looking to execute 'order_size' in total. The allocator should decide how many shares to attempt to buy. 
        # Let's evaluate splits that sum up to the order_size.
        if sum(alloc) != order_size:
            continue
            
        cost = compute_cost(alloc, venues, order_size, lambda_over, lambda_under, theta_queue)

        if cost < best_cost:
            best_cost = cost
            best_split = list(alloc)
            
    # If no split summing to order_size is found (for example, if order_size isn't a multiple of step), return an empty list.
    if sum(best_split) == 0 and order_size > 0:
        # Fallback: if the search fails, use a greedy approach for the SOR. This is a safety net.
        sorted_venues = sorted(venues, key=lambda v: v.ask)
        remaining_order = order_size
        temp_split = [0] * len(venues)
        for v in sorted_venues:
            v_index = venues.index(v)
            alloc_size = min(remaining_order, v.ask_size)
            temp_split[v_index] = alloc_size
            remaining_order -= alloc_size
            if remaining_order == 0:
                break
        return temp_split, compute_cost(temp_split, venues, order_size, lambda_over, lambda_under, theta_queue)


    return best_split, best_cost

""" Backtesting and Benchmarking Logic """

# Simulates the execution of a split, returning shares filled and cash spent
def execute_split(split, venues):
    shares_filled = 0
    cash_spent = 0
    for i, v in enumerate(venues):
        fill = min(split[i], v.ask_size)
        shares_filled += fill
        cash_spent += fill * (v.ask + v.fee)
    return shares_filled, cash_spent

# Runs the main SOR simulation
def run_sor_simulation(snapshots, parameters, total_shares_to_buy):
    lambda_over, lambda_under, theta_queue = parameters
    
    unfilled_shares = total_shares_to_buy
    total_cash_spent = 0
    
    for snapshot in snapshots:
        if unfilled_shares == 0:
            break
            
        # assuming a fixed fee structure for all venues
        venues = [Venue(ask=v['ask_px_00'], ask_size=v['ask_sz_00'], fee=0.003, rebate=0.002, id=v['publisher_id']) for v in snapshot]
        
        # We need to decide how many shares to attempt in this slice. A simple approach is to try to fill the entire remaining order.
        shares_to_attempt = unfilled_shares

        best_split, _ = allocate(shares_to_attempt, venues, lambda_over, lambda_under, theta_queue)

        if not best_split or sum(best_split) == 0:
            # If allocator returns no valid split, do nothing in this tick.
            continue
            
        shares_filled, cash_spent = execute_split(best_split, venues)
        
        unfilled_shares -= shares_filled
        total_cash_spent += cash_spent
        
    # Assume any remaining unfilled shares are bought at the last available best ask price
    if unfilled_shares > 0 and snapshots:
        last_snapshot = snapshots[-1]
        best_last_ask = min(v['ask_px_00'] for v in last_snapshot)
        total_cash_spent += unfilled_shares * (best_last_ask + 0.003) # Assume fee
        
    return total_cash_spent

# Baseline: Naively hit the best ask price across all venues
def run_best_ask_baseline(snapshots, total_shares_to_buy):
    unfilled_shares = total_shares_to_buy
    total_cash_spent = 0
    
    for snapshot in snapshots:
        if unfilled_shares == 0:
            break
            
        venues = [Venue(ask=v['ask_px_00'], ask_size=v['ask_sz_00'], fee=0.003, rebate=0.002, id=v['publisher_id']) for v in snapshot]
        
        # Find the single best venue in this snapshot
        best_venue = min(venues, key=lambda v: v.ask)
        
        fill_amount = min(unfilled_shares, best_venue.ask_size)
        
        if fill_amount > 0:
            total_cash_spent += fill_amount * (best_venue.ask + best_venue.fee)
            unfilled_shares -= fill_amount

    if unfilled_shares > 0 and snapshots:
        last_snapshot = snapshots[-1]
        best_last_ask = min(v['ask_px_00'] for v in last_snapshot)
        total_cash_spent += unfilled_shares * (best_last_ask + 0.003)

    return total_cash_spent

# Baseline: Execute shares in equal intervals over time (TWAP)
def run_twap_baseline(snapshots, total_shares_to_buy, sim_duration_seconds):
    unfilled_shares = total_shares_to_buy
    total_cash_spent = 0
    
    interval = 60  # 60-second intervals
    num_intervals = max(1, sim_duration_seconds // interval)
    shares_per_interval = total_shares_to_buy / num_intervals
    
    start_time = snapshots[0][0]['ts_event']
    start_time = pd.to_datetime(start_time)
    
    interval_end_time = start_time + pd.Timedelta(seconds=interval)
    shares_to_buy_this_interval = shares_per_interval
    
    for snapshot in snapshots:
        if unfilled_shares == 0:
            break
            
        current_time = pd.to_datetime(snapshot[0]['ts_event'])
        
        if current_time > interval_end_time:
             # Move to next interval
            interval_end_time += pd.Timedelta(seconds=interval)
            shares_to_buy_this_interval += shares_per_interval

        if shares_to_buy_this_interval >= 1:
            venues = [Venue(ask=v['ask_px_00'], ask_size=v['ask_sz_00'], fee=0.003, rebate=0.002, id=v['publisher_id']) for v in snapshot]
            best_venue = min(venues, key=lambda v: v.ask)
            
            fill_amount = min(shares_to_buy_this_interval, best_venue.ask_size)
            
            if fill_amount > 0:
                total_cash_spent += fill_amount * (best_venue.ask + best_venue.fee)
                unfilled_shares -= fill_amount
                shares_to_buy_this_interval -= fill_amount

    if unfilled_shares > 0 and snapshots:
        last_snapshot = snapshots[-1]
        best_last_ask = min(v['ask_px_00'] for v in last_snapshot)
        total_cash_spent += unfilled_shares * (best_last_ask + 0.003)
        
    return total_cash_spent

#Baseline: Execute shares weighted by volume (VWAP)
def run_vwap_baseline(snapshots, total_shares_to_buy, sim_duration_seconds):
    total_cash_spent = 0
    
    interval = 60  # 60-second intervals
    
    df = pd.DataFrame([item for sublist in snapshots for item in sublist])
    df['ts_event'] = pd.to_datetime(df['ts_event'])
    
    df['interval'] = (df.ts_event - df.ts_event.min()).dt.total_seconds() // interval
    
    interval_volume = df.groupby('interval')['ask_sz_00'].sum()
    total_volume = interval_volume.sum()
    
    if total_volume == 0: # Avoid division by zero
        return run_twap_baseline(snapshots, total_shares_to_buy, sim_duration_seconds)

    shares_per_interval = (interval_volume / total_volume) * total_shares_to_buy
    
    unfilled_shares = total_shares_to_buy
    
    for interval_idx, shares_to_buy in shares_per_interval.items():
        if unfilled_shares == 0: break
        
        interval_start_time = df.ts_event.min() + pd.Timedelta(seconds=interval_idx * interval)
        
        # Find the first snapshot in this interval
        first_snapshot_in_interval = None
        for snapshot in snapshots:
            snap_time = pd.to_datetime(snapshot[0]['ts_event'])
            if snap_time >= interval_start_time:
                first_snapshot_in_interval = snapshot
                break

        if first_snapshot_in_interval:
            venues = [Venue(ask=v['ask_px_00'], ask_size=v['ask_sz_00'], fee=0.003, rebate=0.002, id=v['publisher_id']) for v in first_snapshot_in_interval]
            
            # Simple greedy fill for the shares in this interval
            remaining_for_interval = shares_to_buy
            sorted_venues = sorted(venues, key=lambda v: v.ask)

            for v in sorted_venues:
                if remaining_for_interval == 0: break
                fill = min(remaining_for_interval, v.ask_size)
                if fill > 0:
                    total_cash_spent += fill * (v.ask + v.fee)
                    unfilled_shares -= fill
                    remaining_for_interval -= fill

    if unfilled_shares > 0 and snapshots:
        last_snapshot = snapshots[-1]
        best_last_ask = min(v['ask_px_00'] for v in last_snapshot)
        total_cash_spent += unfilled_shares * (best_last_ask + 0.003)

    return total_cash_spent


def main():
    topic = 'mock_l1_stream'
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        consumer_timeout_ms=15000  # Stop if no message for 15s
    )

    print("Consumer connected. Waiting for data...")
    snapshots = []
    for message in consumer:
        snapshots.append(message.value['snapshot'])
    
    consumer.close()
    
    if not snapshots:
        print("No data received from Kafka. Exiting.")
        sys.exit(1)
        
    print(f"Received {len(snapshots)} snapshots from Kafka.")
    
    # Parameter Search
    total_shares = 5000
    # Define a small grid for searching to keep it fast
    param_grid = {
        'lambda_over': [0.1, 0.5, 1.0],
        'lambda_under': [0.1, 0.5, 1.0],
        'theta_queue': [0.1, 0.5, 1.0]
    }
    
    best_params = {}
    lowest_cost = float('inf')
    
    # Generate all combinations of parameters
    keys, values = zip(*param_grid.items())
    param_combinations = [dict(zip(keys, v)) for v in product(*values)]
    
    print(f"Starting parameter search over {len(param_combinations)} combinations...")
    
    for params in param_combinations:
        cost = run_sor_simulation(snapshots, (params['lambda_over'], params['lambda_under'], params['theta_queue']), total_shares)
        if cost < lowest_cost:
            lowest_cost = cost
            best_params = params
            
    print(f"Parameter search complete. Best cost: {lowest_cost}, Best params: {best_params}")

    # Final Runs
    optimized_cash = run_sor_simulation(snapshots, (best_params['lambda_over'], best_params['lambda_under'], best_params['theta_queue']), total_shares)
    
    best_ask_cash = run_best_ask_baseline(snapshots, total_shares)
    
    sim_start_time = pd.to_datetime(snapshots[0][0]['ts_event'])
    sim_end_time = pd.to_datetime(snapshots[-1][0]['ts_event'])
    sim_duration = (sim_end_time - sim_start_time).total_seconds()
    
    twap_cash = run_twap_baseline(snapshots, total_shares, sim_duration)
    vwap_cash = run_vwap_baseline(snapshots, total_shares, sim_duration)

    # Result Formatting
    avg_fill_px_opt = optimized_cash / total_shares
    avg_fill_px_ask = best_ask_cash / total_shares
    avg_fill_px_twap = twap_cash / total_shares
    avg_fill_px_vwap = vwap_cash / total_shares

    def calculate_savings_bps(baseline_px, optimized_px):
        if baseline_px == 0: return 0
        return (baseline_px - optimized_px) / baseline_px * 10000

    results = {
        "best_parameters": best_params,
        "optimized": {
            "total_cash": round(optimized_cash, 2),
            "avg_fill_px": round(avg_fill_px_opt, 4)
        },
        "baselines": {
            "best_ask": {
                "total_cash": round(best_ask_cash, 2),
                "avg_fill_px": round(avg_fill_px_ask, 4)
            },
            "twap": {
                "total_cash": round(twap_cash, 2),
                "avg_fill_px": round(avg_fill_px_twap, 4)
            },
            "vwap": {
                "total_cash": round(vwap_cash, 2),
                "avg_fill_px": round(avg_fill_px_vwap, 4)
            }
        },
        "savings_vs_baselines_bps": {
            "best_ask": round(calculate_savings_bps(avg_fill_px_ask, avg_fill_px_opt), 2),
            "twap": round(calculate_savings_bps(avg_fill_px_twap, avg_fill_px_opt), 2),
            "vwap": round(calculate_savings_bps(avg_fill_px_vwap, avg_fill_px_opt), 2)
        }
    }
    
    # Print the final JSON to stdout
    print(json.dumps(results, indent=2))

if __name__ == "__main__":
    # Give Kafka a moment to be ready
    print("Waiting 10 seconds for Kafka producer to start streaming...")
    time.sleep(10)
    main()