# pip install pandas xlwings numpy
import pandas as pd
from datetime import datetime, timedelta
import calendar
import random
import warnings
import xlwings as xw
import time
from collections import defaultdict, Counter
import numpy as np
import math
import os
import sys

# 抑制 pandas 的 UserWarning，讓輸出更簡潔
warnings.filterwarnings("ignore", category=UserWarning)

# --- 輔助函數 (Helper Functions) ---

def get_qod_statistics(final_datedf: pd.DataFrame, ordered_people: list = None) -> pd.DataFrame:
    """統計每位人員發生 QOD (隔一天值班) 的次數。"""
    df = final_datedf.sort_values('date')
    scheduled_people = df['man'].unique()
    qod_counts = {}

    for person in scheduled_people:
        if pd.isna(person) or person == 'ZAZ':
            continue
        
        duty_dates = sorted(df[df['man'] == person]['date'].astype(int).tolist())
        count = 0
        for i in range(len(duty_dates) - 1):
            if duty_dates[i+1] - duty_dates[i] == 2:
                count += 1
        qod_counts[person] = count

    qod_df = pd.DataFrame.from_dict(qod_counts, orient='index', columns=['QOD'])
    qod_df.index.name = '人員'

    if ordered_people:
        qod_df = qod_df.reindex(ordered_people, fill_value=0)
    
    return qod_df

def sort_headers_by_row_values(df: pd.DataFrame) -> list:
    """根據 DataFrame 數值出現的 headers 總數進行隨機排列串接。"""
    flattened_data = df.stack().rename_axis(['row', 'header'])
    value_to_headers = {}
    for (row, header), value in flattened_data.items():
        if value not in value_to_headers:
            value_to_headers[value] = []
        value_to_headers[value].append(header)
    
    sorted_values = sorted(value_to_headers.keys(), reverse=True)
    final_sorted_list = []
    
    for value in sorted_values:
        headers = value_to_headers[value]
        random.shuffle(headers)
        final_sorted_list.extend(headers)
        
    return final_sorted_list

def fill_df_randomly_prioritize_rows(headers: list, values_to_fill: list, limit_df) -> pd.DataFrame:
    """隨機填充 DataFrame，優先填充較前面的行。"""
    df = pd.DataFrame([[np.nan] * len(headers)], columns=headers)
    num_values_to_fill = len(values_to_fill)
    num_columns = len(headers)

    required_rows_count = 1 if num_values_to_fill == 0 else math.ceil(num_values_to_fill / num_columns)

    current_rows = df.shape[0]
    if required_rows_count > current_rows:
        rows_to_add = required_rows_count - current_rows
        nan_rows_df = pd.DataFrame(np.nan, index=range(rows_to_add), columns=headers)
        df = pd.concat([df, nan_rows_df], ignore_index=True)
    else:
        df = df.head(required_rows_count)

    shuffled_values = list(values_to_fill)
    random.shuffle(shuffled_values)
    value_idx_pointer = 0

    for row_idx in range(required_rows_count):
        remaining_values = num_values_to_fill - value_idx_pointer
        if remaining_values == 0:
            break

        num_to_fill_in_current_row = min(remaining_values, num_columns)
        column_indices_to_fill = random.sample(range(num_columns), num_to_fill_in_current_row)
        values_for_current_row = shuffled_values[value_idx_pointer : value_idx_pointer + num_to_fill_in_current_row]

        for i, col_idx in enumerate(column_indices_to_fill):
            df.iloc[row_idx, col_idx] = values_for_current_row[i]
        
        value_idx_pointer += num_to_fill_in_current_row
    
    return df

def append_values_to_df_prioritize_column_sum(existing_df, new_values_to_fill, prioritize_larger_sum=False):
    """將新值填入 DataFrame，優先填補最下方一列 NaN 並考慮列總和。"""
    df = existing_df.copy()
    num_new_values = len(new_values_to_fill)
    num_columns = len(df.columns)

    if num_new_values == 0:
        return df
    
    shuffled_new_values = list(new_values_to_fill)
    random.shuffle(shuffled_new_values)
    value_idx_pointer = 0

    def get_prioritized_column_indices(current_df, available_col_indices, num_to_select, sort_reverse):
        if not available_col_indices:
            return []
        col_sums = current_df.fillna(0).sum()
        prioritized_cols = []
        for col_idx in available_col_indices:
            col_name = current_df.columns[col_idx]
            prioritized_cols.append((col_sums[col_name], col_idx))
        
        prioritized_cols.sort(key=lambda x: x[0], reverse=sort_reverse)
        return [col_idx for _, col_idx in prioritized_cols][:num_to_select]

    # 階段 1: 填補最下方一列
    if not df.empty:
        last_row_idx = df.shape[0] - 1
        nan_cols = np.where(df.iloc[last_row_idx].isnull())[0].tolist()
        num_to_fill = min(len(nan_cols), num_new_values - value_idx_pointer)
        
        if num_to_fill > 0:
            p_cols = get_prioritized_column_indices(df, nan_cols, num_to_fill, prioritize_larger_sum)
            for col_idx in p_cols:
                if value_idx_pointer < num_new_values:
                    df.iloc[last_row_idx, col_idx] = shuffled_new_values[value_idx_pointer]
                    value_idx_pointer += 1

    # 階段 2: 新增行繼續填充
    if value_idx_pointer < num_new_values:
        rem_values = num_new_values - value_idx_pointer
        rows_needed = math.ceil(rem_values / num_columns)
        start_row_idx = df.shape[0]
        nan_rows_df = pd.DataFrame(np.nan, index=range(rows_needed), columns=df.columns)
        df = pd.concat([df, nan_rows_df], ignore_index=True)

        for row_idx in range(start_row_idx, df.shape[0]):
            rem_in_phase = num_new_values - value_idx_pointer
            if rem_in_phase == 0: break
            num_fill = min(rem_in_phase, num_columns)
            p_cols = get_prioritized_column_indices(df, list(range(num_columns)), num_fill, prioritize_larger_sum)
            values_assign = shuffled_new_values[value_idx_pointer : value_idx_pointer + num_fill]
            for i, col_idx in enumerate(p_cols):
                df.iloc[row_idx, col_idx] = values_assign[i]
            value_idx_pointer += num_fill
            
    return df

def get_holidayrank_statistics(final_datedf: pd.DataFrame) -> pd.DataFrame:
    """統計每位人員在不同 holidayrank 類型日期的值班數量。"""
    if 'holidayrank' not in final_datedf.columns or 'man' not in final_datedf.columns:
        raise ValueError("必須包含 'holidayrank' 和 'man' 欄位。")

    holidayrank_counts = pd.pivot_table(
        final_datedf, values='date', index='man', columns='holidayrank',
        aggfunc='count', fill_value=0
    )

    desired_ranks = [-1, 0, 1, 2, 3]
    ordered_columns = [r for r in desired_ranks if r in holidayrank_counts.columns]
    holidayrank_counts = holidayrank_counts[ordered_columns]
    holidayrank_counts['總值班天數'] = holidayrank_counts.sum(axis=1)
    holidayrank_counts.index.name = '值班日加權'

    return holidayrank_counts

def get_duty_statistics(final_datedf: pd.DataFrame) -> pd.DataFrame:
    """統計每位人員在各個星期幾的值班數據。"""
    if 'WD' not in final_datedf.columns or 'man' not in final_datedf.columns:
        raise ValueError("必須包含 'WD' 和 'man' 欄位。")

    duty_counts = pd.pivot_table(
        final_datedf, values='date', index='man', columns='WD',
        aggfunc='count', fill_value=0
    )

    weekday_order = ['一', '二', '三', '四', '五', '六', '日']
    duty_counts = duty_counts[weekday_order]
    duty_counts['總值班天數'] = duty_counts.sum(axis=1)
    duty_counts.index.name = 'weekday'

    return duty_counts

def holiday_or_no(weekday_str):
    return 1 if weekday_str in ["星期六", "星期日"] else 0

# --- 核心排班函數 (Core Scheduling Functions) ---

def _the_same_optimized(man_arr, timing, theman, qod, high_unavailable_people=None): 
    length = len(man_arr)
    if high_unavailable_people is None: high_unavailable_people = set()
    person_qod = True if (qod or theman in high_unavailable_people) else False
    
    minus1 = man_arr[timing - 1] if timing >= 1 else None
    add1 = man_arr[timing + 1] if timing < length - 1 else None

    if theman == minus1 or theman == add1:
        return True

    if not person_qod:
        minus2 = man_arr[timing - 2] if timing >= 2 else None
        add2 = man_arr[timing + 2] if timing < length - 2 else None
        if theman == minus2 or theman == add2:
            return True

    return False

def _exchangable_optimized(man_arr, holiday_arr, date_arr, timing, qod, person_unavailable_dates, high_unavailable_people=None):
    current_man = man_arr[timing]
    current_holiday_rank = holiday_arr[timing]
    current_date_num = date_arr[timing] 
    
    for i in range(len(man_arr)):
        if i == timing: continue
        target_man, target_holiday_rank, target_date_num = man_arr[i], holiday_arr[i], date_arr[i]

        if target_holiday_rank == current_holiday_rank: 
            if not _the_same_optimized(man_arr, i, current_man, qod, high_unavailable_people) and \
               not _the_same_optimized(man_arr, timing, target_man, qod, high_unavailable_people) and \
               current_man != target_man:
                
                if current_man in person_unavailable_dates and target_date_num in person_unavailable_dates[current_man]:
                    continue 
                if target_man in person_unavailable_dates and current_date_num in person_unavailable_dates[target_man]:
                    continue 
                return True
    return False

def _find_exchangable_optimized(man_arr, holiday_arr, date_arr, timing, qod, person_unavailable_dates, search_range_indices=None, strict_holidayrank=True, locked_indices=None, high_unavailable_people=None):
    current_man = man_arr[timing]
    current_holiday_rank = holiday_arr[timing]
    current_date_num = date_arr[timing]

    possible_indices = list(range(len(man_arr))) if search_range_indices is None else list(search_range_indices)
    random.shuffle(possible_indices)

    for timing2 in possible_indices:
        if timing2 == timing: continue
        if locked_indices and timing2 in locked_indices: continue
        
        target_man, target_date_num = man_arr[timing2], date_arr[timing2]
        if strict_holidayrank and holiday_arr[timing2] != current_holiday_rank: continue

        if not _the_same_optimized(man_arr, timing2, current_man, qod, high_unavailable_people) and \
           not _the_same_optimized(man_arr, timing, target_man, qod, high_unavailable_people) and \
           current_man != target_man:
            
            if current_man in person_unavailable_dates and target_date_num in person_unavailable_dates[current_man]: continue
            if target_man in person_unavailable_dates and current_date_num in person_unavailable_dates[target_man]: continue
            return timing2 
            
    return None

def arrange_the_schedule(datedf, hope_no_duty_df, wanted_duty_dict, qod, counting, threshold, high_unavailable_people=None, locked_dates=None):
    current_datedf = datedf.copy()
    man_array, holidayrank_array, date_array = current_datedf['man'].values, current_datedf['holidayrank'].values, current_datedf['date'].values 

    person_unavailable_dates = {col: set(hope_no_duty_df[col].dropna().astype(int).tolist()) for col in hope_no_duty_df.columns}

    locked_indices = set()
    if locked_dates:
        for idx, date_val in enumerate(date_array):
            if date_val in locked_dates:
                locked_indices.add(idx)

    if counting > threshold * 5:
        holidayrank_array[holidayrank_array == -1] = 0
    if counting > threshold * 10:
        holidayrank_array[holidayrank_array == 1] = 0

    priority_order = [3, 2, 1, -1, 0] 

    for t1_pass in range(16): 
        violations_fixed = False 
        for rank in priority_order:
            indices = np.where(holidayrank_array == rank)[0].tolist()
            random.shuffle(indices) 

            for timing_idx in indices:
                if timing_idx in locked_indices: continue
                current_man_val = man_array[timing_idx]
                
                if _the_same_optimized(man_array, timing_idx, current_man_val, qod, high_unavailable_people): 
                    target_idx = None
                    if _exchangable_optimized(man_array, holidayrank_array, date_array, timing_idx, qod, person_unavailable_dates, high_unavailable_people): 
                        target_idx = _find_exchangable_optimized(man_array, holidayrank_array, date_array, timing_idx, qod, person_unavailable_dates, strict_holidayrank=True, locked_indices=locked_indices, high_unavailable_people=high_unavailable_people)
                    
                    if target_idx is None:
                        nearby = [timing_idx + o for o in [-2, -1, 1, 2] if 0 <= timing_idx + o < len(man_array)]
                        swap_nearby = [i for i in nearby if man_array[i] == current_man_val and i not in locked_indices]
                        if swap_nearby:
                            target_idx = _find_exchangable_optimized(man_array, holidayrank_array, date_array, timing_idx, qod, person_unavailable_dates, search_range_indices=swap_nearby, strict_holidayrank=False, locked_indices=locked_indices, high_unavailable_people=high_unavailable_people)

                    if target_idx is not None:
                        man_array[timing_idx], man_array[target_idx] = man_array[target_idx], man_array[timing_idx]
                        violations_fixed = True 

        if not violations_fixed: break
        if t1_pass == 15 and violations_fixed: 
            current_datedf['man'] = man_array 
            return False
            
    unresolved_conflicts = []
    for person in hope_no_duty_df.columns:
        unavailable_days = person_unavailable_dates.get(person, set()) 
        conflict_indices = [idx for idx, (m, d) in enumerate(zip(man_array, date_array)) if m == person and d in unavailable_days]
        random.shuffle(conflict_indices) 

        for conflict_idx in conflict_indices:
            if conflict_idx in locked_indices: continue
            found_swap = False
            target_idx = _find_exchangable_optimized(man_array, holidayrank_array, date_array, conflict_idx, qod, person_unavailable_dates, strict_holidayrank=True, locked_indices=locked_indices, high_unavailable_people=high_unavailable_people)
            
            if target_idx is None:
                nearby = [conflict_idx + o for o in [-2, -1, 1, 2] if 0 <= conflict_idx + o < len(man_array)]
                target_idx = _find_exchangable_optimized(man_array, holidayrank_array, date_array, conflict_idx, qod, person_unavailable_dates, search_range_indices=nearby, strict_holidayrank=False, locked_indices=locked_indices, high_unavailable_people=high_unavailable_people)

            if target_idx is not None:
                man_array[conflict_idx], man_array[target_idx] = man_array[target_idx], person
                found_swap = True
            
            if not found_swap: unresolved_conflicts.append(conflict_idx) 

    if unresolved_conflicts and counting <= threshold:
        current_datedf['man'] = man_array
        return False 

    for idx in range(len(man_array)):
        cur_man = man_array[idx]
        if _the_same_optimized(man_array, idx, cur_man, qod, high_unavailable_people):
            neighbors = []
            if idx > 0 and man_array[idx-1] == cur_man: neighbors.append(idx-1)
            if idx < len(man_array)-1 and man_array[idx+1] == cur_man: neighbors.append(idx+1)
            p_qod = True if (qod or (high_unavailable_people and cur_man in high_unavailable_people)) else False
            if not p_qod:
                if idx > 1 and man_array[idx-2] == cur_man: neighbors.append(idx-2)
                if idx < len(man_array)-2 and man_array[idx+2] == cur_man: neighbors.append(idx+2)
            
            if not all(idx in locked_indices and n in locked_indices for n in neighbors):
                current_datedf['man'] = man_array
                return False 

    for person in hope_no_duty_df.columns:
        unavailable = person_unavailable_dates.get(person, set())
        for idx in range(len(man_array)):
            if idx not in locked_indices and man_array[idx] == person and date_array[idx] in unavailable:
                current_datedf['man'] = man_array
                return False 

    current_datedf['man'] = man_array 
    return current_datedf

def extract_and_create_dataframe_strict(datedf, dfprior, person_unavailable_dates, qod, date=None, high_unavailable_people=None, locked_dates=None):
    if high_unavailable_people is None: high_unavailable_people = set()
    if locked_dates is None: locked_dates = set()

    if date is not None:
        current_man = datedf.loc[datedf['date'] == date, 'man'].values
        if len(current_man) > 0 and current_man[0] != 'ZAZ':
            return datedf, dfprior
        
        holidayrank_temp = datedf.loc[datedf['date'] == date, 'holidayrank'].values
        if len(holidayrank_temp) > 0:
            value = holidayrank_temp[0]
            exclude_1 = set(datedf.loc[(datedf['date'].between(date-1, date+1)) & (datedf['date'] != date), 'man'].unique())
            exclude_2 = set(datedf.loc[(datedf['date'].between(date-2, date+2)) & (datedf['date'] != date), 'man'].unique())

            unavail = [p for p, d in person_unavailable_dates.items() if date in d]
            cols_val = [c for c in dfprior.columns if value in dfprior[c].values]
            
            available = []
            for col in cols_val:
                if col in unavail or col == 'ZAZ' or col in exclude_1: continue
                p_qod = True if (qod or col in high_unavailable_people) else False
                if not p_qod and col in exclude_2: continue
                available.append(col)

            if available:
                chosen = np.random.choice(available)
                idx = dfprior[dfprior[chosen] == value].index
                if len(idx) > 0:
                    dfprior.at[np.random.choice(idx), chosen] = np.nan
                    datedf.loc[datedf['date'] == date, 'man'] = chosen
            else:
                already = datedf[(datedf['holidayrank'] == value) & (datedf['man'] != 'ZAZ') & (datedf['date'] != date) & (~datedf['date'].isin(locked_dates))]
                swap_ok = False
                if not already.empty:
                    sched_list = already[['date', 'man']].values.tolist()
                    random.shuffle(sched_list)
                    for t_date, s_person in sched_list:
                        if swap_ok: break
                        if s_person in unavail or s_person in (exclude_1 - {s_person}): continue
                        p_qod = True if (qod or s_person in high_unavailable_people) else False
                        if not p_qod and s_person in (exclude_2 - {s_person}): continue
                        
                        ex_1_t = set(datedf.loc[(datedf['date'].between(t_date-1, t_date+1)) & (datedf['date']!=t_date), 'man'].unique()) - {s_person, 'ZAZ'}
                        ex_2_t = set(datedf.loc[(datedf['date'].between(t_date-2, t_date+2)) & (datedf['date']!=t_date), 'man'].unique()) - {s_person, 'ZAZ'}
                        
                        cands = list(cols_val)
                        random.shuffle(cands)
                        for cand in cands:
                            if cand == 'ZAZ' or t_date in person_unavailable_dates.get(cand, set()) or cand in ex_1_t: continue
                            c_qod = True if (qod or cand in high_unavailable_people) else False
                            if not c_qod and cand in ex_2_t: continue
                            
                            idx = dfprior[dfprior[cand] == value].index
                            if len(idx) > 0:
                                dfprior.at[np.random.choice(idx), cand] = np.nan
                                datedf.loc[datedf['date'] == date, 'man'] = s_person
                                datedf.loc[datedf['date'] == t_date, 'man'] = cand
                                swap_ok = True; break
                if not swap_ok: return False
    return datedf, dfprior

def extract_and_create_dataframe(datedf, dfprior, person_unavailable_dates, qod, date=None, high_unavailable_people=None):
    if high_unavailable_people is None: high_unavailable_people = set()
    if date is None: return datedf, dfprior

    cur_man = datedf.loc[datedf['date'] == date, 'man'].values
    if len(cur_man) > 0 and cur_man[0] != 'ZAZ': return datedf, dfprior

    rank_val = datedf.loc[datedf['date'] == date, 'holidayrank'].values
    if len(rank_val) == 0: return datedf, dfprior
    value = rank_val[0]

    ex_1 = set(datedf.loc[(datedf['date'].between(date-1, date+1)) & (datedf['date'] != date), 'man'].unique())
    ex_2 = set(datedf.loc[(datedf['date'].between(date-2, date+2)) & (datedf['date'] != date), 'man'].unique())
    unavail = [p for p, d in person_unavailable_dates.items() if date in d]
    all_poss = [c for c in dfprior.columns if value in dfprior[c].values]

    pref = []
    for col in all_poss:
        if col in unavail or col == 'ZAZ' or col in ex_1: continue
        p_qod = True if (qod or col in high_unavailable_people) else False
        if not p_qod and col in ex_2: continue
        pref.append(col)

    chosen = np.random.choice(pref) if pref else (np.random.choice(all_poss) if all_poss else None)
    if chosen:
        idx = dfprior[dfprior[chosen] == value].index
        if len(idx) > 0:
            dfprior.at[np.random.choice(idx), chosen] = np.nan
            datedf.loc[datedf['date'] == date, 'man'] = chosen
            return datedf, dfprior
    return False

def get_datedf(input_ym_str, holiday_dates=None, non_holiday_dates=None, hope_no_duty_df=None, wanted_duty_dict=None, qod=False, threshold=1000, limit_df=pd.DataFrame(), high_unavailable_QOD=True, high_unavailable_cutoff=7):
    holiday_dates = holiday_dates or []
    non_holiday_dates = non_holiday_dates or []
    wanted_duty_dict = wanted_duty_dict or {}
    
    try:
        year, month = map(int, input_ym_str.split('/'))
    except:
        raise ValueError("格式錯誤，請使用 'YYYY/MM'。")

    hope_no_duty_df = hope_no_duty_df if hope_no_duty_df is not None else pd.DataFrame()
    people_names = list(hope_no_duty_df.columns)
    num_days = calendar.monthrange(year, month)[1] 
    
    datedf = pd.DataFrame({'date': range(1, num_days + 1)})
    datedf['dated'] = pd.to_datetime(f"{year}-{month:02d}-" + datedf['date'].astype(str))
    
    ny, nm = (year+1, 1) if month == 12 else (year, month+1)
    next_month_df = pd.DataFrame({'date': [num_days+1, num_days+2], 'dated': [datetime(ny, nm, 1), datetime(ny, nm, 2)]})
    datedf = pd.concat([datedf, next_month_df], ignore_index=True)

    weekday_map = {'Monday': '星期一', 'Tuesday': '星期二', 'Wednesday': '星期三', 'Thursday': '星期四', 'Friday': '星期五', 'Saturday': '星期六', 'Sunday': '星期日'}
    datedf['weekday'] = datedf['dated'].dt.day_name().map(weekday_map)
    datedf['holiday'] = datedf['weekday'].apply(holiday_or_no)
    
    for h in holiday_dates:
        h_dt = pd.to_datetime(h)
        if h_dt.year == year and h_dt.month == month: datedf.loc[datedf['dated'] == h_dt, 'holiday'] = 1
    for nh in non_holiday_dates:
        nh_dt = pd.to_datetime(nh)
        if nh_dt.year == year and nh_dt.month == month:
            if not datedf.loc[datedf['dated'] == nh_dt, 'weekday'].empty:
                if datedf.loc[datedf['dated'] == nh_dt, 'weekday'].iloc[0] in ["星期六", "星期日"]:
                    datedf.loc[datedf['dated'] == nh_dt, 'holiday'] = 0

    datedf['next_holiday'] = datedf['holiday'].shift(-1).fillna(0).astype(int) 
    datedf['next_next_holiday'] = datedf['holiday'].shift(-2).fillna(0).astype(int) 

    if num_days > 0:
        next_day = datetime(year, month, num_days) + timedelta(days=1)
        comp_h = holiday_or_no(weekday_map[next_day.strftime('%A')])
        for h in holiday_dates:
            if pd.to_datetime(h) == next_day: comp_h = 1; break
        for nh in non_holiday_dates:
            if pd.to_datetime(nh) == next_day: comp_h = 0; break
        datedf.loc[num_days-1, 'next_holiday'] = comp_h

    cond = [
        (datedf['holiday'] == 1) & (datedf['next_holiday'] == 0),
        (datedf['holiday'] == 1) & (datedf['next_holiday'] == 1),
        (datedf['holiday'] == 0) & (datedf['next_holiday'] == 1),
        (datedf['holiday'] == 0) & (datedf['next_holiday'] == 0) & (datedf['next_next_holiday'] == 1),
        (datedf['holiday'] == 0) & (datedf['next_holiday'] == 0) & (datedf['next_next_holiday'] == 0)
    ]
    datedf['holidayrank'] = np.select(cond, [2, 3, 1, -1, 0], default=0) 
    datedf2 = datedf.drop(columns=['next_holiday', 'next_next_holiday']).iloc[:-2]
    
    temporal, counting = False, 1
    orig_cutoff = high_unavailable_cutoff
    
    while temporal is False:
        duty_limit = limit_df.copy()
        random.shuffle(people_names)
        datedf = datedf2.copy()
        datedf['man'] = "ZAZ"
        
        person_unavail = {col: set(hope_no_duty_df[col].dropna().astype(int).tolist()) for col in hope_no_duty_df.columns}
        high_unavail_p = {p for p, d in person_unavail.items() if len(d) > high_unavailable_cutoff} if high_unavailable_QOD else set()

        # 構建 dfprior
        ranks = [-1, 0, 1, 2, 3]
        rank_counts = {r: datedf[datedf['holidayrank'] == r].index.tolist() for r in ranks}
        
        limit_cols = []
        people_new = people_names.copy()
        if not duty_limit.empty and 'limit' in duty_limit.index:
            limit_row = duty_limit.loc['limit']
            limit_cols = [c for c in limit_row.index if pd.to_numeric(limit_row[c], errors='coerce') > 0]
            people_new = [n for n in people_names if n not in limit_cols]
            duty_limit = duty_limit.drop('limit').reset_index(drop=True)

        new_ranks = {r: [r]*len(rank_counts[r]) for r in ranks}
        for r in ranks:
            c = (duty_limit == r).sum().sum()
            if c > 0: new_ranks[r] = new_ranks[r][:-c]

        dfprior = fill_df_randomly_prioritize_rows(people_new, new_ranks[3], duty_limit)
        for r in [2, 1, 0]: dfprior = append_values_to_df_prioritize_column_sum(dfprior, new_ranks[r])
        dfprior = append_values_to_df_prioritize_column_sum(dfprior, new_ranks[-1], False)

        if not duty_limit.empty and limit_cols:
            dfprior = pd.concat([dfprior, duty_limit[limit_cols]], axis=1)

        if counting > 500:
            m1_c = (dfprior == -1).sum()
            gs, rs = m1_c[m1_c > 1].index.tolist(), m1_c[m1_c == 0].index.tolist()
            if gs and rs:
                g, r = random.choice(gs), random.choice(rs)
                g_idx = random.choice(dfprior[dfprior[g] == -1].index.tolist())
                r_idx_list = dfprior[dfprior[r].notna()].index.tolist()
                if r_idx_list:
                    ri = random.choice(r_idx_list)
                    dfprior.at[g_idx, g], dfprior.at[ri, r] = dfprior.at[ri, r], -1

        if counting >= 1000 and counting % 500 == 0:
            high_unavailable_cutoff = max(0, high_unavailable_cutoff - 1)
        if counting > 3000:
            datedf.loc[datedf['holidayrank'] == -1, 'holidayrank'] = 0
        if counting > 8000: qod = True

        print(f"Attempting schedule: {counting}")
        all_un = [d for ds in person_unavail.values() for d in ds]
        d_counts = Counter(all_un)

        def sort_d(r):
            ds = datedf[datedf['holidayrank'] == r]['date'].tolist()
            return [x[0] for x in sorted([(d, random.random()) for d in ds], key=lambda x: (d_counts.get(x[0], 0), x[1]), reverse=True)]

        sorted_dates = {r: sort_d(r) for r in ranks}
        datedf_try, locked_dates = datedf.copy(), set()
        
        try:
            if wanted_duty_dict:
                w_people = list(wanted_duty_dict.keys())
                random.shuffle(w_people)
                for p in w_people:
                    if p not in dfprior.columns: continue
                    for wd in wanted_duty_dict[p]:
                        row = datedf_try[datedf_try['date'] == wd]
                        if row.empty: continue
                        t_idx, t_rank = row.index[0], row['holidayrank'].values[0]
                        if datedf_try.at[t_idx, 'man'] != 'ZAZ':
                            locked_dates.add(wd); continue
                        
                        q_idx = dfprior[dfprior[p] == t_rank].index
                        if len(q_idx) > 0:
                            dfprior.at[np.random.choice(q_idx), p] = np.nan
                            datedf_try.at[t_idx, 'man'] = p
                        else:
                            p_swap = {3:[2], 2:[3,1], 1:[2,0], 0:[-1,1], -1:[0]}
                            has_idx = dfprior[dfprior[p].notna()].index
                            if len(has_idx) > 0:
                                pref_idx = dfprior.loc[has_idx, p][dfprior.loc[has_idx, p].isin(p_swap.get(t_rank, []))].index
                                trade_idx = np.random.choice(pref_idx) if len(pref_idx)>0 else np.random.choice(has_idx)
                                v_val = dfprior.at[trade_idx, p]
                                others = [op for op in dfprior.columns if op != p]
                                random.shuffle(others)
                                s_ok = False
                                for v in others:
                                    v_idx = dfprior[dfprior[v] == t_rank].index
                                    if len(v_idx) > 0:
                                        dfprior.at[v_idx[0], v], dfprior.at[trade_idx, p] = v_val, np.nan
                                        datedf_try.at[t_idx, 'man'], s_ok = p, True; break
                                if not s_ok: datedf_try.at[t_idx, 'man'] = p
                        locked_dates.add(wd)

            if counting >= 3000: locked_dates.clear()

            for r in [-1, 1, 3, 2]:
                for d in sorted_dates[r]:
                    datedf_try, dfprior = extract_and_create_dataframe_strict(datedf_try, dfprior, person_unavail, qod, d, high_unavail_p, locked_dates)
            for d in sorted_dates[0]:
                datedf_try, dfprior = extract_and_create_dataframe(datedf_try, dfprior, person_unavail, qod, d, high_unavail_p)
            
            temporal = arrange_the_schedule(datedf_try, hope_no_duty_df, wanted_duty_dict, qod, counting, threshold, high_unavail_p, locked_dates)
        except: pass
        
        counting += 1
        if counting > threshold * 200: break

    return temporal if temporal is not False else None

def run_schedule_generation(input_year_month_str='2025/06', input_holiday_dates=None, input_non_holiday_dates=None, personnel_df=None, limit_df=pd.DataFrame(), apply_qod=False, high_unavailable_QOD=True, high_unavailable_cutoff=7):
    input_holiday_dates = input_holiday_dates or ['2025/06/05', '2025/06/06']
    input_non_holiday_dates = input_non_holiday_dates or ['2025/06/01']
    
    if personnel_df is None:
        personnel_data = {
            'DR': ['A', 'B', 'C', 'D', 'E'],
            'Name': ['皮諾可', '黑傑克', '南方仁', '龍之介', '張無忌'],
            'ID': [123.0, 234.0, 345.0, 456.0, 567.0],
            'Unavailable Dates': [[4,5,6,7,8,9,10,11,12,13,31], [3,21,22], [9,15,16], [1,8], [25,26]],
            'Wanted dates': [[], [], [], [], []]
        }
        personnel_df = pd.DataFrame(personnel_data)

    hnd_data, wd_data = {}, {}
    for _, row in personnel_df.iterrows():
        hnd_data[row['DR']] = row['Unavailable Dates']
        if 'Wanted dates' in row and isinstance(row['Wanted dates'], list):
            wd_data[row['DR']] = row['Wanted dates']
    
    max_len = max((len(v) for v in hnd_data.values()), default=0)
    padded_hnd = {k: v + [0] * (max_len - len(v)) for k, v in hnd_data.items()}
    hope_no_duty_df = pd.DataFrame(padded_hnd)

    final_datedf = get_datedf(input_year_month_str, input_holiday_dates, input_non_holiday_dates, hope_no_duty_df, wd_data, apply_qod, 2000, limit_df, high_unavailable_QOD, high_unavailable_cutoff)

    if final_datedf is not None:
        output = pd.DataFrame({
            'date': final_datedf['date'],
            'weekday': final_datedf['weekday'],
            'WD': final_datedf['weekday'].str.replace('星期', ''), 
            'holiday': final_datedf['holiday'],
            'holidayrank': final_datedf['holidayrank'],
            'man': final_datedf['man']
        })
        return output.T
    return None

def read_open_excel_sheet_with_year_month(sheet_name='員工年假預假', data_range='A1:AK15', year_cell='B19', month_cell='B20', holiday_start_cell='B25', nonholiday_start_cell='B26', duty_limit_df='AM1:AX15', qod_cell='B30', high_qod_cell='B31'):
    try:
        app = xw.apps.active
        excel_path = os.path.abspath("真排班小幫手改.xlsx")
        file_name = os.path.basename(excel_path)
        
        if app is None:
            if not os.path.exists(excel_path): raise Exception(f"找不到檔案：{excel_path}")
            book = xw.Book(excel_path)
            sheet = book.sheets[sheet_name] if sheet_name in [s.name for s in book.sheets] else book.sheets.active
        else:
            try: book = app.books[file_name]
            except: book = app.books.open(excel_path)
            sheet = book.sheets[sheet_name]

        y, m = sheet.range(year_cell).value, sheet.range(month_cell).value
        qod_status = str(sheet.range(qod_cell).value).strip().upper() == 'V'
        
        high_q_val = sheet.range(high_qod_cell).value
        h_q_status, h_q_cutoff = True, 7
        if high_q_val is not None:
            v_str = str(high_q_val).strip().upper()
            if v_str == 'X': h_q_status = False
            else:
                try: h_q_cutoff = int(float(high_q_val))
                except: pass

        df = sheet.range(data_range).options(pd.DataFrame, header=1, index=False).value.dropna(how='all').reset_index(drop=True)
        
        ldf = sheet.range(duty_limit_df).options(pd.DataFrame, header=0, index=False).value.dropna(how='all').reset_index(drop=True)
        if not ldf.empty:
            ldf.columns = ldf.iloc[0]; ldf = ldf[1:]; ldf = ldf.set_index(ldf.columns[0])
            ldf.index.name = None; ldf = ldf[ldf.index != 0].T.replace({None: np.nan})

        def fmt_d(v):
            if not isinstance(v, list): v = [v]
            res = []
            for d in v:
                if isinstance(d, datetime): res.append(d.strftime('%Y/%m/%d'))
                elif pd.notna(d) and str(d).strip():
                    try: res.append(pd.to_datetime(d).strftime('%Y/%m/%d'))
                    except: res.append(str(d))
            return res

        hols = fmt_d(sheet.range(holiday_start_cell).expand('right').value)
        nhols = fmt_d(sheet.range(nonholiday_start_cell).expand('right').value)
        ym = f"{int(y)}/{int(m):02d}"

        return df, ym, hols, nhols, ldf, qod_status, h_q_status, h_q_cutoff
    except Exception as e:
        print(f"Excel 讀取錯誤：{e}"); raise

def calculate_unavailable_dates(df: pd.DataFrame) -> pd.DataFrame:
    res = df[['DR', 'Name', 'ID']].copy()
    un_list, wa_list = [], []

    for _, row in df.iterrows():
        un, wa = set(), set()
        for col in [c for c in df.columns if str(c).startswith('年休')]:
            if pd.notna(row[col]):
                d = int(row[col]); un.add(d)
                if d > 1: un.add(d - 1)
        for col in [c for c in df.columns if str(c).startswith('預假')]:
            if pd.notna(row[col]): un.add(int(row[col]))
        for col in [c for c in df.columns if str(c).startswith('預值')]:
            if pd.notna(row[col]): wa.add(int(row[col]))
        un_list.append(sorted(list(un))); wa_list.append(sorted(list(wa)))

    res['Unavailable Dates'], res['Wanted dates'] = un_list, wa_list
    return res

# --- 執行排班程式 ---
if __name__ == "__main__":
    try:
        df2, ym, hol, nhol, limit_df, q_flag, hq_flag, cutoff = read_open_excel_sheet_with_year_month()
        df2 = calculate_unavailable_dates(df2)
        print(f"排班月份: {ym}\nQOD 模式: {q_flag}")

        final_output = None
        if q_flag:
            best_q, best_s, start = float('inf'), None, time.time()
            for i in range(50):
                if time.time() - start > 60: break
                print(f"優化嘗試: {i+1}/50")
                temp = run_schedule_generation(ym, hol, nhol, df2, limit_df, True, hq_flag, cutoff)
                if temp is not None:
                    score = get_qod_statistics(temp.T)['QOD'].sum()
                    if score < best_q:
                        best_q, best_s = score, temp
                        if best_q == 0: break
            final_output = best_s
        else:
            final_output = run_schedule_generation(ym, hol, nhol, df2, limit_df, False, hq_flag, cutoff)

        if final_output is not None:
            out_T = final_output.T
            duty_s = get_duty_statistics(out_T)
            hol_s = get_holidayrank_statistics(out_T)
            qod_s = get_qod_statistics(out_T, df2['DR'].tolist())

            app = xw.apps.active; book = app.books.active
            if '班表' not in [s.name for s in book.sheets]: book.sheets.add('班表')
            sheet = book.sheets['班表']; sheet.clear(); sheet.activate()
            
            final_renamed = final_output.rename(index={'man':'人員', 'holidayrank':'值班日加權'})
            sheet.range('A1').options(header=False).value = final_renamed
            sheet.range('A15').value, sheet.range('N15').value, sheet.range('X15').value = duty_s, hol_s, qod_s

            rows = duty_s.shape[0]; end = 15 + rows
            sheet.range(f'B16:H{end}').formula = '=COUNTIFS($6:$6,$A16,$3:$3,B$15)'
            sheet.range(f'I16:I{end}').formula = '=SUM(B16:H16)'
            sheet.range(f'O16:S{end}').formula = '=COUNTIFS($6:$6,$N16,$5:$5,O$15)'
            sheet.range(f'T16:T{end}').formula = '=SUM(O16:S16)'
            print("排班成功並已寫入 Excel。")
        else:
            print("排班失敗。")
    except Exception as e:
        print(f"執行錯誤：{e}")