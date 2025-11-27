import streamlit as st
import requests
import pandas as pd
from datetime import datetime, timedelta
import calendar
import io
import pytz
import logging
from bs4 import BeautifulSoup 
import re 
import numpy as np # NumPyを追加
from typing import List

# ロギング設定 (デバッグ用)
logging.basicConfig(level=logging.INFO)

# --- 定数設定 ---
# タイムチャージ請求書ページのURL
SR_TIME_CHARGE_URL = "https://www.showroom-live.com/organizer/show_rank_time_charge_hist_invoice_format" 
# プレミアムライブ請求書ページのURL
SR_PREMIUM_LIVE_URL = "https://www.showroom-live.com/organizer/paid_live_hist_invoice_format" 
# ルーム売上請求書ページのURL
SR_ROOM_SALES_URL = "https://www.showroom-live.com/organizer/point_hist_with_mixed_rate" 

# 処理するデータの種類とそれに対応するURL
DATA_TYPES = {
    "room_sales": {
        "label": "ルーム売上",
        "url": SR_ROOM_SALES_URL,
        "type": "room_sales"
    },
    "premium_live": {
        "label": "プレミアムライブ売上",
        "url": SR_PREMIUM_LIVE_URL,
        "type": "standard"
    },
    "time_charge": {
        "label": "タイムチャージ売上",
        "url": SR_TIME_CHARGE_URL,
        "type": "standard" 
    }
}

# 処理対象ライバーファイルのURL
TARGET_LIVER_FILE_URL = "https://mksoul-pro.com/showroom/file/shiharai-taishou.csv"

# 【新規追加】売上履歴ファイルのURLベース (ファイル名は {file_name} で置換)
SALES_HISTORY_BASE_URL = "https://mksoul-pro.com/showroom/csv/uriage_{file_name}.xlsx"

# 日本のタイムゾーン
JST = pytz.timezone('Asia/Tokyo')

# --- 設定ロードと認証 ---
try:
    # オーガナイザーCookieを取得
    AUTH_COOKIE_STRING = st.secrets["showroom"]["auth_cookie_string"]
    LOGIN_ID = st.secrets["showroom"]["login_id"]
    
except KeyError as e:
    AUTH_COOKIE_STRING = "DUMMY"
    LOGIN_ID = "DUMMY"
    st.error(f"🚨 認証設定がされていません。`.streamlit/secrets.toml`を確認してください。不足: {e}")
    st.stop()


# --- 支払額計算関数 (既存) ---

# --- ルーム売上支払想定額計算関数 ---
def calculate_payment_estimate(individual_rank, mk_rank, individual_revenue, is_invoice_registered):
    """
    個別ランク、MKランク、個別分配額、インボイス登録有無から支払想定額を計算する
    """
    # エラーチェック
    if individual_revenue == "#N/A" or individual_rank == "#N/A":
        return "#N/A"

    try:
        # 入力をfloatに変換
        individual_revenue = float(individual_revenue)
        # 個別ランクに応じた基本レートの辞書 (mk_rank 1, 3, 5, 7, 9, 11 のキーを使用)
        rank_rates = {
            'D': {1: 0.750, 3: 0.755, 5: 0.760, 7: 0.765, 9: 0.770, 11: 0.775},
            'E': {1: 0.725, 3: 0.730, 5: 0.735, 7: 0.740, 9: 0.745, 11: 0.750},
            'C': {1: 0.775, 3: 0.780, 5: 0.785, 7: 0.790, 9: 0.795, 11: 0.800},
            'B': {1: 0.800, 3: 0.805, 5: 0.810, 7: 0.815, 9: 0.820, 11: 0.825},
            'A': {1: 0.825, 3: 0.830, 5: 0.835, 7: 0.840, 9: 0.845, 11: 0.850},
            'S': {1: 0.850, 3: 0.855, 5: 0.860, 7: 0.865, 9: 0.870, 11: 0.875},
            'SS': {1: 0.875, 3: 0.880, 5: 0.885, 7: 0.890, 9: 0.895, 11: 0.900},
            'SSS': {1: 0.900, 3: 0.905, 5: 0.910, 7: 0.915, 9: 0.920, 11: 0.925},
        }

        # MKランクに応じてキーを決定 (1,2 -> 1, 3,4 -> 3, ...)
        if mk_rank in [1, 2]:
            key = 1
        elif mk_rank in [3, 4]:
            key = 3
        elif mk_rank in [5, 6]:
            key = 5
        elif mk_rank in [7, 8]:
            key = 7
        elif mk_rank in [9, 10]:
            key = 9
        elif mk_rank == 11:
            key = 11
        else:
            return "#ERROR_MK"

        # 適用レートの取得
        rate = rank_rates.get(individual_rank, {}).get(key)
        
        if rate is None:
            return "#ERROR_RANK"
            
        # ★★★ 最終防衛線: 厳格なブール値チェック (文字列 'False' や NaN の文字列化に対応) ★★★
        is_registered = is_invoice_registered
        if not isinstance(is_registered, bool):
            # 文字列 'False', 'NaN', None などが渡された場合に、PythonでTrueとして扱われるのを防ぐ
            is_registered = not (str(is_registered).lower().strip() in ('', 'false', '0', 'nan', 'none'))


        # インボイス登録有無による計算式の切り替え
        if is_registered:
            # インボイス登録者ロジック: (individual_revenue * 1.10 * rate) / 1.10
            payment_estimate = (individual_revenue * 1.10 * rate) / 1.10
        else:
            # インボイス非登録者ロジック (既存): (individual_revenue * 1.08 * rate) / 1.10
            payment_estimate = (individual_revenue * 1.08 * rate) / 1.10
        
        # 結果を小数点以下を四捨五入して整数に丸める
        return round(payment_estimate) 

    except Exception:
        return "#ERROR_CALC"
        
# --- プレミアムライブ支払想定額計算関数 ---
def calculate_paid_live_payment_estimate(paid_live_amount, is_invoice_registered):
    """
    プレミアムライブ分配額、インボイス登録有無から支払想定額を計算する
    """
    # プレミアムライブ分配額がない場合はNaNを返す
    if pd.isna(paid_live_amount):
        return np.nan
        
    try:
        # 分配額を数値に変換 
        individual_revenue = float(paid_live_amount)

        # ★★★ 最終防衛線: 厳格なブール値チェック ★★★
        is_registered = is_invoice_registered
        if not isinstance(is_registered, bool):
            is_registered = not (str(is_registered).lower().strip() in ('', 'false', '0', 'nan', 'none'))
        
        # インボイス登録有無による計算式の切り替え
        if is_registered:
            # インボイス登録者ロジック: (individual_revenue * 1.10 * 0.9) / 1.10
            payment_estimate = (individual_revenue * 1.10 * 0.9) / 1.10
        else:
            # インボイス非登録者ロジック (既存): (individual_revenue * 1.08 * 0.9) / 1.10
            payment_estimate = (individual_revenue * 1.08 * 0.9) / 1.10
        
        # 結果を小数点以下を四捨五入して整数に丸める
        return round(payment_estimate)

    except Exception:
        return "#ERROR_CALC"

# --- タイムチャージ支払想定額計算関数 ---
def calculate_time_charge_payment_estimate(time_charge_amount, is_invoice_registered):
    """
    タイムチャージ分配額、インボイス登録有無から支払想定額を計算する
    """
    # タイムチャージ分配額がない場合はNaNを返す
    if pd.isna(time_charge_amount):
        return np.nan

    try:
        # 分配額を数値に変換 
        individual_revenue = float(time_charge_amount)
        
        # ★★★ 最終防衛線: 厳格なブール値チェック ★★★
        is_registered = is_invoice_registered
        if not isinstance(is_registered, bool):
            is_registered = not (str(is_registered).lower().strip() in ('', 'false', '0', 'nan', 'none'))

        # インボイス登録有無による計算式の切り替え
        if is_registered:
            # インボイス登録者ロジック: (individual_revenue * 1.10 * 1.00) / 1.10
            payment_estimate = (individual_revenue * 1.10 * 1.00) / 1.10
        else:
            # インボイス非登録者ロジック (既存): (individual_revenue * 1.08 * 1.00) / 1.10
            payment_estimate = (individual_revenue * 1.08 * 1.00) / 1.10
        
        # 結果を小数点以下を四捨五入して整数に丸める
        return round(payment_estimate)

    except Exception:
        return "#ERROR_CALC"


# --- ユーティリティ関数（ランク判定ロジック） (既存) ---

def get_individual_rank(sales_amount):
    """
    ルーム売上分配額（数値）から個別ランクを判定する
    """
    if pd.isna(sales_amount) or sales_amount is None:
        return "#N/A"
    
    amount = float(sales_amount)
    
    if amount < 0:
        return "E"
    
    if amount >= 900001:
        return "SSS"
    elif amount >= 450001:
        return "SS"
    elif amount >= 270001:
        return "S"
    elif amount >= 135001:
        return "A"
    elif amount >= 90001:
        return "B"
    elif amount >= 45001:
        return "C"
    elif amount >= 22501:
        return "D"
    elif amount >= 0:
        return "E"
    else:
        return "E" 
        

def get_mk_rank(revenue):
    """
    全体分配額合計からMKランク（1〜11）を判定する
    """
    if revenue <= 175000:
        return 1
    elif revenue <= 350000:
        return 2
    elif revenue <= 525000:
        return 3
    elif revenue <= 700000:
        return 4
    elif revenue <= 875000:
        return 5
    elif revenue <= 1050000:
        return 6
    elif revenue <= 1225000:
        return 7
    elif revenue <= 1400000:
        return 8
    elif revenue <= 1575000:
        return 9
    elif revenue <= 1750000:
        return 10
    else:
        return 11
        

# --- 【新規】繰越処理のためのユーティリティ関数 ---

def ym_to_timestamp(ym_str: str) -> int | None:
    """
    'YYYY/MM'形式の文字列から、その月の1日0時0分0秒のUNIXタイムスタンプを返す。
    """
    try:
        year, month = map(int, ym_str.split('/'))
        dt_naive = datetime(year, month, 1, 0, 0, 0)
        dt_obj_jst = JST.localize(dt_naive, is_dst=None)
        return int(dt_obj_jst.timestamp())
    except Exception as e:
        logging.error(f"YYYY/MMからタイムスタンプへの変換エラー ({ym_str}): {e}")
        return None


def load_liver_sales_history(file_name: str) -> pd.DataFrame:
    """
    ライバーのファイル名に基づき、売上履歴ファイルを読み込み、DataFrameを返す。
    .xlsxを試み、失敗したら.csvも試みる（ユーザーの提供ファイルに合わせた柔軟な処理）。
    """
    # ユーザー指示に基づきURLを生成（.xlsx形式をベースとする）
    base_url = SALES_HISTORY_BASE_URL.replace("{file_name}", file_name)
    
    st.info(f"ライバー履歴ファイル読み込み中... URL: {base_url}")
    
    # 1. .xlsx (Excel) としての読み込みを試みる
    try:
        df_history = pd.read_excel(base_url, engine='openpyxl')
        st.success(f"履歴ファイル ({file_name}) の読み込みが完了しました (Excel形式)。")
    except Exception as e_excel:
        logging.warning(f"⚠️ Excel形式での読み込みに失敗。CSV形式を試行します。エラー: {e_excel}")
        # 2. .xlsxを.csvに置換してCSVとしての読み込みを試みる (ユーザー提供ファイル形式に合わせる)
        try:
            csv_url = base_url.replace(".xlsx", ".csv")
            df_history = pd.read_csv(csv_url, encoding='utf_8_sig', header=0)
            st.success(f"履歴ファイル ({file_name}) の読み込みが完了しました (CSV形式)。")
        except Exception as e_csv:
            st.warning(f"⚠️ 履歴ファイル ({file_name}) の読み込みに失敗しました (エラー: {e_csv})。このライバーの繰越処理はスキップします。")
            return pd.DataFrame()

    # 読み込み成功後の共通処理
    
    # 列名から不要な改行や空白を除去
    df_history.columns = df_history.columns.str.replace('\n', ' ').str.strip()
    
    # 必須列の確認
    required_cols = ['配信月', '支払/繰越']
    for col in required_cols:
        if col not in df_history.columns:
            st.error(f"🚨 履歴ファイル ({file_name}) に必須の列 **'{col}'** が見つかりません。")
            return pd.DataFrame()

    # 配信月を文字列に統一
    df_history['配信月'] = df_history['配信月'].astype(str).str.strip()

    return df_history


def get_carryover_months(df_history: pd.DataFrame, selected_month_label: str) -> List[str]:
    """
    履歴DataFrameから、選択された配信月（'YYYY年MM月分'）の直前の「繰越」となっている月を遡って取得する。
    戻り値は 'YYYY/MM' 形式のリスト。
    """
    # '2025年10月分' -> '2025/10' に変換
    target_ym = selected_month_label.replace('年', '/').replace('月分', '').strip()
    
    carryover_months = []
    
    # 1. 選択された月 (target_ym) の行を見つける
    target_row = df_history[df_history['配信月'] == target_ym]
    
    if target_row.empty:
        st.warning(f"履歴ファイルに選択された配信月 **{target_ym}** の行が見つかりません。")
        return []

    # 2. 選択された月のインデックスを取得
    target_index = target_row.index[0]
    
    # 3. 選択された月の次の行（時間的に前の月）から順に「繰越」を探す
    # target_index + 1 から末尾までをループ
    for index in range(target_index + 1, len(df_history)):
        row = df_history.iloc[index]
        
        payment_status = row.get('支払/繰越', '').strip()
        distribution_month = row.get('配信月', '').strip()
        
        if payment_status == '繰越':
            carryover_months.append(distribution_month)
            # 繰越が続く限り追加
        elif payment_status == '支払':
            # 「支払」を見つけたら、そこで遡り処理を終了
            break
        
    if carryover_months:
        st.info(f"🔑 繰越対象月が見つかりました: **{', '.join(carryover_months)}**")
    # 繰越がない場合は、何も表示しない
        
    return carryover_months

# --- 既存の load_target_livers, get_target_months, create_authenticated_session, fetch_and_process_data, get_and_extract_sales_data は省略 ---
# ※ 既存のコードはそのまま

# --- 【新規】繰越処理を実行し、結果を最終データに結合する関数 ---

def get_carryover_data_for_liver(liver_row: pd.Series, selected_month_label: str, auth_cookie_string: str) -> List[pd.DataFrame]:
    """
    特定のライバーの繰越月を判定し、該当する月のデータをSHOWROOMから取得・計算してDataFrameのリストを返す。
    """
    file_name = liver_row['ファイル名']
    room_id = liver_row['ルームID']
    st.markdown(f"##### 🚀 ルームID: {room_id} ({file_name}) の繰越処理を開始")

    # 1. ライバーの売上履歴ファイルを読み込む
    df_history = load_liver_sales_history(file_name)
    
    if df_history.empty:
        return []

    # 2. 繰越となっている月を遡って特定する (YYYY/MM のリスト)
    carryover_months_ym = get_carryover_months(df_history, selected_month_label)
    
    if not carryover_months_ym:
        st.info(f"ルームID: {room_id} には繰越データがありませんでした。")
        return []
    
    # 3. 繰越月のデータをSHOWROOMから取得・計算する
    all_carryover_dfs = []
    
    # 取得する必要があるのは、特定された「繰越」の月のデータ
    for ym_str in carryover_months_ym:
        st.markdown(f"###### ⏳ 繰越データ取得中: 配信月 **{ym_str}**")
        
        # YYYY/MM -> UNIXタイムスタンプに変換
        target_timestamp = ym_to_timestamp(ym_str)
        if target_timestamp is None:
            continue
        
        # 配信月ラベルを生成 (例: '2025年09月分')
        carryover_month_label = ym_str.replace('/', '年') + '月分'
        
        # --- SHOWROOM売上データの取得と計算 ---
        
        df_sales_list = []
        df_mk_sales = pd.DataFrame()
        
        # 1. SHOWROOM売上データの取得 (この月分のMK全体分配額を取得するため、ルーム売上を最初に処理)
        for data_type_key in DATA_TYPES.keys():
            sr_url = DATA_TYPES[data_type_key]["url"]
            df_sales = fetch_and_process_data(target_timestamp, auth_cookie_string, sr_url, data_type_key)
            
            if df_sales is not None and not df_sales.empty:
                # MKsoul行を分離して、残りをdf_sales_listに追加
                if data_type_key == "room_sales":
                    df_mk_sales = df_sales[df_sales['ルームID'] == 'MKsoul'].copy()
                    df_sales = df_sales[df_sales['ルームID'] != 'MKsoul'].copy()
                
                if not df_sales.empty:
                    df_sales_list.append(df_sales)
        
        if not df_sales_list:
            st.warning(f"⚠️ {carryover_month_label} の売上データがSHOWROOMから取得できませんでした。")
            continue
            
        all_sales_data = pd.concat(df_sales_list)
        
        # 2. 処理対象ライバー（この関数に渡された単一行）と売上データを結合・計算
        # 処理対象ライバーは単一行だが、処理を簡潔にするためDataFrameにする
        df_liver_single = pd.DataFrame([liver_row])
        df_liver_single['ルームID'] = df_liver_single['ルームID'].astype(str) # 念のため型を合わせる

        # ルームIDをキーに処理対象ライバーと結合 (ルームIDが一致する行のみを抽出)
        df_merged_carryover = pd.merge(
            df_liver_single, # 1行のライバー情報
            all_sales_data,  # その月の全売上データ
            on='ルームID',
            how='left'
        )
        
        # 念の為、'ファイル名'列がない場合は追加（後の処理で必要になるため）
        if 'ファイル名' not in df_merged_carryover.columns and 'ファイル名' in df_liver_single.columns:
             df_merged_carryover.insert(1, 'ファイル名', df_liver_single.iloc[0]['ファイル名'])
        
        # 'インボイス'、'is_invoice_registered'列が欠落するのを防ぐ
        for col in ['インボイス', 'is_invoice_registered']:
             if col not in df_merged_carryover.columns and col in df_liver_single.columns:
                 df_merged_carryover[col] = df_liver_single.iloc[0][col]

        # 売上データがないライバー（NULL行）の分配額を0として処理
        df_merged_carryover['分配額'] = df_merged_carryover['分配額'].fillna(0).astype(int)
        df_merged_carryover['データ種別'] = df_merged_carryover['データ種別'].fillna('売上データなし')
        df_merged_carryover['配信月'] = carryover_month_label
        df_merged_carryover['アカウントID'] = df_merged_carryover['アカウントID'].fillna(st.session_state.login_account_id)
        
        if 'is_invoice_registered' in df_merged_carryover.columns:
            df_merged_carryover['is_invoice_registered'] = df_merged_carryover['is_invoice_registered'].astype(bool)


        # 3. ランク・支払額の計算

        df_room_sales_only = df_merged_carryover[df_merged_carryover['データ種別'] == 'ルーム売上'].copy()
        df_other_sales = df_merged_carryover[df_merged_carryover['データ種別'] != 'ルーム売上'].copy()
        
        # 3-1. ルーム売上処理
        if not df_room_sales_only.empty:
            
            # MKランクの決定: 取得したMKsoulの分配額から計算
            mk_sales_total = df_mk_sales['分配額'].iloc[0].item() if not df_mk_sales.empty else 0
            mk_rank_value = get_mk_rank(mk_sales_total)
            
            df_room_sales_only['MKランク'] = mk_rank_value
            df_room_sales_only['個別ランク'] = df_room_sales_only['分配額'].apply(get_individual_rank)
            df_room_sales_only['適用料率'] = '適用料率：' + df_room_sales_only['MKランク'].astype(str) + df_room_sales_only['個別ランク']
            
            df_room_sales_only['支払額'] = df_room_sales_only.apply(
                lambda row: calculate_payment_estimate(
                    row['個別ランク'],
                    row['MKランク'],
                    row['分配額'],
                    row['is_invoice_registered']
                ), axis=1)

        else:
            df_room_sales_only['MKランク'] = np.nan
            df_room_sales_only['個別ランク'] = np.nan
            df_room_sales_only['適用料率'] = '-'
            df_room_sales_only['支払額'] = np.nan


        # 3-2. その他売上処理
        df_other_sales['MKランク'] = '-'
        df_other_sales['個別ランク'] = '-'
        df_other_sales['適用料率'] = '-'
        df_other_sales['支払額'] = np.nan # 初期化

        # プレミアムライブ売上
        premium_live_mask = df_other_sales['データ種別'] == 'プレミアムライブ売上'
        if premium_live_mask.any():
            df_other_sales.loc[premium_live_mask, '支払額'] = df_other_sales[premium_live_mask].apply(
                lambda row: calculate_paid_live_payment_estimate(row['分配額'], row['is_invoice_registered']), axis=1
            )

        # タイムチャージ売上
        time_charge_mask = df_other_sales['データ種別'] == 'タイムチャージ売上'
        if time_charge_mask.any():
            df_other_sales.loc[time_charge_mask, '支払額'] = df_other_sales[time_charge_mask].apply(
                lambda row: calculate_time_charge_payment_estimate(row['分配額'], row['is_invoice_registered']), axis=1
            )
            
        # 売上データがない行の支払額は0
        no_sales_mask = df_other_sales['データ種別'] == '売上データなし'
        df_other_sales.loc[no_sales_mask, '支払額'] = 0

        # 4. 最終的なDataFrameを再結合して整形
        df_final = pd.concat([df_room_sales_only, df_other_sales], ignore_index=True)
        
        # 支払額列の表示形式を調整（整数としてNaN以外を扱う）
        df_final['支払額'] = df_final['支払額'].replace(['#ERROR_CALC', '#ERROR_MK', '#ERROR_RANK', '#N/A'], np.nan)
        df_final['支払額'] = pd.to_numeric(df_final['支払額'], errors='coerce').fillna(0).astype('Int64')

        if not df_final.empty:
            all_carryover_dfs.append(df_final)
        
        st.success(f"✅ {carryover_month_label} の繰越データの取得・計算が完了しました。")

    return all_carryover_dfs


def append_carryover_data(df_extracted_initial: pd.DataFrame, df_livers: pd.DataFrame, selected_month_label: str, auth_cookie_string: str) -> pd.DataFrame:
    """
    主要な繰越処理を実行する関数。単月処理後のデータフレームを受け取り、繰越データを追記して返す。
    """
    st.markdown("---")
    st.markdown("## 4. 繰越データの探索と追加 (新規処理)")
    
    # MKsoul行は処理対象外
    df_livers_target = df_livers[df_livers['ルームID'] != 'MKsoul'].copy()
    
    if df_livers_target.empty:
        st.warning("処理対象ライバーが見つからなかったため、繰越処理は実行しません。")
        return df_extracted_initial

    all_carryover_data = []

    # 処理対象ライバーを1人ずつループ
    for index, liver_row in df_livers_target.iterrows():
        
        # 繰越データを取得・計算
        dfs_carryover = get_carryover_data_for_liver(liver_row, selected_month_label, auth_cookie_string)
        
        if dfs_carryover:
            all_carryover_data.extend(dfs_carryover)

    if all_carryover_data:
        # 繰越データを全て結合
        df_carryover_final = pd.concat(all_carryover_data, ignore_index=True)
        
        # 最終的な単月データと繰越データを結合（行を追加）
        df_final_combined = pd.concat([df_extracted_initial, df_carryover_final], ignore_index=True)
        
        # ソートして見やすくする（オプション）
        df_final_combined = df_final_combined.sort_values(by=['ルームID', '配信月', 'データ種別'], ascending=[True, False, False]).reset_index(drop=True)
        
        # 最終的な支払額の型を整える
        df_final_combined['支払額'] = pd.to_numeric(df_final_combined['支払額'], errors='coerce').fillna(0).astype('Int64')

        st.success(f"🎉 繰越データ ({len(df_carryover_final)}行) の取得・追加が完了しました。")
        
        st.subheader("✅ 抽出・結合された最終データ（繰越データ含む）")
        st.dataframe(df_final_combined)
        
        return df_final_combined

    else:
        st.info("すべての処理対象ライバーについて、繰越データは見つかりませんでした。")
        st.subheader("✅ 抽出・結合された最終データ（繰越データなし）")
        st.dataframe(df_extracted_initial)
        return df_extracted_initial

# --- Streamlit UI (既存) ---

def main():
    st.set_page_config(page_title="SHOWROOM 支払明細書作成補助ツール", layout="wide")
    st.markdown(
        "<h1 style='font-size:28px; text-align:left; color:#1f2937;'>SHOWROOM 支払明細書作成補助ツール (データ取得・抽出)</h1>",
        unsafe_allow_html=True
    )
    st.markdown("<p style='text-align: left;'>💡 <b>データの取得と、対象ライバーデータへの紐付け（抽出）までを行います。</b></p>", unsafe_allow_html=True)
    st.markdown("---")
    
    # セッションステートの初期化
    if 'df_room_sales' not in st.session_state:
        st.session_state['df_room_sales'] = pd.DataFrame()
    if 'df_premium_live' not in st.session_state:
        st.session_state['df_premium_live'] = pd.DataFrame()
    if 'df_time_charge' not in st.session_state:
        st.session_state['df_time_charge'] = pd.DataFrame()
    
    # 新しいセッションステートの初期化
    if 'selected_month_label' not in st.session_state:
        st.session_state['selected_month_label'] = None
    if 'login_account_id' not in st.session_state:
        st.session_state['login_account_id'] = LOGIN_ID


    # 1. 対象月選択
    st.markdown("#### 1. 対象月選択")
    month_options_tuple = get_target_months()
    month_labels = [label for label, _, _ in month_options_tuple] 
    
    selected_label = st.selectbox(
        "処理対象の**配信月**を選択してください:",
        options=month_labels,
        key='month_selector' # keyを追加し、選択を追跡
    )
    
    selected_data = next(((ts, ym) for label, ts, ym in month_options_tuple if label == selected_label), (None, None))
    selected_timestamp = selected_data[0]
    
    if selected_timestamp is None:
        st.warning("有効な月が選択されていません。")
        return

    # 選択された配信月をセッションステートに保存
    st.session_state['selected_month_label'] = selected_label
    
    st.info(f"選択された月: **{selected_label}**")
    
    # 2. 実行ボタン
    st.markdown("#### 2. データ取得と抽出の実行")
    
    if st.button("🚀 データの取得・抽出を実行", type="primary"):
        st.markdown("---")
        
        # 処理対象ライバーファイルの読み込み
        df_livers = load_target_livers(TARGET_LIVER_FILE_URL)
        st.session_state['df_livers'] = df_livers # セッションステートに保存
        
        if df_livers.empty:
            st.error("処理対象ライバーファイルが読み込めなかったため、処理を中断します。")
            return
            
        with st.spinner(f"処理中: {selected_label}の売上データをSHOWROOMから取得しています..."):
            
            # --- SHOWROOM売上データの取得 (単月処理: 既存ロジック) ---
            
            # ルーム売上
            get_and_extract_sales_data("room_sales", selected_timestamp, AUTH_COOKIE_STRING)

            # プレミアムライブ売上
            get_and_extract_sales_data("premium_live", selected_timestamp, AUTH_COOKIE_STRING)

            # タイムチャージ売上
            get_and_extract_sales_data("time_charge", selected_timestamp, AUTH_COOKIE_STRING) 
        
        st.balloons()
        st.success("🎉 **売上データの取得とセッションステートへの格納が完了しました！**")

    # --- 取得・抽出結果の表示 ---
    
    if not st.session_state.df_room_sales.empty or 'df_livers' in st.session_state:

        st.markdown("## 3. 抽出結果の確認、ランク・支払額の付与") # タイトルを修正
        st.markdown("---")

        if 'df_livers' in st.session_state and not st.session_state.df_livers.empty:
            df_livers = st.session_state.df_livers
            st.subheader("処理対象ライバー一覧")
            
            expected_cols = ['ルームID', 'ファイル名', 'インボイス', 'is_invoice_registered']
            display_cols = [col for col in expected_cols if col in df_livers.columns]
            
            st.dataframe(df_livers[display_cols], height=150)
            
            # --- 売上データを結合して抽出 (単月処理の実行) ---
            
            # 取得した売上データを結合
            all_sales_data = pd.concat([
                st.session_state.df_room_sales,
                st.session_state.df_premium_live,
                st.session_state.df_time_charge
            ])
            
            if not all_sales_data.empty:
                st.subheader("全売上データ (取得元) - 合計")
                st.dataframe(all_sales_data, height=150)
                
                # ルームIDをキーに処理対象ライバーと結合
                df_merged = pd.merge(
                    df_livers,
                    all_sales_data,
                    on='ルームID',
                    how='left'
                )

                # 売上データがないライバー（NULL行）の分配額を0として処理
                df_merged['分配額'] = df_merged['分配額'].fillna(0).astype(int)
                
                # 表示用に、売上がゼロの行のデータ種別をNaNから「売上なし」などに変換
                df_merged['データ種別'] = df_merged['データ種別'].fillna('売上データなし')
                
                # 配信月とアカウントIDを追加
                df_merged['配信月'] = st.session_state.selected_month_label
                # アカウントIDを埋める
                df_merged['アカウントID'] = df_merged.apply(
                    lambda row: row['アカウントID'] if pd.notna(row['アカウントID']) else st.session_state.login_account_id if row['ルームID'] == 'MKsoul' else np.nan, axis=1
                )
                
                # ★★★ 修正点3: マージ直後にis_invoice_registered列を明示的にbool型に再キャストする (二重の防御) ★★★
                if 'is_invoice_registered' in df_merged.columns:
                    df_merged['is_invoice_registered'] = df_merged['is_invoice_registered'].astype(bool)


                # 🌟 ルーム売上のみにランク情報を付与 🌟
                # df_mergedを「ルーム売上」データと「その他」データに分割
                df_room_sales_only = df_merged[df_merged['データ種別'] == 'ルーム売上'].copy()
                df_other_sales = df_merged[df_merged['データ種別'] != 'ルーム売上'].copy()
                
                
                if not df_room_sales_only.empty:
                    
                    # 1. MKランク（全体ランク）の決定
                    df_raw_room_sales = st.session_state.df_room_sales
                    
                    try:
                        mk_sales_total = df_raw_room_sales[df_raw_room_sales['ルームID'] == 'MKsoul']['分配額'].iloc[0].item() 
                        if mk_sales_total == 0:
                            st.warning("⚠️ MK全体分配額が0です。SHOWROOM側のデータがないか、合計金額の抽出に失敗している可能性があります。")
                    except IndexError:
                        mk_sales_total = 0
                        st.error("🚨 重大なエラー: 合計売上を示す 'MKsoul' 行がデータ取得元から見つかりませんでした。")
                    except Exception as e:
                        mk_sales_total = 0
                        st.error(f"🚨 重大なエラー: 合計売上計算中に予期せぬエラーが発生しました: {e}")
                    
                    mk_rank_value = get_mk_rank(mk_sales_total)
                    st.info(f"🔑 **MK全体分配額**: {mk_sales_total:,}円 (→ **MKランク: {mk_rank_value}**)")
                    
                    # MKランク、個別ランクの設定
                    df_room_sales_only['MKランク'] = mk_rank_value
                    df_room_sales_only['個別ランク'] = df_room_sales_only['分配額'].apply(get_individual_rank)
                    
                    # 適用料率の生成
                    df_room_sales_only['適用料率'] = np.where(
                        df_room_sales_only['ルームID'] == 'MKsoul',
                        '-',
                        '適用料率：' + df_room_sales_only['MKランク'].astype(str) + df_room_sales_only['個別ランク']
                    )
                    
                    # 4. ルーム売上支払額の計算
                    df_room_sales_only['支払額'] = np.where(
                        df_room_sales_only['ルームID'] == 'MKsoul',
                        np.nan, # MKsoul行は支払額なし
                        df_room_sales_only.apply(
                            lambda row: calculate_payment_estimate(
                                row['個別ランク'], 
                                row['MKランク'], 
                                row['分配額'],
                                row['is_invoice_registered'] # 厳格チェック付きの関数に渡す
                            ), axis=1)
                    )
                    
                else:
                    st.warning("ルーム売上データ（「ルーム売上」データ種別）が存在しないため、ランク判定・支払額計算はスキップしました。")
                    mk_sales_total = 0 
                    mk_rank_value = get_mk_rank(mk_sales_total) 
                    st.info(f"🔑 **MK全体分配額**: 0円 (→ **MKランク: {mk_rank_value}**)")

                    df_room_sales_only['MKランク'] = np.nan
                    df_room_sales_only['個別ランク'] = np.nan
                    df_room_sales_only['適用料率'] = '-'
                    df_room_sales_only['支払額'] = np.nan

                
                # 5. その他の売上行のランク列を埋める
                df_other_sales['MKランク'] = '-'
                df_other_sales['個別ランク'] = '-'
                df_other_sales['適用料率'] = '-'

                # 6. その他の売上支払額の計算
                df_other_sales['支払額'] = np.nan # 初期化

                # プレミアムライブ売上
                premium_live_mask = df_other_sales['データ種別'] == 'プレミアムライブ売上'
                if premium_live_mask.any():
                    df_other_sales.loc[premium_live_mask, '支払額'] = df_other_sales[premium_live_mask].apply(
                        lambda row: calculate_paid_live_payment_estimate(
                            row['分配額'],
                            row['is_invoice_registered'] # 厳格チェック付きの関数に渡す
                        ), axis=1
                    )

                # タイムチャージ売上
                time_charge_mask = df_other_sales['データ種別'] == 'タイムチャージ売上'
                if time_charge_mask.any():
                    df_other_sales.loc[time_charge_mask, '支払額'] = df_other_sales[time_charge_mask].apply(
                        lambda row: calculate_time_charge_payment_estimate(
                            row['分配額'],
                            row['is_invoice_registered'] # 厳格チェック付きの関数に渡す
                        ), axis=1
                    )
                
                # 売上データがない行の支払額は0
                no_sales_mask = df_other_sales['データ種別'] == '売上データなし'
                df_other_sales.loc[no_sales_mask, '支払額'] = 0

                # 7. 最終的なDataFrameを再結合
                df_extracted_single_month = pd.concat([df_room_sales_only, df_other_sales], ignore_index=True)
                
                # 8. 不要な列を整理し、抽出が完了したDataFrameを表示 (ランク情報を追加)
                final_display_cols = ['ルームID']
                if 'ファイル名' in df_livers.columns:
                    final_display_cols.append('ファイル名')
                if 'インボイス' in df_livers.columns:
                    final_display_cols.append('インボイス')
                
                final_display_cols.extend(['is_invoice_registered', 'データ種別', '分配額', '個別ランク', 'MKランク', '適用料率', '支払額', 'アカウントID', '配信月'])
                
                # DataFrameに存在しない列を除外
                df_extracted_cols = [col for col in final_display_cols if col in df_extracted_single_month.columns]
                df_extracted_single_month = df_extracted_single_month[df_extracted_cols]
                
                # 支払額列の表示形式を調整（整数としてNaN以外を扱う）
                df_extracted_single_month['支払額'] = df_extracted_single_month['支払額'].replace(['#ERROR_CALC', '#ERROR_MK', '#ERROR_RANK', '#N/A'], np.nan)
                df_extracted_single_month['支払額'] = pd.to_numeric(df_extracted_single_month['支払額'], errors='coerce').fillna(0).astype('Int64') # Int64でNaNを許容する整数型に

                # ソートして見やすくする（オプション）
                df_extracted_single_month = df_extracted_single_month.sort_values(by=['ルームID', 'データ種別'], ascending=[True, False]).reset_index(drop=True)

                st.subheader("✅ 抽出・結合された最終データ (単月処理完了)")
                st.info(f"このデータで、分配額から**支払額**の計算が完了しました。合計 {len(df_livers)}件のライバー情報に対して、{len(df_extracted_single_month)}件の売上明細行が紐付けられました。")
                st.dataframe(df_extracted_single_month)
                
                # ★★★ 新規追加: 繰越処理を実行し、結果を最終データとして表示 ★★★
                # この処理が、お客様の要求する「②上記処理後に（①の処理後に）、繰越データがあるか探しに行って、繰越データがある場合、その配信月のデータも同様に①同様の処理を行い、データを追加。合算ではなく行（レコード）を追加。繰越対象が無くなるまで実施。」に該当します。
                final_df = append_carryover_data(
                    df_extracted_single_month, 
                    df_livers, 
                    st.session_state.selected_month_label, 
                    AUTH_COOKIE_STRING
                )
                
                st.session_state['df_extracted'] = final_df # 最終結果をセッションステートに保持

            else:
                st.warning("結合対象の売上データがありません。")
                st.session_state['df_extracted'] = pd.DataFrame() 
        else:
            st.info("実行ボタンを押して、処理対象ライバーファイルの読み込みと売上データの取得を行ってください。")

if __name__ == "__main__":
    main()