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
import urllib.parse # URLエンコード用に追加

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
# ライバー売上履歴CSVのベースURL (新設)
LIVER_HISTORY_BASE_URL = "https://mksoul-pro.com/showroom/csv/"


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


# --- 支払額計算関数 (既存のまま) ---

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


# --- ユーティリティ関数（既存のまま） ---

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
        
        
def load_target_livers(url):
    """処理対象ライバーファイルを読み込み、DataFrameとして返し、インボイスフラグを追加する"""
    st.info(f"処理対象ライバーファイルを読み込み中... URL: {url}")
    
    # 既存の読み込みロジック (省略せず保持)
    try:
        df_livers = pd.read_csv(url, encoding='utf_8_sig')
        st.success(f"処理対象ライバーデータ ({len(df_livers)}件) の読み込みが完了しました。(エンコーディング: UTF-8 BOM)")
    except Exception as e_utf8:
        try:
            df_livers = pd.read_csv(url, encoding='utf-8')
            st.success(f"処理対象ライバーデータ ({len(df_livers)}件) の読み込みが完了しました。(エンコーディング: UTF-8)")
        except Exception as e_shiftjis:
            try:
                df_livers = pd.read_csv(url, encoding='shift_jis')
                st.success(f"処理対象ライバーデータ ({len(df_livers)}件) の読み込みが完了しました。(エンコーディング: Shift-JIS)")
            except Exception as e_final:
                st.error(f"🚨 処理対象ライバーファイルの読み込みに失敗しました。エンコーディングエラー: {e_final}")
                return pd.DataFrame()

    # 読み込み成功後の共通処理

    # ★★★ 修正点1: 列名から前後の空白文字を全て除去する（KeyError対策） ★★★
    df_livers.columns = df_livers.columns.str.strip()

    # ルームIDを文字列として扱い、結合キーとする
    if 'ルームID' in df_livers.columns:
        df_livers['ルームID'] = df_livers['ルームID'].astype(str)
    else:
        st.error("🚨 処理対象ライバーファイルに必須の列 **'ルームID'** が見つかりません。")
        return pd.DataFrame()
    
    # ★★★ 決定的な修正: インボイス登録判定ロジックのバグフィックス ★★★
    # CSVの空欄（NaN）が文字列化されて 'nan' になり、Trueと誤判定される問題を解消
    if 'インボイス' in df_livers.columns:
        
        # 1. 列を文字列化し、前後の空白を除去、小文字に統一
        s_invoice = df_livers['インボイス'].astype(str).str.strip().str.lower()
        
        # 2. 厳格な判定: 以下のいずれかの場合は False (非登録者) とする
        #    - '' (空白のみのセル由来)
        #    - 'nan' (CSVのブランクセル由来)
        #    - 'false', '0', 'none', 'n/a' などの明示的な否定文字列
        is_registered_series = ~s_invoice.isin(['', 'nan', 'false', '0', 'none', 'n/a'])
        
        # 3. 純粋なbool型としてis_invoice_registered列を作成
        df_livers['is_invoice_registered'] = is_registered_series.astype(bool)

    else:
        # インボイス列がない場合は全てFalseとする
        st.warning("⚠️ 処理対象ライバーファイルに **'インボイス'** 列が見つかりません。全てのライバーを非登録者として処理します。")
        df_livers['is_invoice_registered'] = False
    
    st.info(f"インボイス登録者 ({df_livers['is_invoice_registered'].sum()}名) のフラグ付けが完了しました。")
    
    return df_livers


def get_target_months():
    """2023年10月以降の月リストを 'YYYY年MM月分' 形式で生成し、正確なUNIXタイムスタンプを計算する"""
    START_YEAR = 2023
    START_MONTH = 10
    
    today = datetime.now(JST)
    months = []
    
    current_year = today.year
    current_month = today.month
    
    while True:
        if current_year < START_YEAR or (current_year == START_YEAR and current_month < START_MONTH):
            break 

        # 支払い対象月を決定
        # 配信月: YYYY/MM (例: 2025/10)
        # 支払月: 配信月の2ヶ月後 (例: 2025/12)
           
        # 支払月計算
        payment_month = current_month + 2
        payment_year = current_year
        if payment_month > 12:
            payment_month -= 12
            payment_year += 1
            
        month_str = f"{current_year}年{current_month:02d}月分 (支払月:{payment_year}/{payment_month:02d})"
        
        try:
            dt_naive = datetime(current_year, current_month, 1, 0, 0, 0)
            dt_obj_jst = JST.localize(dt_naive, is_dst=None)
            timestamp = int(dt_obj_jst.timestamp())
            ym_str = f"{current_year}/{current_month:02d}"
            
            months.append((month_str, timestamp, ym_str)) # (ラベル, UNIXタイムスタンプ, YYYY/MM)
        except Exception as e:
            logging.error(f"日付計算エラー ({month_str}): {e}")
            
        # 次の月（前の月）へ移動
        if current_month == 1:
            current_month = 12
            current_year -= 1
        else:
            current_month -= 1
            
    return months


def get_previous_month_data(current_ym_str, month_options):
    """
    'YYYY/MM'形式の文字列を受け取り、その前月の (UNIXタイムスタンプ, YYYY/MM) を返す
    """
    try:
        year, month = map(int, current_ym_str.split('/'))
        if month == 1:
            prev_month = 12
            prev_year = year - 1
        else:
            prev_month = month - 1
            prev_year = year

        prev_ym_str = f"{prev_year}/{prev_month:02d}"

        # タイムスタンプを再計算
        dt_naive = datetime(prev_year, prev_month, 1, 0, 0, 0)
        dt_obj_jst = JST.localize(dt_naive, is_dst=None)
        prev_timestamp = int(dt_obj_jst.timestamp())

        return prev_timestamp, prev_ym_str
    except Exception:
        return None, None


def create_authenticated_session(cookie_string):
    """手動で取得したCookie文字列から認証済みRequestsセッションを構築する"""
    session = requests.Session()
    try:
        cookies_dict = {}
        for item in cookie_string.split(';'):
            item = item.strip()
            if '=' in item:
                name, value = item.split('=', 1)
                cookies_dict[name.strip()] = value.strip()
        cookies_dict['i18n_redirected'] = 'ja'
        session.cookies.update(cookies_dict)
        
        if not cookies_dict:
            st.error("🚨 有効な認証セッションを解析できませんでした。")
            return None
            
        return session
    except Exception as e:
        st.error(f"認証セッションを解析中にエラーが発生しました: {e}")
        return None


def fetch_and_process_data(timestamp, cookie_string, sr_url, data_type_key):
    """
    指定されたタイムスタンプに基づいてSHOWROOMからデータを取得し、DataFrameに整形して返す
    （既存のまま）
    """
    st.info(f"データ取得中... **{DATA_TYPES[data_type_key]['label']}** (URL: {sr_url}, タイムスタンプ: {timestamp})")
    session = create_authenticated_session(cookie_string)
    if not session:
        return None
    
    try:
        # 1. データ取得
        url = f"{sr_url}?from={timestamp}" 
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'ja,en-US;q=0.9,en;q=0.8',
            'Referer': sr_url
        }
        
        response = session.get(url, headers=headers, timeout=30)
        response.raise_for_status() 
        
        # 2. HTMLからのデータ抽出
        soup = BeautifulSoup(response.text, 'html5lib') 
        table = soup.find('table', class_='table-type-02') 
        
        if not table:
            if "ログイン" in response.text or "会員登録" in response.text:
                st.error("🚨 認証切れです。Cookieが古いか無効になっています。")
                return None
            st.warning(f"**{DATA_TYPES[data_type_key]['label']}**: HTMLから売上データテーブルを検出できませんでした。データがまだ生成されていないか、ページ構造が変更されました。")
            return pd.DataFrame(columns=['ルームID', '分配額', 'アカウントID', 'データ種別']) 
            
        # 3. データをBeautifulSoupで抽出 (ライバー個別のデータ)
        table_data = []
        rows = table.find_all('tr')
        
        for row in rows[1:]: 
            td_tags = row.find_all('td')
            
            if len(td_tags) >= 5:
                room_id_str = td_tags[0].text.strip() 
                amount_str = td_tags[3].text.strip().replace(',', '') 
                account_id = td_tags[4].text.strip()
                
                if amount_str.isnumeric():
                    table_data.append({
                        'ルームID': room_id_str, 
                        '分配額': int(amount_str), 
                        'アカウントID': account_id
                    })
        
        # 4. DataFrameに変換
        df_cleaned = pd.DataFrame(table_data)
        
        # --- ルーム売上 (room_sales) の特殊処理: MKsoulの合計行を追加 ---
        if data_type_key == "room_sales":
            
            # 修正: class属性と正規表現をご提示のパターンに合わせる
            total_amount_tag = soup.find('p', class_='fs-b4 bg-light-gray p-b3 mb-b2 link-light-green')
            total_amount_int = 0
            
            if total_amount_tag:
                # <span>タグ内を検索して、支払い金額（税抜）を抽出
                match = re.search(r'支払い金額（税抜）:\s*<span[^>]*>\s*([\d,]+)円', str(total_amount_tag))
                
                if match:
                    total_amount_str = match.group(1).replace(',', '') 
                    if total_amount_str.isnumeric():
                        total_amount_int = int(total_amount_str)
                        st.info(f"✅ スクレイピングによるMK全体分配額の取得に成功しました: **{total_amount_int:,}円**")
                    else:
                        st.error("🚨 抽出した文字列が数値に変換できませんでした。")
                else:
                    st.error("🚨 HTMLの指定タグ内で「支払い金額（税抜）：[金額]円」のパターンが見つかりませんでした。")
            else:
                st.error("🚨 合計金額を示すタグ (`p` class='fs-b4...') がHTML内に見つかりませんでした。")


            header_data = [{
                'ルームID': 'MKsoul', # ルームIDは固定値
                '分配額': total_amount_int,
                'アカウントID': LOGIN_ID # secretsから取得したログインID
            }]
            header_df = pd.DataFrame(header_data)
            
            if not df_cleaned.empty:
                df_final = pd.concat([header_df, df_cleaned], ignore_index=True)
                st.success(f"**{DATA_TYPES[data_type_key]['label']}**: ライバー個別データ ({len(df_cleaned)}件) と合計値 ({total_amount_int:,}円) の抽出が完了しました。")
            else:
                df_final = header_df
                st.warning(f"**{DATA_TYPES[data_type_key]['label']}**: ライバー個別のデータ行を抽出できませんでした。合計値 ({total_amount_int:,}円) のみを含む1行データとして処理を続行します。")

        else: # time_charge or premium_live
            if df_cleaned.empty:
                st.warning(f"**{DATA_TYPES[data_type_key]['label']}**: 有効なデータ行を抽出できませんでした。")
                df_final = pd.DataFrame(columns=['ルームID', '分配額', 'アカウントID']) 
            else:
                df_final = df_cleaned
                st.success(f"**{DATA_TYPES[data_type_key]['label']}**: データ ({len(df_final)}件) の抽出が完了しました。")

        # 5. データ種別列を追加
        df_final['データ種別'] = DATA_TYPES[data_type_key]['label']
        
        # ルームIDを結合キーとして文字列に統一
        df_final['ルームID'] = df_final['ルームID'].astype(str)
        
        return df_final
        
    except requests.exceptions.HTTPError as e:
        st.error(f"HTTPエラーが発生しました: {e.response.status_code}. 認証Cookieが無効になっている可能性があります。")
        return None
    except Exception as e:
        st.error(f"予期せぬエラーが発生しました: {e}")
        logging.error("データ取得・整形エラー", exc_info=True)
        return None


def get_and_extract_sales_data(data_type_key, selected_timestamp, auth_cookie_string):
    """
    指定されたデータタイプの売上データを取得し、セッションステートに格納する
    """
    data_label = DATA_TYPES[data_type_key]["label"]
    sr_url = DATA_TYPES[data_type_key]["url"]
    
    # 1. データ取得と整形
    df_sales = fetch_and_process_data(selected_timestamp, auth_cookie_string, sr_url, data_type_key)
    
    if df_sales is not None:
        # セッションステートに格納
        st.session_state[f'df_{data_type_key}'] = df_sales
    else:
        st.session_state[f'df_{data_type_key}'] = pd.DataFrame(columns=['ルームID', '分配額', 'アカウントID', 'データ種別'])
    
    st.markdown("---")
    return st.session_state[f'df_{data_type_key}'] # 戻り値として返すように変更


# --- 既存の単月処理を関数化 (①の処理) ---

def run_single_month_processing(target_timestamp, target_label, df_livers, auth_cookie_string):
    """
    単一の配信月（例: 2025/10分）について、SHOWROOMデータ取得、結合、計算を実行し、結果DFを返す
    既存のmain関数内の処理本体をここに移動・整理した
    """
    st.subheader(f"🔄 単月処理実行: {target_label}のデータ抽出と計算")
    
    # --- SHOWROOM売上データの取得 (処理の流れ ④) ---
        
    # ルーム売上
    df_room_sales = get_and_extract_sales_data("room_sales", target_timestamp, auth_cookie_string)
    # プレミアムライブ売上
    df_premium_live = get_and_extract_sales_data("premium_live", target_timestamp, auth_cookie_string)
    # タイムチャージ売上
    df_time_charge = get_and_extract_sales_data("time_charge", target_timestamp, auth_cookie_string) 

    st.success(f"🎉 **{target_label}** の売上データの取得とセッションステートへの格納が完了しました！")
    
    # --- 取得・抽出結果の表示 ---
    if not df_livers.empty:
        
        # 取得した売上データを結合
        all_sales_data = pd.concat([df_room_sales, df_premium_live, df_time_charge])
        
        if all_sales_data.empty:
            st.warning(f"**{target_label}**: 結合対象の売上データがありません。")
            # 売上データがない場合でも、処理対象ライバー（MKsoul含む）の行は残す
            df_extracted = df_livers[['ルームID', 'ファイル名', 'インボイス', 'is_invoice_registered']].copy()
            df_extracted['データ種別'] = '売上データなし'
            df_extracted['分配額'] = 0
            df_extracted['アカウントID'] = np.nan
        else:
            # ルームIDをキーに処理対象ライバーと結合
            df_extracted = pd.merge(
                df_livers,
                all_sales_data,
                on='ルームID',
                how='left'
            )

        # 売上データがないライバー（NULL行）の分配額を0として処理
        df_extracted['分配額'] = df_extracted['分配額'].fillna(0).astype(int)
        # 表示用に、売上がゼロの行のデータ種別をNaNから「売上なし」などに変換
        df_extracted['データ種別'] = df_extracted['データ種別'].fillna('売上データなし')
        
        # 配信月とアカウントIDを追加
        df_extracted['配信月'] = target_label # 配信月をそのまま使用
        # アカウントIDを埋める
        df_extracted['アカウントID'] = df_extracted.apply(
            lambda row: row['アカウントID'] if pd.notna(row['アカウントID']) else st.session_state.login_account_id if row['ルームID'] == 'MKsoul' else np.nan, axis=1
        )
        
        # ★★★ 修正点3: マージ直後にis_invoice_registered列を明示的にbool型に再キャストする (二重の防御) ★★★
        if 'is_invoice_registered' in df_extracted.columns:
            # マージで列がNaNになる可能性があるため、NaNはFalseとして扱う
            df_extracted['is_invoice_registered'] = df_extracted['is_invoice_registered'].fillna(False).astype(bool)


        # 🌟 ルーム売上のみにランク情報を付与 🌟
        df_room_sales_only = df_extracted[df_extracted['データ種別'] == 'ルーム売上'].copy()
        df_other_sales = df_extracted[df_extracted['データ種別'] != 'ルーム売上'].copy()
        
        
        if not df_room_sales_only.empty:
            # 1. MKランク（全体ランク）の決定
            try:
                mk_sales_total = df_room_sales_only[df_room_sales_only['ルームID'] == 'MKsoul']['分配額'].iloc[0].item() 
            except Exception:
                mk_sales_total = 0
                st.warning("⚠️ 'MKsoul'行の売上計算に失敗したため、MK全体分配額を0として計算を続行します。")
                
            mk_rank_value = get_mk_rank(mk_sales_total)
            
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
                        row['is_invoice_registered']
                    ), axis=1)
            )
            
        else:
            mk_sales_total = 0 
            mk_rank_value = get_mk_rank(mk_sales_total) 
            st.warning(f"ルーム売上データなし。MK全体分配額: 0円 (→ MKランク: {mk_rank_value})")

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
                    row['is_invoice_registered']
                ), axis=1
            )

        # タイムチャージ売上
        time_charge_mask = df_other_sales['データ種別'] == 'タイムチャージ売上'
        if time_charge_mask.any():
            df_other_sales.loc[time_charge_mask, '支払額'] = df_other_sales[time_charge_mask].apply(
                lambda row: calculate_time_charge_payment_estimate(
                    row['分配額'],
                    row['is_invoice_registered']
                ), axis=1
            )
        
        # 売上データがない行の支払額は0
        no_sales_mask = df_other_sales['データ種別'] == '売上データなし'
        df_other_sales.loc[no_sales_mask, '支払額'] = 0

        # 7. 最終的なDataFrameを再結合
        df_extracted = pd.concat([df_room_sales_only, df_other_sales], ignore_index=True)

        # 8. 支払額列の表示形式を調整
        df_extracted['支払額'] = df_extracted['支払額'].replace(['#ERROR_CALC', '#ERROR_MK', '#ERROR_RANK', '#N/A'], np.nan)
        df_extracted['支払額'] = pd.to_numeric(df_extracted['支払額'], errors='coerce').fillna(0).astype('Int64') # Int64でNaNを許容する整数型に
        
        # ソートして見やすくする（オプション）
        df_extracted = df_extracted.sort_values(by=['ルームID', 'データ種別'], ascending=[True, False]).reset_index(drop=True)
        
        return df_extracted
    
    return pd.DataFrame()


# --- 新規追加: ライバー売上履歴CSVの読み込み ---

def load_liver_sales_history(file_name):
    """
    ライバーのファイル名に基づいて売上履歴CSVを取得し、DataFrameとして返す
    """
    if pd.isna(file_name) or file_name == '-':
        return pd.DataFrame()

    # ファイル名は「uriage_350565_emily.xlsx」から「uriage_350565_emily」の部分
    # URLはベースURL + ファイル名 + ".xlsx" (または.csv)
    # ユーザーの例に従い、一旦 .xlsx を想定
    file_path = f"{LIVER_HISTORY_BASE_URL}{file_name}.xlsx"
    file_path_encoded = urllib.parse.quote(file_path, safe=':/') # URLエンコード

    st.info(f"履歴ファイル読み込み中... URL: {file_path_encoded}")

    try:
        # 複数のエンコーディングで試行
        for encoding in ['utf_8_sig', 'utf-8', 'shift_jis']:
            try:
                df_history = pd.read_csv(file_path_encoded, encoding=encoding)
                st.success(f"履歴データ ({file_name}, {len(df_history)}行) の読み込みが完了しました。")
                
                # 列名をクリーンアップ
                df_history.columns = df_history.columns.str.strip()
                
                # 必須列の存在チェックとクリーンアップ
                if '配信月' not in df_history.columns or '支払/繰越' not in df_history.columns:
                    st.error(f"🚨 履歴ファイル ({file_name}) に必須の列 '配信月' または '支払/繰越' が見つかりません。")
                    return pd.DataFrame()
                
                # '支払/繰越' 列の空白を除去
                df_history['支払/繰越'] = df_history['支払/繰越'].astype(str).str.strip()
                
                return df_history
            except Exception:
                continue # 次のエンコーディングを試す
        
        st.error(f"🚨 履歴ファイル ({file_name}) の読み込みに失敗しました。（アクセス/エンコーディングエラー）")
        return pd.DataFrame()

    except Exception as e:
        st.error(f"履歴ファイル ({file_name}) の取得中に予期せぬエラー: {e}")
        return pd.DataFrame()


# --- 新規追加: 繰越処理の実行（メインロジック ②） ---

def handle_carry_forward_processing(initial_df_extracted, initial_target_ym_str, df_livers, auth_cookie_string, month_options):
    """
    繰越データを遡って探し、該当する月のデータを追加処理する
    """
    st.markdown("## 4. 繰越データ処理の開始")
    final_results = [initial_df_extracted] # 初期データ（単月分）をリストに追加

    # 処理対象ライバー（MKsoul以外）をフィルタ
    target_livers_with_file = df_livers[df_livers['ルームID'] != 'MKsoul'].dropna(subset=['ファイル名'])
    
    if target_livers_with_file.empty:
        st.warning("ファイル名を持つ処理対象ライバーがいないため、繰越処理は実行されません。")
        return initial_df_extracted
    
    for index, liver_row in target_livers_with_file.iterrows():
        liver_id = liver_row['ルームID']
        file_name = liver_row['ファイル名']
        
        with st.expander(f"ライバー {liver_id} ({file_name}) の繰越データ検索"):
            
            # 1. 履歴CSVの読み込み
            df_history = load_liver_sales_history(file_name)
            if df_history.empty:
                st.warning(f"ライバー {liver_id} の履歴データが取得できないか、無効です。繰越データ検索をスキップします。")
                continue

            current_processing_ym_str = initial_target_ym_str # 2025/10 (選択月のYYYY/MM)
            found_carry_forward = False
            
            # 履歴DFを降順（最新月→過去月）にソートしておく
            df_history['配信月_dt'] = pd.to_datetime(df_history['配信月'], format='%Y/%m', errors='coerce')
            df_history = df_history.sort_values(by='配信月_dt', ascending=False).reset_index(drop=True)

            # 選択月（現在の処理月）より古いデータから検索開始
            # 最初に'支払'となっている行のインデックスを探す
            initial_payment_row = df_history[
                (df_history['配信月'] == current_processing_ym_str) & 
                (df_history['支払/繰越'] == '支払')
            ]
            
            if initial_payment_row.empty:
                st.warning(f"ライバー {liver_id}: 選択月 {current_processing_ym_str} に『支払』の履歴行がないため、繰越検索はスキップします。")
                continue
            
            # 選択月のインデックスを取得
            start_index = initial_payment_row.index[0]

            # 2. 過去に遡って「繰越」行を探し、処理を繰り返す
            for i in range(start_index + 1, len(df_history)):
                row = df_history.iloc[i]
                prev_delivery_ym = row['配信月']
                carry_forward_status = row['支払/繰越']
                
                # 繰越が継続している場合
                if carry_forward_status == '繰越':
                    st.info(f"🔑 **繰越を発見**: 配信月 **{prev_delivery_ym}** のデータを行追加対象として処理します。")
                    found_carry_forward = True
                    
                    # 該当前月のタイムスタンプとラベルを取得
                    # YYYY/MM形式からUnixタイムスタンプを再計算
                    try:
                        y, m = map(int, prev_delivery_ym.split('/'))
                        dt_naive = datetime(y, m, 1, 0, 0, 0)
                        dt_obj_jst = JST.localize(dt_naive, is_dst=None)
                        prev_timestamp = int(dt_obj_jst.timestamp())
                        
                        # 支払月も計算してラベルを作成
                        pm = m + 2
                        py = y
                        if pm > 12:
                            pm -= 12
                            py += 1
                        prev_label = f"{y}年{m:02d}月分 (繰越分/支払月:{py}/{pm:02d})"
                        
                        # 既存の単月処理を実行（SHOWROOMからデータを再取得）
                        df_carry_forward = run_single_month_processing(prev_timestamp, prev_label, df_livers, auth_cookie_string)
                        
                        # 当該ライバーのデータのみを抽出し、追加する
                        df_liver_carry_forward = df_carry_forward[df_carry_forward['ルームID'] == liver_id].copy()
                        final_results.append(df_liver_carry_forward)
                        st.success(f"✅ {prev_delivery_ym} 分のデータ ({len(df_liver_carry_forward)}行) を最終結果に追加しました。")

                    except Exception as e:
                        st.error(f"🚨 繰越月 {prev_delivery_ym} の処理中にエラーが発生しました: {e}")

                # 再び「支払」行に到達した場合、繰越の連鎖は終了
                elif carry_forward_status == '支払':
                    st.info(f"🎉 配信月 **{prev_delivery_ym}** は『支払』済みのため、繰越の遡り処理を終了します。")
                    break
                
                else:
                    st.info(f"配信月 {prev_delivery_ym} は {carry_forward_status} のためスキップします。")
                    
            if not found_carry_forward:
                st.info(f"ライバー {liver_id} について、選択月より前の繰越データは見つかりませんでした。")
            
    # 全ての処理結果を結合し、最終的なデータとする
    final_df = pd.concat(final_results, ignore_index=True)
    return final_df.sort_values(by=['ルームID', '配信月', 'データ種別'], ascending=[True, False, False]).reset_index(drop=True)


# --- Streamlit UI ---

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
    if 'df_livers' not in st.session_state:
        st.session_state['df_livers'] = pd.DataFrame()
    
    # 新しいセッションステートの初期化
    if 'selected_month_label' not in st.session_state:
        st.session_state['selected_month_label'] = None
    if 'login_account_id' not in st.session_state:
        st.session_state['login_account_id'] = LOGIN_ID
    if 'initial_target_ym_str' not in st.session_state:
        st.session_state['initial_target_ym_str'] = None


    # 1. 対象月選択
    st.markdown("#### 1. 対象月選択")
    month_options_tuple = get_target_months()
    month_labels = [label for label, _, _ in month_options_tuple] 
    
    selected_label = st.selectbox(
        "処理対象の**配信月**を選択してください:",
        options=month_labels,
        key='month_selector'
    )
    
    selected_data = next(((ts, ym) for label, ts, ym in month_options_tuple if label == selected_label), (None, None))
    selected_timestamp = selected_data[0]
    selected_ym_str = selected_data[1] # YYYY/MM形式
    
    if selected_timestamp is None:
        st.warning("有効な月が選択されていません。")
        return

    # 選択された配信月をセッションステートに保存
    st.session_state['selected_month_label'] = selected_label
    st.session_state['initial_target_ym_str'] = selected_ym_str
    
    st.info(f"選択された月: **{selected_label}** (配信月: {selected_ym_str})")
    
    # 2. 実行ボタン
    st.markdown("#### 2. データ取得と抽出の実行")
    
    if st.button("🚀 全てのデータ取得・抽出を実行", type="primary"):
        st.markdown("---")
        
        # 処理対象ライバーファイルの読み込み
        df_livers = load_target_livers(TARGET_LIVER_FILE_URL)
        st.session_state['df_livers'] = df_livers # セッションステートに保存
        
        if df_livers.empty:
            st.error("処理対象ライバーファイルが読み込めなかったため、処理を中断します。")
            return
            
        with st.spinner(f"処理中: {selected_label}の売上データと繰越データを処理しています..."):
            
            # ① 選択月の単月処理を実行
            df_initial_extracted = run_single_month_processing(selected_timestamp, selected_label, df_livers, AUTH_COOKIE_STRING)
            
            # ② 繰越データを遡って処理（メインの新規ロジック）
            df_final = handle_carry_forward_processing(
                initial_df_extracted=df_initial_extracted, 
                initial_target_ym_str=selected_ym_str, 
                df_livers=df_livers, 
                auth_cookie_string=AUTH_COOKIE_STRING,
                month_options=month_options_tuple
            )

        st.balloons()
        st.success("🎉 **全てのデータ処理（選択月＋繰越分）が完了しました！**")
        st.session_state['df_extracted'] = df_final # 最終結果をセッションステートに保持

    # --- 最終結果の表示 ---
    
    if 'df_extracted' in st.session_state and not st.session_state.df_extracted.empty:

        df_final = st.session_state.df_extracted
        df_livers = st.session_state.df_livers
        
        st.markdown("## 5. 最終的な処理結果")
        st.markdown("---")
        
        final_display_cols = ['ルームID']
        if 'ファイル名' in df_livers.columns:
            final_display_cols.append('ファイル名')
        if 'インボイス' in df_livers.columns:
            final_display_cols.append('インボイス')
        
        # is_invoice_registered列は、計算に使われた「真のブール値」を示すため、表示列に残します
        final_display_cols.extend(['is_invoice_registered', '配信月', 'データ種別', '分配額', '個別ランク', 'MKランク', '適用料率', '支払額', 'アカウントID'])
        
        # DataFrameに存在しない列を除外
        df_extracted_cols = [col for col in final_display_cols if col in df_final.columns]
        df_final_display = df_final[df_extracted_cols]

        st.subheader("✅ 最終データ（選択月分 ＋ 繰り越されていた過去月分の行）")
        st.info(f"合計 {len(df_final_display)} 件の明細行が抽出されました。（繰越分の行は **『配信月』** の情報で識別可能です）")
        st.dataframe(df_final_display)

    else:
        st.info("実行ボタンを押して、処理を開始してください。")

if __name__ == "__main__":
    main()