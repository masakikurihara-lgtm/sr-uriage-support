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
# import openpyxl # pandasがエンジンとして使用するため、明示的なimportは必須ではない

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

# ★★★ 追加: ライバー履歴ファイルURLベース ★★★
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


# --- 支払額計算関数 (修正済み: 厳密な型チェックを追加) ---

# --- ルーム売上支払想定額計算関数 ---
def calculate_payment_estimate(individual_rank, mk_rank, individual_revenue, is_invoice_registered):
# ... (変更なし) ...
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
# ... (変更なし) ...
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
# ... (変更なし) ...
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


# --- ユーティリティ関数（ランク判定ロジック） ---

def get_individual_rank(sales_amount):
# ... (変更なし) ...
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
# ... (変更なし) ...
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
# ... (変更なし) ...
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
        #    - '' (空白のみのセル由来)
        #    - 'nan' (CSVのブランクセル由来)
        #    - 'false', '0', 'none', 'n/a' などの明示的な否定文字列
        is_registered_series = ~s_invoice.isin(['', 'nan', 'false', '0', 'none', 'n/a'])
        
        # 3. 純粋なbool型としてis_invoice_registered列を作成
        df_livers['is_invoice_registered'] = is_registered_series.astype(bool)

    else:
        # インボイス列がない場合は全てFalseとする
        st.warning("⚠️ 処理対象ライバーファイルに **'インボイス'** 列が見つかりません。全てのライバーを非登録者として処理します。")
        df_livers['is_invoice_registered'] = False
    
    st.info(f"インボイス登録者 ({df_livers['is_invoice_registered'].sum()}名) のフラグ付けが完了しました。")
    
    return df_livers


# ★★★ 修正: YYYY/MM形式の文字列を戻り値に追加 ★★★
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

        month_str = f"{current_year}年{current_month:02d}月分"
        
        try:
            dt_naive = datetime(current_year, current_month, 1, 0, 0, 0)
            dt_obj_jst = JST.localize(dt_naive, is_dst=None)
            timestamp = int(dt_obj_jst.timestamp())
            ym_str = f"{current_year}{current_month:02d}"
            ym_match = f"{current_year}/{current_month:02d}" # YYYY/MM 形式を追加
            
            months.append((month_str, timestamp, ym_str, ym_match)) # (ラベル, UNIXタイムスタンプ, YYYYMM, YYYY/MM)
        except Exception as e:
            logging.error(f"日付計算エラー ({month_str}): {e}")
            
        # 次の月（前の月）へ移動
        if current_month == 1:
            current_month = 12
            current_year -= 1
        else:
            current_month -= 1
            
    return months


def create_authenticated_session(cookie_string):
# ... (変更なし) ...
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
# ... (変更なし) ...
    """
    指定されたタイムスタンプに基づいてSHOWROOMからデータを取得し、DataFrameに整形して返す
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
# ... (変更なし) ...
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

# ★★★ 新規追加: ライバー履歴ファイル読み込み関数とエラー修正 ★★★
def load_liver_history_data(room_id, file_name_base, target_ym_match):
    """
    ライバー個別の履歴ファイル（.xlsx）を読み込み、指定月のデータ（または繰越データ）を取得する
    エラーの原因となるNaN行をフィルタリングする処理を追加
    """
    file_name = f"uriage_{file_name_base}.xlsx"
    file_url = f"{LIVER_HISTORY_BASE_URL}{file_name}"

    st.info(f"ライバー履歴ファイル読み込み中: {file_url}")

    try:
        # Excelファイルを読み込む (openpyxlが必要)
        df_history = pd.read_excel(file_url, sheet_name=0, engine='openpyxl')
        
        # 列名から不要な改行や空白を除去
        df_history.columns = df_history.columns.str.strip().str.replace('\n', '')

        if '配信月' not in df_history.columns:
            st.error(f"🚨 履歴ファイル ({file_name_base}) に必須の列 '配信月' が見つかりません。")
            return pd.DataFrame()
            
        # ★★★ 決定的な修正: NaN (空欄) の行を除去してから日付処理を行う ★★★
        # NaNやNoneなどの欠損値を含む行を削除 (これがエラー回避のキモ)
        df_history_cleaned = df_history.dropna(subset=['配信月'])
        
        if df_history_cleaned.empty:
            st.warning(f"履歴ファイル ({file_name_base}): '配信月'が有効なデータ行がありませんでした。")
            return pd.DataFrame()

        # '配信月' を文字列に変換し、前後の空白を除去
        # Excelの読み込み時にdatetimeオブジェクトになっている可能性があるため、strftimeで'YYYY/MM'形式に変換（文字列に変換後、'/XX'を削除する）
        try:
             # まず日付型に変換できるか試す
            df_history_cleaned['配信月_str'] = pd.to_datetime(df_history_cleaned['配信月'], errors='coerce').dt.strftime('%Y/%m')
        except:
             # 失敗した場合（すでに文字列など）、そのまま使用
            df_history_cleaned['配信月_str'] = df_history_cleaned['配信月'].astype(str).str.strip()
            # YYYY/MM/DD形式などにも対応するため、最初の7文字(YYYY/MM)のみ取得
            df_history_cleaned['配信月_str'] = df_history_cleaned['配信月_str'].str[:7]


        # 配信月が target_ym_match (例: '2025/10') と一致する行を抽出
        target_month_data = df_history_cleaned[
            df_history_cleaned['配信月_str'] == target_ym_match
        ].copy()

        # 繰越データ (支払/繰越 = '繰越') も抽出
        carry_over_data = df_history_cleaned[
            df_history_cleaned['支払/繰越'].astype(str).str.strip() == '繰越'
        ].copy()
        
        # 結合して返す
        df_result = pd.concat([target_month_data, carry_over_data], ignore_index=True)
        
        # 必要な列を抽出・整形し、一般的な売上データフレームの形式に合わせる
        
        # 存在しない可能性のある列をNaNで埋めるための列リスト
        required_cols = ['ルームID', '分配額', 'アカウントID', 'データ種別', '支払額']

        # '合計支払想定額'の列名を探す (スペース/改行除去後の列名を使用)
        payment_col = '合計支払想定額'
        if payment_col in df_result.columns:
            df_result['支払額'] = df_result[payment_col]
        else:
            df_result['支払額'] = np.nan
        
        df_result['ルームID'] = str(room_id)
        df_result['分配額'] = np.nan # 履歴データでは「分配額」は使わないためNaN
        df_result['アカウントID'] = np.nan # 履歴データでは「アカウントID」は使わないためNaN
        df_result['データ種別'] = '繰越履歴'
        
        # 必要な列のみに絞り、欠損列を補完
        df_result = df_result.reindex(columns=required_cols)
        
        st.success(f"✅ 履歴ファイル ({file_name_base}) の読み込みとフィルタリングが完了しました。取得行数: {len(df_result)}")
        return df_result

    except Exception as e:
        st.error(f"🚨 履歴ファイル ({file_name_base}) の処理中に予期せぬエラーが発生しました: {e}。このライバーの履歴データはスキップします。")
        return pd.DataFrame()


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
    # ★★★ 追加: 履歴データ用ステート初期化 ★★★
    if 'df_history_all' not in st.session_state:
        st.session_state['df_history_all'] = pd.DataFrame()

    # 新しいセッションステートの初期化
    if 'selected_month_label' not in st.session_state:
        st.session_state['selected_month_label'] = None
    if 'login_account_id' not in st.session_state:
        st.session_state['login_account_id'] = LOGIN_ID


    # 1. 対象月選択 (処理の流れ ①)
    st.markdown("#### 1. 対象月選択")
    month_options_tuple = get_target_months()
    # ★★★ 修正: month_options_tupleの要素数が4つになったため、ラベル抽出も修正 ★★★
    month_labels = [label for label, _, _, _ in month_options_tuple] 
    
    selected_label = st.selectbox(
        "処理対象の**配信月**を選択してください:",
        options=month_labels,
        key='month_selector' # keyを追加し、選択を追跡
    )
    
    # 4要素タプルから、ラベルに一致する要素を取得 (ts: timestamp, ym: YYYYMM, ym_match: YYYY/MM)
    selected_data = next(((ts, ym, ym_match) for label, ts, ym, ym_match in month_options_tuple if label == selected_label), (None, None, None))
    selected_timestamp = selected_data[0]
    selected_ym_match = selected_data[2] # YYYY/MM形式を取得
    
    if selected_timestamp is None:
        st.warning("有効な月が選択されていません。")
        return

    # 選択された配信月をセッションステートに保存
    st.session_state['selected_month_label'] = selected_label
    st.session_state['selected_ym_match'] = selected_ym_match

    # ★★★ 修正: ログに合わせた情報表示 ★★★
    st.info(f"選択された月: **{selected_label}** (繰越判定用: {selected_ym_match})")
    
    # 2. データ取得と抽出の実行
    st.markdown("#### 2. データ取得と抽出の実行")
    
    if st.button("🚀 データの取得・抽出を実行", type="primary"):
        st.markdown("---")
        
        # 処理対象ライバーファイルの読み込み (処理の流れ ③)
        df_livers = load_target_livers(TARGET_LIVER_FILE_URL)
        st.session_state['df_livers'] = df_livers # セッションステートに保存
        
        if df_livers.empty:
            st.error("処理対象ライバーファイルが読み込めなかったため、処理を中断します。")
            return
            
        with st.spinner(f"処理中: {selected_label}の売上データをSHOWROOMから取得しています..."):
            
            # --- SHOWROOM売上データの取得 (処理の流れ ④) ---
            
            # ルーム売上
            get_and_extract_sales_data("room_sales", selected_timestamp, AUTH_COOKIE_STRING)

            # プレミアムライブ売上
            get_and_extract_sales_data("premium_live", selected_timestamp, AUTH_COOKIE_STRING)

            # タイムチャージ売上
            get_and_extract_sales_data("time_charge", selected_timestamp, AUTH_COOKIE_STRING) 
            
            # --- ★★★ 追加: ライバー履歴ファイルの読み込みと抽出 ★★★ ---
            st.subheader("ライバー履歴ファイルの処理 (繰越データ抽出)")
            df_history_list = []
            
            # ルームID='MKsoul'の行と、ファイル名がない行はスキップ
            df_target_livers_only = df_livers[
                (df_livers['ルームID'] != 'MKsoul') & 
                (pd.notna(df_livers['ファイル名']))
            ]
            
            for _, row in df_target_livers_only.iterrows():
                room_id = row['ルームID']
                file_name_base = row['ファイル名'] # uriage_XXXXXX_name の XXXXXX_name の部分
                
                # 履歴ファイル読み込み関数を呼び出し、エラーハンドリングは関数内で行う
                df_history = load_liver_history_data(room_id, file_name_base, selected_ym_match)
                
                if not df_history.empty:
                    df_history_list.append(df_history)

            if df_history_list:
                df_history_all = pd.concat(df_history_list, ignore_index=True)
                st.session_state['df_history_all'] = df_history_all
                st.success(f"✅ 全ライバーの履歴データ抽出が完了しました。（合計 {len(df_history_all)}行）")
            else:
                st.session_state['df_history_all'] = pd.DataFrame()
                st.warning("履歴ファイルから抽出されたデータはありませんでした。")
            # -----------------------------------------------------------------

        st.balloons()
        st.success("🎉 **売上データの取得とセッションステートへの格納が完了しました！**")

    # --- 取得・抽出結果の表示 ---
    
    if not st.session_state.df_room_sales.empty or 'df_livers' in st.session_state:

        st.markdown("## 3. 抽出結果の確認、ランク・支払額の付与") # タイトルを修正
        st.markdown("---")

        if 'df_livers' in st.session_state and not st.session_state.df_livers.empty:
            df_livers = st.session_state.df_livers
            st.subheader("処理対象ライバー一覧")
            
            # 存在しない列の参照による KeyError を防ぐため、表示列を動的に決定する
            expected_cols = ['ルームID', 'ファイル名', 'インボイス', 'is_invoice_registered']
            display_cols = [col for col in expected_cols if col in df_livers.columns]
            
            # 「インボイス」列は、入力データそのものとして保持し、計算に使われる 'is_invoice_registered' (純粋なbool) と比較可能とする
            st.dataframe(df_livers[display_cols], height=150)
            
            # --- 売上データを結合して抽出 ---
            
            # 取得した売上データを結合
            all_sales_data = pd.concat([
                st.session_state.df_room_sales,
                st.session_state.df_premium_live,
                st.session_state.df_time_charge,
                # ★★★ 修正: 履歴データ（繰越）を結合対象に追加 ★★★
                st.session_state.get('df_history_all', pd.DataFrame(columns=['ルームID', '分配額', 'アカウントID', 'データ種別', '支払額']))
            ])
            
            if not all_sales_data.empty:
                st.subheader("全売上データ (取得元) - 合計")
                st.dataframe(all_sales_data, height=150)
                
                # ルームIDをキーに処理対象ライバーと結合
                # 履歴データは多重行になるため、マージはせず、結合されたall_sales_dataをそのまま処理対象とする
                # ただし、ライバー情報（インボイスフラグ）を付与するために、df_liversとマージする
                
                # df_liversから必要な列のみ抽出
                livers_info = df_livers[['ルームID', 'ファイル名', 'インボイス', 'is_invoice_registered']].copy()
                
                # all_sales_dataにライバー情報を紐付け
                df_merged = pd.merge(
                    all_sales_data,
                    livers_info,
                    on='ルームID',
                    how='left'
                )

                # 売上データがないライバー（NULL行）の分配額を0として処理 (繰越履歴はNaNのままにする)
                df_merged['分配額'] = df_merged['分配額'].fillna(0)
                
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
                    # is_invoice_registeredがNaNの場合はFalseに設定する
                    df_merged['is_invoice_registered'] = df_merged['is_invoice_registered'].fillna(False).astype(bool)


                # 🌟 ルーム売上のみにランク情報を付与 🌟
                # df_mergedを「ルーム売上」データと「その他・履歴」データに分割
                df_room_sales_only = df_merged[df_merged['データ種別'] == 'ルーム売上'].copy()
                df_other_history_sales = df_merged[df_merged['データ種別'] != 'ルーム売上'].copy()
                
                
                if not df_room_sales_only.empty:
                    
                    # 1. MKランク（全体ランク）の決定
                    # df_merged内からMKsoulの分配額を取得（念のため）
                    mk_sales_total = df_room_sales_only[df_room_sales_only['ルームID'] == 'MKsoul']['分配額'].iloc[0].item() if not df_room_sales_only[df_room_sales_only['ルームID'] == 'MKsoul'].empty else 0
                    
                    if mk_sales_total == 0:
                        st.warning("⚠️ MK全体分配額が0です。SHOWROOM側のデータがないか、合計金額の抽出に失敗している可能性があります。")

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

                
                # 5. その他の売上行・履歴行のランク列を埋める
                df_other_history_sales['MKランク'] = '-'
                df_other_history_sales['個別ランク'] = '-'
                df_other_history_sales['適用料率'] = '-'

                # 6. その他の売上支払額の計算
                # 履歴データには支払額がすでに入っている可能性があるため、NaNの場合のみ計算する
                
                # プレミアムライブ売上 (支払額がNaNの場合のみ計算)
                premium_live_mask = (df_other_history_sales['データ種別'] == 'プレミアムライブ売上') & pd.isna(df_other_history_sales['支払額'])
                if premium_live_mask.any():
                    df_other_history_sales.loc[premium_live_mask, '支払額'] = df_other_history_sales[premium_live_mask].apply(
                        lambda row: calculate_paid_live_payment_estimate(
                            row['分配額'],
                            row['is_invoice_registered'] # 厳格チェック付きの関数に渡す
                        ), axis=1
                    )

                # タイムチャージ売上 (支払額がNaNの場合のみ計算)
                time_charge_mask = (df_other_history_sales['データ種別'] == 'タイムチャージ売上') & pd.isna(df_other_history_sales['支払額'])
                if time_charge_mask.any():
                    df_other_history_sales.loc[time_charge_mask, '支払額'] = df_other_history_sales[time_charge_mask].apply(
                        lambda row: calculate_time_charge_payment_estimate(
                            row['分配額'],
                            row['is_invoice_registered'] # 厳格チェック付きの関数に渡す
                        ), axis=1
                    )
                
                # 売上データがない行の支払額は0
                no_sales_mask = (df_other_history_sales['データ種別'] == '売上データなし') & pd.isna(df_other_history_sales['支払額'])
                df_other_history_sales.loc[no_sales_mask, '支払額'] = 0
                
                # 履歴データ（繰越履歴）は、Excelから読み込んだ「支払額」を使用し、NaNの場合は0とする
                history_mask = df_other_history_sales['データ種別'] == '繰越履歴'
                df_other_history_sales.loc[history_mask, '支払額'] = df_other_history_sales.loc[history_mask, '支払額'].fillna(0)


                # 7. 最終的なDataFrameを再結合
                df_extracted = pd.concat([df_room_sales_only, df_other_history_sales], ignore_index=True)
                
                # 8. 不要な列を整理し、抽出が完了したDataFrameを表示 (ランク情報を追加)
                final_display_cols = ['ルームID']
                if 'ファイル名' in df_livers.columns:
                    final_display_cols.append('ファイル名')
                if 'インボイス' in df_livers.columns:
                    final_display_cols.append('インボイス')
                    
                # is_invoice_registered列は、計算に使われた「真のブール値」を示すため、表示列に残します
                final_display_cols.extend(['is_invoice_registered', 'データ種別', '分配額', '個別ランク', 'MKランク', '適用料率', '支払額', 'アカウントID', '配信月'])
                
                # DataFrameに存在しない列を除外
                df_extracted_cols = [col for col in final_display_cols if col in df_extracted.columns]
                df_extracted = df_extracted[df_extracted_cols]
                
                # 支払額列の表示形式を調整（整数としてNaN以外を扱う）
                # エラー文字列はNaN/0として処理
                error_values = ['#ERROR_CALC', '#ERROR_MK', '#ERROR_RANK', '#N/A']
                df_extracted['支払額'] = df_extracted['支払額'].replace(error_values, np.nan)
                df_extracted['支払額'] = pd.to_numeric(df_extracted['支払額'], errors='coerce').fillna(0).astype('Int64') # Int64でNaNを許容する整数型に

                # ソートして見やすくする（オプション）
                df_extracted = df_extracted.sort_values(by=['ルームID', 'データ種別'], ascending=[True, False]).reset_index(drop=True)

                st.subheader("✅ 抽出・結合された最終データ (支払額計算済み)")
                st.info(f"このデータで、分配額から**支払額**の計算が完了しました。合計 {len(df_livers)}件のライバー情報に対して、{len(df_extracted)}件の売上明細行が紐付けられました。")
                st.dataframe(df_extracted)
                
                # 計算ステップのためにセッションステートに保持
                st.session_state['df_extracted'] = df_extracted
            
            else:
                st.warning("結合対象の売上データがありません。")
        else:
            st.info("実行ボタンを押して、処理対象ライバーファイルの読み込みと売上データの取得を行ってください。")

if __name__ == "__main__":
    main()