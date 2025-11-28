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


# --- ユーティリティ関数（ランク判定ロジック） ---

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
            
            months.append((month_str, timestamp, ym_str)) # (ラベル, UNIXタイムスタンプ, YYYYMM)
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

# --- Streamlit UI ---


# -------------------------
# ヘルパー: 履歴Excelから「最新支払行」起点で連続する繰越配信月を取得する
# -------------------------
def get_kurikoshi_months_from_excel(file_basename, target_payment_month_str):
    """
    file_basename: '350565_emily' のようにファイル名部分（拡張子無し）
    target_payment_month_str: 'YYYY/MM' (例 '2025/12')  --- 履歴内の '支払月' と合わせる形式
    戻り値: ['YYYY/MM', 'YYYY/MM', ...] 最新(今回支払) → 古い 順で返す
    """
    import pandas as _pd
    import requests as _req
    import io as _io

    url_xlsx = f"https://mksoul-pro.com/showroom/csv/uriage_{file_basename}.xlsx"
    try:
        r = _req.get(url_xlsx, timeout=15)
        r.raise_for_status()
        df_hist = _pd.read_excel(_io.BytesIO(r.content))
    except Exception:
        # Excel取得/解析に失敗したら空で返す（PDF対応は必要なら別途実装）
        return []

    # 列名整形
    df_hist.columns = df_hist.columns.str.strip()

    # 必須列チェック
    expected = ['配信月', '支払月', '支払/繰越']
    if not all(col in df_hist.columns for col in expected):
        return []

    # 配信月/支払月を 'YYYY/MM' 形式へ正規化する関数
    def norm_month_to_yyyy_mm(val):
        # 既に 'YYYY/MM' の文字列なら整形して返す
        if isinstance(val, str) and '/' in val:
            parts = val.split('/')
            if len(parts) >= 2:
                y = parts[0].zfill(4)
                m = parts[1].zfill(2)
                return f"{y}/{m}"
            return val
        # datetime型やその他を pandas でパース
        try:
            dt = _pd.to_datetime(val, errors='coerce')
            if not _pd.isna(dt):
                return f"{dt.year}/{dt.month:02d}"
        except Exception:
            pass
        return str(val).strip()

    df_hist['配信月'] = df_hist['配信月'].apply(norm_month_to_yyyy_mm)
    df_hist['支払月'] = df_hist['支払月'].apply(norm_month_to_yyyy_mm)
    df_hist['支払/繰越'] = df_hist['支払/繰越'].astype(str).str.strip()

    # 履歴は上が最新（想定）か下が最新か不明なので、最新が上に来るよう一意な配信月で先頭保持
    uniq = df_hist.drop_duplicates(subset=['配信月'], keep='first').reset_index(drop=True)

    # target_payment_month_str（例 '2025/12'）に該当する '支払' の行を探す
    mask = (uniq['支払月'] == target_payment_month_str) & (uniq['支払/繰越'] == '支払')
    idxs = uniq.index[mask].tolist()
    if not idxs:
        return []

    base_idx = idxs[0]
    result = []
    result.append(uniq.loc[base_idx, '配信月'])  # 最新（今回支払対象の配信月）

    # base_idx の次（より古い行）から連続して '繰越' を追加、途中で '支払' が出たら終了
    i = base_idx + 1
    while i < len(uniq):
        kind = str(uniq.loc[i, '支払/繰越']).strip()
        if kind == '繰越':
            result.append(uniq.loc[i, '配信月'])
            i += 1
            continue
        else:
            break

    return result



def main():
    st.set_page_config(page_title="SHOWROOM 支払明細書作成補助ツール", layout="wide")
    st.markdown(
        "<h1 style='font-size:28px; text-align:left; color:#1f2937;'>SHOWROOM 支払明細書作成補助ツール</h1>",
        unsafe_allow_html=True
    )
    st.markdown("<p style='text-align: left;'>⚠️ <b>注意</b>: このツールは、<b>Secretsに設定されたCookieが有効な間のみ</b>動作します。</p>", unsafe_allow_html=True)
    st.markdown("<p style='text-align: left;'>⚠️ <b>注意</b>: <b>処理対象ライバーファイル（ https://mksoul-pro.com/showroom/file/shiharai-taishou.csv ）の内容が適切か確認してください</b>。</p>", unsafe_allow_html=True)
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


    # 1. 対象月選択 (処理の流れ ①)
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
    
    # 2. 実行ボタン (処理の流れ ②)
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
                df_extracted = pd.concat([df_room_sales_only, df_other_sales], ignore_index=True)


                # --- 繰越追加処理（ここから） ---
                # 各ライバーの履歴ファイルを参照して、連続する繰越配信月分を取得し
                # 同じ単月処理と同等の行を作成して df_extracted に追加する

                # selected_month_label 例: '2025年10月分' -> 支払月 = 選択配信月 + 2ヶ月 -> 'YYYY/MM'
                sel_label = st.session_state.get('selected_month_label', '')
                m = re.match(r'(\d{4})年(\d{2})月分', str(sel_label))
                if m:
                    sel_year = int(m.group(1)); sel_month = int(m.group(2))
                else:
                    sel_year = None; sel_month = None

                if sel_year and sel_month:
                    # 支払月 = 選択された配信月 + 2ヶ月
                    pay_year = sel_year
                    pay_month = sel_month + 2
                    if pay_month > 12:
                        pay_month -= 12
                        pay_year += 1
                    pay_month_str = f"{pay_year}/{pay_month:02d}"  # 履歴Excelの '支払月' と照合する形式

                    # df_livers は既にロード済み
                    if 'df_livers' in locals() or 'df_livers' in globals() or 'df_livers' in st.session_state:
                        df_livers_local = st.session_state.get('df_livers', pd.DataFrame()).copy()
                        if not df_livers_local.empty:
                            # 1ライバーずつ処理
                            for _, liver_row in df_livers_local.iterrows():
                                file_basename = liver_row.get('ファイル名')
                                room_id = str(liver_row.get('ルームID', '')).strip()
                                if not file_basename or pd.isna(file_basename):
                                    continue

                                try:
                                    months_list = get_kurikoshi_months_from_excel(str(file_basename), pay_month_str)
                                except Exception:
                                    months_list = []

                                # months_list の先頭は今回処理済みの配信月（既に df_extracted に含まれている）
                                if len(months_list) <= 1:
                                    continue
                                months_to_add = months_list[1:]  # 例 ['2025/09','2025/08',...]

                                for mstr in months_to_add:
                                    # mstr は 'YYYY/MM' 形式
                                    try:
                                        y_s, mm_s = mstr.split('/')
                                        y_i = int(y_s); m_i = int(mm_s)
                                        # タイムスタンプに変換（fetch_and_process_data が受けるタイムスタンプ）
                                        dt_naive = datetime(y_i, m_i, 1, 0, 0, 0)
                                        dt_obj_jst = JST.localize(dt_naive, is_dst=None)
                                        ts = int(dt_obj_jst.timestamp())
                                    except Exception:
                                        continue

                                    # その月に関する SHOWROOM の3種データを取得（既存関数を再利用）
                                    df_room_month = fetch_and_process_data(ts, AUTH_COOKIE_STRING, SR_ROOM_SALES_URL, "room_sales")
                                    df_premium_month = fetch_and_process_data(ts, AUTH_COOKIE_STRING, SR_PREMIUM_LIVE_URL, "premium_live")
                                    df_time_month = fetch_and_process_data(ts, AUTH_COOKIE_STRING, SR_TIME_CHARGE_URL, "time_charge")

                                    # 取得失敗や None の場合はスキップ
                                    if df_room_month is None or df_premium_month is None or df_time_month is None:
                                        continue

                                    # MK全体合計は df_room_month の MKsoul 行から取得（既存ロジックに合わせる）
                                    try:
                                        mk_total = int(df_room_month[df_room_month['ルームID'] == 'MKsoul']['分配額'].iloc[0])
                                    except Exception:
                                        mk_total = 0
                                    mk_rank_value = get_mk_rank(mk_total)

                                    # 対象ライバーの行だけ抽出
                                    all_sales_month = pd.concat([df_room_month, df_premium_month, df_time_month], ignore_index=True)
                                    sel_rows = all_sales_month[all_sales_month['ルームID'] == room_id].copy()

                                    if sel_rows.empty:
                                        # 売上データなしの行を既存の形式に合わせて作る
                                        no_row = {
                                            'ルームID': room_id,
                                            '分配額': 0,
                                            'アカウントID': np.nan,
                                            'データ種別': '売上データなし',
                                            '配信月': f"{y_i}年{m_i:02d}月分",
                                            'is_invoice_registered': bool(liver_row.get('is_invoice_registered', False))
                                        }
                                        df_add = pd.DataFrame([no_row])
                                    else:
                                        sel_rows['配信月'] = f"{y_i}年{m_i:02d}月分"
                                        sel_rows['is_invoice_registered'] = bool(liver_row.get('is_invoice_registered', False))

                                        # ルーム売上は個別ランク・MKランク・支払額を付与
                                        df_room_part = sel_rows[sel_rows['データ種別'] == 'ルーム売上'].copy()
                                        df_other_part = sel_rows[sel_rows['データ種別'] != 'ルーム売上'].copy()

                                        if not df_room_part.empty:
                                            df_room_part['MKランク'] = mk_rank_value
                                            df_room_part['個別ランク'] = df_room_part['分配額'].apply(get_individual_rank)
                                            df_room_part['適用料率'] = np.where(
                                                df_room_part['ルームID'] == 'MKsoul',
                                                '-',
                                                '適用料率：' + df_room_part['MKランク'].astype(str) + df_room_part['個別ランク']
                                            )
                                            df_room_part['支払額'] = df_room_part.apply(
                                                lambda row: calculate_payment_estimate(
                                                    row['個別ランク'],
                                                    row['MKランク'],
                                                    row['分配額'],
                                                    row['is_invoice_registered']
                                                ), axis=1
                                            )
                                        else:
                                            df_room_part = pd.DataFrame(columns=sel_rows.columns.tolist() + ['MKランク','個別ランク','適用料率','支払額'])

                                        # その他（プレミアム/タイムチャージ）
                                        if not df_other_part.empty:
                                            df_other_part['MKランク'] = '-'
                                            df_other_part['個別ランク'] = '-'
                                            df_other_part['適用料率'] = '-'
                                            mask_pre = df_other_part['データ種別'] == 'プレミアムライブ売上'
                                            if mask_pre.any():
                                                df_other_part.loc[mask_pre, '支払額'] = df_other_part[mask_pre].apply(
                                                    lambda r: calculate_paid_live_payment_estimate(r['分配額'], r.get('is_invoice_registered', False)), axis=1
                                                )
                                            mask_time = df_other_part['データ種別'] == 'タイムチャージ売上'
                                            if mask_time.any():
                                                df_other_part.loc[mask_time, '支払額'] = df_other_part[mask_time].apply(
                                                    lambda r: calculate_time_charge_payment_estimate(r['分配額'], r.get('is_invoice_registered', False)), axis=1
                                                )
                                        else:
                                            df_other_part = pd.DataFrame(columns=sel_rows.columns.tolist() + ['MKランク','個別ランク','適用料率','支払額'])

                                        df_add = pd.concat([df_room_part, df_other_part], ignore_index=True)

                                    # 最終形式に沿って列を揃え、支払額の型を整える
                                    cols_to_keep = [c for c in ['ルームID','ファイル名','インボイス','is_invoice_registered','データ種別','分配額','個別ランク','MKランク','適用料率','支払額','アカウントID','配信月'] if c in df_add.columns]
                                    df_add = df_add[cols_to_keep]
                                    if '支払額' in df_add.columns:
                                        df_add['支払額'] = df_add['支払額'].replace(['#ERROR_CALC','#ERROR_MK','#ERROR_RANK','#N/A'], np.nan)
                                        df_add['支払額'] = pd.to_numeric(df_add['支払額'], errors='coerce').fillna(0).astype('Int64')

                                    # df_extracted に連結（既存の順序を崩さない）
                                    df_extracted = pd.concat([df_extracted, df_add], ignore_index=True)

                # --- 繰越追加処理（ここまで） ---


                
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
                df_extracted['支払額'] = df_extracted['支払額'].replace(['#ERROR_CALC', '#ERROR_MK', '#ERROR_RANK', '#N/A'], np.nan)
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