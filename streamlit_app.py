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
# 個別ライバー売上履歴ファイルのベースURL
LIVER_HISTORY_BASE_URL = "https://mksoul-pro.com/showroom/csv/uriage_"

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


# --- 支払額計算関数 (変更なし) ---

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

        # 最終防衛線: 厳格なブール値チェック
        is_registered = is_invoice_registered
        if not isinstance(is_registered, bool):
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
    if pd.isna(paid_live_amount):
        return np.nan

    try:
        individual_revenue = float(paid_live_amount)

        # 最終防衛線: 厳格なブール値チェック
        is_registered = is_invoice_registered
        if not isinstance(is_registered, bool):
            is_registered = not (str(is_registered).lower().strip() in ('', 'false', '0', 'nan', 'none'))

        if is_registered:
            payment_estimate = (individual_revenue * 1.10 * 0.9) / 1.10
        else:
            payment_estimate = (individual_revenue * 1.08 * 0.9) / 1.10

        return round(payment_estimate)

    except Exception:
        return "#ERROR_CALC"

# --- タイムチャージ支払想定額計算関数 ---
def calculate_time_charge_payment_estimate(time_charge_amount, is_invoice_registered):
    """
    タイムチャージ分配額、インボイス登録有無から支払想定額を計算する
    """
    if pd.isna(time_charge_amount):
        return np.nan

    try:
        individual_revenue = float(time_charge_amount)

        # 最終防衛線: 厳格なブール値チェック
        is_registered = is_invoice_registered
        if not isinstance(is_registered, bool):
            is_registered = not (str(is_registered).lower().strip() in ('', 'false', '0', 'nan', 'none'))

        if is_registered:
            payment_estimate = (individual_revenue * 1.10 * 1.00) / 1.10
        else:
            payment_estimate = (individual_revenue * 1.08 * 1.00) / 1.10

        return round(payment_estimate)

    except Exception:
        return "#ERROR_CALC"


# --- 新規: 繰越月判定ロジック (変更なし) ---

def get_timestamp_from_ym(ym_str):
    """'YYYY/MM'形式をUNIXタイムスタンプ（月の初日0時JST）に変換する"""
    try:
        year, month = map(int, ym_str.split('/'))
        dt_naive = datetime(year, month, 1, 0, 0, 0)
        dt_obj_jst = JST.localize(dt_naive, is_dst=None)
        return int(dt_obj_jst.timestamp())
    except Exception:
        return None

def get_required_fetch_months(file_name, current_ym_str, session):
    """
    ライバーの履歴ファイルから、現在の月を含め、繰越が必要な配信月(YYYY/MM)のリストを返す。
    """
    # 履歴ファイルURLを構築 (xlsxと仮定)
    url = f"{LIVER_HISTORY_BASE_URL}{file_name}.xlsx"
    st.info(f"ライバー履歴ファイル読み込み中: {url}")

    required_ym_list = []

    try:
        # HTTP GETリクエストでファイルを取得
        response = session.get(url, timeout=10)
        response.raise_for_status()

        # ExcelファイルをDataFrameとして読み込み
        df_history = pd.read_excel(io.BytesIO(response.content), engine='openpyxl')

        # 列名から前後の空白文字を全て除去
        df_history.columns = df_history.columns.str.strip()

        if '配信月' not in df_history.columns or '支払/繰越' not in df_history.columns:
            st.error(f"🚨 履歴ファイル ({file_name}) に必須の列 ('配信月' または '支払/繰越') が見つかりません。")
            return [current_ym_str] # 処理対象月のみを返す

        # 配信月を文字列に変換し、'/'区切りを強制
        df_history['配信月'] = df_history['配信月'].astype(str).str.replace(r'(\d{4})/(\d{1,2})', r'\1/\2', regex=True).str.strip()

        # 処理対象月以降の行を除外 (例: 2025/11以降のデータが入っている場合を考慮)
        df_history = df_history[df_history['配信月'].apply(lambda x: datetime.strptime(x, '%Y/%m')) <= datetime.strptime(current_ym_str, '%Y/%m')].copy()

        # 最新月 (current_ym_str) を確認し、リストに追加
        current_row = df_history[df_history['配信月'] == current_ym_str]

        if current_row.empty:
            st.warning(f"⚠️ 履歴ファイル ({file_name}) に選択された月 ({current_ym_str}) のデータが見つかりませんでした。この月のみ処理します。")
            return [current_ym_str]

        # 繰越ロジック
        required_ym_list.append(current_ym_str)

        # 現在の行より前の行を逆順にチェック
        # df_historyはExcelの読み込み順（通常、最新月が最初）でソートされていると仮定
        current_index = current_row.index[0]

        for idx in range(current_index + 1, len(df_history)):
            row = df_history.iloc[idx]
            ym_str = row['配信月']
            status = str(row['支払/繰越']).strip()

            if status == '繰越':
                required_ym_list.append(ym_str)
            elif status == '支払':
                # 繰越の連鎖がここで途切れる
                break

        st.success(f"✅ 繰越判定完了: {file_name} の処理対象月は {required_ym_list} です。")
        return required_ym_list

    except requests.exceptions.HTTPError as e:
        st.error(f"🚨 履歴ファイル ({file_name}) の取得に失敗しました (HTTPエラー: {e.response.status_code})。この月のみ処理します。")
        return [current_ym_str]
    except Exception as e:
        st.error(f"🚨 履歴ファイル ({file_name}) の処理中に予期せぬエラーが発生しました: {e}。この月のみ処理します。")
        return [current_ym_str]


# --- 既存関数 (微修正) ---

# load_target_livers（変更なし）
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
    df_livers.columns = df_livers.columns.str.strip()

    if 'ルームID' in df_livers.columns:
        df_livers['ルームID'] = df_livers['ルームID'].astype(str)
    else:
        st.error("🚨 処理対象ライバーファイルに必須の列 **'ルームID'** が見つかりません。")
        return pd.DataFrame()

    # ★★★ 決定的な修正: インボイス登録判定ロジックのバグフィックス (NaN->'nan'対策) ★★★
    if 'インボイス' in df_livers.columns:
        s_invoice = df_livers['インボイス'].astype(str).str.strip().str.lower()
        is_registered_series = ~s_invoice.isin(['', 'nan', 'false', '0', 'none', 'n/a'])
        df_livers['is_invoice_registered'] = is_registered_series.astype(bool)
    else:
        st.warning("⚠️ 処理対象ライバーファイルに **'インボイス'** 列が見つかりません。全てのライバーを非登録者として処理します。")
        df_livers['is_invoice_registered'] = False

    st.info(f"インボイス登録者 ({df_livers['is_invoice_registered'].sum()}名) のフラグ付けが完了しました。")

    return df_livers


# fetch_and_process_data (変更なし)
def fetch_and_process_data(timestamp, cookie_string, sr_url, data_type_key):
    """
    単月売上データを取得し、DataFrameに整形して返す (既存関数を単月取得用として維持)
    """
    st.info(f"単月データ取得中... **{DATA_TYPES[data_type_key]['label']}** (URL: {sr_url}, タイムスタンプ: {timestamp})")
    session = create_authenticated_session(cookie_string)
    if not session:
        return None

    try:
        # 1. データ取得
        url = f"{sr_url}?from={timestamp}"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image:apng,*/*;q=0.8',
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
                # 認証切れはここでエラーを出す
                raise requests.exceptions.HTTPError("認証切れの可能性")
            # データなしは警告として処理
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

            total_amount_tag = soup.find('p', class_='fs-b4 bg-light-gray p-b3 mb-b2 link-light-green')
            total_amount_int = 0

            if total_amount_tag:
                match = re.search(r'支払い金額（税抜）:\s*<span[^>]*>\s*([\d,]+)円', str(total_amount_tag))

                if match:
                    total_amount_str = match.group(1).replace(',', '')
                    if total_amount_str.isnumeric():
                        total_amount_int = int(total_amount_str)
                        st.info(f"✅ スクレイピングによるMK全体分配額の取得に成功しました: **{total_amount_int:,}円**")

            header_data = [{
                'ルームID': 'MKsoul', # ルームIDは固定値
                '分配額': total_amount_int,
                'アカウントID': LOGIN_ID # secretsから取得したログインID
            }]
            header_df = pd.DataFrame(header_data)

            # MKsoulのデータとライバーデータを結合 (この時点では結合は維持)
            if not df_cleaned.empty:
                df_final = pd.concat([header_df, df_cleaned], ignore_index=True)
            else:
                df_final = header_df

        else: # time_charge or premium_live
            df_final = df_cleaned

        # 5. データ種別列を追加
        df_final['データ種別'] = DATA_TYPES[data_type_key]['label']
        df_final['配信月タイムスタンプ'] = timestamp # どの月のデータか識別するためにタイムスタンプを保存

        # ルームIDを結合キーとして文字列に統一
        df_final['ルームID'] = df_final['ルームID'].astype(str)

        return df_final

    except requests.exceptions.HTTPError as e:
        if str(e) == "認証切れの可能性":
             st.error("🚨 認証切れです。Cookieが古いか無効になっています。")
        else:
            st.error(f"HTTPエラーが発生しました: {e}. 認証Cookieが無効になっている可能性があります。")
        return None
    except Exception as e:
        st.error(f"予期せぬエラーが発生しました: {e}")
        logging.error("データ取得・整形エラー", exc_info=True)
        return None


def fetch_and_process_data_for_liver(df_liver_row, required_months_ym, auth_cookie_string):
    """
    単一ライバーの繰越分を含む全売上データ (月ごと、種別ごと) を取得し、統合する。
    ※ここでは月ごとの非合算データを取得・結合するのみで、計算はmain()で行う。
    """
    room_id = df_liver_row['ルームID'].iloc[0]
    file_name = df_liver_row['ファイル名'].iloc[0]

    st.subheader(f"🔄 ライバー: {room_id} ({file_name}) の売上データ取得")
    all_data = []

    for ym_str in required_months_ym:
        timestamp = get_timestamp_from_ym(ym_str)
        if timestamp is None:
            st.error(f"🚨 日付変換エラー: {ym_str} は無効な形式です。スキップします。")
            continue

        st.info(f"   ▶️ 配信月 **{ym_str}** (Timestamp: {timestamp}) のデータを取得中...")

        # 各データ種別について取得
        for data_type_key in DATA_TYPES.keys():
            df_monthly = fetch_and_process_data(timestamp, auth_cookie_string, DATA_TYPES[data_type_key]['url'], data_type_key)

            if df_monthly is not None and not df_monthly.empty:
                # 取得したデータから、対象ライバー（とMKsoul）の行のみを抽出
                # ※MKsoulはroom_salesのみに存在し、レート判定に必要
                df_filtered = df_monthly[df_monthly['ルームID'].isin([room_id, 'MKsoul'])].copy()
                if not df_filtered.empty:
                    df_filtered['配信月'] = ym_str
                    df_filtered['処理キー'] = f"{room_id}-{data_type_key}-{ym_str}" # 結合後の特定キー (ユニーク化)
                    all_data.append(df_filtered)

    if all_data:
        # 非合算の全レコードを結合
        df_combined = pd.concat(all_data, ignore_index=True)
        return df_combined
    else:
        st.warning(f"   データ取得失敗: {room_id} の {required_months_ym} の売上データが見つかりませんでした。")
        return pd.DataFrame()


# --- Streamlit UI (ロジックを大幅に変更) ---

def main():
    # 既存の main() の設定と初期化 (省略せず保持)

    st.set_page_config(page_title="SHOWROOM 支払明細書作成補助ツール", layout="wide")
    st.markdown(
        "<h1 style='font-size:28px; text-align:left; color:#1f2937;'>SHOWROOM 支払明細書作成補助ツール (非合算レコード出力版)</h1>",
        unsafe_allow_html=True
    )
    st.markdown("<p style='text-align: left; color:red;'>🚨 <b>重要: このバージョンは、売上データを月ごと/種別ごとの非合算レコードとして表示します。最終的な合計額の算出機能は削除されています。</b></p>", unsafe_allow_html=True)
    st.markdown("---")

    # セッションステートの初期化
    if 'df_livers' not in st.session_state:
        st.session_state['df_livers'] = pd.DataFrame()
    if 'df_extracted' not in st.session_state:
        st.session_state['df_extracted'] = pd.DataFrame()
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
    # 選択された月のUNIXタイムスタンプ
    selected_timestamp = selected_data[0]
    # 選択された月の YYYYMM 形式
    selected_ym_raw = selected_data[1]

    if selected_timestamp is None:
        st.warning("有効な月が選択されていません。")
        return

    # 選択された配信月をセッションステートに保存
    st.session_state['selected_month_label'] = selected_label

    # YYYY/MM 形式に変換 (繰越ロジックで使用)
    selected_ym_str = f"{selected_ym_raw[:4]}/{selected_ym_raw[4:]}"
    st.info(f"選択された月: **{selected_label}** (繰越判定用: {selected_ym_str})")

    # 2. 実行ボタン (処理の流れ ②)
    st.markdown("#### 2. データ取得と抽出の実行")

    if st.button("🚀 データの取得・抽出を実行 (繰越対応・非合算出力)", type="primary"):
        st.markdown("---")

        # 処理対象ライバーファイルの読み込み (処理の流れ ③)
        df_livers = load_target_livers(TARGET_LIVER_FILE_URL)
        st.session_state['df_livers'] = df_livers # セッションステートに保存

        if df_livers.empty:
            st.error("処理対象ライバーファイルが読み込めなかったため、処理を中断します。")
            return

        # 認証セッションを作成
        session = create_authenticated_session(AUTH_COOKIE_STRING)
        if not session:
             st.error("認証セッションの構築に失敗しました。処理を中断します。")
             return

        final_extracted_rows = []
        mk_sales_total = 0 # MK全体の合計分配額を追跡
        mk_rank_value = 1 # 初期値

        with st.spinner(f"処理中: {selected_label}の売上データと繰越分をSHOWROOMから取得しています..."):

            # --- ライバーごとの繰越月判定とデータ取得 ---
            for index, liver_row in df_livers.iterrows():
                room_id = liver_row['ルームID']
                file_name = liver_row['ファイル名']
                is_invoice_registered = liver_row['is_invoice_registered']

                # 1. 繰越月判定
                required_months_ym = get_required_fetch_months(file_name, selected_ym_str, session)

                # 2. 複数月売上データの取得・結合 (非合算のまま)
                df_liver_sales = fetch_and_process_data_for_liver(
                    df_livers[df_livers['ルームID'] == room_id], # 単一行のDataFrameを渡す
                    required_months_ym,
                    AUTH_COOKIE_STRING
                )

                if df_liver_sales.empty:
                    # 売上がない場合は0行の明細を追加する必要はない (表示しない)
                    continue

                # 3. MKsoulの全体分配額を取得（最新月データのみを使用、重複排除）とMKランクの確定
                df_mk_latest = df_liver_sales[
                    (df_liver_sales['ルームID'] == 'MKsoul') &
                    (df_liver_sales['配信月'] == selected_ym_str)
                ]

                if not df_mk_latest.empty:
                    # 最新月のMKsoul行から合計を取得
                    current_mk_sales_total = df_mk_latest['分配額'].iloc[0].item()

                    if current_mk_sales_total > 0:
                        mk_sales_total = current_mk_sales_total
                        mk_rank_value = get_mk_rank(mk_sales_total)
                        st.info(f"🔑 MKsoulデータ更新: 最新月の全体分配額 **{mk_sales_total:,}円** (→ **MKランク: {mk_rank_value}**)")

                # 4. ライバー個別の全レコードを月ごと・種別ごとに計算し、追加
                df_liver_only_sales = df_liver_sales[df_liver_sales['ルームID'] == room_id].copy()

                if df_liver_only_sales.empty:
                    st.warning(f"   データ取得失敗: {room_id} の {required_months_ym} の売上データが見つかりませんでした。")
                    continue

                for _, sales_row in df_liver_only_sales.iterrows():
                    data_type_label = sales_row['データ種別']
                    monthly_revenue = sales_row['分配額']
                    monthly_ym_str = sales_row['配信月']

                    individual_rank = '-'
                    payment_estimate = 0
                    rate_label = '-'
                    mk_rank = mk_rank_value # 確定したMKランクを適用

                    if data_type_label == 'ルーム売上':
                        # ルーム売上: 月別分配額に基づいてランクと支払額を計算
                        individual_rank = get_individual_rank(monthly_revenue)
                        rate_label = f"MK{mk_rank}/個{individual_rank}"
                        payment_estimate = calculate_payment_estimate(
                            individual_rank,
                            mk_rank,
                            monthly_revenue,
                            is_invoice_registered
                        )
                    elif data_type_label == 'プレミアムライブ売上':
                        # プレミアムライブ
                        payment_estimate = calculate_paid_live_payment_estimate(
                            monthly_revenue,
                            is_invoice_registered
                        )
                    elif data_type_label == 'タイムチャージ売上':
                        # タイムチャージ
                        payment_estimate = calculate_time_charge_payment_estimate(
                            monthly_revenue,
                            is_invoice_registered
                        )
                    else:
                        continue # その他のデータ種別やMKsoul行はスキップ

                    # 新しいレコードの作成 (非合算の1行)
                    new_row = {
                        'ルームID': room_id,
                        'ファイル名': file_name,
                        'インボイス': liver_row.get('インボイス', np.nan),
                        'is_invoice_registered': is_invoice_registered,
                        'データ種別': data_type_label, # 非合算のラベル
                        '分配額': monthly_revenue,
                        '個別ランク': individual_rank,
                        'MKランク': mk_rank,
                        '適用料率': rate_label,
                        '支払額': payment_estimate,
                        'アカウントID': sales_row['アカウントID'],
                        '配信月': monthly_ym_str, # 個別の配信月
                        '処理キー': f"{room_id}-{data_type_label}-{monthly_ym_str}",
                    }
                    final_extracted_rows.append(pd.Series(new_row))


            # --- 全てのライバーの処理が完了 ---

            if final_extracted_rows:
                df_extracted = pd.DataFrame(final_extracted_rows).reset_index(drop=True)

                # 支払額列の表示形式を調整
                df_extracted['支払額'] = df_extracted['支払額'].replace(['#ERROR_CALC', '#ERROR_MK', '#ERROR_RANK', '#N/A'], np.nan)
                df_extracted['支払額'] = pd.to_numeric(df_extracted['支払額'], errors='coerce').fillna(0).astype('Int64')

                # ソート (配信月の新しい順にソート)
                df_extracted['配信月ソートキー'] = df_extracted['配信月'].str.replace('/', '').astype(int)
                df_extracted = df_extracted.sort_values(
                    by=['ルームID', '配信月ソートキー', 'データ種別'],
                    ascending=[True, False, False]
                ).drop(columns=['配信月ソートキー']).reset_index(drop=True)


                st.session_state['df_extracted'] = df_extracted
                st.balloons()
                st.success("🎉 **繰越処理を含む売上データの取得、計算が完了しました！** (データは非合算で表示されています)")

            else:
                st.warning("処理対象ライバー全員について、売上データが取得できませんでした。")
                st.session_state['df_extracted'] = pd.DataFrame()


    # --- 取得・抽出結果の表示 ---

    if 'df_livers' in st.session_state and not st.session_state.df_livers.empty:
        st.markdown("## 3. 抽出結果の確認、ランク・支払額の付与")
        st.markdown("---")

        # 処理対象ライバー一覧の表示 (省略せず保持)
        df_livers = st.session_state.df_livers
        st.subheader("処理対象ライバー一覧")
        expected_cols = ['ルームID', 'ファイル名', 'インボイス', 'is_invoice_registered']
        display_cols = [col for col in expected_cols if col in df_livers.columns]
        st.dataframe(df_livers[display_cols], height=150)

        # 最終結果の表示
        if not st.session_state.df_extracted.empty:
            df_extracted = st.session_state.df_extracted

            st.subheader("✅ 抽出された最終データ (配信月・データ種別ごとの非合算レコード)")
            st.info(f"このデータは、各ライバーについて**配信月ごと、データ種別ごと**のレコードを**非合算**で示しています。")

            # 表示列の整理
            final_display_cols = ['ルームID', 'ファイル名', 'インボイス', 'データ種別', '配信月', '分配額', '個別ランク', 'MKランク', '適用料率', '支払額']
            df_display = df_extracted[[col for col in final_display_cols if col in df_extracted.columns]].copy()

            # データ種別と配信月を結合した「明細」列を作成して、ご要望の表示形式に近づけます
            df_display['明細'] = df_display['配信月'].str.replace('/', '月').str.replace('月$', '月配信分') + 'の' + df_display['データ種別']
            df_display = df_display[['ルームID', '明細', '分配額', '個別ランク', 'MKランク', '適用料率', '支払額', 'インボイス', 'ファイル名']].copy()

            # 整形後の表示
            st.dataframe(df_display, use_container_width=True)

            # --- 合計の表示ブロックは削除されました ---

        else:
            st.info("実行ボタンを押して、繰越処理を含む売上データの取得を行ってください。")

# --- ユーティリティ関数（ランク判定、MKランク、セッション作成、月生成）は変更なし ---

# get_target_months (省略)
def get_target_months():
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
            months.append((month_str, timestamp, ym_str))
        except Exception as e:
            logging.error(f"日付計算エラー ({month_str}): {e}")
        if current_month == 1:
            current_month = 12
            current_year -= 1
        else:
            current_month -= 1
    return months

# create_authenticated_session (省略)
def create_authenticated_session(cookie_string):
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

# get_individual_rank (省略)
def get_individual_rank(sales_amount):
    if pd.isna(sales_amount) or sales_amount is None:
        return "#N/A"
    amount = float(sales_amount)
    if amount < 0: return "E"
    if amount >= 900001: return "SSS"
    elif amount >= 450001: return "SS"
    elif amount >= 270001: return "S"
    elif amount >= 135001: return "A"
    elif amount >= 90001: return "B"
    elif amount >= 45001: return "C"
    elif amount >= 22501: return "D"
    elif amount >= 0: return "E"
    else: return "E"

# get_mk_rank (省略)
def get_mk_rank(revenue):
    if revenue <= 175000: return 1
    elif revenue <= 350000: return 2
    elif revenue <= 525000: return 3
    elif revenue <= 700000: return 4
    elif revenue <= 875000: return 5
    elif revenue <= 1050000: return 6
    elif revenue <= 1225000: return 7
    elif revenue <= 1400000: return 8
    elif revenue <= 1575000: return 9
    elif revenue <= 1750000: return 10
    else: return 11

if __name__ == "__main__":
    main()