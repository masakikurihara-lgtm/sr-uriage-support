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


# --- ユーティリティ関数 ---

@st.cache_data
def load_target_livers(url):
    """処理対象ライバーファイルを読み込み、DataFrameとして返す"""
    st.info(f"処理対象ライバーファイルを読み込み中... URL: {url}")
    try:
        # 1. UTF-8 with BOM (utf_8_sig) を最初に試行 (最も一般的なWeb上のCSV形式)
        #    これにより、BOM付きUTF-8による 0xef のエラーを回避できます。
        df_livers = pd.read_csv(url, encoding='utf_8_sig')
        st.success(f"処理対象ライバーデータ ({len(df_livers)}件) の読み込みが完了しました。(エンコーディング: UTF-8 BOM)")
        
    except Exception as e_utf8:
        # 2. UTF-8 (BOMなし) を試行
        try:
            df_livers = pd.read_csv(url, encoding='utf-8')
            st.success(f"処理対象ライバーデータ ({len(df_livers)}件) の読み込みが完了しました。(エンコーディング: UTF-8)")
        
        # 3. 最後に Shift-JIS を試行 (従来の日本のCSV形式)
        except Exception as e_shiftjis:
            try:
                df_livers = pd.read_csv(url, encoding='shift_jis')
                st.success(f"処理対象ライバーデータ ({len(df_livers)}件) の読み込みが完了しました。(エンコーディング: Shift-JIS)")
            
            except Exception as e_final:
                # すべて失敗した場合
                st.error(f"🚨 処理対象ライバーファイルの読み込みに失敗しました。エンコーディングエラー: {e_final}")
                return pd.DataFrame()

    # ヘッダーを確認し、必要に応じて整形 (読み込み成功後の共通処理)
    df_livers = df_livers.rename(columns={
        'ルームID': 'ルームID', 
        'ファイル名': 'ファイル名', 
        'インボイス': 'インボイス'
    })
    # ルームIDを文字列として扱い、結合キーとする
    df_livers['ルームID'] = df_livers['ルームID'].astype(str)
    
    # 処理対象ライバーファイルの読み込みが成功した場合はここでDataFrameを返す
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
            # データがない場合は空のDataFrameを返す (後続処理でエラーにならないように)
            return pd.DataFrame(columns=['ルームID', '分配額', 'アカウントID', 'データ種別']) 
            
        # 3. データをBeautifulSoupで抽出 (ライバー個別のデータ)
        table_data = []
        rows = table.find_all('tr')
        
        # ヘッダー行をスキップし、データ行のみを処理
        for row in rows[1:]: 
            td_tags = row.find_all('td')
            
            # HTML構造: [0: ルームID, 1: ルームURL, 2: ルーム名, 3: 分配額, 4: アカウントID]
            if len(td_tags) >= 5:
                # ルームID, 分配額, アカウントIDを抽出
                room_id_str = td_tags[0].text.strip() # 1列目のルームID (文字列)
                amount_str = td_tags[3].text.strip().replace(',', '') # 4列目の分配額 (カンマ除去)
                account_id = td_tags[4].text.strip()
                
                # 分配額が数値であることを確認（合計行などを除外）
                if amount_str.isnumeric():
                    table_data.append({
                        'ルームID': room_id_str, # ルームIDを追加
                        '分配額': int(amount_str), # int型に変換
                        'アカウントID': account_id
                    })
        
        # 4. DataFrameに変換
        df_cleaned = pd.DataFrame(table_data)
        
        # ルーム売上 (room_sales) の特殊処理: MKsoulの合計行を追加
        if data_type_key == "room_sales":
            
            total_amount_tag = soup.find('p', class_='fs-b4 bg-light-gray p-b3 mb-b2 link-light-green')
            total_amount_int = 0
            if total_amount_tag:
                match = re.search(r'支払い金額（税抜）:\s*<span[^>]*>\s*([\d,]+)円', str(total_amount_tag))
                if match:
                    total_amount_str = match.group(1).replace(',', '') 
                    if total_amount_str.isnumeric():
                        total_amount_int = int(total_amount_str)

            header_data = [{
                'ルームID': 'MKsoul', # ルームIDは固定値
                '分配額': total_amount_int,
                'アカウントID': LOGIN_ID # secretsから取得したログインID
            }]
            header_df = pd.DataFrame(header_data)
            
            if not df_cleaned.empty:
                 # ライバーデータが存在する場合、header_dfの後ろに連結
                df_final = pd.concat([header_df, df_cleaned], ignore_index=True)
                st.success(f"**{DATA_TYPES[data_type_key]['label']}**: ライバー個別データ ({len(df_cleaned)}件) と合計値 ({total_amount_int}) の抽出が完了しました。")
            else:
                 # ライバーデータが存在しない場合、header_df（1行）のみ
                df_final = header_df
                st.warning(f"**{DATA_TYPES[data_type_key]['label']}**: ライバー個別のデータ行を抽出できませんでした。合計値 ({total_amount_int}) のみを含む1行データとして処理を続行します。")

        else: # time_charge or premium_live
            if df_cleaned.empty:
                st.warning(f"**{DATA_TYPES[data_type_key]['label']}**: 有効なデータ行を抽出できませんでした。")
                # ゼロ件データ用のDataFrame
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
        #st.dataframe(df_sales) # デバッグ用
    else:
        st.session_state[f'df_{data_type_key}'] = pd.DataFrame(columns=['ルームID', '分配額', 'アカウントID', 'データ種別'])
    
    st.markdown("---")

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

        st.markdown("## 3. 抽出結果の確認 (処理の流れ ④の結果)")
        st.markdown("---")

        if 'df_livers' in st.session_state and not st.session_state.df_livers.empty:
            df_livers = st.session_state.df_livers
            st.subheader("処理対象ライバー一覧")
            st.dataframe(df_livers, height=150)
            
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
                
                # ルームIDをキーに処理対象ライバーと結合 (処理の流れ ④)
                # how='left'で、すべてのライバー情報（ルームID）を保持し、該当する売上データを付加
                df_merged = pd.merge(
                    df_livers,
                    all_sales_data,
                    on='ルームID',
                    how='left'
                )

                # 🌟 新しい列の追加 🌟

                # 1. 配信月
                # 選択された月ラベルを新しい列として追加
                df_merged['配信月'] = st.session_state.selected_month_label
                
                # 2. アカウントID
                # ルーム売上 (room_sales) 以外はアカウントIDがNaNになるため、
                # ログイン時のアカウントID (LOGIN_ID) を埋める（後続の処理で利用）
                df_merged['アカウントID'] = df_merged['アカウントID'].fillna(st.session_state.login_account_id)


                # 売上データがないライバー（NULL行）の分配額を0として処理
                df_merged['分配額'] = df_merged['分配額'].fillna(0).astype(int)
                
                # 表示用に、売上がゼロの行のデータ種別をNaNから「売上なし」などに変換
                df_merged['データ種別'] = df_merged['データ種別'].fillna('売上データなし')
                
                # 不要な列を整理し、抽出が完了したDataFrameを表示 (アカウントID, 配信月を追加)
                df_extracted = df_merged[['ルームID', 'ファイル名', 'インボイス', 'データ種別', '分配額', 'アカウントID', '配信月']]
                
                

                st.subheader("✅ 抽出・結合された最終データ (支払明細書のもと)")
                st.info(f"このデータに、後のステップで報酬率などの計算ロジックを適用します。合計 {len(df_livers)}件のライバー情報に対して、{len(df_extracted)}件の売上明細行が紐付けられました。")
                st.dataframe(df_extracted)
                
                # 計算ステップのためにセッションステートに保持
                st.session_state['df_extracted'] = df_extracted
            
            else:
                st.warning("結合対象の売上データがありません。")
        else:
            st.info("実行ボタンを押して、処理対象ライバーファイルの読み込みと売上データの取得を行ってください。")

if __name__ == "__main__":
    main()