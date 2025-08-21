import streamlit as st
import pandas as pd
import io

# --- Streamlitページの初期設定 ---
st.set_page_config(page_title="Excelデータ整理ツール", layout="wide")


# --- メインの処理関数 ---
def consolidate_excel_sheets(excel_file):
    """
    アップロードされたExcelファイルの「シート1」内にある複数のテーブルを読み込み、
    A列を基準にデータを結合して一つの時系列の表を作成する。
    テーブル間は空行で区切られていることを想定。
    和集合を取り、欠損値は0で補完する。
    """
    try:
        # Excelファイルを開き、'シート1'が存在するか確認
        xls = pd.ExcelFile(excel_file)
        sheet_name_to_read = 'シート1' if 'シート1' in xls.sheet_names else xls.sheet_names[0]
        df_full = pd.read_excel(xls, sheet_name=sheet_name_to_read, header=None)
        st.info(f"シート「{sheet_name_to_read}」を処理しています...")
    except Exception as e:
        st.error(f"Excelファイルの読み込みに失敗しました: {e}")
        return None

    # 空行のインデックスを見つける (全てのセルが空の行)
    blank_row_indices = df_full[df_full.isnull().all(axis=1)].index.tolist()

    # 空行を区切りとして、各テーブルの範囲(開始行, 終了行)を特定する
    table_boundaries = []
    last_index = -1
    for blank_index in blank_row_indices:
        if blank_index > last_index + 1:
            table_boundaries.append((last_index + 1, blank_index))
        last_index = blank_index
    table_boundaries.append((last_index + 1, len(df_full)))

    data_frames_to_merge = []

    # 特定した範囲ごとにテーブルを処理
    for start, end in table_boundaries:
        sub_df = df_full.iloc[start:end].copy()
        sub_df.dropna(how='all', inplace=True)
        sub_df.dropna(how='all', axis=1, inplace=True)
        if sub_df.empty:
            continue

        # 1行目をヘッダーとして設定
        header = sub_df.iloc[0]
        sub_df = sub_df[1:]
        sub_df.columns = header
        
        if sub_df.columns.isnull().any():
            st.warning("ヘッダー行に空のセルが含まれているテーブルをスキップしました。")
            continue

        # 1列目を「共通項目」として統一
        item_column_name = sub_df.columns[0]
        sub_df.rename(columns={item_column_name: '共通項目'}, inplace=True)

        sub_df.dropna(subset=['共通項目'], inplace=True)
        if sub_df.empty:
            continue

        sub_df.drop_duplicates(subset=['共通項目'], keep='first', inplace=True)

        sub_df.set_index('共通項目', inplace=True)
        
        data_frames_to_merge.append(sub_df)

    if not data_frames_to_merge:
        st.error(f"処理できるデータテーブルが見つかりませんでした。シート「{sheet_name_to_read}」の形式を確認してください。")
        return None

    try:
        # 和集合を基準に全テーブルを外部結合
        consolidated_df = pd.concat(data_frames_to_merge, axis=1, join='outer')
        # 欠損値を0で補完
        consolidated_df.fillna(0, inplace=True)
    except Exception as e:
        st.error(f"データの結合中にエラーが発生しました: {e}")
        return None

    consolidated_df.reset_index(inplace=True)
    
    return consolidated_df


# --- StreamlitのUI部分 ---
st.title("📊 Excelシート内 データ整理ツール")
st.write("Excelファイルの「シート1」内にある複数の表を、共通項目（A列）の**和集合**を基準に一つの表にまとめます。")
st.write("各表は**空行で区切って**ください。存在しないデータは 0 で補完します。")

# --- ファイルアップロード ---
uploaded_file = st.file_uploader(
    "処理したいExcelファイル（.xlsx）をアップロードしてください",
    type="xlsx"
)

# --- 実行と結果表示 ---
if uploaded_file:
    st.info(f"ファイル名: `{uploaded_file.name}`")

    if st.button("整理開始 ▶️", type="primary"):
        with st.spinner("データを整理中..."):
            df_result = consolidate_excel_sheets(uploaded_file)

        if df_result is not None and not df_result.empty:
            st.success("✅ データの整理が完了しました！")
            
            st.subheader("整理後のデータプレビュー")
            st.dataframe(df_result)

            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                df_result.to_excel(writer, index=False, sheet_name='シート2')
            output.seek(0)

            st.download_button(
                label="📥 整理後のExcelファイルをダウンロード",
                data=output,
                file_name=f"整理済み_{uploaded_file.name}",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        else:
            st.error("データの整理に失敗しました。ファイルの形式を確認してください。")
else:
    st.warning("☝️ 上のボタンからExcelファイルをアップロードしてください。")
