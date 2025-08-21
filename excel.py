import streamlit as st
import pandas as pd
import io
import re

# --- Streamlitページの初期設定 ---
st.set_page_config(page_title="損益計算書データ整理ツール", layout="wide")

def consolidate_pl_sheets(excel_file):
    """
    アップロードされたExcelファイルを読み込み、
    「年度ごとの損益計算書」を共通項目（A列）を基準に横方向に結合する。
    欠損は0で埋める。
    """
    try:
        xls = pd.ExcelFile(excel_file)
        sheet_name_to_read = xls.sheet_names[0]
        df_full = pd.read_excel(xls, sheet_name=sheet_name_to_read, header=None)
        st.info(f"シート「{sheet_name_to_read}」を処理しています...")
    except Exception as e:
        st.error(f"Excelファイルの読み込みに失敗しました: {e}")
        return None

    # 年度を示すセルを探す（例: 2022, 2023 ...）
    year_pattern = re.compile(r"^20\d{2}$")
    year_rows = df_full[df_full[1].apply(lambda x: bool(year_pattern.match(str(x))) if pd.notna(x) else False)].index.tolist()

    if not year_rows:
        st.error("年度を示すセル（例: 2022）が見つかりませんでした。")
        return None

    data_frames = []

    # 各年度ごとに処理
    for year_row in year_rows:
        year = int(df_full.loc[year_row, 1])
        sub_df = df_full.iloc[year_row+1:, :2].copy()

        # 項目と金額
        sub_df.columns = ["共通項目", year]
        sub_df.dropna(subset=["共通項目"], inplace=True)
        sub_df = sub_df[sub_df["共通項目"].astype(str).str.strip() != ""]

        # 金額を数値化
        sub_df[year] = sub_df[year].replace({",": ""}, regex=True)
        sub_df[year] = pd.to_numeric(sub_df[year], errors="coerce").fillna(0)

        # 重複削除
        sub_df.drop_duplicates(subset=["共通項目"], keep="first", inplace=True)

        sub_df.set_index("共通項目", inplace=True)
        data_frames.append(sub_df)

    # 横方向に和集合で結合
    consolidated_df = pd.concat(data_frames, axis=1, join="outer").fillna(0).astype(int)
    consolidated_df.reset_index(inplace=True)

    return consolidated_df


# --- StreamlitのUI部分 ---
st.title("📊 損益計算書 データ整理ツール")
st.write("Excelファイルの中から年度ごとの表を抽出し、共通項目を基準に横方向に統合します。")
st.write("欠損データは 0 で補完されます。")

# --- ファイルアップロード ---
uploaded_file = st.file_uploader("処理したいExcelファイル（.xlsx）をアップロードしてください", type="xlsx")

# --- 実行と結果表示 ---
if uploaded_file:
    st.info(f"ファイル名: `{uploaded_file.name}`")

    if st.button("整理開始 ▶️", type="primary"):
        with st.spinner("データを整理中..."):
            df_result = consolidate_pl_sheets(uploaded_file)

        if df_result is not None and not df_result.empty:
            st.success("✅ データの整理が完了しました！")
            
            st.subheader("整理後のデータプレビュー")
            st.dataframe(df_result)

            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                df_result.to_excel(writer, index=False, sheet_name='整理結果')
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
