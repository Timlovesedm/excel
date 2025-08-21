import streamlit as st
import pandas as pd

# --- アプリの設定 ---
st.set_page_config(layout="wide", page_title="損益計算書ビューア")

st.title("損益計算書データ表示アプリ")

# --- ファイルアップロード ---
uploaded_file = st.file_uploader("損益計算書のExcelファイルをアップロードしてください", type=["xlsx"])

if uploaded_file is not None:
    try:
        # "抽出結果"シートを読み込み
        df = pd.read_excel(uploaded_file, sheet_name="抽出結果")

        st.subheader("読み込んだ生データ")
        st.dataframe(df)

        # 年号らしき列を判定
        year_cols = [col for col in df.columns if str(col).isdigit()]

        if len(year_cols) == 0:
            st.error("年号の列が見つかりませんでした。シートの形式を確認してください。")
        else:
            # 年号列を数値に変換してソート
            sorted_years = sorted([int(col) for col in year_cols])
            sorted_years = [str(y) for y in sorted_years]

            # 年度列を昇順に並べ替え
            other_cols = [col for col in df.columns if col not in year_cols]
            df_sorted = df[other_cols + sorted_years]

            st.subheader("年度を昇順に並べ替えたデータ")
            st.dataframe(df_sorted)

    except Exception as e:
        st.error(f"データの処理に失敗しました: {e}")
else:
    st.info("エクセルファイルをアップロードしてください。")
