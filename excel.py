#!/usr/bin/env python
# -*- coding: utf-8 -*-

import streamlit as st
import pandas as pd
import io
import re
from functools import reduce
from collections import defaultdict

# --- Streamlitページの初期設定 ---
st.set_page_config(page_title="損益計算書データ整理・分析ツール", layout="wide")


def create_df_from_sub_chunk(df_sub_chunk):
    """
    年度データを含むブロックを受け取り、項目、年度、金額の
    「ロングフォーマット」DataFrameを作成する。「その他」には一時的なIDを付与。
    """
    if df_sub_chunk.empty:
        return None

    year_pat = re.compile(r"^\s*20\d{2}\s*$")
    year_cells = []
    for r in range(df_sub_chunk.shape[0]):
        for c in range(df_sub_chunk.shape[1]):
            cell_value = df_sub_chunk.iat[r, c]
            if pd.notna(cell_value) and bool(year_pat.match(str(cell_value))):
                year_cells.append({"row": r, "col": c, "year": int(str(cell_value).strip())})

    if not year_cells:
        return None

    all_rows = []
    for cell in year_cells:
        year = cell['year']
        val_col = cell['col']
        start_row = cell['row'] + 1
        
        if 0 not in df_sub_chunk.columns or val_col not in df_sub_chunk.columns:
            continue
        
        sub = df_sub_chunk.iloc[start_row:, [0, val_col]].copy()
        sub.columns = ["共通項目", "金額"]
        sub['年度'] = year
        all_rows.append(sub)

    if not all_rows:
        return None

    long_df = pd.concat(all_rows, ignore_index=True)

    long_df["共通項目"] = long_df["共通項目"].astype(str).str.strip()
    long_df.dropna(subset=["共通項目"], inplace=True)
    long_df = long_df[long_df["共通項目"] != ""].copy()
    
    long_df["金額"] = long_df["金額"].replace({",": ""}, regex=True)
    long_df["金額"] = pd.to_numeric(long_df["金額"], errors="coerce").fillna(0)
    
    # 「その他」に一時的なユニークIDを付与して、個別の項目として保持
    is_sonota = long_df['共通項目'] == 'その他'
    if is_sonota.any():
        long_df.loc[is_sonota, '共通項目'] = (
            long_df.loc[is_sonota, '共通項目'] + 
            '_temp_' + 
            long_df.groupby('共通項目').cumcount().astype(str)
        )

    # このチャンク内で項目と年度が重複するものは集計しておく
    long_df = long_df.groupby(['共通項目', '年度']).sum().reset_index()

    return long_df


def calculate_yoy(df):
    """前年比と増減額を計算する"""
    df_for_yoy = df.groupby("共通項目").sum().reset_index()
    df_yoy = df_for_yoy.set_index("共通項目")
    
    df_diff = df_yoy.diff(axis=1)
    df_diff.columns = [f"{col} 増減額" for col in df_diff.columns]
    
    df_pct = df_yoy.pct_change(axis=1) * 100
    df_pct.columns = [f"{col} 増減率(%)" for col in df_pct.columns]

    df_merged = pd.concat([df_yoy, df_diff, df_pct], axis=1)
    
    sorted_cols = []
    year_cols = sorted([col for col in df_yoy.columns if isinstance(col, int)])
    for year in year_cols:
        sorted_cols.append(year)
        if f"{year} 増減額" in df_merged.columns:
            sorted_cols.append(f"{year} 増減額")
        if f"{year} 増減率(%)" in df_merged.columns:
            sorted_cols.append(f"{year} 増減率(%)")
            
    df_merged = df_merged[sorted_cols].reset_index()
    return df_merged


def process_files_and_tables(excel_file):
    """
    ファイルを「ファイル名」で分割し、各ファイル内を「--- ページ」で分割。
    同じ順番の表（1番目、2番目...）をすべて集めて統合し、最終的なまとめ表のリストを作成する。
    """
    try:
        xls = pd.ExcelFile(excel_file)
        sheet_name_to_read = "抽出結果" if "抽出結果" in xls.sheet_names else xls.sheet_names[0]
        df_full = pd.read_excel(xls, sheet_name=sheet_name_to_read, header=None)
        st.info(f"シート「{sheet_name_to_read}」を処理しています...")
    except Exception as e:
        st.error(f"Excelファイルの読み込みに失敗しました: {e}")
        return None

    df_full[0] = df_full[0].astype(str)
    
    # --- 1. 「ファイル名:」で全体を分割 ---
    file_indices = df_full[df_full[0].str.contains(r'ファイル名:', na=False)].index.tolist()
    file_chunks = []
    if not file_indices:
        file_chunks.append(df_full)
    else:
        for i in range(len(file_indices)):
            start_idx = file_indices[i]
            end_idx = file_indices[i+1] if i + 1 < len(file_indices) else len(df_full)
            file_chunks.append(df_full.iloc[start_idx:end_idx].reset_index(drop=True))

    # --- 2. 各ファイル内を「--- ページ」で分割し、表ごとにグループ化 ---
    grouped_tables = defaultdict(list)

    for file_chunk in file_chunks:
        page_indices = file_chunk[file_chunk[0].str.contains(r'--- ページ', na=False)].index.tolist()
        
        table_chunks = []
        last_idx = 0
        for idx in page_indices:
            chunk = file_chunk.iloc[last_idx:idx]
            if not chunk.empty:
                table_chunks.append(chunk)
            last_idx = idx
        final_chunk = file_chunk.iloc[last_idx:]
        if not final_chunk.empty:
            table_chunks.append(final_chunk)

        for i, table_chunk in enumerate(table_chunks):
            processed_df = create_df_from_sub_chunk(table_chunk.reset_index(drop=True))
            if processed_df is not None and not processed_df.empty:
                grouped_tables[i].append(processed_df)

    # --- 3. グループ化された表をそれぞれ統合 ---
    final_summaries = []
    for table_index in sorted(grouped_tables.keys()):
        list_of_long_dfs = grouped_tables[table_index]
        if not list_of_long_dfs: continue

        # 3-1. 同じ順番の表（ロングフォーマット）をすべて縦に結合
        combined_long_df = pd.concat(list_of_long_dfs, ignore_index=True)

        # 3-2. pivot_tableで集計し、ワイドフォーマットに変換
        # aggfunc='sum'により、異なるファイルの同じ項目・年度の数値が合計される
        summary_table = pd.pivot_table(
            combined_long_df,
            values='金額',
            index='共通項目',
            columns='年度',
            aggfunc='sum',
            fill_value=0
        )

        # 3-3. 最終的な整形
        summary_table.reset_index(inplace=True)
        
        # 「その他」の一時的なIDを削除
        summary_table['共通項目'] = summary_table['共通項目'].str.replace(r'_temp_\d+$', '', regex=True)
        
        # 年度列を昇順にソート
        year_cols = sorted([col for col in summary_table.columns if isinstance(col, (int, float))])
        final_cols = ['共通項目'] + year_cols
        summary_table = summary_table[final_cols]

        for col in year_cols:
            summary_table[col] = summary_table[col].astype(int)
        
        final_summaries.append(summary_table)
            
    return final_summaries


# --- StreamlitのUI部分 ---
st.title("📊 損益計算書 統合データ作成ツール（ファイル・ページ別）")
st.write("""
`ファイル名:` で区切られた各データ内にある、同じ順番の表（`--- ページ`区切り）をそれぞれ集計し、統合した「まとめ表」を作成します。
""")

uploaded_file = st.file_uploader("処理したいExcelファイル（.xlsx）をアップロードしてください", type=["xlsx"])

if uploaded_file:
    st.info(f"ファイル名: `{uploaded_file.name}`")

    if st.button("統合まとめ表を作成 ▶️", type="primary"):
        with st.spinner("データを整理・分析中..."):
            all_summaries = process_files_and_tables(uploaded_file)

        if all_summaries:
            st.success(f"✅ {len(all_summaries)}個の統合まとめ表が作成されました！")

            output_excel = io.BytesIO()
            with pd.ExcelWriter(output_excel, engine="xlsxwriter") as writer:
                for i, summary_df in enumerate(all_summaries):
                    summary_df.to_excel(writer, sheet_name=f"統合まとめ表_{i+1}", index=False)
            
            st.download_button(
                label="📥 全ての統合まとめ表をExcelで一括ダウンロード",
                data=output_excel.getvalue(),
                file_name=f"統合まとめ表_{uploaded_file.name}",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
            st.divider()

            for i, summary_df in enumerate(all_summaries):
                with st.expander(f"▼ **統合まとめ表 {i+1}** の分析結果を見る"):
                    tab1, tab2, tab3 = st.tabs(["整理後データ", "📈 推移グラフ", "🆚 前年比・増減"])
                    
                    with tab1:
                        st.dataframe(summary_df)
                    
                    with tab2:
                        st.subheader("主要項目の年度推移グラフ")
                        items = summary_df["共通項目"].unique()
                        default_items = [item for item in ["売上高", "営業利益", "経常利益", "当期純利益"] if item in items]
                        selected_items = st.multiselect(
                            "グラフに表示する項目を選択", options=items, default=default_items, key=f"chart_{i}"
                        )
                        if selected_items:
                            df_chart = summary_df[summary_df["共通項目"].isin(selected_items)].groupby("共通項目").sum().T
                            df_chart.index.name = "年度"
                            st.line_chart(df_chart)
                    
                    with tab3:
                        st.subheader("前年比・増減額")
                        df_yoy_result = calculate_yoy(summary_df)
                        st.dataframe(df_yoy_result.style.format(precision=2, na_rep='-'))

        else:
            st.error("有効なデータを抽出できませんでした。ファイルの形式をご確認ください。")
else:
    st.warning("☝️ 上のボタンからExcelファイルをアップロードしてください。")
