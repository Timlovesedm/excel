#!/usr/bin/env python
# -*- coding: utf-8 -*-

import streamlit as st
import pandas as pd
import io
import re
from functools import reduce

# --- Streamlitページの初期設定 ---
st.set_page_config(page_title="損益計算書データ整理・分析ツール", layout="wide")


def create_summary_from_chunk(df_chunk):
    """
    単一のデータブロック（DataFrame）を受け取り、内部の全年度データを統合したまとめ表を作成する。
    """
    if df_chunk.empty:
        return None

    # --- 1. ブロック内の年度ヘッダーをすべて検索 ---
    year_pat = re.compile(r"^\s*20\d{2}\s*$")
    year_cells = []
    for r in range(df_chunk.shape[0]):
        for c in range(df_chunk.shape[1]):
            cell_value = df_chunk.iat[r, c]
            if pd.notna(cell_value) and bool(year_pat.match(str(cell_value))):
                year_cells.append({"row": r, "col": c, "year": int(str(cell_value).strip())})

    if not year_cells:
        return None

    year_cells.sort(key=lambda x: x['row'])
    
    unique_year_cells = []
    processed_years = set()
    for cell in year_cells:
        if cell['year'] not in processed_years:
            unique_year_cells.append(cell)
            processed_years.add(cell['year'])

    block_definitions = []
    for i, cell in enumerate(unique_year_cells):
        start_data_row = cell['row'] + 1
        # 終了行は、次の年度セルの直前か、ブロックの最後まで
        end_data_row = unique_year_cells[i+1]['row'] if i + 1 < len(unique_year_cells) else len(df_chunk)
        block_definitions.append({
            "year": cell['year'], "col": cell['col'],
            "start": start_data_row, "end": end_data_row
        })

    # --- 2. 年度ごとのデータを抽出し、DataFrameのリストを作成 ---
    dfs = []
    for block in block_definitions:
        year = block['year']
        val_col = block['col']
        
        if 0 not in df_chunk.columns or val_col not in df_chunk.columns:
            continue
        
        sub = df_chunk.iloc[block['start']:block['end'], [0, val_col]].copy()
        sub.columns = ["共通項目", year]

        sub["共通項目"] = sub["共通項目"].astype(str).str.strip()
        sub.dropna(subset=["共通項目"], inplace=True)
        sub = sub[sub["共通項目"] != ""]
        if sub.empty: continue

        # 「その他」は重複を許容し、他は削除
        sonota_df = sub[sub["共通項目"] == "その他"].copy()
        other_items_df = sub[sub["共通項目"] != "その他"].copy()
        other_items_df.drop_duplicates(subset=["共通項目"], keep="first", inplace=True)
        sub = pd.concat([sonota_df, other_items_df]).sort_index()

        sub[year] = sub[year].replace({",": ""}, regex=True)
        sub[year] = pd.to_numeric(sub[year], errors="coerce").fillna(0)
        
        sub.set_index("共通項目", inplace=True)
        dfs.append(sub)

    if not dfs:
        return None

    # --- 3. 全年度データを結合して整形 ---
    consolidated = pd.concat(dfs, axis=1, join="outer").fillna(0)
    
    # 年度列を昇順にソート
    year_cols = sorted([col for col in consolidated.columns if isinstance(col, (int, float))])
    consolidated = consolidated[year_cols]

    # 金額を整数に変換
    for col in year_cols:
        consolidated[col] = consolidated[col].astype(int)

    consolidated.reset_index(inplace=True)
    return consolidated


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


def process_file_by_page_delimiter(excel_file):
    """
    アップロードされたExcelファイルを「--- ページ」で分割し、ブロックごとにまとめ表を作成する。
    """
    try:
        xls = pd.ExcelFile(excel_file)
        sheet_name_to_read = "抽出結果" if "抽出結果" in xls.sheet_names else xls.sheet_names[0]
        df_full = pd.read_excel(xls, sheet_name=sheet_name_to_read, header=None)
        st.info(f"シート「{sheet_name_to_read}」を処理しています...")
    except Exception as e:
        st.error(f"Excelファイルの読み込みに失敗しました: {e}")
        return None

    # --- 1. 「--- ページ」を基準にファイルをブロックに分割 ---
    df_full[0] = df_full[0].astype(str)
    # 区切り文字を含む行のインデックスを取得
    split_indices = df_full[df_full[0].str.contains(r'--- ページ', na=False)].index.tolist()

    data_chunks = []
    last_idx = 0
    for idx in split_indices:
        chunk = df_full.iloc[last_idx:idx]
        if not chunk.empty:
            data_chunks.append(chunk.reset_index(drop=True))
        last_idx = idx
    # 最後の区切り文字以降のデータを追加
    final_chunk = df_full.iloc[last_idx:]
    if not final_chunk.empty:
        data_chunks.append(final_chunk.reset_index(drop=True))

    if not data_chunks:
        # 区切りが見つからない場合は全体を1ブロックとして扱う
        data_chunks.append(df_full)

    # --- 2. 各ブロックを処理して、まとめ表のリストを作成 ---
    summary_tables = []
    for chunk in data_chunks:
        summary_table = create_summary_from_chunk(chunk)
        if summary_table is not None and not summary_table.empty:
            summary_tables.append(summary_table)
            
    return summary_tables


# --- StreamlitのUI部分 ---
st.title("📊 損益計算書 データ整理ツール（ページ区切り対応）")
st.write("""
`--- ページ` という記載を区切りとして、各ブロックのデータを統合した「まとめ表」をそれぞれ作成します。
""")

uploaded_file = st.file_uploader("処理したいExcelファイル（.xlsx）をアップロードしてください", type=["xlsx"])

if uploaded_file:
    st.info(f"ファイル名: `{uploaded_file.name}`")

    if st.button("まとめ表を作成 ▶️", type="primary"):
        with st.spinner("データを整理・分析中..."):
            all_summaries = process_file_by_page_delimiter(uploaded_file)

        if all_summaries:
            st.success(f"✅ {len(all_summaries)}個のまとめ表が作成されました！")

            # --- 一括ダウンロードボタン ---
            output_excel = io.BytesIO()
            with pd.ExcelWriter(output_excel, engine="xlsxwriter") as writer:
                for i, summary_df in enumerate(all_summaries):
                    summary_df.to_excel(writer, sheet_name=f"まとめ表_{i+1}", index=False)
            
            st.download_button(
                label="📥 全てのまとめ表をExcelで一括ダウンロード",
                data=output_excel.getvalue(),
                file_name=f"まとめ表_{uploaded_file.name}",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
            st.divider()

            # --- 各まとめ表をExpanderで表示 ---
            for i, summary_df in enumerate(all_summaries):
                with st.expander(f"▼ **まとめ表 {i+1}** の分析結果を見る"):
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
