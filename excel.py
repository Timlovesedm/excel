#!/usr/bin/env python
# -*- coding: utf-8 -*-

import streamlit as st
import pandas as pd
import io
import re
from collections import defaultdict

# --- Streamlitページの初期設定 ---
st.set_page_config(page_title="損益計算書データ整理・分析ツール", layout="wide")


def extract_data_from_chunk(df_chunk):
    """
    単一の表ブロック(DataFrame)を受け取り、3つのルールに従ってデータを抽出する。
    1. 年号は最初に出現したものだけを採用。
    2. 「その他」は位置を保持して複数出現を許容。
    3. 項目は出現順を保持。
    """
    if df_chunk.empty:
        return None, []

    year_pat = re.compile(r"^\s*20\d{2}\s*$")
    year_cells = []
    for r in range(df_chunk.shape[0]):
        for c in range(df_chunk.shape[1]):
            cell_value = df_chunk.iat[r, c]
            if pd.notna(cell_value) and bool(year_pat.match(str(cell_value))):
                year_cells.append({"row": r, "col": c, "year": int(str(cell_value).strip())})

    if not year_cells:
        return None, []

    # 行番号、列番号の順でソートし、処理順を確定
    year_cells.sort(key=lambda x: (x['row'], x['col']))

    processed_years = set()
    
    # 最初にA列の全項目を抽出し、順序を確定
    initial_items = df_chunk[0].astype(str).str.strip().dropna()
    initial_items = initial_items[initial_items != ""]
    
    # 「その他」にユニークIDを付与
    is_sonota = initial_items == 'その他'
    if is_sonota.any():
        sonota_counts = initial_items.groupby(initial_items).cumcount()
        initial_items.loc[is_sonota] = initial_items.loc[is_sonota] + '_temp_' + sonota_counts[is_sonota].astype(str)
    
    # 重複を削除しつつ順序を保持
    all_items_ordered = initial_items.drop_duplicates(keep='first').tolist()
    df_result = pd.DataFrame({'共通項目': all_items_ordered})
    
    for cell in year_cells:
        year = cell['year']
        # --- ① 年号は最初に出てきたもののみ使用 ---
        if year in processed_years:
            continue
        processed_years.add(year)

        val_col = cell['col']
        
        # 年号ヘッダー以降のデータを抽出
        temp_df = df_chunk.iloc[cell['row'] + 1:, [0, val_col]].copy()
        temp_df.columns = ["共通項目", year]
        
        temp_df["共通項目"] = temp_df["共通項目"].astype(str).str.strip()
        temp_df.dropna(subset=["共通項目"], inplace=True)
        temp_df = temp_df[temp_df["共通項目"] != ""]

        # --- ② "その他"の位置を保持 ---
        is_sonota = temp_df['共通項目'] == 'その他'
        if is_sonota.any():
            sonota_counts = temp_df.groupby('共通項目').cumcount()
            temp_df.loc[is_sonota, '共通項目'] = temp_df.loc[is_sonota, '共通項目'] + '_temp_' + sonota_counts[is_sonota].astype(str)
        
        # 金額を数値化
        temp_df[year] = pd.to_numeric(temp_df[year].astype(str).str.replace(",", ""), errors='coerce').fillna(0)
        
        # 重複項目は最初に出てきたものを採用
        temp_df = temp_df.drop_duplicates(subset=['共通項目'], keep='first')
        
        # 結果のDataFrameにマージ
        df_result = pd.merge(df_result, temp_df, on='共通項目', how='left')

    return df_result, all_items_ordered


def calculate_yoy(df):
    """前年比と増減額を計算する"""
    df_yoy = df.set_index("共通項目")
    
    # 「その他」を集計してから計算
    df_yoy.index = df_yoy.index.str.replace(r'_temp_\d+$', '', regex=True)
    df_yoy = df_yoy.groupby(df_yoy.index, sort=False).sum()
    
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
    try:
        xls = pd.ExcelFile(excel_file)
        sheet_name_to_read = "抽出結果" if "抽出結果" in xls.sheet_names else xls.sheet_names[0]
        df_full = pd.read_excel(xls, sheet_name=sheet_name_to_read, header=None)
        st.info(f"シート「{sheet_name_to_read}」を処理しています...")
    except Exception as e:
        st.error(f"Excelファイルの読み込みに失敗しました: {e}")
        return None

    df_full[0] = df_full[0].astype(str)
    
    file_indices = df_full[df_full[0].str.contains(r'ファイル名:', na=False)].index.tolist()
    file_chunks = []
    if not file_indices:
        file_chunks.append(df_full)
    else:
        for i in range(len(file_indices)):
            start_idx = file_indices[i]
            end_idx = file_indices[i+1] if i + 1 < len(file_indices) else len(df_full)
            file_chunks.append(df_full.iloc[start_idx:end_idx].reset_index(drop=True))

    grouped_tables = defaultdict(list)
    master_item_order = defaultdict(list)

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
            processed_df, item_order = extract_data_from_chunk(table_chunk.reset_index(drop=True))
            if processed_df is not None and not processed_df.empty:
                grouped_tables[i].append(processed_df)
                # --- ③ 項目は出てきた順に上から記載 ---
                current_order = master_item_order[i]
                for item in item_order:
                    if item not in current_order:
                        current_order.append(item)

    final_summaries = []
    for table_index in sorted(grouped_tables.keys()):
        list_of_dfs = grouped_tables[table_index]
        ordered_items = master_item_order[table_index]
        if not list_of_dfs: continue

        # 統合先のベースとなるDataFrameを、マスターの項目順で作成
        result_df = pd.DataFrame({'共通項目': ordered_items})

        # 各ファイルから抽出したDataFrameを順番にマージ
        for df_to_merge in list_of_dfs:
            # 結合前に、result_dfに既に存在する列をdf_to_mergeから削除（先勝ちルール）
            cols_to_drop = [col for col in df_to_merge.columns if col in result_df.columns and col != '共通項目']
            df_filtered = df_to_merge.drop(columns=cols_to_drop)
            result_df = pd.merge(result_df, df_filtered, on='共通項目', how='left')

        result_df.fillna(0, inplace=True)
        
        # 項目列以外の列名（年度）を取得
        year_cols = [col for col in result_df.columns if col != '共通項目']
        # 年度列を数値に変換してソート
        sorted_year_cols = sorted([col for col in year_cols if isinstance(col, (int, float)) or str(col).isdigit()], key=int)
        
        final_cols = ['共通項目'] + sorted_year_cols
        result_df = result_df[final_cols]

        for col in sorted_year_cols:
            result_df[col] = result_df[col].astype(int)
        
        # 「その他」のユニークIDを削除
        result_df['共通項目'] = result_df['共通項目'].str.replace(r'_temp_\d+$', '', regex=True)

        final_summaries.append(result_df)
            
    return final_summaries


# --- StreamlitのUI部分 ---
st.title("� 損益計算書 統合データ作成ツール（ファイル・ページ別）")
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
                        # グラフ用に「その他」を一旦集計
                        df_for_chart = summary_df.copy()
                        df_for_chart['共通項目'] = df_for_chart['共通項目'].str.replace(r'_temp_\d+$', '', regex=True)
                        df_for_chart = df_for_chart.groupby('共通項目', sort=False).sum()
                        
                        items = df_for_chart.index.tolist()
                        default_items = [item for item in ["売上高", "営業利益", "経常利益", "当期純利益"] if item in items]
                        selected_items = st.multiselect(
                            "グラフに表示する項目を選択", options=items, default=default_items, key=f"chart_{i}"
                        )
                        if selected_items:
                            st.line_chart(df_for_chart.loc[selected_items].T)
                    
                    with tab3:
                        st.subheader("前年比・増減額")
                        df_yoy_result = calculate_yoy(summary_df)
                        st.dataframe(df_yoy_result.style.format(precision=2, na_rep='-'))
        else:
            st.error("有効なデータを抽出できませんでした。ファイルの形式をご確認ください。")
else:
    st.warning("☝️ 上のボタンからExcelファイルをアップロードしてください。")
