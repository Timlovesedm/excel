import streamlit as st
import pandas as pd
import io
import re

# --- Streamlitページの初期設定 ---
st.set_page_config(page_title="損益計算書データ整理・分析ツール", layout="wide")


def consolidate_pl_sheets(df_full):
    """
    単一の企業データブロック（DataFrame）を受け取り、損益計算書データを整理する。
    - データブロック全体から年度ヘッダー（例: 2022）を検索し、データブロックを特定。
    - 項目名（A列）をキーに、各年度のデータを横方向へ結合する。
    - 重複する年度は最初に出現したものだけを採用する。
    - 「その他」項目は重複を許容し、それ以外の項目は重複を削除する。
    - 欠損値は0で補完し、年度列は昇順に並べ替える。
    """
    if df_full.empty:
        return None

    # --- 1. 年度ヘッダーの検索とブロック定義 ---
    year_pat = re.compile(r"^\s*20\d{2}\s*$")
    year_cells = []
    for r in range(df_full.shape[0]):
        for c in range(df_full.shape[1]):
            cell_value = df_full.iat[r, c]
            if pd.notna(cell_value) and bool(year_pat.match(str(cell_value))):
                year_cells.append({"row": r, "col": c, "year": int(str(cell_value).strip())})

    if not year_cells:
        return None # 有効な年度が見つからなければスキップ

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
        end_data_row = unique_year_cells[i+1]['row'] if i + 1 < len(unique_year_cells) else len(df_full)
        block_definitions.append({
            "year": cell['year'], "col": cell['col'],
            "start": start_data_row, "end": end_data_row
        })

    # --- 2. 各ブロックのデータを抽出・整形 ---
    dfs = []
    for block in block_definitions:
        year = block['year']
        val_col = block['col']
        
        # 項目はA列(0)、金額は年度ヘッダーと同じ列(val_col)から抽出
        # 列が存在するかチェック
        if 0 not in df_full.columns or val_col not in df_full.columns:
            continue
        
        sub = df_full.iloc[block['start']:block['end'], :].copy()
        # 必要な列だけを選択
        sub = sub[[0, val_col]]
        sub.columns = ["共通項目", year]

        sub["共通項目"] = sub["共通項目"].astype(str).str.strip()
        sub.dropna(subset=["共通項目"], inplace=True)
        sub = sub[sub["共通項目"] != ""]
        if sub.empty: continue

        sonota_df = sub[sub["共通項目"] == "その他"].copy()
        other_items_df = sub[sub["共通項目"] != "その他"].copy()
        other_items_df.drop_duplicates(subset=["共通項目"], keep="first", inplace=True)
        sub = pd.concat([sonota_df, other_items_df]).sort_index()

        sub[year] = sub[year].replace({",": ""}, regex=True)
        sub[year] = pd.to_numeric(sub[year], errors="coerce").fillna(0)

        is_sonota = sub['共通項目'] == 'その他'
        if is_sonota.any():
            sonota_counts = sub.groupby('共通項目').cumcount()
            sub.loc[is_sonota, '共通項目'] = sub.loc[is_sonota, '共通項目'] + '_temp_' + sonota_counts[is_sonota].astype(str)
        
        sub.set_index("共通項目", inplace=True)
        dfs.append(sub)

    if not dfs:
        return None

    # --- 3. 全年度データを結合 ---
    consolidated = pd.concat(dfs, axis=1, join="outer").fillna(0).astype(int)
    
    year_cols = sorted([col for col in consolidated.columns if isinstance(col, int)])
    consolidated = consolidated[year_cols]
    
    consolidated.reset_index(inplace=True)
    consolidated['共通項目'] = consolidated['共通項目'].str.replace(r'_temp_\d+$', '', regex=True)

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


def process_batch_file(excel_file):
    """
    アップロードされたExcelファイルを読み込み、複数のデータブロックに分割してそれぞれ処理する。
    """
    try:
        xls = pd.ExcelFile(excel_file)
        sheet_name_to_read = "抽出結果" if "抽出結果" in xls.sheet_names else xls.sheet_names[0]
        df_full = pd.read_excel(xls, sheet_name=sheet_name_to_read, header=None)
        st.info(f"シート「{sheet_name_to_read}」を処理しています...")
    except Exception as e:
        st.error(f"Excelファイルの読み込みに失敗しました: {e}")
        return None

    # A列の文字列を元に分割点を特定
    df_full[0] = df_full[0].astype(str)
    split_indices = df_full[df_full[0].str.contains(r'ファイル名:|----------', na=False)].index.tolist()

    if not split_indices:
        # 分割点がない場合は全体を1ブロックとして処理
        file_name = "単一データ"
        df_result = consolidate_pl_sheets(df_full)
        if df_result is not None:
             return {file_name: df_result}
        else:
            return {}

    results = {}
    start_idx = 0
    current_filename = "不明なファイル_1"

    # 最初の分割点が0行目でない場合、それより前も1ブロックとして扱う
    if split_indices[0] != 0:
        split_indices.insert(0, 0)
    
    for i in range(len(split_indices)):
        # ファイル名の行を取得
        header_row_idx = split_indices[i]
        header_text = df_full.loc[header_row_idx, 0]
        if 'ファイル名:' in header_text:
            current_filename = header_text.replace('ファイル名:', '').strip()
        elif '----------' in header_text and i > 0:
             # 前のブロックのファイル名を使って連番を振る
             base_name = re.sub(r'_\d+$', '', list(results.keys())[-1] if results else "不明なファイル")
             current_filename = f"{base_name}_{len(results) + 1}"

        # データブロックの範囲を決定
        start_block_idx = header_row_idx
        end_block_idx = split_indices[i+1] if i + 1 < len(split_indices) else len(df_full)
        
        data_chunk = df_full.iloc[start_block_idx:end_block_idx].reset_index(drop=True)
        
        # チャンクを処理
        df_result = consolidate_pl_sheets(data_chunk)
        
        if df_result is not None and not df_result.empty:
            # ファイル名が重複しないように連番を追加
            final_filename = current_filename
            counter = 1
            while final_filename in results:
                counter += 1
                final_filename = f"{current_filename}_{counter}"
            results[final_filename] = df_result
            
    return results


# --- StreamlitのUI部分 ---
st.title("📊 損益計算書 データ一括整理・分析ツール")
st.write("""
`ファイル名:` や `----------` を区切りとして、1つのファイル内の複数データを一括で整理・分析します。  
結果はファイルごとに表示され、まとめてExcel形式でダウンロードできます。
""")

uploaded_file = st.file_uploader("処理したいExcelファイル（.xlsx）をアップロードしてください", type=["xlsx"])

if uploaded_file:
    st.info(f"ファイル名: `{uploaded_file.name}`")

    if st.button("一括 整理・分析開始 ▶️", type="primary"):
        with st.spinner("全データを一括で整理・分析中..."):
            all_results = process_batch_file(uploaded_file)

        if all_results:
            st.success(f"✅ {len(all_results)}件のデータの整理・分析が完了しました！")

            # --- ダウンロード用のExcelファイルを作成 ---
            output_consolidated = io.BytesIO()
            output_yoy = io.BytesIO()
            with pd.ExcelWriter(output_consolidated, engine="xlsxwriter") as writer_consolidated, \
                 pd.ExcelWriter(output_yoy, engine="xlsxwriter") as writer_yoy:
                
                for filename, df_result in all_results.items():
                    # シート名に使えない文字を置換し、長さを調整
                    safe_sheet_name = re.sub(r'[\\/*?:"<>|]', '_', filename)[:31]
                    
                    # 整理後データを書き込み
                    df_result.to_excel(writer_consolidated, sheet_name=safe_sheet_name, index=False)
                    
                    # 前年比データを計算して書き込み
                    df_yoy_result = calculate_yoy(df_result)
                    if df_yoy_result is not None:
                        df_yoy_result.to_excel(writer_yoy, sheet_name=safe_sheet_name, index=False)

            st.divider()
            col1, col2 = st.columns(2)
            with col1:
                st.download_button(
                    label="📥 全ての整理後データを一括ダウンロード",
                    data=output_consolidated.getvalue(),
                    file_name=f"全社_整理済み_{uploaded_file.name}",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )
            with col2:
                st.download_button(
                    label="📥 全ての前年比データを一括ダウンロード",
                    data=output_yoy.getvalue(),
                    file_name=f"全社_前年比分析_{uploaded_file.name}",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )
            st.divider()

            # --- 各結果をExpanderで表示 ---
            for filename, df_result in all_results.items():
                with st.expander(f"▼ **{filename}** の分析結果を見る"):
                    tab1, tab2, tab3 = st.tabs(["整理後データ", "� 推移グラフ", "🆚 前年比・増減"])
                    
                    with tab1:
                        st.dataframe(df_result)
                    
                    with tab2:
                        items = df_result["共通項目"].unique()
                        default_items = [item for item in ["売上高", "営業利益", "経常利益", "当期純利益"] if item in items]
                        selected_items = st.multiselect(
                            "グラフに表示する項目を選択", options=items, default=default_items, key=f"chart_{filename}"
                        )
                        if selected_items:
                            df_chart = df_result[df_result["共通項目"].isin(selected_items)].groupby("共通項目").sum().T
                            df_chart.index.name = "年度"
                            st.line_chart(df_chart)
                    
                    with tab3:
                        df_yoy_result = calculate_yoy(df_result)
                        st.dataframe(df_yoy_result.style.format(precision=2, na_rep='-'))

        else:
            st.error("有効なデータを抽出できませんでした。ファイルの形式をご確認ください。")
else:
    st.warning("☝️ 上のボタンからExcelファイルをアップロードしてください。")
�
