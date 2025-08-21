import streamlit as st
import pandas as pd
import io
import re

# --- Streamlitページの初期設定 ---
st.set_page_config(page_title="損益計算書データ整理ツール", layout="wide")


def consolidate_pl_sheets(excel_file):
    """
    アップロードされたExcelファイルを読み込み、損益計算書データを整理する。
    - シート全体から年度ヘッダー（例: 2022）を検索し、データブロックを特定。
    - 項目名（A列）をキーに、各年度のデータを横方向へ結合する。
    - 重複する年度は最初に出現したものだけを採用する。
    - 「その他」項目は重複を許容し、それ以外の項目は重複を削除する。
    - 欠損値は0で補完し、年度列は昇順に並べ替える。
    """
    try:
        xls = pd.ExcelFile(excel_file)
        sheet_name_to_read = "抽出結果" if "抽出結果" in xls.sheet_names else xls.sheet_names[0]
        df_full = pd.read_excel(xls, sheet_name=sheet_name_to_read, header=None)
        st.info(f"シート「{sheet_name_to_read}」を処理しています...")
    except Exception as e:
        st.error(f"Excelファイルの読み込みに失敗しました: {e}")
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
        st.error("年度セル（例: 2022）が見つかりませんでした。シートの形式をご確認ください。")
        return None

    # 行番号でソートし、最初に出てきた年度を優先する
    year_cells.sort(key=lambda x: x['row'])
    
    # --- ① 年度の重複を排除（最初に出現したもの勝ち） ---
    unique_year_cells = []
    processed_years = set()
    for cell in year_cells:
        if cell['year'] not in processed_years:
            unique_year_cells.append(cell)
            processed_years.add(cell['year'])

    # 各年度データの範囲を定義
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
        sub = df_full.iloc[block['start']:block['end'], [0, val_col]].copy()
        sub.columns = ["共通項目", year]

        # 「共通項目」列を整形
        sub["共通項目"] = sub["共通項目"].astype(str).str.strip()
        sub.dropna(subset=["共通項目"], inplace=True)
        sub = sub[sub["共通項目"] != ""]
        if sub.empty: continue

        # --- ② 「その他」項目の重複を許容 ---
        sonota_df = sub[sub["共通項目"] == "その他"].copy()
        other_items_df = sub[sub["共通項目"] != "その他"].copy()
        other_items_df.drop_duplicates(subset=["共通項目"], keep="first", inplace=True)
        sub = pd.concat([sonota_df, other_items_df]).sort_index()

        # 金額を数値化
        sub[year] = sub[year].replace({",": ""}, regex=True)
        sub[year] = pd.to_numeric(sub[year], errors="coerce").fillna(0)

        # 結合のために「その他」のインデックスを一時的にユニークにする
        is_sonota = sub['共通項目'] == 'その他'
        if is_sonota.any():
            # cumcount()で各グループ内での連番を生成 (0, 1, 2...)
            sonota_counts = sub.groupby('共通項目').cumcount()
            sub.loc[is_sonota, '共通項目'] = sub.loc[is_sonota, '共通項目'] + '_temp_' + sonota_counts[is_sonota].astype(str)
        
        sub.set_index("共通項目", inplace=True)
        dfs.append(sub)

    if not dfs:
        st.error("有効なデータを抽出できませんでした。")
        return None

    # --- 3. 全年度データを結合 ---
    consolidated = pd.concat(dfs, axis=1, join="outer").fillna(0).astype(int)
    
    # 年度列を昇順に並べ替え
    year_cols = sorted([col for col in consolidated.columns if isinstance(col, int)])
    consolidated = consolidated[year_cols]
    
    consolidated.reset_index(inplace=True)
    
    # 一時的にユニークにした「その他」の項目名を元に戻す
    consolidated['共通項目'] = consolidated['共通項目'].str.replace(r'_temp_\d+$', '', regex=True)

    return consolidated


def calculate_yoy(df):
    """前年比と増減額を計算する"""
    df_yoy = df.set_index("共通項目")
    
    # 増減額
    df_diff = df_yoy.diff(axis=1)
    df_diff.columns = [f"{col} 増減額" for col in df_diff.columns]
    
    # 変化率（%）
    df_pct = df_yoy.pct_change(axis=1) * 100
    df_pct.columns = [f"{col} 増減率(%)" for col in df_pct.columns]

    # 元データと結合
    df_merged = pd.concat([df_yoy, df_diff, df_pct], axis=1)
    
    # 列を年度ごとに並べ替え
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


# --- StreamlitのUI部分 ---
st.title("📊 損益計算書 データ整理・分析ツール")
st.write("""
年度セル（例: 2022）を区切りとして、各年の損益計算書を横方向に統合します。  
整理されたデータは、推移グラフや前年比分析にも活用できます。
""")

# --- ファイルアップロード ---
uploaded_file = st.file_uploader("処理したいExcelファイル（.xlsx）をアップロードしてください", type=["xlsx"])

# --- 実行と結果表示 ---
if uploaded_file:
    st.info(f"ファイル名: `{uploaded_file.name}`")

    if st.button("整理・分析開始 ▶️", type="primary"):
        with st.spinner("データを整理・分析中..."):
            df_result = consolidate_pl_sheets(uploaded_file)

        if df_result is not None and not df_result.empty:
            st.success("✅ データの整理・分析が完了しました！")

            # --- 結果をタブで表示 ---
            tab1, tab2, tab3 = st.tabs(["整理後データ", "📈 推移グラフ", "🆚 前年比・増減"])

            with tab1:
                st.subheader("整理後のデータプレビュー（年度昇順）")
                st.dataframe(df_result)

                # Excelダウンロードボタン
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
                    df_result.to_excel(writer, index=False, sheet_name="整理結果")
                output.seek(0)
                
                st.download_button(
                    label="📥 整理後のExcelファイルをダウンロード",
                    data=output,
                    file_name=f"整理済み_{uploaded_file.name}",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )

            with tab2:
                st.subheader("主要項目の年度推移グラフ")
                
                # グラフ化する項目を選択
                items = df_result["共通項目"].unique()
                default_items = [item for item in ["売上高", "営業利益", "経常利益", "当期純利益"] if item in items]
                
                selected_items = st.multiselect(
                    "グラフに表示する項目を選択してください（複数選択可）",
                    options=items,
                    default=default_items
                )

                if selected_items:
                    # グラフ用にデータを整形
                    df_chart = df_result[df_result["共通項目"].isin(selected_items)]
                    # 「その他」が複数ある場合を考慮し、項目名で集計してからグラフ化
                    df_chart = df_chart.groupby("共通項目").sum()
                    df_chart = df_chart.T # 行と列を入れ替え
                    df_chart.index.name = "年度"
                    
                    st.line_chart(df_chart)
                else:
                    st.info("グラフで表示したい項目を選択してください。")

            with tab3:
                st.subheader("前年比・増減額")
                with st.spinner("前年比を計算中..."):
                    # 「その他」が複数ある場合、計算前に集計する
                    df_for_yoy = df_result.groupby("共通項目").sum().reset_index()
                    df_yoy_result = calculate_yoy(df_for_yoy)
                st.dataframe(df_yoy_result.style.format(precision=2, na_rep='-'))

                # Excelダウンロードボタン
                output_yoy = io.BytesIO()
                with pd.ExcelWriter(output_yoy, engine="xlsxwriter") as writer:
                    df_yoy_result.to_excel(writer, index=False, sheet_name="前年比分析結果")
                output_yoy.seek(0)

                st.download_button(
                    label="📥 前年比データのExcelをダウンロード",
                    data=output_yoy,
                    file_name=f"前年比分析_{uploaded_file.name}",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )

        else:
            st.error("データの整理に失敗しました。ファイルの形式をご確認ください。")
else:
    st.warning("☝️ 上のボタンからExcelファイルをアップロードしてください。")
