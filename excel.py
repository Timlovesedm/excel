import streamlit as st
import pandas as pd
import io
import re

# --- Streamlitページの初期設定 ---
st.set_page_config(page_title="損益計算書データ整理ツール", layout="wide")

def consolidate_pl_sheets(excel_file):
    """
    アップロードされたExcelファイルを読み込み、
    「年度ごとの損益計算書」を共通項目（A列）をキーに横方向へ結合（和集合）。
    欠損は 0 で補完し、最後に年度列を昇順で並べ替える。
    """
    try:
        xls = pd.ExcelFile(excel_file)
        sheet_name_to_read = "抽出結果" if "抽出結果" in xls.sheet_names else xls.sheet_names[0]
        df_full = pd.read_excel(xls, sheet_name=sheet_name_to_read, header=None)
        st.info(f"シート「{sheet_name_to_read}」を処理しています...")
    except Exception as e:
        st.error(f"Excelファイルの読み込みに失敗しました: {e}")
        return None

    # B列（インデックス1）にある「西暦4桁のみ」を年度とみなす（例: 2022）
    year_pat = re.compile(r"^\s*20\d{2}\s*$")
    year_rows = df_full[df_full[1].apply(lambda x: bool(year_pat.match(str(x))) if pd.notna(x) else False)].index.tolist()

    if not year_rows:
        st.error("年度セル（例: 2022）が見つかりませんでした。シートの形式をご確認ください。")
        return None

    # 年度セルの行インデックスを昇順にし、各年度ブロックの範囲を決める
    year_rows = sorted(year_rows)
    # 各年の開始行（年度セルの次の行）から、次の年度セルの直前までをブロックとする
    block_ranges = []
    for i, r in enumerate(year_rows):
        start = r + 1
        end = year_rows[i+1] if i+1 < len(year_rows) else len(df_full)
        block_ranges.append((r, start, end))  # (年セル行, データ開始行, データ終了行)

    dfs = []
    for r, start, end in block_ranges:
        # 年を取得（B列）
        try:
            year = int(str(df_full.loc[r, 1]).strip())
        except Exception:
            # 念のため数値化できない場合はスキップ
            continue

        # この年の「項目(A列) + 金額(B列)」を抽出
        sub = df_full.iloc[start:end, :2].copy()
        sub.columns = ["共通項目", year]

        # 「共通項目」空行の除去・前後空白除去
        sub["共通項目"] = sub["共通項目"].astype(str).str.strip()
        sub = sub[sub["共通項目"].notna() & (sub["共通項目"] != "")]

        # 金額を数値化（カンマ除去・非数は0）
        sub[year] = sub[year].replace({",": ""}, regex=True)
        sub[year] = pd.to_numeric(sub[year], errors="coerce").fillna(0)

        # 重複項目は先勝ち
        sub = sub.drop_duplicates(subset=["共通項目"], keep="first").set_index("共通項目")

        dfs.append(sub)

    if not dfs:
        st.error("年度ブロックから有効なデータを抽出できませんでした。")
        return None

    # 和集合で横結合し、欠損を0で補完
    consolidated = pd.concat(dfs, axis=1, join="outer").fillna(0)

    # 年度列（整数）を昇順に整列
    year_cols = [c for c in consolidated.columns if isinstance(c, (int, float)) or (isinstance(c, str) and str(c).isdigit())]
    # 文字として入ってしまっても安全に数値化して昇順に
    year_cols_sorted = sorted([int(str(c)) for c in year_cols])

    # 指定順に並べ替え
    consolidated = consolidated[year_cols_sorted]

    # 表示／書き出し用にインデックスを列へ戻す
    consolidated = consolidated.reset_index()  # 「共通項目」列が先頭に戻る

    return consolidated


# --- StreamlitのUI部分 ---
st.title("📊 損益計算書 データ整理ツール（年度昇順）")
st.write("年度セル（例: 2022）を区切りとして、各年の損益計算書を横方向に統合します。欠損は 0 で補完し、年度列は左から小さい順に並べます。")

# --- ファイルアップロード ---
uploaded_file = st.file_uploader("処理したいExcelファイル（.xlsx）をアップロードしてください", type=["xlsx"])

# --- 実行と結果表示 ---
if uploaded_file:
    st.info(f"ファイル名: `{uploaded_file.name}`")

    if st.button("整理開始 ▶️", type="primary"):
        with st.spinner("データを整理中..."):
            df_result = consolidate_pl_sheets(uploaded_file)

        if df_result is not None and not df_result.empty:
            st.success("✅ データの整理が完了しました！")

            st.subheader("整理後のデータプレビュー（年度は昇順）")
            st.dataframe(df_result)

            # Excelダウンロード
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
        else:
            st.error("データの整理に失敗しました。ファイルの形式をご確認ください。")
else:
    st.warning("☝️ 上のボタンからExcelファイルをアップロードしてください。")
