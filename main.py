"""
读取目录下的csv.gz文件
生成result.csv文件
列:
file_name: 文件名称
column_name: 列名称
is_all_empty: 列值全部为空
empty_column_counts: 空值的行数
"""
from pathlib import Path

import pandas as pd
from loguru import logger


def check_file(file_path: Path) -> pd.DataFrame:
    try:
        df = pd.read_csv(
            file_path,
            encoding="utf-8",
            compression="gzip",
            # quotechar='"',
            delimiter="\x03",
            error_bad_lines=False,
        )
        column_names = list(df.columns)
        empty_col_counts = df.isnull().sum()
        is_all_empty = [df[col].isna().all() for col in df.columns]
        total_rows = len(df.index)

        result = {
            "file_name": file_path.name,
            "column_name": column_names,
            "is_all_empty": is_all_empty,
            "empty_column_counts": empty_col_counts,
            "total_rows": total_rows,
        }
        output = pd.DataFrame(result)
        return output
    except Exception:
        logger.exception("load file failed")


def main(path: str) -> None:
    dfs: list[pd.DataFrame] = []
    data_files: Path = Path(path).glob("*.csv.gz")
    for data_file in data_files:

        result: pd.DataFrame = check_file(data_file)
        dfs.append(result)

    merged_df = pd.concat(dfs, axis=0, ignore_index=True)
    merged_df.to_csv("result.csv", index=False)


if __name__ == "__main__":
    main("data")
