#!/usr/bin/env bash

# 设置目标目录与输出文件
SRC_DIR="rust/src"
OUTPUT_FILE="rust_src_summary.md"

# 如果输出文件已存在，先清空/删除
if [ -f "$OUTPUT_FILE" ]; then
    rm "$OUTPUT_FILE"
fi

echo "正在合并 ${SRC_DIR} 下的代码文件（已忽略 frb_generated ...）"

# 递归遍历 rust/src 目录下的所有普通文件
find "$SRC_DIR" -type f | while read -r filepath; do
    filename=$(basename "$filepath")

    # 忽略文件名包含 frb_generated 的文件
    if [[ "$filename" == *frb_generated* ]]; then
        echo "[已跳过生成文件]: $filepath"
        continue
    fi

    echo "正在处理: $filepath"

    # 追加文件相对路径标题与 Markdown 代码块
    {
        echo "## File: $filepath"
        echo ""
        echo '```rust'
        cat "$filepath"
        echo ""
        echo '```'
        echo ""
        echo "---"
        echo ""
    } >>"$OUTPUT_FILE"
done

echo ""
echo "完成！结果已输出至: $OUTPUT_FILE"
