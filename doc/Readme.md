
# 环境介绍

使用 anaconda 64位版本 Python 3.7 version
mongodb 4.0.10
tushare  1.2.36

stocker_env.yaml 是conda的环境文件
conda env export > stocker_env.yaml    # 将当前运行环境的package信息导出到YAML文件
conda env create --force stocker_env.yaml  # 使用YAML文件创建运行环

