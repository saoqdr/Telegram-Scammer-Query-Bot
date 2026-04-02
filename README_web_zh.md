# ZPF 机器人 + 网页控制台版

这是在你原始 `zpf.py` 基础上自动生成的增强版本：`zpf_web.py`。
主要变化：

1. 沿用原有 Telegram 机器人全部功能（TeleBot + Telethon + OKPay 回调等）。
2. 复用同一个 Flask 应用，新增了简单网页控制台：
   - `/`         总览 + 快速查询入口
   - `/query`    风险查询（ID / @用户名）
   - `/stats`    运行状态（对应 /stats 命令）
   - `/leaderboard` 赞助排行榜（对应 /leaderboard）

## 如何使用（本地 / 服务器）

1. 安装依赖：

   ```bash
   pip install -r requirements.txt
   ```

2. 根据需要修改 `zpf_web.py` 顶部的 `CONFIG["SERVER_PUBLIC_IP"]`、`CONFIG["WEBHOOK_PORT"]` 等配置；
   请确认数据库路径 `CONFIG["DATABASE_FILE"]` 正确指向你的历史数据库。

3. 启动：

   ```bash
   python zpf_web.py
   ```

   - Telegram 机器人会照常启动（轮询 + Telethon 抓取）。
   - Flask Web 控制台会监听 `0.0.0.0:CONFIG["WEBHOOK_PORT"]`，默认 1010。

4. 在浏览器访问：

   - 例如：`http://你的服务器IP:1010/`
   - 或者将你的域名通过 1panel 反向代理到对应端口，即可通过 `https://你的域名/` 访问。

> 注意：这是简单的管理控制台，没有登录密码，请务必只对“可信环境”开放，
> 生产环境建议再加一层反向代理的 Basic Auth 或 1panel 内置的访问控制。

