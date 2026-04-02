# Telegram诈骗犯查询机器人

一个基于 **Telethon + pyTelegramBotAPI + Flask** 的 Telegram 反诈查询机器人与网页查询面板。

## 功能

- Telegram 机器人查询
- 网页查询与展示
- 举报 / 投稿数据读取
- 可选赞助回调（OKPay）
- Docker / 1Panel 部署

## 开源说明

这个仓库是 **脱敏后的可公开版本**：

- 已移除真实 Token、API Hash、会话文件、数据库、日志和运行时数据
- 所有敏感配置都应通过 `.env` 注入
- 你需要先复制 `.env.example` 为 `.env`，再填入你自己的配置

## 快速开始

```bash
cp .env.example .env
pip install -r requirements.txt
python zpf_web.py
```

或使用 Docker / 1Panel：

```bash
docker compose up -d
```

## 重要安全提醒

请不要把以下文件提交到 Git：

- `.env`
- `*.session`
- `*.db` / `*.sqlite*`
- `reports.json` / `channels.json`
- 日志文件

## 目录说明

- `zpf_web.py`：机器人主程序（含 Web/回调逻辑）
- `app.py`：网页查询面板
- `templates/`：网页模板
- `static/`：静态资源
- `docker-compose.yml`：容器部署示例
- `run.sh`：容器启动脚本

## 许可

MIT
