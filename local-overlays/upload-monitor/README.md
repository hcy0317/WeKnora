# WeKnora 本地上传任务外挂

这个目录提供不修改 WeKnora Vue 源码的上传任务列表。当前行为版本为 `1.3.1`。

## 能力

- 显示每个文件的上传百分比、大小和累计耗时。
- 浏览器到服务器的文件传输成功后立即标记为“已上传”。
- 摘要、问题生成、Wiki 等后台流程不再计入上传任务状态，也不会让上传徽标持续显示活动任务。
- 旧版本中保存的“等待解析 / 解析中 / 后处理中 / 处理完成”记录会自动迁移为“已上传”。
- 页面刷新后保留最近记录；未完成的浏览器上传请求无法续传，会标记为“上传中断”，不推断服务器后处理状态。
- 单独把知识库文件上传超时从前端默认 30 秒提高到 3 分钟。
- 把 Nginx 请求体读取超时从默认 60 秒提高到 3 分钟，避免文件读取暂停时返回 408。
- 使用 Shadow DOM 隔离样式，避免与 WeKnora 前端样式互相覆盖。
- 全局层级固定为 `1800`，低于上传确认弹窗、文档详情抽屉和 Trace 抽屉；停靠 rail 只补充位置，不抢占业务弹层。
- 文档详情打开时隐藏空闲入口，避免遮挡正文；上传仍在进行时改为停靠在详情抽屉左缘的窄入口，任务列表只在用户主动打开后显示。
- 根据 drawer 实际左边界判断停靠方式：外侧足够时保持 rail 外停靠；375px、800px 全宽 drawer 等空间不足场景退回 viewport 内，活动入口保持 44px 可点击，展开面板固定留出左右 16px。
- 最小化任务面板后把键盘焦点归还给仍可见的上传任务入口，避免焦点滞留在 hidden 子树。
- 监听根节点的 `theme-mode` 属性，深浅色切换即时生效，无需刷新页面。
- 仅在 drawer 根节点和当前 content wrapper 监听 class/style；body 只监听 Teleport 节点增删，并合并同一帧内的同步请求。
- 上传百分比和每秒耗时都在现有任务行内更新，不重建任务列表；独立 `role=status` 区域只播报“服务器接收中 / 已上传 / 上传失败”等传输状态转换。

## 持久方式

`docker-compose.override.yml` 将下面两个文件只读挂载进前端容器：

- `upload-monitor.js` -> `/usr/share/nginx/html/weknora-upload-monitor.js`
- `upload-monitor.conf` -> `/etc/nginx/conf.d/upload-monitor.conf`

Nginx 在返回 HTML 时用 `/weknora-upload-monitor.js?v=3` 插入外挂脚本；`v3` 与行为版本 `1.3.1` 配套，便于缓存刷新和运行时诊断。升级或替换官方前端镜像不会覆盖宿主机上的文件，也不需要维护 Vue 源码补丁。

## 兼容边界

外挂只依赖以下稳定接口：

- `POST /api/v1/knowledge-bases/{kbId}/knowledge/file`

如果未来 WeKnora 修改这个 API 路径或响应字段，需要同步调整外挂；普通前端页面、组件或样式升级不会影响它。

## 验证

```powershell
node --check local-overlays/upload-monitor/upload-monitor.js
node --test local-overlays/upload-monitor/upload-monitor.behavior.test.mjs
docker compose -f docker-compose.yml -f docker-compose.override.yml config --quiet
docker exec WeKnora-frontend nginx -t
```

访问知识库页面后，右下角应显示“上传任务”按钮。
