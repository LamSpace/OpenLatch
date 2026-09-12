/*
 * Copyright 2026 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.github.lamspace.openlatch.console.web;

import io.github.lamspace.openlatch.console.ConsoleConfig;
import io.github.lamspace.openlatch.console.admin.AdminClient;
import io.github.lamspace.openlatch.console.admin.AdminClientPool;
import io.github.lamspace.openlatch.console.metrics.MetricsScraper;
import io.github.lamspace.openlatch.protocol.AdminKeyDetailResponse;
import io.github.lamspace.openlatch.protocol.AdminListKeysResponse;
import io.github.lamspace.openlatch.protocol.AdminListSessionsResponse;
import io.github.lamspace.openlatch.protocol.AdminSummaryResponse;
import io.github.lamspace.openlatch.protocol.ClusterView;
import io.github.lamspace.openlatch.protocol.StatusCode;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.ToDoubleFunction;

/**
 * 控制台五页面路由：全部为
 * GET 只读渲染（MUST NOT 存在任何写操作入口）；每页面按节点"尽力而为"
 * 聚合并行级降级（失败节点标注、认证被拒聚合横幅）。
 *
 * <p><b>多节点合并口径</b>：列表/会话页对全部（或 ?node= 选定的）配置节点
 * 逐一查询并按节点分块呈现——服务端"仅本节点视角"的语义在页面上保持
 * 分块可溯，不做跨节点强合并。
 */
@Controller
public final class ConsoleController {

    /** sparkline 公共画布：宽 160、高 36（viewBox 与 points 同尺）。 */
    private static final int SPARK_W = 160;
    /** sparkline 画布高。 */
    private static final int SPARK_H = 36;

    /** 节点连接池。 */
    private final AdminClientPool pool;
    /** 指标抓取器。 */
    private final MetricsScraper scraper;
    /** 配置（刷新间隔/指标端口渲染参数）。 */
    private final ConsoleConfig config;

    /**
     * 构造路由。
     *
     * @param pool    节点连接池
     * @param scraper 指标抓取器
     * @param config  控制台配置
     */
    public ConsoleController(AdminClientPool pool, MetricsScraper scraper, ConsoleConfig config) {
        this.pool = pool;
        this.scraper = scraper;
        this.config = config;
    }

    /**
     * 概览页：SUMMARY 数字 + 每节点三线 sparkline。
     *
     * @param model Thymeleaf 模型
     * @return 模板名
     */
    @GetMapping("/")
    public String overview(Model model) {
        List<WebModels.NodeResult<AdminSummaryResponse>> rows = new ArrayList<>();
        Map<String, WebModels.Spark> sparks = new LinkedHashMap<>();
        for (Map.Entry<String, AdminClient> en : pool.nodes().entrySet()) {
            String name = en.getKey();
            AdminClient client = en.getValue();
            rows.add(call(name, client::summary));
            scrape(name, client, sparks);
        }
        decorate(model, rows, sparks);
        model.addAttribute("rows", rows);
        model.addAttribute("sparks", sparks);
        return "overview";
    }

    /**
     * 锁列表页：分页/前缀过滤/进详情链接（可按 ?node= 选定单节点）。
     *
     * @param node   节点展示名（空=全部节点）
     * @param page   页号（0 起）
     * @param prefix key 前缀
     * @param model  Thymeleaf 模型
     * @return 模板名
     */
    @GetMapping("/keys")
    public String keys(@RequestParam(name = "node", defaultValue = "") String node,
                       @RequestParam(name = "page", defaultValue = "0") int page,
                       @RequestParam(name = "prefix", defaultValue = "") String prefix,
                       Model model) {
        List<WebModels.NodeResult<AdminListKeysResponse>> rows = new ArrayList<>();
        for (String name : selectedNodes(node)) {
            rows.add(call(name, () -> pool.node(name).listKeys(Math.max(page, 0),
                    KEYS_PAGE_SIZE, prefix)));
        }
        model.addAttribute("pageTitle", "锁列表");
        model.addAttribute("nodeNames", pool.nodeNames());
        model.addAttribute("node", node);
        model.addAttribute("page", Math.max(page, 0));
        model.addAttribute("prefix", prefix);
        model.addAttribute("pageSize", KEYS_PAGE_SIZE);
        model.addAttribute("rows", rows);
        model.addAttribute("authBanner", anyAuthFailure(rows));
        model.addAttribute("refreshSeconds", config.refreshSeconds());
        return "keys";
    }

    /** 锁列表页大小（与服务端 {@code MAX_PAGE_SIZE} 上限内取运维可读值）。 */
    private static final int KEYS_PAGE_SIZE = 50;

    /**
     * 锁详情页：持有明细、等待队列（Leader 标注）、剩余租约。
     *
     * @param node  节点展示名
     * @param key   锁键
     * @param model Thymeleaf 模型
     * @return 模板名
     */
    @GetMapping("/key")
    public String keyDetail(@RequestParam(name = "node") String node,
                            @RequestParam(name = "key") String key,
                            Model model) {
        WebModels.NodeResult<AdminKeyDetailResponse> result =
                pool.node(node) == null
                        ? new WebModels.NodeResult<>(node, false, false, "节点未在配置列表中", null)
                        : call(node, () -> pool.node(node).keyDetail(key));
        model.addAttribute("pageTitle", "锁详情");
        model.addAttribute("node", node);
        model.addAttribute("key", key);
        model.addAttribute("result", result);
        model.addAttribute("authBanner", result.authFailure());
        model.addAttribute("refreshSeconds", config.refreshSeconds());
        return "key-detail";
    }

    /**
     * 会话列表页：各节点接入会话与其持锁/等待关联。
     *
     * @param model Thymeleaf 模型
     * @return 模板名
     */
    @GetMapping("/sessions")
    public String sessions(Model model) {
        List<WebModels.NodeResult<AdminListSessionsResponse>> rows = new ArrayList<>();
        for (Map.Entry<String, AdminClient> en : pool.nodes().entrySet()) {
            rows.add(call(en.getKey(), en.getValue()::listSessions));
        }
        model.addAttribute("pageTitle", "会话列表");
        model.addAttribute("rows", rows);
        model.addAttribute("authBanner", anyAuthFailure(rows));
        model.addAttribute("refreshSeconds", config.refreshSeconds());
        return "sessions";
    }

    /**
     * 节点视图页：各节点 {@code CLUSTER_VIEW} 成员表 + Leader 标识 +
     * SUMMARY 角色/版本（单机节点如实报"单机部署"）。
     *
     * @param model Thymeleaf 模型
     * @return 模板名
     */
    @GetMapping("/nodes")
    public String nodes(Model model) {
        List<WebModels.NodeResult<AdminSummaryResponse>> summaries = new ArrayList<>();
        Map<String, WebModels.NodeResult<ClusterView>> views = new LinkedHashMap<>();
        for (Map.Entry<String, AdminClient> en : pool.nodes().entrySet()) {
            String name = en.getKey();
            AdminClient client = en.getValue();
            summaries.add(call(name, client::summary));
            WebModels.NodeResult<ClusterView> v = call(name, client::clusterView);
            // 单机节点的 CLUSTER_VIEW 以 INVALID_REQUEST 自述（wire-protocol v2
            // 口径）：页面据此渲染"单机部署"（value 归一为 null）而非错误。
            if (v.ok() && v.value().getStatus() == StatusCode.INVALID_REQUEST) {
                v = WebModels.NodeResult.ok(name, null);
            }
            views.put(name, v);
        }
        model.addAttribute("pageTitle", "节点视图");
        model.addAttribute("summaries", summaries);
        model.addAttribute("views", views);
        model.addAttribute("authBanner",
                summaries.stream().anyMatch(WebModels.NodeResult::authFailure)
                        || views.values().stream().anyMatch(WebModels.NodeResult::authFailure));
        model.addAttribute("refreshSeconds", config.refreshSeconds());
        return "nodes";
    }

    // ===================== 装配助手 =====================

    /**
     * 选定节点集：空串=全部配置节点。
     *
     * @param node 页面参数
     * @return 展示名列表（未配置的名字被忽略）
     */
    private List<String> selectedNodes(String node) {
        if (node == null || node.isBlank()) {
            return pool.nodeNames();
        }
        return pool.node(node) == null ? List.of() : List.of(node);
    }

    /**
     * 单节点查询的降级收口：任何异常折算失败结果（页面标注），MUST NOT
     * 让单节点故障上抛成整页 5xx。
     *
     * @param display 节点展示名
     * @param call    查询
     * @param <T>     应答类型
     * @return 尽力而为结果
     */
    private static <T> WebModels.NodeResult<T> call(String display, ThrowingSupplier<T> call) {
        try {
            return WebModels.NodeResult.ok(display, call.get());
        } catch (Exception e) {
            return WebModels.NodeResult.fail(display, e);
        }
    }

    /**
     * 可抛异常的查询供应商（内部收口专用）。
     *
     * @param <T> 应答类型
     */
    @FunctionalInterface
    private interface ThrowingSupplier<T> {
        /**
         * 执行查询。
         *
         * @return 应答
         * @throws Exception 任何失败（由 {@link #call} 收口降级）
         */
        T get() throws Exception;
    }

    /**
     * 指标抓取并折算 sparkline 点集（本轮失败即降级，缓冲旧序列仍展示）。
     *
     * @param name   节点展示名
     * @param client 节点客户端（address() 取 host:port 目标）
     * @param sink   结果收集表
     */
    private void scrape(String name, AdminClient client, Map<String, WebModels.Spark> sink) {
        Optional<MetricsScraper.Sample> latest = scraper.scrapeAndRecord(
                name, client.address(), config.metricsPort());
        if (latest.isEmpty()) {
            List<MetricsScraper.Sample> old = scraper.samples(name);
            sink.put(name, old.isEmpty() ? WebModels.Spark.unavailable()
                    : buildSpark(true, old));
            return;
        }
        sink.put(name, buildSpark(true, scraper.samples(name)));
    }

    /**
     * 样本序（新→旧）→ 三线点集（按时间轴旧→新绘制）。
     *
     * @param ok     本轮抓取是否成功
     * @param samples 样本序
     * @return spark
     */
    private static WebModels.Spark buildSpark(boolean ok, List<MetricsScraper.Sample> samples) {
        if (samples.isEmpty()) {
            return WebModels.Spark.unavailable();
        }
        List<MetricsScraper.Sample> chrono = samples.reversed();
        String held = points(chrono, MetricsScraper.Sample::held);
        String waiters = points(chrono, MetricsScraper.Sample::waiters);
        String sessions = points(chrono, MetricsScraper.Sample::sessions);
        MetricsScraper.Sample last = chrono.get(chrono.size() - 1);
        return new WebModels.Spark(ok, held, waiters, sessions,
                fmt(last.held()), fmt(last.waiters()), fmt(last.sessions()));
    }

    /**
     * 单线点集：x 均分画布宽、y 按序列最大值归一化（全零序列居中平线）。
     *
     * @param chrono 时间序样本（旧→新）
     * @param getter 取值线
     * @return SVG points 属性串
     */
    private static String points(List<MetricsScraper.Sample> chrono,
                                 ToDoubleFunction<MetricsScraper.Sample> getter) {
        double max = 0;
        for (MetricsScraper.Sample s : chrono) {
            max = Math.max(max, getter.applyAsDouble(s));
        }
        StringBuilder sb = new StringBuilder();
        int n = chrono.size();
        for (int i = 0; i < n; i++) {
            double x = n == 1 ? SPARK_W / 2.0 : (double) i * SPARK_W / (n - 1);
            double y = max == 0 ? SPARK_H / 2.0
                    : SPARK_H - getter.applyAsDouble(chrono.get(i)) / max * (SPARK_H - 2) - 1;
            sb.append(fmt(x)).append(',').append(fmt(y)).append(' ');
        }
        return sb.toString().trim();
    }

    /**
     * 数值紧凑格式化（一位小数、去尾零）。
     *
     * @param v 值
     * @return 字符串
     */
    private static String fmt(double v) {
        if (v == Math.rint(v) && !Double.isInfinite(v)) {
            return String.valueOf((long) v);
        }
        return String.format("%.1f", v);
    }

    /**
     * 概览页公共装饰（刷新间隔 + 认证横幅判定）。
     *
     * @param model 模型
     * @param rows  结果行
     * @param sparks spark 表（本轮全缺时降级提示）
     */
    private void decorate(Model model, List<WebModels.NodeResult<AdminSummaryResponse>> rows,
                          Map<String, WebModels.Spark> sparks) {
        model.addAttribute("pageTitle", "概览");
        model.addAttribute("refreshSeconds", config.refreshSeconds());
        model.addAttribute("authBanner", anyAuthFailure(rows));
        model.addAttribute("metricsBanner", !rows.isEmpty()
                && rows.stream().allMatch(WebModels.NodeResult::ok)
                && sparks.values().stream().noneMatch(WebModels.Spark::ok));
    }

    /**
     * 结果行集中是否存在认证被拒。
     *
     * @param rows 结果行
     * @return 任一 AUTH 返回 true
     */
    private static boolean anyAuthFailure(List<? extends WebModels.NodeResult<?>> rows) {
        return rows.stream().anyMatch(WebModels.NodeResult::authFailure);
    }
}
