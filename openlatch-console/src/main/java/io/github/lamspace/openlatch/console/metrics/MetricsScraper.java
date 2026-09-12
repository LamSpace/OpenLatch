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

package io.github.lamspace.openlatch.console.metrics;

import io.github.lamspace.openlatch.console.ConsoleConfig;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 节点指标端点代理抓取器：
 * 拉取 {@code http://<host>:<metrics-port>/metrics}（Prometheus 文本），
 * 解析概览页曲线所需的三条线（{@code openlatch_server_locks_held} 按 type
 * 求和、{@code openlatch_server_waiters}、{@code openlatch_server_sessions}），
 * 样本进控制台内存环形缓冲（每节点最近 {@value #CAPACITY} 点；页面刷新
 * 即追加采样，不要求跨重启保序列）。
 *
 * <p><b>降级语义</b>：连接失败/超时/非 200/解析不到任何目标线——返回
 * {@link Optional#empty()}，调用方（Web 层）据实标注"指标不可用"，
 * MUST NOT 以零值冒充、MUST NOT 使整页错误。
 *
 * <p><b>线程模型</b>：多 Web 线程并发抓取不同/相同节点：环形缓冲按节点
 * {@code synchronized} 追加；HttpClient 天然线程安全。
 */
@Component
public final class MetricsScraper {

    /** 每节点环形样本容量（约一次刷新周期 × 10 分钟量级）。 */
    static final int CAPACITY = 120;

    /** 节点指标 HTTP 客户端（连接超时 2s，短于页面预算）。 */
    private final HttpClient http = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(2))
            .build();

    /**
     * 构造抓取器（Spring 组件：HTTP 客户端随字段初始化，无显式资源持有）。
     */
    public MetricsScraper() {
    }
    /** 展示名 → 环形样本序。 */
    private final Map<String, Deque<Sample>> ring = new ConcurrentHashMap<>();

    /**
     * 一条概览曲线样本（epoch 时刻 + 三线读数）。
     *
     * @param atMs     采样时刻
     * @param held     持有中条目总数（锁+Semaphore 求和）
     * @param waiters  等待者总数
     * @param sessions 活跃会话数
     */
    public record Sample(long atMs, double held, double waiters, double sessions) {
    }

    /**
     * 抓取并记入环形缓冲。
     *
     * @param display     节点展示名（host:port）
     * @param address     节点地址
     * @param metricsPort 指标端口
     * @return 本次样本；不可用时为 {@link Optional#empty()}（已同步标注，
     *         缓冲保持旧序列不掺假点）
     */
    public Optional<Sample> scrapeAndRecord(String display, ConsoleConfig.Address address,
                                            int metricsPort) {
        Optional<Sample> sample = scrape(address, metricsPort);
        if (sample.isPresent()) {
            Deque<Sample> q = ring.computeIfAbsent(display, k -> new ArrayDeque<>());
            synchronized (q) {
                q.addFirst(sample.get());
                trim(q);
            }
        }
        return sample;
    }

    /**
     * 节点的近期样本序（新→旧）。
     *
     * @param display 节点展示名
     * @return 不可变快照（无样本返回空表）
     */
    public List<Sample> samples(String display) {
        Deque<Sample> q = ring.get(display);
        if (q == null) {
            return List.of();
        }
        synchronized (q) {
            trim(q);
            return List.copyOf(q);
        }
    }

    /**
     * 单次 HTTP 抓取与解析（不触碰缓冲）。
     *
     * @param address     节点地址
     * @param metricsPort 指标端口
     * @return 样本；任何失败为空
     */
    private Optional<Sample> scrape(ConsoleConfig.Address address, int metricsPort) {
        try {
            HttpRequest req = HttpRequest.newBuilder(
                            URI.create("http://" + address.host() + ":" + metricsPort + "/metrics"))
                    .timeout(Duration.ofSeconds(3))
                    .GET().build();
            HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() != 200) {
                return Optional.empty();
            }
            return parse(resp.body());
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    /**
     * 解析 Prometheus 文本的三条目标线。
     *
     * @param body 抓取响应体
     * @return 样本（三线齐备才算成功——半缺序列的曲线会误导运维）
     */
    static Optional<Sample> parse(String body) {
        double held = Double.NaN;
        double waiters = Double.NaN;
        double sessions = Double.NaN;
        double heldSum = 0;
        boolean heldSeen = false;
        for (String raw : body.split("\n")) {
            String line = raw.trim();
            if (line.isEmpty() || line.startsWith("#")) {
                continue;
            }
            int sp = line.indexOf(' ');
            if (sp < 0) {
                continue;
            }
            String name = line.substring(0, sp);
            double value;
            try {
                value = Double.parseDouble(line.substring(sp + 1).trim());
            } catch (NumberFormatException e) {
                continue;
            }
            if (name.startsWith("openlatch_server_locks_held")) {
                heldSum += value;
                heldSeen = true;
            } else if (name.equals("openlatch_server_waiters")) {
                waiters = value;
            } else if (name.equals("openlatch_server_sessions")) {
                sessions = value;
            }
        }
        if (!heldSeen || Double.isNaN(waiters) || Double.isNaN(sessions)) {
            return Optional.empty();
        }
        held = heldSum;
        return Optional.of(new Sample(System.currentTimeMillis(), held, waiters, sessions));
    }

    /**
     * 环形缓冲裁剪（追加与读取双侧防御）。
     *
     * @param q 目标队列
     */
    private static void trim(Deque<Sample> q) {
        // 队尾即最旧样本：容量外逐出（新→旧序，addFirst 写入）。
        while (q.size() > CAPACITY) {
            q.pollLast();
        }
    }
}
