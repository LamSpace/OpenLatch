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

package io.github.lamspace.openlatch.server.metrics;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * 测试侧 Prometheus 0.0.4 文本exposition解析器（L2 断言工具）：新
 * Prometheus 客户端库不提供官方解析器，文本格式合规性以真 Prometheus
 * 抓取联调验证为准，本工具仅承担
 * "线名/标签/值"的结构化提取以便逐项差值断言。
 *
 * <p>支持：{@code name{label="value",...} number} 样本行；跳过注释与空行；
 * 标签值内转义引号不出现于本词表（无分隔符歧义），按简单逗号切分。
 */
public final class MetricsText {

    /**
     * 私有构造：工具类。
     */
    private MetricsText() {
    }

    /**
     * 一条样本：指标名 + 标签表 + 数值。
     *
     * @param name   指标线名
     * @param labels 标签（插入序）
     * @param value  样本值
     */
    public record Sample(String name, Map<String, String> labels, double value) {

        /**
         * 序列标识：线名 + 按 key 升序的稳定化标签串（差值比对的键）。
         *
         * @return 稳定序列键
         */
        public String seriesKey() {
            return name + new TreeMap<>(labels);
        }
    }

    /**
     * 解析 exposition 文本为样本列表。
     *
     * @param text {@code GET /metrics} 响应体
     * @return 样本列表（顺序保留）
     * @throws IllegalStateException 样本行不可解析（格式违约即测试失败）
     */
    public static List<Sample> parse(String text) {
        List<Sample> out = new ArrayList<>();
        for (String raw : text.split("\n")) {
            String line = raw.trim();
            if (line.isEmpty() || line.startsWith("#")) {
                continue;
            }
            int sp = line.lastIndexOf(' ');
            if (sp <= 0) {
                throw new IllegalStateException("不可解析的样本行: " + line);
            }
            String head = line.substring(0, sp);
            double value;
            try {
                value = Double.parseDouble(line.substring(sp + 1));
            } catch (NumberFormatException e) {
                throw new IllegalStateException("样本值非法: " + line, e);
            }
            int brace = head.indexOf('{');
            String name;
            Map<String, String> labels = new LinkedHashMap<>();
            if (brace < 0) {
                name = head;
            } else {
                name = head.substring(0, brace);
                String body = head.substring(brace + 1, head.length() - 1);
                for (String pair : body.split(",")) {
                    int eq = pair.indexOf('=');
                    if (eq <= 0) {
                        throw new IllegalStateException("标签非法: " + line);
                    }
                    labels.put(pair.substring(0, eq).trim(),
                            pair.substring(eq + 1).replace("\"", "").trim());
                }
            }
            out.add(new Sample(name, labels, value));
        }
        return out;
    }

    /**
     * 解析并聚合为"序列键 → 值"的映射（同名多标签线共存）。
     *
     * @param text exposition 文本
     * @return 序列映射
     */
    public static Map<String, Double> samples(String text) {
        Map<String, Double> map = new LinkedHashMap<>();
        for (Sample s : parse(text)) {
            map.put(s.seriesKey(), s.value());
        }
        return map;
    }
}
