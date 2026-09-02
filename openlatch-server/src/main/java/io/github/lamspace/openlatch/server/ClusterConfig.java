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

package io.github.lamspace.openlatch.server;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * 集群配置（详设 §9，spec"集群配置体系"；{@code openlatch.cluster.*} 键族）。
 *
 * <p><b>与 {@link ServerConfig} 的关系</b>：同一 Properties 文件的集群子集，
 * 独立记录而非扩展既有记录——保持 {@code ServerConfig} 构造签名稳定
 * （Phase 1 既有装配与测试不受扰动）。默认实例 {@link #disabled()} 即
 * "关闭集群 = Phase 1 单机行为"（同一二进制，§9）。
 *
 * <p><b>校验契约</b>：{@code enabled=true} 时 {@code node-id} 与 {@code peers}
 * 必填且 {@code peers} 必须包含本节点条目；缺失/非法以指明配置键的
 * {@link IllegalArgumentException} 快速失败，MUST NOT 静默降级为单机。
 * {@code clientAddresses} 为可选项：配置时逐条校验 {@code id@host:port} 格式
 * 与 id 唯一（未配置不阻塞启动，Leader 提示的地址字段降级为空串，
 * 客户端以种子发现兜底，见 s3 design D4）。
 *
 * @param enabled           是否启用集群（{@code false} 即 Phase 1 单机）
 * @param nodeId            本节点唯一 id（启用时必填，≥1；参与 sessionId 高位编码）
 * @param peers             全成员列表 {@code id@host:port}（启用时必填，含本节点）
 * @param clientAddresses   各节点客户端接入地址列表 {@code id@host:port}（可选，
 *                          v2 Leader 提示与 {@code CLUSTER_VIEW} 作答的地址来源）
 * @param raftPort          本节点 Raft 复制通信监听端口
 * @param dataDir           Raft 日志与快照目录
 * @param snapshotThreshold 快照触发条目数（S4 起接入 Ratis 自动触发阈值）
 * @param electionTimeoutMs 选举超时上界（Raft 层语义透传）
 * @param logSegmentBytes   Raft 日志 segment 上限字节（Raft 层语义透传，
 *                          {@code 0} 取库默认；S4 追赶用例用小值驱动截断与
 *                          快照安装流）
 */
public record ClusterConfig(
        boolean enabled,
        int nodeId,
        List<String> peers,
        List<String> clientAddresses,
        int raftPort,
        String dataDir,
        long snapshotThreshold,
        long electionTimeoutMs,
        int logSegmentBytes) {

    /**
     * 兼容构造：未配置 {@code clientAddresses} 的 7 参形态（地址映射为空表，
     * Leader 提示地址降级为空串，日志 segment 取库默认）。
     *
     * @param enabled           是否启用集群
     * @param nodeId            本节点 id
     * @param peers             全成员列表
     * @param raftPort          Raft 复制端口
     * @param dataDir           数据目录
     * @param snapshotThreshold 快照触发条目数
     * @param electionTimeoutMs 选举超时
     */
    public ClusterConfig(boolean enabled, int nodeId, List<String> peers, int raftPort,
                         String dataDir, long snapshotThreshold, long electionTimeoutMs) {
        this(enabled, nodeId, peers, List.of(), raftPort, dataDir, snapshotThreshold,
                electionTimeoutMs, 0);
    }

    /**
     * 兼容构造：未配置 {@code logSegmentBytes} 的 8 参形态（segment 取库默认）。
     *
     * @param enabled           是否启用集群
     * @param nodeId            本节点 id
     * @param peers             全成员列表
     * @param clientAddresses   客户端接入地址表（可为空表）
     * @param raftPort          Raft 复制端口
     * @param dataDir           数据目录
     * @param snapshotThreshold 快照触发条目数
     * @param electionTimeoutMs 选举超时
     */
    public ClusterConfig(boolean enabled, int nodeId, List<String> peers,
                         List<String> clientAddresses, int raftPort, String dataDir,
                         long snapshotThreshold, long electionTimeoutMs) {
        this(enabled, nodeId, peers, clientAddresses, raftPort, dataDir, snapshotThreshold,
                electionTimeoutMs, 0);
    }

    /** 配置键前缀（§9）。 */
    public static final String KEY_PREFIX = "openlatch.cluster.";

    /** 默认关闭（单机）。 */
    public static final boolean DEFAULT_ENABLED = false;
    /** 默认 Raft 复制端口。 */
    public static final int DEFAULT_RAFT_PORT = 9411;
    /** 默认数据目录。 */
    public static final String DEFAULT_DATA_DIR = "./data";
    /** 默认快照触发条目数。 */
    public static final long DEFAULT_SNAPSHOT_THRESHOLD = 1_000_000L;
    /** 默认选举超时（毫秒）。 */
    public static final long DEFAULT_ELECTION_TIMEOUT_MS = 3_000L;
    /** 默认日志 segment 上限（{@code 0}=取 Raft 库默认）。 */
    public static final int DEFAULT_LOG_SEGMENT_BYTES = 0;

    /**
     * 全默认（关闭集群）。
     *
     * @return 单机配置
     */
    public static ClusterConfig disabled() {
        return new ClusterConfig(DEFAULT_ENABLED, 0, List.of(), List.of(), DEFAULT_RAFT_PORT,
                DEFAULT_DATA_DIR, DEFAULT_SNAPSHOT_THRESHOLD, DEFAULT_ELECTION_TIMEOUT_MS,
                DEFAULT_LOG_SEGMENT_BYTES);
    }

    /**
     * 从 Properties 解析集群子集；键缺省回落默认值。
     *
     * @param props 已加载的属性表
     * @return 解析并校验后的配置
     * @throws IllegalArgumentException 任一配置项非法或启用时必填项缺失
     */
    public static ClusterConfig fromProperties(Properties props) {
        boolean enabled = boolOf(props, KEY_PREFIX + "enabled", DEFAULT_ENABLED);
        int nodeId = intOf(props, KEY_PREFIX + "node-id", 0);
        List<String> peers = listOf(props, KEY_PREFIX + "peers");
        List<String> clientAddresses = listOf(props, KEY_PREFIX + "client-addresses");
        int raftPort = intOf(props, KEY_PREFIX + "raft-port", DEFAULT_RAFT_PORT);
        String dataDir = props.getProperty(KEY_PREFIX + "data-dir", DEFAULT_DATA_DIR);
        long snapshotThreshold = longOf(props, KEY_PREFIX + "snapshot-threshold", DEFAULT_SNAPSHOT_THRESHOLD);
        long electionTimeoutMs = longOf(props, KEY_PREFIX + "election-timeout-ms", DEFAULT_ELECTION_TIMEOUT_MS);
        int logSegmentBytes = intOf(props, KEY_PREFIX + "log-segment-bytes", DEFAULT_LOG_SEGMENT_BYTES);
        ClusterConfig cfg = new ClusterConfig(enabled, nodeId, peers, clientAddresses, raftPort,
                dataDir, snapshotThreshold, electionTimeoutMs, logSegmentBytes);
        cfg.validate();
        return cfg;
    }

    /**
     * 从 {@code path} 指向的 Properties 文件加载集群配置；{@code path} 为
     * {@code null} 或空白返回 {@link #disabled()}。
     *
     * @param path 配置文件路径（与 {@link ServerConfig#load(String)} 同源）
     * @return 加载后的配置
     * @throws IllegalArgumentException 文件不可读或配置项非法
     */
    public static ClusterConfig load(String path) {
        if (path == null || path.isBlank()) {
            return disabled();
        }
        Properties props = new Properties();
        Path file = Path.of(path);
        try (InputStream in = Files.newInputStream(file)) {
            props.load(in);
        } catch (IOException e) {
            throw new IllegalArgumentException("无法读取配置文件: " + file + " (" + e.getMessage() + ")");
        }
        return fromProperties(props);
    }

    /**
     * 全量校验（spec"缺必填项启动失败"）：{@code enabled=true} 时
     * {@code node-id >= 1}；{@code peers} 非空、逐条
     * {@code id@host:port} 合法、id 唯一，且必须包含本节点的条目；
     * {@code clientAddresses} 可选——未配置直接放行，配置时逐条
     * {@code id@host:port} 合法且 id 唯一；{@code raftPort} 合法端口；
     * {@code dataDir} 非空白；{@code snapshotThreshold >= 1}；
     * {@code electionTimeoutMs > 0}；{@code logSegmentBytes >= 0}。
     *
     * @throws IllegalArgumentException 任一配置项非法（消息指明配置键）
     */
    public void validate() {
        if (!enabled) {
            return;
        }
        if (nodeId < 1) {
            throw new IllegalArgumentException(
                    "配置项 " + KEY_PREFIX + "node-id 缺失或非法（启用集群时应为 >=1 的整数）: " + nodeId);
        }
        if (peers.isEmpty()) {
            throw new IllegalArgumentException(
                    "配置项 " + KEY_PREFIX + "peers 缺失（启用集群时应为 id@host:port 列表，含本节点）");
        }
        boolean selfIncluded = false;
        List<Integer> ids = new ArrayList<>();
        for (String p : peers) {
            int at = p.indexOf('@');
            String hostPort = at >= 0 ? p.substring(at + 1).trim() : p;
            if (at >= 0) {
                try {
                    int id = Integer.parseInt(p.substring(0, at).trim());
                    ids.add(id);
                    selfIncluded |= id == nodeId;
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException(
                            "配置项 " + KEY_PREFIX + "peers 非法（id 应为整数）: " + p);
                }
            }
            int colon = hostPort.lastIndexOf(':');
            if (at < 0 || colon <= 0 || colon == hostPort.length() - 1
                    || !hostPort.substring(colon + 1).chars().allMatch(Character::isDigit)) {
                throw new IllegalArgumentException(
                        "配置项 " + KEY_PREFIX + "peers 非法（应为 id@host:port）: " + p);
            }
        }
        if (ids.stream().distinct().count() != ids.size()) {
            throw new IllegalArgumentException("配置项 " + KEY_PREFIX + "peers 含重复节点 id: " + peers);
        }
        if (!selfIncluded) {
            throw new IllegalArgumentException("配置项 " + KEY_PREFIX + "peers 必须包含本节点（node-id="
                    + nodeId + "）的条目: " + peers);
        }
        // 可选地址映射：未配置放行（降级为空串提示），配置则逐条校验且 id 唯一。
        List<Integer> addrIds = new ArrayList<>();
        for (String a : clientAddresses) {
            addrIds.add(entryId(a, KEY_PREFIX + "client-addresses"));
        }
        if (addrIds.stream().distinct().count() != addrIds.size()) {
            throw new IllegalArgumentException(
                    "配置项 " + KEY_PREFIX + "client-addresses 含重复节点 id: " + clientAddresses);
        }
        if (raftPort < 1 || raftPort > 65535) {
            throw new IllegalArgumentException(
                    "配置项 " + KEY_PREFIX + "raft-port 非法（应为 1-65535）: " + raftPort);
        }
        if (dataDir.isBlank()) {
            throw new IllegalArgumentException("配置项 " + KEY_PREFIX + "data-dir 不能为空白");
        }
        if (snapshotThreshold < 1) {
            throw new IllegalArgumentException(
                    "配置项 " + KEY_PREFIX + "snapshot-threshold 非法（应 >= 1）: " + snapshotThreshold);
        }
        if (electionTimeoutMs <= 0) {
            throw new IllegalArgumentException(
                    "配置项 " + KEY_PREFIX + "election-timeout-ms 非法（应 > 0）: " + electionTimeoutMs);
        }
        if (logSegmentBytes < 0) {
            throw new IllegalArgumentException(
                    "配置项 " + KEY_PREFIX + "log-segment-bytes 非法（应 >= 0，0 取库默认）: " + logSegmentBytes);
        }
    }

    /**
     * 解析布尔配置项（仅 {@code true} 当真，大小写不敏感）。
     *
     * @param props    属性表
     * @param key      配置键
     * @param fallback 缺省回落值
     * @return 配置值或回落值
     */
    private static boolean boolOf(Properties props, String key, boolean fallback) {
        String v = props.getProperty(key);
        return v == null || v.isBlank() ? fallback : Boolean.parseBoolean(v.trim());
    }

    /**
     * 解析整数配置项；非整数抛 {@link IllegalArgumentException} 指明配置键。
     *
     * @param props    属性表
     * @param key      配置键
     * @param fallback 缺省回落值
     * @return 配置值或回落值
     * @throws IllegalArgumentException 值非整数
     */
    private static int intOf(Properties props, String key, int fallback) {
        String v = props.getProperty(key);
        if (v == null || v.isBlank()) {
            return fallback;
        }
        try {
            return Integer.parseInt(v.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("配置项 " + key + " 非法（应为整数）: " + v);
        }
    }

    /**
     * 解析长整数配置项；非长整数抛 {@link IllegalArgumentException} 指明配置键。
     *
     * @param props    属性表
     * @param key      配置键
     * @param fallback 缺省回落值
     * @return 配置值或回落值
     * @throws IllegalArgumentException 值非长整数
     */
    private static long longOf(Properties props, String key, long fallback) {
        String v = props.getProperty(key);
        if (v == null || v.isBlank()) {
            return fallback;
        }
        try {
            return Long.parseLong(v.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("配置项 " + key + " 非法（应为长整数）: " + v);
        }
    }

    /**
     * 解析逗号分隔列表配置项（去空白、去空项）。
     *
     * @param props 属性表
     * @param key   配置键
     * @return 列表（不可变），缺省为空
     */
    private static List<String> listOf(Properties props, String key) {
        String v = props.getProperty(key);
        if (v == null || v.isBlank()) {
            return List.of();
        }
        List<String> out = new ArrayList<>();
        for (String part : v.split(",")) {
            String t = part.trim();
            if (!t.isEmpty()) {
                out.add(t);
            }
        }
        return List.copyOf(out);
    }

    /**
     * 本节点的成员 id 表示（Ratis {@code RaftPeerId} 字符串，与
     * {@code RaftSubsystem} 装配约定一致："n" + nodeId）。
     *
     * @return 本节点 peer id 字符串
     */
    public String selfPeerId() {
        return "n" + nodeId;
    }

    /**
     * 客户端接入地址映射（nodeId → {@code host:port}），由可选配置键
     * {@code openlatch.cluster.client-addresses} 解析；构造前须已 {@link #validate()}。
     *
     * @return 不可变映射；未配置为空表
     */
    public Map<Integer, String> clientAddressMap() {
        Map<Integer, String> map = new LinkedHashMap<>();
        for (String entry : clientAddresses) {
            int at = entry.indexOf('@');
            map.put(Integer.parseInt(entry.substring(0, at).trim()),
                    entry.substring(at + 1).trim());
        }
        return Map.copyOf(map);
    }

    /**
     * 指定节点的客户端接入地址。
     *
     * @param nodeId 目标节点 id
     * @return {@code host:port}；未配置该节点映射时为空串（提示降级，
     *         客户端以种子发现兜底，design D4）
     */
    public String clientAddress(int nodeId) {
        return clientAddressMap().getOrDefault(nodeId, "");
    }

    /**
     * 校验并提取 {@code id@host:port} 形态列表项的节点 id。
     *
     * @param entry 列表项
     * @param key   配置键（错误信息定位用）
     * @return 节点 id
     * @throws IllegalArgumentException 形态非法（消息指明配置键与非法项）
     */
    private static int entryId(String entry, String key) {
        int at = entry.indexOf('@');
        if (at <= 0) {
            throw new IllegalArgumentException("配置项 " + key + " 非法（应为 id@host:port）: " + entry);
        }
        int id;
        try {
            id = Integer.parseInt(entry.substring(0, at).trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("配置项 " + key + " 非法（id 应为整数）: " + entry);
        }
        String hostPort = entry.substring(at + 1).trim();
        int colon = hostPort.lastIndexOf(':');
        if (colon <= 0 || colon == hostPort.length() - 1
                || !hostPort.substring(colon + 1).chars().allMatch(Character::isDigit)) {
            throw new IllegalArgumentException("配置项 " + key + " 非法（应为 id@host:port）: " + entry);
        }
        return id;
    }
}
