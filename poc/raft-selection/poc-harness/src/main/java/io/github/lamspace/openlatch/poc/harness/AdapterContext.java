package io.github.lamspace.openlatch.poc.harness;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 节点启动上下文（driver 经命令行传入，适配器解析）。
 *
 * @param nodeId            本节点 id（1 起）
 * @param peers             全部成员 nodeId → host:raftPort
 * @param dataDir           本节点数据目录（日志 + 快照）
 * @param clientPort        行协议监听端口（driver 接入）
 * @param raftPort          本节点 Raft 通信端口（冗余，便于日志）
 * @param electionTimeoutMs 选举超时（各库按语义透传；0 = 库默认）
 * @param snapshotThreshold 自动快照条目阈值；0 = 关闭自动快照（PoC 以手动 SNAP 为主）
 */
public record AdapterContext(
        int nodeId,
        Map<Integer, String> peers,
        String dataDir,
        int clientPort,
        int raftPort,
        long electionTimeoutMs,
        long snapshotThreshold) {

    /**
     * 解析 {@code id@host:port,id@host:port} 形态的成员表。
     *
     * @param spec 成员串
     * @return 有序 id → host:port 映射
     */
    public static Map<Integer, String> parsePeers(String spec) {
        Map<Integer, String> m = new LinkedHashMap<>();
        for (String part : spec.split(",")) {
            String[] kv = part.split("@");
            m.put(Integer.parseInt(kv[0].trim()), kv[1].trim());
        }
        return m;
    }
}
