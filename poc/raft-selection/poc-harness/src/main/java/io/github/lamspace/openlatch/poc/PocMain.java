package io.github.lamspace.openlatch.poc;

import io.github.lamspace.openlatch.core.CoreConfig;
import io.github.lamspace.openlatch.poc.driver.Driver;
import io.github.lamspace.openlatch.poc.harness.AdapterContext;
import io.github.lamspace.openlatch.poc.harness.LineProtocolServer;
import io.github.lamspace.openlatch.poc.harness.LockStateMachineCore;
import io.github.lamspace.openlatch.poc.harness.PocNodeAdapter;

import java.util.Map;

/**
 * PoC 统一入口：{@code PocMain node ...} 启动库节点，{@code PocMain driver ...}
 * 运行实验（driver 以 ProcessBuilder 派生 node 进程，可 destroyForcibly 真实杀主）。
 */
public final class PocMain {

    private PocMain() {
    }

    /**
     * 入口。
     *
     * @param args node|driver + 参数
     */
    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.err.println("usage: PocMain node|driver ...");
            System.exit(2);
        }
        switch (args[0]) {
            case "node" -> node(argMap(args, 1));
            case "driver" -> Driver.main(java.util.Arrays.copyOfRange(args, 1, args.length));
            default -> {
                System.err.println("unknown command: " + args[0]);
                System.exit(2);
            }
        }
    }

    private static void node(Map<String, String> a) throws Exception {
        AdapterContext ctx = new AdapterContext(
                Integer.parseInt(a.get("node-id")),
                AdapterContext.parsePeers(a.get("peers")),
                a.get("data-dir"),
                Integer.parseInt(a.get("client-port")),
                Integer.parseInt(a.get("raft-port")),
                Long.parseLong(a.getOrDefault("election-timeout-ms", "0")),
                Long.parseLong(a.getOrDefault("snapshot-threshold", "0")));

        Class<?> cls = Class.forName(a.get("adapter"));
        PocNodeAdapter adapter = (PocNodeAdapter) cls.getDeclaredConstructor().newInstance();
        LockStateMachineCore core = new LockStateMachineCore(new CoreConfig());

        long t0 = System.nanoTime();
        adapter.start(ctx, core);
        LineProtocolServer server = new LineProtocolServer(adapter, core, ctx.clientPort());
        // stdout 单行就绪标记，供 driver 等待进程存活
        System.out.println("NODE_READY " + ctx.nodeId()
                + " " + ((System.nanoTime() - t0) / 1_000_000) + "ms");
        Thread.currentThread().join();
    }

    /** 解析 {@code --key value} 形态参数表。 */
    public static Map<String, String> argMap(String[] args, int from) {
        Map<String, String> m = new java.util.LinkedHashMap<>();
        for (int i = from; i < args.length; i++) {
            if (args[i].startsWith("--") && i + 1 < args.length && !args[i + 1].startsWith("--")) {
                m.put(args[i].substring(2), args[++i]);
            } else if (args[i].startsWith("--")) {
                m.put(args[i].substring(2), "true");
            }
        }
        return m;
    }
}
