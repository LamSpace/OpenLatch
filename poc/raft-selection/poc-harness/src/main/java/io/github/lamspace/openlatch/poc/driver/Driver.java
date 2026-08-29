package io.github.lamspace.openlatch.poc.driver;

import io.github.lamspace.openlatch.poc.PocMain;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

/**
 * PoC driver（P2-01）：派生并管理 3 个节点 JVM 进程（destroyForcibly 真实杀主，
 * design D1），编排四类实验（bench / kill / snapshot / smoke），结果以 JSON 落盘
 * 供 P2-04 汇总（门槛测量口径见 spec 与 design D4/D11）。
 *
 * <p>用法：{@code PocMain driver <exp> --candidate ratis|jraft|noraft --round N
 * [--duration 300] [--keys 100000] [--workdir results] [--base-port 28000] [--jar <path>]}
 */
public final class Driver {

    /** 候选 → 适配器 FQCN（PocMain 反射加载）。 */
    private static final Map<String, String> ADAPTERS = Map.of(
            "ratis", "io.github.lamspace.openlatch.poc.ratis.RatisNodeAdapter",
            "jraft", "io.github.lamspace.openlatch.poc.jraft.JRaftNodeAdapter",
            "noraft", "io.github.lamspace.openlatch.poc.adapter.NoRaftAdapter");

    private record NodeSpec(int id, int raftPort, int clientPort, Path dataDir, Process process) { }

    private final String exp;
    private final String candidate;
    private final int round;
    private final long durationS;
    private final int keys;
    private final Path workdir;
    private final int basePort;
    private final Path jar;
    private final int nodeCount;
    private final List<NodeSpec> nodes = new ArrayList<>();
    private final InvariantMonitor monitor = new InvariantMonitor();
    private volatile boolean stopLoad;
    private long seqGen = 1000;

    private Driver(String[] args) throws Exception {
        this.exp = args[0];
        Map<String, String> a = PocMain.argMap(args, 1);
        this.candidate = a.getOrDefault("candidate", "noraft");
        if (!ADAPTERS.containsKey(candidate)) {
            throw new IllegalArgumentException("unknown candidate: " + candidate);
        }
        this.round = Integer.parseInt(a.getOrDefault("round", "0"));
        this.durationS = Long.parseLong(a.getOrDefault("duration", "300"));
        this.keys = Integer.parseInt(a.getOrDefault("keys", "100000"));
        this.workdir = Path.of(a.getOrDefault("workdir", "results"));
        this.basePort = Integer.parseInt(a.getOrDefault("base-port",
                String.valueOf(28000 + java.util.concurrent.ThreadLocalRandom.current().nextInt(0, 9) * 1000)));
        this.nodeCount = candidate.equals("noraft") ? 1 : 3;
        String jarProp = a.get("jar");
        if (jarProp == null) {
            jarProp = System.getProperty("poc.jar");
        }
        if (jarProp == null) {
            jarProp = Path.of(Driver.class.getProtectionDomain().getCodeSource()
                    .getLocation().toURI()).toString();
        }
        this.jar = Path.of(jarProp);
        Files.createDirectories(workdir);
    }

    /**
     * 入口。
     *
     * @param args 实验名 + 选项
     */
    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.err.println("usage: driver <bench|kill|snapshot|smoke> --candidate ...");
            System.exit(2);
        }
        System.exit(new Driver(args).run());
    }

    private int run() throws Exception {
        Json out;
        try {
            out = switch (exp) {
                case "smoke" -> smoke();
                case "bench" -> bench();
                case "kill" -> kill();
                case "snapshot" -> snapshot();
                default -> throw new IllegalArgumentException("unknown exp " + exp);
            };
        } catch (Exception e) {
            e.printStackTrace();
            stopCluster();
            System.err.println("DRIVER FAIL: " + e);
            return 1;
        }
        out.put("meta", new Json()
                        .put("candidate", candidate).put("exp", exp).put("round", round)
                        .put("java", System.getProperty("java.version"))
                        .put("cpus", Runtime.getRuntime().availableProcessors())
                        .put("jar", jar.getFileName().toString()))
                .put("endedAt", System.currentTimeMillis())
                .put("invariantViolated", monitor.violated())
                .put("invariantEvents", monitor.events().stream()
                        .map(e -> new Json().put("at", e.at()).put("tsMs", e.tsMs())
                                .put("detail", e.detail())).toList());
        Path f = workdir.resolve(String.format("%s-%s-%d.json", candidate, exp, round));
        Files.writeString(f, out.toString() + System.lineSeparator(), StandardCharsets.UTF_8);
        System.out.println("RESULT " + f.toAbsolutePath());
        stopCluster();
        return monitor.violated() ? 3 : 0;
    }

    // ---------- cluster ----------

    private void startCluster(long snapshotThreshold) throws Exception {
        // 清理陈旧数据（上一轮失败运行的 meta/log 会卡投票/回放）
        Path clusterDir = workdir.resolve(candidate + "-" + exp + "-" + round);
        if (Files.exists(clusterDir)) {
            try (var walk = Files.walk(clusterDir)) {
                for (Path p : walk.sorted(Comparator.reverseOrder()).toList()) {
                    Files.deleteIfExists(p);
                }
            }
        }
        String peers = buildPeers();
        for (int i = 1; i <= nodeCount; i++) {
            spawnNode(i, peers, snapshotThreshold);
        }
        for (NodeSpec n : nodes) {
            waitNodeReady(n);
        }
        waitLeader(30_000);
    }

    private String buildPeers() {
        StringBuilder sb = new StringBuilder();
        for (int i = 1; i <= nodeCount; i++) {
            if (i > 1) {
                sb.append(',');
            }
            sb.append(i).append("@127.0.0.1:").append(raftPort(i));
        }
        return sb.toString();
    }

    private void spawnNode(int id, String peers, long snapshotThreshold) throws IOException {
        Path dataDir = workdir.resolve(candidate + "-" + exp + "-" + round + "/node" + id);
        Files.createDirectories(dataDir);
        List<String> cmd = List.of(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-jar", jar.toString(), "node",
                "--adapter", ADAPTERS.get(candidate),
                "--node-id", String.valueOf(id),
                "--peers", peers,
                "--data-dir", dataDir.toString(),
                "--client-port", String.valueOf(clientPort(id)),
                "--raft-port", String.valueOf(raftPort(id)),
                "--snapshot-threshold", String.valueOf(snapshotThreshold));
        Process p = new ProcessBuilder(cmd)
                .redirectErrorStream(true)
                .redirectOutput(dataDir.resolve("node.log").toFile())
                .start();
        nodes.add(new NodeSpec(id, raftPort(id), clientPort(id), dataDir, p));
    }

    private void killNode(NodeSpec n) throws InterruptedException {
        n.process().destroyForcibly();
        n.process().waitFor();
    }

    private void stopCluster() {
        for (int i = nodes.size() - 1; i >= 0; i--) {
            NodeSpec n = nodes.get(i);
            try {
                if (n.process().isAlive()) {
                    try (NodeConn c = new NodeConn("127.0.0.1", n.clientPort())) {
                        c.request("SHUTDOWN");
                    }
                    n.process().waitFor(10, TimeUnit.SECONDS);
                }
            } catch (Exception ignored) {
                // best effort
            }
            if (n.process().isAlive()) {
                n.process().destroyForcibly();
            }
        }
    }

    private int raftPort(int id) {
        return basePort + 1000 + id;
    }

    private int clientPort(int id) {
        return basePort + id;
    }

    private void waitNodeReady(NodeSpec n) throws Exception {
        long deadline = System.currentTimeMillis() + 60_000;
        while (System.currentTimeMillis() < deadline) {
            if (!n.process().isAlive()) {
                throw new IOException("node " + n.id() + " died; see " + n.dataDir().resolve("node.log"));
            }
            try (NodeConn c = new NodeConn("127.0.0.1", n.clientPort())) {
                c.request("STAT");
                return;
            } catch (IOException e) {
                Thread.sleep(200);
            }
        }
        throw new IOException("node " + n.id() + " not ready");
    }

    /** 轮询全节点 STAT，返回当前 Leader 的 nodeId（无则 -1）。 */
    private int discoverLeader() {
        for (NodeSpec n : nodes) {
            try (NodeConn c = new NodeConn("127.0.0.1", n.clientPort())) {
                String[] a = c.request("STAT").split(" ");
                if (a.length > 1 && a[1].equals("1")) {
                    return n.id();
                }
            } catch (IOException ignored) {
                // node may be down or electing
            }
        }
        return -1;
    }

    private void waitLeader(long timeoutMs) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (discoverLeader() > 0) {
                return;
            }
            Thread.sleep(250);
        }
        throw new IOException("no leader within " + timeoutMs + "ms");
    }

    private NodeConn leaderConn() throws IOException {
        int lid = discoverLeader();
        if (lid < 0) {
            throw new IOException("no leader");
        }
        return new NodeConn("127.0.0.1", clientPort(lid));
    }

    private NodeSpec nodeById(int id) {
        return nodes.stream().filter(n -> n.id() == id).findFirst()
                .orElseThrow(() -> new IllegalArgumentException("no node " + id));
    }

    private static int requireLeader(int v) throws IOException {
        if (v < 0) {
            throw new IOException("no leader");
        }
        return v;
    }

    // ---------- line helpers ----------

    private String line(NodeConn c, String cmd) throws IOException {
        String r = c.request(cmd);
        if (r.startsWith("ERR")) {
            throw new IOException("line error [" + cmd + "]: " + r);
        }
        return r;
    }

    /** 一次 ACQ 往返（REENTRANT、lease 300s、不排队）。 */
    private String[] acquire(NodeConn c, long sid, String key) throws IOException {
        long seq = ++seqGen;
        String[] a = line(c, "ACQ " + sid + " " + seq + " " + key + " 0 1 300000 0").split(" ");
        return a; // RES seq status token leaseMs pos
    }

    /** 一次 REL 往返。 */
    private String[] release(NodeConn c, long sid, String key, long token) throws IOException {
        long seq = ++seqGen;
        return line(c, "REL " + sid + " " + seq + " " + key + " " + token + " 1").split(" ");
    }

    private String digestOf(int nodeId) throws IOException {
        try (NodeConn c = new NodeConn("127.0.0.1", clientPort(nodeId))) {
            return c.request("DUMP").split(" ")[1];
        }
    }

    // ---------- smoke ----------

    private Json smoke() throws Exception {
        startCluster(0);
        boolean acquireGranted;
        boolean mutexHeld;
        boolean releaseOk;
        boolean regained;
        boolean allSame;
        try (NodeConn c = leaderConn()) {
            line(c, "OPEN 4242 1");
            line(c, "OPEN 4243 2");
            String[] g = acquire(c, 4242, "skey");
            acquireGranted = g[2].equals("0");
            String[] b = acquire(c, 4243, "skey");
            mutexHeld = !b[2].equals("0");
            monitor.acquire("skey", 0, 4242, Long.parseLong(g[3]));
            String[] rel = release(c, 4242, "skey", Long.parseLong(g[3]));
            releaseOk = rel[2].equals("0");
            monitor.release("skey", Integer.parseInt(rel[2]), 4242);
            String[] g2 = acquire(c, 4243, "skey");
            regained = g2[2].equals("0");
            release(c, 4243, "skey", Long.parseLong(g2[3]));
            line(c, "SNAP");
        }
        String ref = digestOf(requireLeader(discoverLeader()));
        allSame = true;
        for (NodeSpec n : nodes) {
            if (!digestOf(n.id()).equals(ref)) {
                allSame = false;
            }
        }
        boolean pass = acquireGranted && mutexHeld && releaseOk && regained && allSame;
        return new Json()
                .put("leaderElected", true)
                .put("acquireGranted", acquireGranted)
                .put("mutexHeld", mutexHeld)
                .put("releaseOk", releaseOk)
                .put("regainedAfterRelease", regained)
                .put("allDigestsEqual", allSame)
                .put("pass", pass);
    }

    // ---------- bench ----------

    private Json bench() throws Exception {
        startCluster(0);
        Stats acq = new Stats();
        Stats rel = new Stats();
        long pairs = 0;
        long startMs;
        try (NodeConn c = leaderConn()) {
            line(c, "OPEN 5001 1");
            long deadline = System.currentTimeMillis() + durationS * 1000L;
            long warmEnd = System.currentTimeMillis() + 5_000;
            startMs = System.currentTimeMillis();
            while (System.currentTimeMillis() < deadline) {
                boolean warm = System.currentTimeMillis() < warmEnd;
                long t0 = System.nanoTime();
                String[] g = acquire(c, 5001, "benchkey");
                long t1 = System.nanoTime();
                if (!warm && g[2].equals("0")) {
                    acq.recordNanos(t1 - t0);
                }
                long token = Long.parseLong(g[3]);
                String[] rl = release(c, 5001, "benchkey", token);
                long t2 = System.nanoTime();
                if (!warm && rl[2].equals("0")) {
                    rel.recordNanos(t2 - t1);
                }
                monitor.acquire("benchkey", Integer.parseInt(g[2]), 5001, token);
                monitor.release("benchkey", Integer.parseInt(rl[2]), 5001);
                pairs++;
            }
        }
        long elapsedMs = System.currentTimeMillis() - startMs;
        return new Json()
                .put("durationS", durationS)
                .put("pairs", pairs)
                .put("throughputPairsPerS", pairs * 1000.0 / elapsedMs)
                .put("grantLatencyP50Ms", acq.pMs(50))
                .put("grantLatencyP99Ms", acq.pMs(99))
                .put("grantLatencyMeanMs", acq.meanMs())
                .put("releaseLatencyP50Ms", rel.pMs(50))
                .put("releaseLatencyP99Ms", rel.pMs(99));
    }

    // ---------- kill ----------

    private Json kill() throws Exception {
        startCluster(0);
        Thread load = new Thread(this::loadLoop, "kill-load");
        load.setDaemon(true);
        load.start();
        NodeConn holder = leaderConn();
        line(holder, "OPEN 9001 1");
        String[] g = acquire(holder, 9001, "holdkey");
        long holdToken = Long.parseLong(g[3]);
        int oldLeader = requireLeader(discoverLeader());

        // 确认授予已复制到全部节点，再杀主（否则断言无意义）
        waitDigests(digestOf(oldLeader), 3_000);

        monitor.setKillWindow(true);
        long tKill0 = System.nanoTime();
        killNode(nodeById(oldLeader));
        long exitMs = (System.nanoTime() - tKill0) / 1_000_000;

        long deadline = System.currentTimeMillis() + 30_000;
        int newLeader = -1;
        long tElectMs = -1;
        while (System.currentTimeMillis() < deadline) {
            int l = discoverLeader();
            if (l > 0 && l != oldLeader) {
                newLeader = l;
                tElectMs = (System.nanoTime() - tKill0) / 1_000_000;
                break;
            }
            Thread.sleep(50);
        }
        if (newLeader < 0) {
            throw new IOException("no new leader after kill");
        }

        long tServeMs = -1;
        boolean lockSurvived = false;
        try (NodeConn nl = new NodeConn("127.0.0.1", clientPort(newLeader))) {
            while (System.currentTimeMillis() < deadline) {
                try {
                    long seq = ++seqGen;
                    String[] probe = nl.request("ACQ 9001 " + seq + " probekey 0 1 300000 0").split(" ");
                    if (probe[2].equals("0")) {
                        tServeMs = (System.nanoTime() - tKill0) / 1_000_000;
                        release(nl, 9001, "probekey", Long.parseLong(probe[3]));
                        break;
                    }
                } catch (IOException ignored) {
                    // election / replication in progress
                }
                Thread.sleep(100);
            }
            if (tServeMs < 0) {
                throw new IOException("no serviceable leader within 30s");
            }
            String[] rel = nl.request("REL 9001 " + (++seqGen) + " holdkey " + holdToken + " 1").split(" ");
            lockSurvived = rel[2].equals("0");
            monitor.release("holdkey", Integer.parseInt(rel[2]), 9001);
        }
        stopLoad = true;
        load.join(2_000);
        monitor.setKillWindow(false);
        holder.close();
        return new Json()
                .put("oldLeader", oldLeader)
                .put("newLeader", newLeader)
                .put("processExitMs", exitMs)
                .put("tElectMs", tElectMs)
                .put("tServeMs", tServeMs)
                .put("recoveryTotalMs", tServeMs)
                .put("lockSurvived", lockSurvived);
    }

    private void loadLoop() {
        while (!stopLoad) {
            try (NodeConn c = leaderConn()) {
                String[] g = acquire(c, 6001, "loadkey");
                if (g[2].equals("0")) {
                    monitor.acquire("loadkey", 0, 6001, Long.parseLong(g[3]));
                    String[] rl = release(c, 6001, "loadkey", Long.parseLong(g[3]));
                    monitor.release("loadkey", Integer.parseInt(rl[2]), 6001);
                }
                Thread.sleep(50);
            } catch (Exception e) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException ie) {
                    return;
                }
            }
        }
    }

    private void waitDigests(String ref, long timeoutMs) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            boolean ok = true;
            for (NodeSpec n : nodes) {
                if (!digestOf(n.id()).equals(ref)) {
                    ok = false;
                }
            }
            if (ok) {
                return;
            }
            Thread.sleep(100);
        }
        throw new IOException("digests not converged before kill (precondition)");
    }

    // ---------- snapshot ----------

    private Json snapshot() throws Exception {
        long threshold = Math.max(1, keys / 2L);
        startCluster(threshold);
        long t0 = System.currentTimeMillis();
        long bulkMs;
        long snapMs;
        long snapSize;
        String leaderDigest;
        try (NodeConn c = leaderConn()) {
            line(c, "OPEN 7001 1");
            int inFlight = 0;
            int window = 256;
            int sent = 0;
            int drained = 0;
            TreeMap<Long, String> unmatched = new TreeMap<>();
            while (drained < keys) {
                while (inFlight < window && sent < keys) {
                    long seq = ++seqGen;
                    c.send("ACQ 7001 " + seq + " snap-" + sent + " 0 1 3600000 0");
                    unmatched.put(seq, "snap-" + sent);
                    sent++;
                    inFlight++;
                }
                String ln = c.readLine();
                if (ln == null) {
                    throw new IOException("bulk conn dropped");
                }
                String[] a = ln.split(" ");
                if (!a[2].equals("0")) {
                    throw new IOException("bulk acquire failed: " + ln);
                }
                unmatched.remove(Long.parseLong(a[1]));
                inFlight--;
                drained++;
            }
            if (!unmatched.isEmpty()) {
                throw new IOException("unmatched bulk seqs: " + unmatched);
            }
            bulkMs = System.currentTimeMillis() - t0;

            long ts = System.nanoTime();
            String[] snap = line(c, "SNAP").split(" ");
            snapMs = (System.nanoTime() - ts) / 1_000_000;
            snapSize = Long.parseLong(snap[2]);
            // 各节点（含 Follower）本地快照：重启恢复 = 自有快照 + 本地日志追赶（§7.3.1）
            for (NodeSpec n : nodes) {
                try (NodeConn nc = new NodeConn("127.0.0.1", n.clientPort())) {
                    nc.request("SNAP");
                }
            }
            // 快照后 delta：追赶必须重放增量才算一致（排除"只加载快照"的假阳性）
            for (int i = 0; i < 200; i++) {
                long seq = ++seqGen;
                String[] r = line(c, "ACQ 7001 " + seq + " delta-" + i + " 0 1 3600000 0").split(" ");
                if (!r[2].equals("0")) {
                    throw new IOException("delta acquire failed: " + r[2]);
                }
            }
            leaderDigest = c.request("DUMP").split(" ")[1];
        }

        // 杀一台非 Leader 节点（保留 data-dir → 重启走快照加载 + 日志追赶）
        int leader = requireLeader(discoverLeader());
        int victimId = leader == 2 ? 3 : 2;
        NodeSpec victim = nodeById(victimId);
        killNode(victim);
        nodes.remove(victim);
        long tRestart0 = System.currentTimeMillis();
        spawnNode(victimId, buildPeers(), threshold);
        waitNodeReady(nodeById(victimId));

        long catchupMs = -1;
        long rebuilds = -1;
        long rebuildFailures = -1;
        long deadline = System.currentTimeMillis() + 120_000;
        while (System.currentTimeMillis() < deadline) {
            try (NodeConn vc = new NodeConn("127.0.0.1", clientPort(victimId))) {
                if (vc.request("DUMP").split(" ")[1].equals(leaderDigest)) {
                    catchupMs = System.currentTimeMillis() - tRestart0;
                    String[] rb = vc.request("REBUILD").split(" ");
                    rebuilds = Long.parseLong(rb[1]);
                    rebuildFailures = Long.parseLong(rb[2]);
                    break;
                }
            } catch (IOException ignored) {
                // starting
            }
            Thread.sleep(200);
        }
        // 追赶期间 Leader 服务持续可用（写入探针）
        boolean leaderServed;
        try (NodeConn c = leaderConn()) {
            String[] probe = acquire(c, 7001, "afterkill");
            leaderServed = probe[2].equals("0");
            release(c, 7001, "afterkill", Long.parseLong(probe[3]));
        } catch (IOException e) {
            leaderServed = false;
        }
        return new Json()
                .put("keys", keys)
                .put("bulkMs", bulkMs)
                .put("bulkOpsPerS", keys * 1000.0 / Math.max(1, bulkMs))
                .put("snapshotWriteMs", snapMs)
                .put("snapshotBytes", snapSize)
                .put("victimId", victimId)
                .put("catchupMs", catchupMs)
                .put("victimRebuilds", rebuilds)
                .put("victimRebuildFailures", rebuildFailures)
                .put("digestEqual", catchupMs >= 0)
                .put("leaderServedDuringCatchup", leaderServed);
    }
}
