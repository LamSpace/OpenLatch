package io.github.lamspace.openlatch.poc.harness;

import com.google.protobuf.ByteString;
import io.github.lamspace.openlatch.poc.proto.RaftPoc.AcquirePayload;
import io.github.lamspace.openlatch.poc.proto.RaftPoc.ApplyResult;
import io.github.lamspace.openlatch.poc.proto.RaftPoc.RaftEntryType;
import io.github.lamspace.openlatch.poc.proto.RaftPoc.RaftLogEntry;
import io.github.lamspace.openlatch.poc.proto.RaftPoc.ReleasePayload;
import io.github.lamspace.openlatch.poc.proto.RaftPoc.SessionPayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

/**
 * 节点最小行协议服务端（design D1）。命令（单行、空格分隔）：
 * <pre>
 * OPEN  &lt;sidT&gt; &lt;seq&gt;                          → RES &lt;seq&gt; &lt;status&gt; 0 0 0
 * ACQ   &lt;sidT&gt; &lt;seq&gt; &lt;key&gt; &lt;lockType&gt; &lt;tid&gt; &lt;leaseMs&gt; &lt;qib&gt; → RES ...
 * REL   &lt;sidT&gt; &lt;seq&gt; &lt;key&gt; &lt;token&gt; &lt;tid&gt;      → RES ...
 * NOOP  &lt;seq&gt;                                   → RES ...
 * STAT                                           → STAT &lt;isLeader&gt; &lt;leaderId&gt; &lt;term&gt; &lt;digest&gt; &lt;lockCount&gt;
 * SNAP                                           → SNAP OK &lt;size&gt; / SNAP ERR &lt;msg&gt;
 * DUMP                                           → DUMP &lt;digest&gt; &lt;lockCount&gt;
 * REBUILD                                        → REBUILD &lt;count&gt; &lt;failures&gt;
 * KILL                                           → KILL BYE 然后 halt(137)（近似 kill -9）
 * SHUTDOWN                                       → 优雅关停退出
 * </pre>
 * RES 行恒带 seq 回显（bulk 阶段 pipeline 匹配用）。非 Leader 提案回
 * {@code ERR NOT_LEADER <leaderId>}（§3.1 重定向语义的行协议投影）。
 */
public final class LineProtocolServer implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(LineProtocolServer.class);
    private static final long PROPOSE_TIMEOUT_MS = 15_000;

    private final PocNodeAdapter adapter;
    private final LockStateMachineCore core;
    private final ServerSocket serverSocket;
    private final Thread acceptor;
    private final java.util.concurrent.ExecutorService pool =
            java.util.concurrent.Executors.newFixedThreadPool(128, r -> {
                Thread t = new Thread(r, "line-protocol-worker");
                t.setDaemon(true);
                return t;
            });
    private volatile boolean running = true;

    /**
     * 绑定端口并启动接受线程。
     *
     * @param adapter 库适配器
     * @param core    共享状态机内核
     * @param port    监听端口
     */
    public LineProtocolServer(PocNodeAdapter adapter, LockStateMachineCore core, int port)
            throws IOException {
        this.adapter = adapter;
        this.core = core;
        this.serverSocket = new ServerSocket(port);
        this.acceptor = new Thread(this::acceptLoop, "line-protocol-acceptor");
        this.acceptor.setDaemon(true);
        this.acceptor.start();
    }

    private void acceptLoop() {
        while (running) {
            try {
                Socket s = serverSocket.accept();
                Thread t = new Thread(() -> handle(s), "line-protocol-conn");
                t.setDaemon(true);
                t.start();
            } catch (IOException e) {
                if (running) {
                    LOG.warn("accept failed", e);
                }
                return;
            }
        }
    }

    private void handle(Socket socket) {
        try (socket;
             BufferedReader in = new BufferedReader(
                     new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
             BufferedWriter out = new BufferedWriter(
                     new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = in.readLine()) != null) {
                String reply = dispatch(line, out);
                if (reply == null) {
                    // 异步提案类命令：回执由工作线程写回
                    continue;
                }
                emit(out, reply);
            }
        } catch (IOException e) {
            LOG.debug("conn closed", e);
        }
    }

    private String dispatch(String line, BufferedWriter out) {
        String[] a = line.split(" ");
        try {
            return switch (a[0]) {
                case "OPEN" -> asyncPropose(out, a[2], RaftEntryType.SESSION_OPEN,
                        SessionPayload.newBuilder().setSessionId(Long.parseLong(a[1])).build());
                case "ACQ" -> asyncPropose(out, a[2], RaftEntryType.LOCK_ACQUIRE_ENTRY,
                        AcquirePayload.newBuilder()
                                .setSessionId(Long.parseLong(a[1]))
                                .setRequestId(Long.parseLong(a[2]))
                                .setKey(a[3])
                                .setLockType(Integer.parseInt(a[4]))
                                .setThreadId(Long.parseLong(a[5]))
                                .setRequestedLeaseMs(Long.parseLong(a[6]))
                                .setQueueIfBusy(a[7].equals("1"))
                                .build());
                case "REL" -> asyncPropose(out, a[2], RaftEntryType.LOCK_RELEASE_ENTRY,
                        ReleasePayload.newBuilder()
                                .setSessionId(Long.parseLong(a[1]))
                                .setKey(a[3])
                                .setLeaseToken(Long.parseLong(a[4]))
                                .setThreadId(Long.parseLong(a[5]))
                                .build());
                case "NOOP" -> {
                    RaftLogEntry e = RaftLogEntry.newBuilder()
                            .setType(RaftEntryType.NOOP)
                            .setSeq(Long.parseLong(a[1]))
                            .setWallClockMs(System.currentTimeMillis())
                            .build();
                    pool.submit(() -> emit(out, completePropose(a[1], e)));
                    yield null;
                }
                case "STAT" -> "STAT " + (adapter.isLeader() ? 1 : 0) + " "
                        + adapter.leaderNodeId() + " " + adapter.term() + " "
                        + core.digest() + " " + core.shadow().lockCount();
                case "DUMP" -> "DUMP " + core.digest() + " " + core.shadow().lockCount();
                case "SNAP" -> {
                    adapter.triggerSnapshot();
                    yield "SNAP OK " + adapter.lastSnapshotBytes();
                }
                case "REBUILD" -> "REBUILD " + core.rebuildCount + " " + core.rebuildFailures;
                case "KILL" -> {
                    LOG.info("KILL received, halting with code 137 (approx. SIGKILL)");
                    emit(out, "KILL BYE");
                    try {
                        Thread.sleep(50);
                    } catch (InterruptedException ignored) {
                        // fall through to halt
                    }
                    Runtime.getRuntime().halt(137);
                    yield null;
                }
                case "SHUTDOWN" -> {
                    Thread stopper = new Thread(() -> {
                        try {
                            adapter.stop();
                        } catch (Exception ignored) {
                            // best effort
                        }
                        Runtime.getRuntime().halt(0);
                    });
                    stopper.setDaemon(true);
                    stopper.start();
                    yield "SHUTDOWN BYE";
                }
                default -> "ERR UNKNOWN " + a[0];
            };
        } catch (Exception e) {
            return "ERR EX " + e.getClass().getSimpleName() + ":" + e.getMessage();
        }
    }

    /**
     * 投递提案到工作线程（读线程不阻塞），回执异步写回。
     *
     * @return 恒为 null（标记该命令已异步处理）
     */
    private String asyncPropose(BufferedWriter out, String seq, RaftEntryType type,
                                com.google.protobuf.Message payload) {
        RaftLogEntry e = RaftLogEntry.newBuilder()
                .setType(type)
                .setSeq(Long.parseLong(seq))
                .setWallClockMs(System.currentTimeMillis())
                .setCommandPayload(payload.toByteString())
                .build();
        pool.submit(() -> emit(out, completePropose(seq, e)));
        return null;
    }

    private static void emit(BufferedWriter out, String line) {
        try {
            synchronized (out) {
                out.write(line);
                out.write('\n');
                out.flush();
            }
        } catch (IOException e) {
            LOG.debug("emit failed", e);
        }
    }

    private String completePropose(String seq, RaftLogEntry e) {
        try {
            ApplyResult r = adapter.propose(e).get(PROPOSE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            return "RES " + seq + " " + r.getStatusValue() + " " + r.getLeaseToken()
                    + " " + r.getLeaseMs() + " " + r.getQueuePosition();
        } catch (PocNodeAdapter.NotLeaderException nle) {
            return "ERR NOT_LEADER " + nle.leaderNodeId;
        } catch (Exception ex) {
            Throwable c = ex.getCause() != null ? ex.getCause() : ex;
            if (c instanceof PocNodeAdapter.NotLeaderException nle) {
                return "ERR NOT_LEADER " + nle.leaderNodeId;
            }
            return "ERR EX " + c.getClass().getSimpleName() + ":" + c.getMessage();
        }
    }

    @Override
    public void close() throws IOException {
        running = false;
        serverSocket.close();
    }
}
