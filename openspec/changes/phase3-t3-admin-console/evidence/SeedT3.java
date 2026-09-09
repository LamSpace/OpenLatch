/*
 * T3 验收证据辅助：以业务 SDK 预置"重入锁持有+排队、Semaphore 2/3+大请求
 * 排队、Latch 等待者"的常驻负载（证据走查时保持进程存活）。
 * 用法：java -cp <openlatch-client 及其依赖> SeedT3 <host:port>
 * 非交付物——仅 L3 实跑记录用（详设 §7 T3 / 验收 §8-4）。
 */

import io.github.lamspace.openlatch.client.OCountDownLatch;
import io.github.lamspace.openlatch.client.OLock;
import io.github.lamspace.openlatch.client.OSemaphore;
import io.github.lamspace.openlatch.client.OpenLatchClient;

public final class SeedT3 {

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("用法: SeedT3 <host:port>");
            System.exit(1);
        }
        try (OpenLatchClient client = OpenLatchClient.builder()
                .address(args[0]).build()) {
            client.connectAsync().get(10, java.util.concurrent.TimeUnit.SECONDS);

            OLock order = client.newReentrantLock("order:1");
            order.lock();
            startDaemon("waiter-order", () -> {
                try {
                    client.newReentrantLock("order:1").lock();
                } catch (InterruptedException ignored) {
                }
            });

            OSemaphore sem = client.newSemaphore("pool:db", 3);
            sem.acquire(2);
            startDaemon("waiter-sem", () -> {
                try {
                    client.newSemaphore("pool:db").acquire(3);
                } catch (InterruptedException ignored) {
                }
            });

            OCountDownLatch gate = client.newCountDownLatch("gate:boot", 2);
            startDaemon("waiter-gate", () -> {
                try {
                    gate.await();
                } catch (InterruptedException ignored) {
                }
            });

            System.out.println("seed ready: order:1 held+1 waiter, pool:db 2/3 +1 waiter,"
                    + " gate:boot 2 +1 awaiter; press Ctrl-C to stop");
            Thread.sleep(Long.MAX_VALUE);
        }
    }

    private static void startDaemon(String name, Runnable body) {
        Thread t = new Thread(body, name);
        t.setDaemon(true);
        t.start();
    }
}
