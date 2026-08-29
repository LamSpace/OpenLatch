package io.github.lamspace.openlatch.poc.driver;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 不变式监视（spec「正确性不变式监视」）：基于 driver 观察到的响应流，
 * 校验"任一时刻同 key 至多一个写持有者"。
 *
 * <p>杀主窗口内未复制完成的授予可合法回滚（§8），此时该 key 标记 suspect
 * 而非违例；suspect 期间同 key 再次授予记为 review 事件（人工复核），
 * 非 suspect 期间的双授予直接记违例。
 */
public final class InvariantMonitor {

    /** 观察事件（JSON 序列化的最小行）。 */
    public record Event(String at, long tsMs, String detail) { }

    private record Holder(long sid, long token) { }

    private final Map<String, Holder> held = new HashMap<>();
    private final Map<String, Holder> suspect = new HashMap<>();
    private final List<Event> events = new ArrayList<>();
    private volatile boolean killWindow;

    /** 标记进入/离开杀主窗口（窗口内授予回滚不算违例）。 */
    public void setKillWindow(boolean v) {
        this.killWindow = v;
    }

    /**
     * 上报一次 ACQ 结果。
     *
     * @param key    锁键
     * @param status ApplyResult.status（0=OK/GRANTED）
     * @param sid    逻辑会话
     * @param token  租约凭证
     */
    public synchronized void acquire(String key, int status, long sid, long token) {
        long ts = System.currentTimeMillis();
        if (status == 0) {
            Holder h = new Holder(sid, token);
            Holder cur = held.get(key);
            if (cur != null && !cur.equals(h)) {
                if (killWindow) {
                    suspect.put(key, cur);
                    events.add(new Event("review", ts,
                            "second grant during kill window key=" + key + " old=" + cur + " new=" + h));
                } else {
                    events.add(new Event("violation", ts,
                            "double grant key=" + key + " old=" + cur + " new=" + h));
                }
            }
            if (cur == null || killWindow) {
                held.remove(key);
            }
            held.put(key, h);
        }
    }

    /** 上报一次 REL 结果（OK 释放清位；NOT_HELD/INVALID 视为回滚或异常，清位）。 */
    public synchronized void release(String key, int status, long sid) {
        long ts = System.currentTimeMillis();
        Holder cur = held.get(key);
        if (status == 0) {
            held.remove(key);
            suspect.remove(key);
        } else if (cur != null && cur.sid() == sid) {
            // 授予回滚（切换窗口）或凭证失效：清位并留痕。
            held.remove(key);
            events.add(new Event("rollback", ts,
                    "release failed key=" + key + " status=" + status + " holder=" + cur));
        }
    }

    /** 是否存在 violation 级事件。 */
    public synchronized boolean violated() {
        return events.stream().anyMatch(e -> e.at().equals("violation"));
    }

    /** 全量事件（含 review/rollback），报告原样附列。 */
    public synchronized List<Event> events() {
        return List.copyOf(events);
    }
}
