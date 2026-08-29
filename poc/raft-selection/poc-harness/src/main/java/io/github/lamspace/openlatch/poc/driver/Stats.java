package io.github.lamspace.openlatch.poc.driver;

import java.util.Arrays;

/**
 * 延迟统计：全量样本（5min 顺序负载 ≤ 1e6 ops，内存可承受），分位数按最近秩。
 */
public final class Stats {

    private long[] data = new long[1024];
    private int size;

    /** 记录一个样本（纳秒转微秒存）。 */
    public void recordNanos(long ns) {
        add(Math.round(ns / 1_000.0));
    }

    /** 记录微秒样本。 */
    public void add(long us) {
        if (size == data.length) {
            data = Arrays.copyOf(data, data.length * 2);
        }
        data[size++] = us;
    }

    /** 样本数。 */
    public int count() {
        return size;
    }

    /** 分位数（0..100），微秒。 */
    public double percentile(double p) {
        if (size == 0) {
            return 0;
        }
        long[] s = Arrays.copyOf(data, size);
        Arrays.sort(s);
        int idx = (int) Math.ceil(p / 100.0 * size) - 1;
        return s[Math.max(0, Math.min(idx, size - 1))];
    }

    /** 均值（微秒）。 */
    public double mean() {
        long sum = 0;
        for (int i = 0; i < size; i++) {
            sum += data[i];
        }
        return size == 0 ? 0 : (double) sum / size;
    }

    /** 毫秒值字符串：x.yz。 */
    public String pMs(double p) {
        return String.format("%.3f", percentile(p) / 1000.0);
    }

    /** 均值毫秒字符串。 */
    public String meanMs() {
        return String.format("%.3f", mean() / 1000.0);
    }
}
