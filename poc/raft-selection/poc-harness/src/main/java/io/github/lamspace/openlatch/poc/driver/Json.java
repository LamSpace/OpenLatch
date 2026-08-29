package io.github.lamspace.openlatch.poc.driver;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 极简 JSON 写出器（PoC 结果专用，避免引第三方依赖）。
 * 仅支持字符串/数字/布尔/null 与嵌套；值中的引号做基础转义。
 */
public final class Json {

    private final Map<String, Object> fields = new LinkedHashMap<>();

    /** 追加字段。 */
    public Json put(String key, Object value) {
        fields.put(key, value);
        return this;
    }

    /** 序列化为一行对象。 */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("{");
        boolean first = true;
        for (Map.Entry<String, Object> e : fields.entrySet()) {
            if (!first) {
                sb.append(',');
            }
            first = false;
            sb.append(quote(e.getKey())).append(':').append(value(e.getValue()));
        }
        return sb.append('}').toString();
    }

    private static String value(Object v) {
        if (v instanceof Json j) {
            return j.toString();
        }
        if (v instanceof Iterable<?> it) {
            StringBuilder sb = new StringBuilder("[");
            boolean first = true;
            for (Object o : it) {
                if (!first) {
                    sb.append(',');
                }
                first = false;
                sb.append(value(o));
            }
            return sb.append(']').toString();
        }
        if (v == null || v instanceof Number || v instanceof Boolean) {
            return String.valueOf(v);
        }
        return quote(String.valueOf(v));
    }

    private static String quote(String s) {
        return '"' + s.replace("\\", "\\\\").replace("\"", "\\\"") + '"';
    }
}
