package io.github.lamspace.openlatch.protocol;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumDescriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * openlatch.proto 编号契约冻结测试（spec wire-protocol"v1 基线冻结"，
 * 变更 s3-leader-discovery-failover D2）。
 *
 * <p>线路协议字段号与枚举值是跨版本兼容性契约：变更、删除或复用既有编号
 * 会让存量客户端/服务端静默错位。双层钉死：
 * <ul>
 *   <li>{@code /openlatch-proto-v1-baseline.txt}：Phase 1 基线清单，每一行
 *       MUST 原样出现在当前契约中（捕获编号变更/删除/改名）；基线文件本身
 *       永久冻结；</li>
 *   <li>{@code /openlatch-proto-contract.txt}：当前全量清单逐行比对（对齐
 *       {@code RaftProtoContractFreezeTest} 先例），新增字段须显式追加 golden，
 *       评审可见。</li>
 * </ul>
 */
class OpenlatchProtoContractFreezeTest {

    /** 当前全量契约 golden（每行 "kind name number" 形态，# 注释与空行忽略）。 */
    private static final String FULL_CONTRACT = "/openlatch-proto-contract.txt";
    /** Phase 1 基线 golden（全量契约的不可变子集）。 */
    private static final String V1_BASELINE = "/openlatch-proto-v1-baseline.txt";

    @Test
    void descriptorMatchesFrozenContract() throws IOException {
        assertThat(dump()).containsExactlyElementsOf(readLines(FULL_CONTRACT));
    }

    @Test
    void phaseOneBaselineNeverMutates() throws IOException {
        List<String> contract = dump();
        for (String baseline : readLines(V1_BASELINE)) {
            assertThat(contract)
                    .as("Phase 1 基线项 %s 被变更、删除或改名", baseline)
                    .contains(baseline);
        }
    }

    /**
     * 从生成描述符导出全量契约清单：顶层枚举按声明序、顶层消息及其字段按声明序。
     *
     * @return 契约行列表（"enum NAME" / "  VALUE n" / "message NAME" / "  field n"）
     */
    private static List<String> dump() {
        FileDescriptor fd = Envelope.getDescriptor().getFile();
        List<String> lines = new ArrayList<>();
        for (EnumDescriptor e : fd.getEnumTypes()) {
            lines.add("enum " + e.getName());
            for (EnumValueDescriptor v : e.getValues()) {
                lines.add("  " + v.getName() + " " + v.getNumber());
            }
        }
        for (Descriptor m : fd.getMessageTypes()) {
            lines.add("message " + m.getName());
            for (FieldDescriptor f : m.getFields()) {
                lines.add("  " + f.getName() + " " + f.getNumber());
            }
        }
        return lines.stream().map(OpenlatchProtoContractFreezeTest::normalize).toList();
    }

    /**
     * 读取 golden 的契约行（忽略 {@code #} 注释与空行，空白规范化）。
     *
     * @param resource classpath 资源路径
     * @return 契约行列表
     * @throws IOException golden 资源缺失或不可读
     */
    private static List<String> readLines(String resource) throws IOException {
        List<String> lines = new ArrayList<>();
        try (InputStream in = OpenlatchProtoContractFreezeTest.class.getResourceAsStream(resource)) {
            if (in == null) {
                throw new IOException("golden 资源缺失: " + resource);
            }
            try (BufferedReader r = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
                String line;
                while ((line = r.readLine()) != null) {
                    String t = line.trim();
                    if (!t.isEmpty() && !t.startsWith("#")) {
                        lines.add(normalize(line));
                    }
                }
            }
        }
        return lines;
    }

    /** 契约行规范化：连续空白折叠为单空格（golden 文件缩进容忍）。 */
    private static String normalize(String line) {
        return line.replaceAll("\\s+", " ").trim();
    }
}
