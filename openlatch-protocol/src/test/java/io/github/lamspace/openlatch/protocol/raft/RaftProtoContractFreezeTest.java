package io.github.lamspace.openlatch.protocol.raft;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumDescriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import io.github.lamspace.openlatch.protocol.Envelope;
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
 * raft.proto 编号契约冻结测试（详设 §4.2 / §7.1，spec"条目编号冻结"）。
 *
 * <p>日志条目与快照的字段号/枚举值一经发布即持久化兼容性契约：变更或复用
 * 编号会让存量日志与快照在回放时静默错位。本测试把生成描述符的全量
 * （名称, 编号）清单与 golden 文件逐行比对，任何差异即构建失败；
 * 新增字段须显式追加 golden（评审可见）。
 *
 * <p>附带锁证"客户端 wire format 零扰动"：{@code Envelope} 的字段 MUST NOT
 * 引用 raft.proto 中的任何消息。
 */
class RaftProtoContractFreezeTest {

    /** golden 资源路径（每行 "kind name number" 形态，# 注释与空行忽略）。 */
    private static final String GOLDEN = "/raft-proto-contract.txt";

    @Test
    void descriptorMatchesFrozenContract() throws IOException {
        List<String> actual = new ArrayList<>();
        dump().forEach(l -> actual.add(normalize(l)));
        assertThat(actual).containsExactlyElementsOf(golden());
    }

    @Test
    void clientEnvelopeNeverReferencesRaftMessages() {
        FileDescriptor raftFile = RaftLogEntry.getDescriptor().getFile();
        for (FieldDescriptor f : Envelope.getDescriptor().getFields()) {
            if (f.getJavaType() == FieldDescriptor.JavaType.MESSAGE) {
                assertThat(f.getMessageType().getFile()).isNotSameAs(raftFile);
            }
        }
    }

    /**
     * 从生成描述符导出全量契约清单：顶层枚举按声明序、顶层消息及其字段按声明序。
     *
     * @return 契约行列表（"enum NAME" / "  VALUE n" / "message NAME" / "  field n"）
     */
    private static List<String> dump() {
        FileDescriptor fd = RaftLogEntry.getDescriptor().getFile();
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
        return lines;
    }

    /**
     * 读取 golden 文件的契约行（忽略 {@code #} 注释与空行）。
     *
     * @return golden 契约行列表
     * @throws IOException golden 资源缺失或不可读
     */
    private static List<String> golden() throws IOException {
        List<String> lines = new ArrayList<>();
        try (InputStream in = RaftProtoContractFreezeTest.class.getResourceAsStream(GOLDEN)) {
            if (in == null) {
                throw new IOException("golden 资源缺失: " + GOLDEN);
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
