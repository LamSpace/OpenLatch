/*
 * Copyright 2026 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.github.lamspace.openlatch.core.result;

import java.util.List;

/**
 * 队列操作结果（QUEUE 家族命令通道的输出值对象）。
 *
 * <p><b>择用规则</b>：{@code outcome} 判别终态——
 * <ul>
 *   <li>{@link Outcome#GRANTED}：写类携带交付读数（TAKE 的 {@code element}、
 *       DRAIN 的 {@code drained}；PUT 交付无载荷，读数仅剩 {@code capacity}
 *       回显），读类携带 {@code peek}/{@code size} 读数；</li>
 *   <li>{@link Outcome#QUEUED}：{@code queuePosition} 有效（1 起，所属轨道内
 *       位次），等待队首通知后同请求重发兑现；</li>
 *   <li>{@link Outcome#DENIED}：立即式不可满足（元素满/无可消费元素），
 *       零迁移终态；</li>
 *   <li>各 {@code REJECT_*}：请求类拒绝（会话/key/形态/容量断言/等待深度），
 *       条目状态零扰动。</li>
 * </ul>
 * 拒绝与 QUEUED/DENIED 态的交付字段恒为空形（{@code element} {@code null}、
 * {@code drained} 空表、{@code size} 0）。
 *
 * <p><b>不可变性</b>：交付字节在构造时防御性复制，记录深不可变。
 *
 * @param outcome      结果判别
 * @param queuePosition QUEUED 时的轨道内位次（1 起；其余 0）
 * @param element      TAKE 交付 / PEEK 读数（{@code null}=无元素或空队/未到期；
 *                     零长度数组=空串元素——两态可区分）
 * @param drained      DRAIN 按出队序的交付列表（空表为合法结果；其余操作恒空表）
 * @param size         SIZE 读数（驻留元素数，延时形态含未到期项；其余操作 0）
 * @param capacity     条目定型容量回显（拒绝态与 key 尚不存在为 0）
 */
public record QueueOpResult(
        Outcome outcome,
        int queuePosition,
        byte[] element,
        List<byte[]> drained,
        long size,
        long capacity) {

    /**
     * 构造并对交付字节做防御性复制。
     */
    public QueueOpResult {
        element = element == null ? null : element.clone();
        drained = List.copyOf(drained);
    }

    /**
     * 请求类拒绝（交付字段全空形）。
     *
     * @param outcome 拒绝判别（含 DENIED 立即式终态）
     * @return 拒绝结果
     */
    public static QueueOpResult rejected(Outcome outcome) {
        return new QueueOpResult(outcome, 0, null, List.of(), 0, 0);
    }

    /**
     * 挂起：入所属等待轨道并返回位次。
     *
     * @param queuePosition 轨道内位次（1 起）
     * @return 挂起结果
     */
    public static QueueOpResult queued(int queuePosition) {
        return new QueueOpResult(Outcome.QUEUED, queuePosition, null, List.of(), 0, 0);
    }

    /**
     * PUT 成功（含去重槽同序重放）：无交付载荷。
     *
     * @param capacity 条目定型容量回显
     * @return 授予结果
     */
    public static QueueOpResult grantedPut(long capacity) {
        return new QueueOpResult(Outcome.GRANTED, 0, null, List.of(), 0, capacity);
    }

    /**
     * TAKE 成功（含去重槽同序重放——重放交付同一份字节）。
     *
     * @param element  交付元素（非 {@code null}；零长度=空串元素）
     * @param capacity 条目定型容量回显
     * @return 授予结果
     */
    public static QueueOpResult grantedTake(byte[] element, long capacity) {
        return new QueueOpResult(Outcome.GRANTED, 0, element, List.of(), 0, capacity);
    }

    /**
     * DRAIN 成功（含去重槽同序重放）：空表为合法结果。
     *
     * @param drained  按出队序的交付列表
     * @param capacity 条目定型容量回显
     * @return 授予结果
     */
    public static QueueOpResult grantedDrain(List<byte[]> drained, long capacity) {
        return new QueueOpResult(Outcome.GRANTED, 0, null, drained, 0, capacity);
    }

    /**
     * PEEK 读数（零迁移）：空队/未到期回 {@code null} 元素。
     *
     * @param element  队首元素读数（可为 {@code null}；零长度=空串元素）
     * @param capacity 条目定型容量回显（key 尚不存在为 0）
     * @return 授予结果
     */
    public static QueueOpResult grantedPeek(byte[] element, long capacity) {
        return new QueueOpResult(Outcome.GRANTED, 0, element, List.of(), 0, capacity);
    }

    /**
     * SIZE 读数（零迁移）。
     *
     * @param size     驻留元素数
     * @param capacity 条目定型容量回显（key 尚不存在为 0）
     * @return 授予结果
     */
    public static QueueOpResult grantedSize(long size, long capacity) {
        return new QueueOpResult(Outcome.GRANTED, 0, null, List.of(), size, capacity);
    }
}
