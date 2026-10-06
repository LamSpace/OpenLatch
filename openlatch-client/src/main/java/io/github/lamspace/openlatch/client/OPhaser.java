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

package io.github.lamspace.openlatch.client;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * 分布式相位器（JDK {@code java.util.concurrent.Phaser} 的可判定子集，协议 v10）。
 *
 * <p><b>职责</b>：跨进程多方会合的动态泛化——参与者注册/离场随进随出，
 * 到场与等待解耦，按相位号分批会合；相位号单调递增不复用（区别于
 * {@link OBarrier} 的可复用世代）。服务端账簿为复制状态：参与者数、到场
 * 计数与相位推进经 Raft 多数派确定，注册跨相位存续（离场才减员）。
 * 句柄无状态：同 key 的多个句柄（含跨进程）绑定同一服务端账簿——
 * JDK 句柄进程内私有的对偶适配，属能力面而非缺陷。
 *
 * <p><b>与 JDK 的保真面</b>：动态注册（{@link #register()}/{@link
 * #bulkRegister(int)}）、{@link #arrive()} 不等待而 {@link
 * #arriveAndAwaitAdvance()} 合一等待、{@link #awaitAdvance(long)} 按已见
 * 相位旁观（无需持有注册配额）、{@link #arriveAndDeregister()} 到场并减员、
 * 到场族返回<b>到场相位号</b>、{@code awaitAdvance} 族返回<b>了结时刻当前
 * 相位</b>（JDK"下一到场相位号"对偶）、相位单调。
 *
 * <p><b>与 JDK 的差异面（全部显式声明）</b>：
 * <ul>
 *   <li><b>降级——无 {@code onAdvance} 钩子</b>：服务端无法执行用户代码，
 *       相位动作不承诺"先于全体放行"的排序；以 {@link #arriveAndAwaitAdvance()}
 *       返回的到场相位号判别"该相位由我触达"后本地执行动作是推荐替代惯用法，
 *       但与他人醒转无先后保证（判例 Barrier 的动作两阶段在本原语刻意不采纳——
 *       级联高频事件每相位挂账一跳不可承受）；</li>
 *   <li><b>降级——无终止态</b>：{@code isTerminated()} 恒假语义不存在
 *       （不提供该词）；注册数归零为<b>空转</b>而非终止——后续
 *       {@link #register()} 自当前相位恢复运转（与 JDK"归零即终止且粘滞"
 *       相反：分布式共享 key 的粘滞终止会把一批死者的影响卡进未来所有
 *       参与者，判例 Barrier 拒绝粘滞破障的同轴论证）；</li>
 *   <li><b>降级——无父子分层派生</b>：分层是本地计数器可扩展性优化，
 *       分布式每到场一次网络往返、红利在 RTT 瓶颈前消失；一 key 一
 *       phaser；</li>
 *   <li><b>差异——相位号为 {@code long}</b>：JDK 的 {@code int} 相位在
 *       2^31 次推进后回绕，本库账簿为 long 单调（方法签名以 {@code long}
 *       承载入参与返回）；</li>
 *   <li><b>差异——配额按会话严格归属</b>：{@link #arriveAndDeregister()}
 *       只能扣减本会话注册配额，配额透支被显式拒绝（JDK 匿名 party 此处
 *       本属未定义行为——严格化使参与者进程死亡的隐式摘除有确定归属域）；</li>
 *   <li><b>差异——每次调用至少一次网络往返</b>：JDK 的 {@code getPhase()}
 *       等为纳秒级本地读，本库各读数方法每次一条 {@code QUERY} 请求
 *       （服务端 Leader 本地读数、零日志）；读数返回即可能过期
 *       （advisory，不承诺线性化伴随——高频观察请应用侧缓存节流）；
 *       {@code arrive()} 与 {@code register()} 各一 RTT，
 *       {@code arriveAndAwaitAdvance()} 的会合成本随参与者数与相位频率
 *       增长（日志条目率运维口径见部署指南）；</li>
 *   <li><b>差异——等待超时与中断</b>：{@link #awaitAdvance(long)} 以客户端
 *       等待总超时兜底（默认 30s 后抛 {@link OpenLatchTimeoutException}，
 *       不存在事实上的永挂路径——超时后循环重入即 JDK 无限等待的等效惯用法）；
 *       超时/中断到期先尽力撤销服务端挂起（fire-and-forget，撤销丢失由
 *       护栏与清理三路回收兜底），随后抛出。</li>
 * </ul>
 *
 * <p><b>增强面（强于 JDK）</b>：
 * <ul>
 *   <li>跨进程参与者同相合拢（JDK 句柄进程内私有）；</li>
 *   <li>参与者<b>会话死亡即时隐式摘除配额且不空转</b>：应到集合缩小可即时
 *       推进唤醒存活方（已到场事实不撤销）——对照 {@link OBarrier}
 *       "死亡即破障"：本原语单死者不炸一锅（动态成员性的泛化兑现）；</li>
 *   <li><b>等待可跨换主无损自愈</b>：唤醒谓词"当前相位 &gt; 已见相位"由
 *       复制账簿承载，换主后 SDK 自动重挂——已推进即刻了结、未推进续挂，
 *       无补偿通道亦无需补偿（对照 {@link OCondition}"signal 是事件、换主窗
 *       丢失不补偿"——两者同用唤醒推送但可靠性分层不同，防混读）。</li>
 * </ul>
 *
 * <p><b>线程模型与幂等</b>：句柄线程安全；同一时刻每方法调用独立成请求。
 * 服务端以 (会话, 请求 id) 幂等：应答丢失的自动重发不双计到场、不双加注册
 * 配额、不双登记等待；但<em>应用层重复调用</em> {@code register()} 即重复
 * 注册（与 JDK 同判）。变异操作在途超时后跨会话切换会放弃（在途到场不跨
 * 会话重放——判例 Barrier 口径），纯等待操作自动换会话续挂。
 *
 * @since v10
 */
public interface OPhaser {

    /**
     * 本句柄绑定的相位器键。
     *
     * @return key
     */
    String key();

    /**
     * 注册一个参与者（等价 {@code bulkRegister(1)}）。
     *
     * @return 注册发生时服务端账簿的当前相位号
     * @throws OpenLatchException    服务端拒绝（配额上限、家族冲突、会话失效等）
     * @throws OpenLatchTimeoutException 请求超时且结果不确定
     */
    long register();

    /**
     * 批量注册 {@code parties} 个参与者。
     *
     * @param parties 注册数（{@code >= 1}）
     * @throws IllegalArgumentException parties 非正
     * @throws OpenLatchException      服务端拒绝
     * @throws OpenLatchTimeoutException 请求超时且结果不确定
     */
    void bulkRegister(int parties);

    /**
     * 到场且不等待。
     *
     * @return 到场相位号（该次到场发生时账簿所处相位；若恰触发推进，推进结果
     *         经后续读数或他人唤醒可见）
     * @throws OpenLatchException       服务端拒绝（无此 phaser、配额透支、家族冲突等）
     * @throws OpenLatchTimeoutException 请求超时且结果不确定
     */
    long arrive();

    /**
     * 到场并离场（扣减本会话一个注册配额；本会话无配额则拒绝——差异声明见类注）。
     *
     * @return 到场相位号
     * @throws OpenLatchException       服务端拒绝（含配额透支）
     * @throws OpenLatchTimeoutException 请求超时且结果不确定
     */
    long arriveAndDeregister();

    /**
     * 到场并等待本相位合拢（JDK {@code arriveAndAwaitAdvance()}）。
     *
     * <p>会合经等待-通知-重发闭环并受等待总超时兜底；若恰由本次到场触达
     * 合拢，即刻返回不挂起。
     *
     * @return 到场相位号
     * @throws InterruptedException     等待被中断（已尽力撤销服务端挂起）
     * @throws OpenLatchTimeoutException 等待总超时或请求超时且结果不确定
     * @throws OpenLatchException       服务端拒绝
     */
    long arriveAndAwaitAdvance() throws InterruptedException;

    /**
     * 等待相位推进越过 {@code phase}（不要求本方注册或到场——JDK
     * {@code awaitAdvance(int)} 的旁观语义；分布式改判有界：等待总超时
     * 后抛异常，循环重入等效无限等待）。
     *
     * @param phase 已见相位号（{@code >= 0}）
     * @return 了结时刻的当前相位号（{@code > phase}）
     * @throws InterruptedException     等待被中断（已尽力撤销挂起）
     * @throws OpenLatchTimeoutException 等待总超时
     * @throws OpenLatchException       服务端拒绝或请求超时结果不确定
     */
    long awaitAdvance(long phase) throws InterruptedException;

    /**
     * 带超时等待相位推进越过 {@code phase}（JDK
     * {@code awaitAdvanceInterruptibly(int, long, TimeUnit)}）。
     *
     * @param phase  已见相位号（{@code >= 0}）
     * @param timeout 等待预算（毫秒，{@code > 0}）
     * @param unit   时间单位
     * @return 了结时刻的当前相位号（{@code > phase}）
     * @throws InterruptedException     等待被中断（已尽力撤销挂起）
     * @throws TimeoutException          预算耗尽（先尽力撤销服务端挂起再抛出）
     * @throws IllegalArgumentException  预算非正
     * @throws OpenLatchException       服务端拒绝或请求超时结果不确定
     */
    long awaitAdvanceInterruptibly(long phase, long timeout, TimeUnit unit)
            throws InterruptedException, TimeoutException;

    /**
     * 读数：当前相位号（一次网络 QUERY，Leader 本地账簿；advisory 语义见类注）。
     *
     * @return 当前相位号
     * @throws OpenLatchException       服务端拒绝（含无此 phaser）
     * @throws OpenLatchTimeoutException 请求超时且结果不确定
     */
    long getPhase();

    /**
     * 读数：注册参与者总数。
     *
     * @return registered 总数
     * @throws OpenLatchException       服务端拒绝
     * @throws OpenLatchTimeoutException 请求超时且结果不确定
     */
    int getRegisteredParties();

    /**
     * 读数：当前相位已到场计数。
     *
     * @return arrived 计数
     * @throws OpenLatchException       服务端拒绝
     * @throws OpenLatchTimeoutException 请求超时且结果不确定
     */
    int getArrivedParties();

    /**
     * 读数：当前相位未到场计数（{@code registered - arrived}，客户端按
     * 同一次 QUERY 回显折算）。
     *
     * @return unarrived 计数（恒 {@code >= 0}）
     * @throws OpenLatchException       服务端拒绝
     * @throws OpenLatchTimeoutException 请求超时且结果不确定
     */
    int getUnarrivedParties();
}
