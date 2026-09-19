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

/**
 * 循环屏障世代的了结形态。
 *
 * <p>一个世代自创建起至了结只有两条去路：到场数达到许可数且动作了结
 * （或无动作即时）合拢放行，或任一当前世代到场者离场（超时/中断/
 * 会话死亡/显式破障）触发破障。了结记录以 {@link #TRIPPED}/{@link #BROKEN}
 * 定型，供旧世代等待项的重发按原世代幂等了结。
 */
public enum BarrierFinal {

    /** 合拢了结：世代到场数达标（动作形态以执行者回报为生效点），全体等待者放行。 */
    TRIPPED,
    /** 破障了结：当前世代有到场者离场，全体在队者即时以破障裁决收场。 */
    BROKEN
}
