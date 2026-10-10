# Evidence — JDK 结构化并发 API 预览状态核验

核验时刻：2026-10-10　环境：JDK 25.0.3（Oracle / sdkman），项目 `maven.compiler.release=25`。

## 1. 运行环境

```
$ java -version
java version "25.0.3" 2026-04-21 LTS
Java(TM) SE Runtime Environment (build 25.0.3+9-LTS-195)
Java HotSpot(TM) 64-Bit Server VM (build 25.0.3+9-LTS-195, mixed mode, sharing)
```

## 2. `StructuredTaskScope` 仍为预览 API（决定"当前不可用"）

```
$ SRC=/home/lam/.sdkman/candidates/java/25.0.3-oracle/lib/src.zip
$ unzip -p "$SRC" java.base/java/util/concurrent/StructuredTaskScope.java \
    | grep -nE "PreviewFeature|@since"
32:import jdk.internal.javac.PreviewFeature;
349: * @since 21
351:@PreviewFeature(feature = PreviewFeature.Feature.STRUCTURED_CONCURRENCY)
365:     * @since 21
367:    @PreviewFeature(feature = PreviewFeature.Feature.STRUCTURED_CONCURRENCY)
374:        @PreviewFeature(feature = PreviewFeature.Feature.STRUCTURED_CONCURRENCY)
501:     * @since 25
504:    @PreviewFeature(feature = PreviewFeature.Feature.STRUCTURED_CONCURRENCY)
```

**结论**：JDK 25.0.3 中 `StructuredTaskScope` 仍标注 `@PreviewFeature(STRUCTURED_CONCURRENCY)`。使用需编译期与运行期同时开启 `--enable-preview`，与库产物禁令（`ROADMAP` 决策记录"库产物面不得启用 `--enable-preview`"）直接冲突 → **当前不可用**。

## 3. ScopedValue 继承面仅经 `fork`（与第三项耦合的实证）

```
$ unzip -p "$SRC" java.base/java/lang/ScopedValue.java | sed -n '138,144p'
 * {@code ScopedValue} supports sharing across threads. This sharing is limited to
 * structured cases where child threads are started and terminate within the bounded
 * period of execution by a parent thread. When using a {@link StructuredTaskScope},
 * scoped value bindings are <em>captured</em> when creating a {@code StructuredTaskScope}
 * and inherited by all threads started in that task scope with the
 * {@link StructuredTaskScope#fork(java.util.concurrent.Callable) fork} method.
```

**结论**：ScopedValue 的跨线程继承面**仅**经 `StructuredTaskScope.fork`。故采纳结构化并发会改变第三项 `EntryClock.APPLY_NOW` 的传播边界（现与 `ThreadLocal` 逐格相同）；第三项在 `StateMachineDeterminismTest` 的边界矩阵绊线将按设计翻红（本 change design D5 记录）。

## 4. 预览轨迹（外部来源，供未来触发时复核）

- JDK 25 = **第五预览**（JEP 505，`open()` / `Joiner` 大改）；JDK 26 = **第六预览**（JEP 525，`anySuccessfulResultOrThrow → anySuccessfulOrThrow`、新增 `Joiner.onTimeout()`）——**仍未转正，且 API 仍在改名**。
- 来源：[JEP 525: Structured Concurrency (Sixth Preview)](https://bugs.openjdk.org/browse/JDK-8366891)、[OpenJDK JEP index — Structured Concurrency](https://openjdk.org/jeps/543)。

> 复核方式：转正触发时重跑 §2 命令；`@PreviewFeature` 消失即转正，再核对 API 在随后 ≥2 发布中是否稳定。
