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

package io.github.lamspace.openlatch.examples;

import java.util.concurrent.TimeUnit;

import io.github.lamspace.openlatch.client.OLock;
import io.github.lamspace.openlatch.client.OpenLatchClient;
import io.github.lamspace.openlatch.server.OpenLatchServer;

/**
 * 示例 1：编程式 API 最小闭环（详设 §9）——lock / tryLock / unlock。
 *
 * <p>运行：{@code mvn -pl openlatch-examples exec:java
 * -Dexec.mainClass=io.github.lamspace.openlatch.examples.QuickStartExample}
 */
public final class QuickStartExample {

    /**
     * 私有构造：入口类。
     */
    private QuickStartExample() {
    }

    /**
     * 入口：内嵌服务器 → 客户端 → 持锁/立即式尝试/释放。
     *
     * @param args 未使用
     * @throws Exception 连接或线程异常
     */
    public static void main(String[] args) throws Exception {
        OpenLatchServer server = ExampleServers.startDefault();
        try (OpenLatchClient client = OpenLatchClient.builder()
                .address("127.0.0.1:" + server.port())
                .build()) {
            client.connectAsync().get(5, TimeUnit.SECONDS);
            OLock lock = client.newReentrantLock("quickstart:demo");

            lock.lock();
            System.out.println("[main ] lock() acquired, holding briefly");
            Thread.sleep(200);

            Thread contender = Thread.ofVirtual().name("contender").start(() -> {
                boolean got = lock.tryLock();
                System.out.println("[other] tryLock() while held by main -> " + got
                        + " (expected false: mutual exclusion)");
            });
            contender.join();

            lock.unlock();
            System.out.println("[main ] unlocked");

            boolean gotAfterRelease = lock.tryLock();
            System.out.println("[main ] tryLock() after unlock -> " + gotAfterRelease
                    + " (expected true)");
            if (gotAfterRelease) {
                lock.unlock();
            }
            System.out.println("[done ] QuickStartExample finished");
        } finally {
            server.stop();
        }
        System.exit(0);
    }
}
