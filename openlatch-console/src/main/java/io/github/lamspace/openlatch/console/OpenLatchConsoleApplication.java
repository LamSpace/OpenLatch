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

package io.github.lamspace.openlatch.console;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.EnvironmentAware;
import org.springframework.context.annotation.Bean;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

/**
 * 控制台入口：独立部署的
 * Spring Boot Web 应用，HTTP 监听默认 9413，页面为服务端渲染 Thymeleaf
 * + 整页轮询刷新（无前端构建链）。
 *
 * <p><b>配置双通道（同一 {@link ConsoleConfig#from} 口径）</b>：
 * <ul>
 *   <li>部署形态：{@code -Dopenlatch.console.config=<path>} 指向 Properties
 *       文件——{@code main} 把文件键桥接为系统属性（仅当调用方未显式给定），
 *       {@code server.port} 取 {@code openlatch.console.port}（默认 9413）；</li>
 *       <li>直配形态：Spring Environment 直接给 {@code openlatch.console.*}
 *       键（测试注入临时端口即走此路）。</li>
 * </ul>
 * 配置非法（地址为空/令牌空白/端口越界）在 {@link #consoleConfig} 构造即
 * 抛 {@link IllegalArgumentException}——容器启动失败退出（不进入半启动）。
 */
@SpringBootApplication
public class OpenLatchConsoleApplication implements EnvironmentAware {

    /** 配置文件路径的系统属性键。 */
    public static final String CONFIG_PATH_PROPERTY = "openlatch.console.config";

    /** 加载后的 Spring 环境（配置桥接与诊断用）。 */
    private org.springframework.core.env.Environment environment;

    /**
     * 构造应用（Spring Boot 主类；{@code Environment} 由容器回调注入）。
     */
    public OpenLatchConsoleApplication() {
    }

    @Override
    public void setEnvironment(org.springframework.core.env.Environment environment) {
        this.environment = environment;
    }

    /**
     * 控制台配置 bean（Environment 通道——文件已被 {@code main} 桥接为
     * 系统属性，直配键同等生效；两通道在此汇成唯一取值点）。
     *
     * @return 已校验配置
     * @throws IllegalArgumentException 任一配置非法（容器启动失败）
     */
    @Bean
    public ConsoleConfig consoleConfig() {
        return ConsoleConfig.from(environment::getProperty);
    }

    /**
     * 进程入口：读配置文件（若有）→ 桥接系统属性 → 启动容器。配置解析
     * 失败打印原因并以非零码退出（与 server 的 main 同纪律）。
     *
     * @param args 命令行参数（不使用）
     */
    public static void main(String[] args) {
        try {
            bridgeConfigFile();
        } catch (IllegalArgumentException | IOException e) {
            System.err.println(e.getMessage());
            System.exit(1);
            return;
        }
        SpringApplication.run(OpenLatchConsoleApplication.class, args);
    }

    /**
     * 把 {@code -Dopenlatch.console.config} 指向的 Properties 文件桥接为
     * 系统属性：仅填充调用方未显式给定的键（命令行 {@code -Dopenlatch.console.*}
     * 优先于文件）；并把控制台端口翻译为 Boot 的 {@code server.port}。
     * 文件缺省（未给路径）不视为错误——直配键/测试场景即此形态。
     *
     * @throws IOException              文件不可读
     * @throws IllegalArgumentException 文件内容非法（校验在 bean 构造处兜底，
     *                                  此处仅做格式解析）
     */
    private static void bridgeConfigFile() throws IOException {
        String path = System.getProperty(CONFIG_PATH_PROPERTY);
        if (path == null || path.isBlank()) {
            return;
        }
        Properties props = new Properties();
        try (InputStream in = Files.newInputStream(Path.of(path))) {
            props.load(in);
        }
        for (String key : props.stringPropertyNames()) {
            System.setProperty(key, props.getProperty(key));
        }
        if (System.getProperty("server.port") == null) {
            String port = props.getProperty(ConsoleConfig.KEY_PREFIX + "port",
                    String.valueOf(ConsoleConfig.DEFAULT_PORT));
            System.setProperty("server.port", port);
        }
    }
}
