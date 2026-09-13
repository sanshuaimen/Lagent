package com.hmdp.agent.controller;

import cn.hutool.core.util.StrUtil;
import com.hmdp.agent.config.AgentProperties;
import com.hmdp.agent.dto.AgentChatRequest;
import com.hmdp.agent.dto.AgentEvent;
import com.hmdp.agent.service.ShopGuideAgentService;
import com.hmdp.agent.session.AgentSessionStore;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import javax.annotation.Resource;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * 【导购 Agent 模块 · 接口层】
 * <p>
 * 作用：对外提供导购对话接口，把 {@link ShopGuideAgentService} 产生的事件流包装成 SSE 返回给前端。
 * <p>
 * <b>接口清单</b>：
 * <ul>
 *   <li>{@code POST /agent/chat} —— 发起一轮对话，返回 text/event-stream 流</li>
 *   <li>{@code DELETE /agent/session/{sessionId}} —— 清空某个会话的历史（前端「新会话」按钮）</li>
 * </ul>
 * <p>
 * <b>为什么用 SSE 而不是 WebSocket</b>：导购是「用户问一句、服务端流式答一段」的单向推送，
 * SSE 基于普通 HTTP，无需额外协议升级和连接管理，够用且简单。
 * <p>
 * <b>为什么是 POST 而不是 GET</b>：浏览器原生 EventSource 只支持 GET，但用户提问可能较长，
 * 放 URL 里不合适。前端用 fetch + ReadableStream 读取响应体即可。
 * <p>
 * <b>开关</b>：{@code hmdp.agent.enabled=false} 时本 Controller 不会被注册，
 * 接口返回 404，其余业务接口（商铺、秒杀、探店）完全不受影响。
 */
//接收 HTTP 请求，创建 SSE 连接，把耗时的 Agent 逻辑丢给专用线程池，把产生的事件流推给前端
@Slf4j
@RestController
@RequestMapping("/agent")
@ConditionalOnProperty(prefix = "hmdp.agent", name = "enabled", havingValue = "true", matchIfMissing = true)
public class AgentController {

    /**
     * SSE 连接超时时间（毫秒）。
     * 模型思考 + 多轮工具调用可能耗时十几秒，默认的 30 秒偏紧，这里放宽到 2 分钟。
     */
    private static final long EMITTER_TIMEOUT_MS = 120_000L;


    @Resource
    private ShopGuideAgentService agentService;

    @Resource
    private AgentSessionStore sessionStore;

    /** Agent 配置，这里主要用来读心跳间隔（hmdp.agent.heartbeat-interval） */
    @Resource
    private AgentProperties agentProperties;

    /**
     * 承载 Agent 对话的后台线程池。
     * <p>
     * SSE 要求 Controller 方法「立即返回」SseEmitter，真正耗时的模型调用必须异步执行，
     * 否则会一直占着 Tomcat 的工作线程。该线程池由 {@code AgentConfig} 创建并托管生命周期。
     */
    @Resource
    private ExecutorService agentExecutor;

    /**
     * SSE 心跳调度器，由 {@code AgentConfig} 创建。
     * 独立于对话线程池，避免心跳任务和模型调用互相争抢线程。
     */
    @Resource
    private ScheduledExecutorService agentHeartbeatScheduler;

    /**
     * 发起一轮导购对话。
     * <p>
     * 请求：{@code POST /agent/chat}，body 为 {@link AgentChatRequest} 的 JSON
     * （{@code message} 必填，{@code sessionId} 可选）。
     * <p>
     * 响应：SSE 事件流，每个事件的 data 是一段 {@link AgentEvent} 的 JSON，
     * 前端按其中的 {@code type} 字段区分处理：
     * text 追加到气泡、tool 显示「正在查询」、done 收尾、error 提示失败。
     *
     * @param request 对话请求，参数来源见 {@link AgentChatRequest}
     * @return SSE 发射器；方法本身立即返回，内容由后台线程持续推送
     */
    @PostMapping(value = "/chat", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter chat(@RequestBody AgentChatRequest request) {
        SseEmitter emitter = new SseEmitter(EMITTER_TIMEOUT_MS);

        // 入参校验：问题为空时直接推一条 error 事件结束，不必浪费一次模型调用
        if (request == null || StrUtil.isBlank(request.getMessage())) {
            try {
                send(emitter, AgentEvent.error("提问内容不能为空"));
            } catch (UncheckedIOException ignored) {
                // 刚创建的 emitter 基本不会发送失败，这里兜底避免把异常抛回给前端
            }
            emitter.complete();
            return emitter;
        }

        try {
            agentExecutor.submit(() -> runChat(request, emitter));
        } catch (RejectedExecutionException e) {
            // 线程池有界队列 + AbortPolicy：并发对话超出处理能力时立刻失败。
            // 这里必须自己回一条明确的错误——否则请求被静默丢弃，用户只会看到连接挂到超时才断，日志里还查不到痕迹。
            log.warn("【导购Agent】Agent 线程池已满，拒绝本轮请求。sessionId={}", request.getSessionId());
            try {
                send(emitter, AgentEvent.error("当前咨询人数较多，请稍后再试"));
            } catch (UncheckedIOException ignored) {
                // 此时 emitter 尚未写出任何内容，发送几乎不会失败；兜底避免异常冒泡
            }
            emitter.complete();
        }

        return emitter;
    }

    /**
     * 在 Agent 专用线程池里真正执行一轮对话，并把结果转成 SSE 帧推给前端。
     * <p>
     * 与 {@link #chat} 拆开是为了让线程池拒绝的异常能在提交处被捕获处理。
     */
    private void runChat(AgentChatRequest request, SseEmitter emitter) {
        // SSE 心跳：模型思考和工具执行期间连接上没有任何字节流动，
        // 代理/浏览器可能判定空闲而断开。定期推注释帧保活（前端收不到，对业务透明）。
        // 间隔来自 hmdp.agent.heartbeat-interval，<=0 表示关闭心跳。
        ScheduledFuture<?> heartbeat = null;
        long heartbeatSeconds = agentProperties.getHeartbeatInterval().getSeconds();
        if (heartbeatSeconds > 0) {
            heartbeat = agentHeartbeatScheduler.scheduleAtFixedRate(
                    () -> sendHeartbeat(emitter),
                    heartbeatSeconds, heartbeatSeconds, TimeUnit.SECONDS);
        }
        try {
            // 核心链路全部在 Service 里，这里只负责把事件转成 SSE 帧
            agentService.chat(request, event -> send(emitter, event));
            emitter.complete();
        } catch (UncheckedIOException e) {
            // 前端主动断开连接（关闭页面/取消提问）。此时中止对话是对的，能省下后续的模型调用开销
            log.debug("【导购Agent】前端连接已断开，中止本轮对话。sessionId={}", request.getSessionId());
            emitter.complete();
        } catch (Exception e) {
            log.error("【导购Agent】对话处理失败。sessionId={}", request.getSessionId(), e);
            try {
                send(emitter, AgentEvent.error("抱歉，服务暂时不可用，请稍后再试"));
            } catch (UncheckedIOException ignored) {
                // 连接已断，无需再发
            }
            emitter.complete();
        } finally {
            // 必须停掉心跳，否则连接关了心跳还在往死连接里写，日志被刷屏、线程白占
            if (heartbeat != null) {
                heartbeat.cancel(false);
            }
        }
    }

    /**
     * 发送一个 SSE 注释帧作为心跳。
     * <p>
     * {@code SseEmitter.event().comment(...)} 产生的是 {@code :hb} 这样的注释行，
     * 它只用于保活，不会触发前端的任何事件监听器。发送失败直接忽略——连接已经断了，
     * 真正的断开处理由主流程的 {@link UncheckedIOException} 分支负责。
     */
    private void sendHeartbeat(SseEmitter emitter) {
        try {
            emitter.send(SseEmitter.event().comment("hb"));
        } catch (Exception ignored) {
            // 连接不可用，静默忽略
        }
    }

    /**
     * 清空指定会话的全部数据：对话历史 + 推荐锚点。
     * <p>
     * 前端点「新会话」时调用。sessionId 由前端生成，来源同
     * {@link AgentChatRequest#getSessionId()}。
     *
     * @param sessionId 会话 id
     * @return 固定文案，前端不解析
     */
    @DeleteMapping("/session/{sessionId}")
    public String clearSession(@PathVariable("sessionId") String sessionId) {
        sessionStore.clearAll(sessionId);
        return "ok";
    }

    /**
     * 发送一条 SSE 事件。
     * <p>
     * 事件名（name）与事件体里的 type 保持一致，方便前端用 {@code addEventListener} 按类型监听。
     *
     * @throws UncheckedIOException 连接已断开时抛出，用于中断整个对话循环
     */
    private void send(SseEmitter emitter, AgentEvent event) {
        try {
            emitter.send(SseEmitter.event().name(event.getType()).data(event));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (IllegalStateException e) {
            // SseEmitter 已完成或超时后再 send 会走到这里，同样视为连接不可用
            throw new UncheckedIOException(new IOException("SSE 连接不可用", e));
        }
    }
}
