package com.example.project.flinktool.utils;

import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class ScheduleUtils {
    
    private static final Logger LOG = LoggerFactory.getLogger(ScheduleUtils.class);


    private final ScheduledExecutorService scheduler;
    private final ConcurrentHashMap<String, ScheduledExecutorService> taskMap;
    private final AtomicInteger taskIdGenerator;

    public ScheduleUtils(ScheduledExecutorService scheduler) {
        this.scheduler = scheduler != null ? scheduler : Executors.newScheduledThreadPool(1);
        this.taskMap = new ConcurrentHashMap<>();
        this.taskIdGenerator = new AtomicInteger();
    }


    /**
     * 添加一个固定频率执行的调度任务
     *
     * @param taskName 任务名称，用于标识任务
     * @param task 要执行的任务逻辑
     * @param initialDelay 初始延迟时间
     * @param period 执行周期
     * @param unit 时间单位
     * @return 生成的任务ID
     */
    public String addFixedRateSchedule(String taskName, Runnable task , long initialDelay, long period, TimeUnit unit) {
        // 生成唯一任务ID
        String taskId = taskName + "_" + taskIdGenerator.incrementAndGet();
        // 调度固定频率任务
        ScheduledFuture<?> future = scheduler.scheduleAtFixedRate(
                () -> {
                    // 输出任务执行日志
                    System.out.println(new Date() + "[ " + Thread.currentThread().getName() + " ] 执行任务" + taskName  + "("+ taskId+ ")");
                    try {
                        task.run();
                    } catch (Exception e) {
                        LOG.error("Error executing task", e);
                    }
                },
                initialDelay, period, unit
        );
        
        // 将任务future存储到map中以便后续管理
        taskMap.put(taskId, scheduler);
        
        return taskId;
    }

    /**
     * 创建一个定时任务，每隔一段时间触发一次savepoint
     *
     * @param jobID      作业ID
     * @param savepointPath savepoint保存路径
     * @param flinkConfig Flink配置
     */
    
    public static void scheduleSavepoint(String jobID, String savepointPath, Configuration flinkConfig) {
        // 定时触发savepoint的代码逻辑
        // 创建一个定时任务，每隔一段时间触发一次savepoint
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        
        scheduler.scheduleAtFixedRate(() -> {
            try {
                // 触发savepoint
                String savepointLocation = SavepointUtil.triggerSavepoint(jobID, savepointPath, flinkConfig);
                LOG.info("Triggered savepoint for job {} at {}", jobID, savepointLocation);
            } catch (Exception e) {
                LOG.error("Error triggering savepoint", e);
            }
        }, 1, 1, TimeUnit.HOURS);
        
        // 注意：在实际应用中，你需要妥善管理scheduler的生命周期
        // 通常在应用关闭时调用 scheduler.shutdown()
        // scheduler.shutdown(); // 注释掉这行，避免立即关闭调度器
    }
}
