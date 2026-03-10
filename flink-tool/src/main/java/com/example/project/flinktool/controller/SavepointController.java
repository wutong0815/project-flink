package com.example.project.flinktool.controller;

import com.example.project.flinktool.utils.SavepointUtil;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.HashMap;
import java.util.Map;

/**
 * Savepoint REST API 控制器
 * 提供HTTP接口用于触发savepoint操作
 */
@Path("/api/v1/savepoint")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class SavepointController {

    private static final Logger LOG = LoggerFactory.getLogger(SavepointController.class);

    /**
     * 触发savepoint
     */
    @POST
    @Path("/trigger")
    public Response triggerSavepoint(Map<String, String> request) {
        try {
            String jobID = request.get("jobId");
            String savepointPath = request.get("savepointPath");
            
            if (jobID == null || savepointPath == null) {
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity("{\"error\":\"Missing jobId or savepointPath parameter\"}")
                        .build();
            }

            // 使用默认Flink配置
            Configuration flinkConfig = SavepointUtil.getDefaultFlinkConfiguration();

            // 触发savepoint
            String savepointLocation = SavepointUtil.triggerSavepoint(jobID, savepointPath, flinkConfig);

            Map<String, String> response = new HashMap<>();
            response.put("status", "success");
            response.put("savepointLocation", savepointLocation);
            
            LOG.info("Successfully triggered savepoint for job {} at {}", jobID, savepointLocation);
            
            return Response.ok(response).build();
        } catch (Exception e) {
            LOG.error("Error triggering savepoint", e);
            Map<String, String> errorResponse = new HashMap<>();
            errorResponse.put("status", "error");
            errorResponse.put("message", e.getMessage());
            
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(errorResponse)
                    .build();
        }
    }

    /**
     * 停止作业并创建savepoint
     */
    @POST
    @Path("/stop-with-savepoint")
    public Response stopJobWithSavepoint(Map<String, String> request) {
        try {
            String jobID = request.get("jobId");
            String savepointPath = request.get("savepointPath");
            
            if (jobID == null || savepointPath == null) {
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity("{\"error\":\"Missing jobId or savepointPath parameter\"}")
                        .build();
            }

            // 使用默认Flink配置
            Configuration flinkConfig = SavepointUtil.getDefaultFlinkConfiguration();

            // 停止作业并创建savepoint
            String savepointLocation = SavepointUtil.stopWithSavepoint(jobID, savepointPath, flinkConfig);

            Map<String, String> response = new HashMap<>();
            response.put("status", "success");
            response.put("savepointLocation", savepointLocation);
            
            LOG.info("Successfully stopped job {} with savepoint at {}", jobID, savepointLocation);
            
            return Response.ok(response).build();
        } catch (Exception e) {
            LOG.error("Error stopping job with savepoint", e);
            Map<String, String> errorResponse = new HashMap<>();
            errorResponse.put("status", "error");
            errorResponse.put("message", e.getMessage());
            
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(errorResponse)
                    .build();
        }
    }
    
    /**
     * 从savepoint恢复作业
     */
    @POST
    @Path("/restore-from-savepoint")
    public Response restoreFromSavepoint(Map<String, String> request) {
        try {
            String jarPath = request.get("jarPath");
            String savepointPath = request.get("savepointPath");
            String className = request.get("className");
            
            if (jarPath == null || savepointPath == null || className == null) {
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity("{\"error\":\"Missing jarPath, savepointPath or className parameter\"}")
                        .build();
            }

            // 使用默认Flink配置
            Configuration flinkConfig = SavepointUtil.getDefaultFlinkConfiguration();
            
            // 解析程序参数
            String[] programArgs = new String[0];
            String programArgsStr = request.get("programArgs");
            if (programArgsStr != null && !programArgsStr.isEmpty()) {
                programArgs = programArgsStr.split("\\s+");
            }

            // 从savepoint恢复作业
            SavepointUtil.restoreFromSavepoint(jarPath, savepointPath, className, programArgs, flinkConfig);

            Map<String, String> response = new HashMap<>();
            response.put("status", "success");
            response.put("message", "Successfully restored job from savepoint");
            
            LOG.info("Successfully restored job from savepoint at {}", savepointPath);
            
            return Response.ok(response).build();
        } catch (Exception e) {
            LOG.error("Error restoring job from savepoint", e);
            Map<String, String> errorResponse = new HashMap<>();
            errorResponse.put("status", "error");
            errorResponse.put("message", e.getMessage());
            
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(errorResponse)
                    .build();
        }
    }
}