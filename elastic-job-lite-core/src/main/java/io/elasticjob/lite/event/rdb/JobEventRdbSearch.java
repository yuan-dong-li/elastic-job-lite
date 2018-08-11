/*
 * Copyright 1999-2015 dangdang.com.
 * <p>
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
 * </p>
 */

package io.elasticjob.lite.event.rdb;

import com.google.common.collect.Lists;
import io.elasticjob.lite.context.ExecutionType;
import io.elasticjob.lite.event.type.JobExecutionEvent;
import io.elasticjob.lite.event.type.JobExecutionEventThrowable;
import io.elasticjob.lite.event.type.JobStatusTraceEvent;
import io.elasticjob.lite.event.type.JobStatusTraceEvent.Source;
import io.elasticjob.lite.event.type.JobStatusTraceEvent.State;
import lombok.extern.slf4j.Slf4j;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

/**
 * 运行痕迹事件数据库检索.
 *
 * @author liguangyun
 * @author Brian
 */
@Slf4j
public final class JobEventRdbSearch extends AbstractJobEventRdbSearch {
    
    private static final String TABLE_JOB_EXECUTION_LOG = "JOB_EXECUTION_LOG";
    
    private static final String TABLE_JOB_STATUS_TRACE_LOG = "JOB_STATUS_TRACE_LOG";
    
    private static final List<String> FIELDS_JOB_EXECUTION_LOG = 
            Lists.newArrayList("id", "hostname", "ip", "task_id", "job_name", "execution_source", "sharding_item", "start_time", "complete_time", "is_success", "failure_cause");
    
    private static final List<String> FIELDS_JOB_STATUS_TRACE_LOG = 
            Lists.newArrayList("id", "job_name", "original_task_id", "task_id", "slave_id", "source", "execution_type", "sharding_item", "state", "message", "creation_time");


    public JobEventRdbSearch(DataSource dataSource) {
        super(dataSource);
    }
    
    /**
     * 检索作业运行执行轨迹.
     * 
     * @param condition 查询条件
     * @return 作业执行轨迹检索结果
     */
    @Override
    public Result<JobExecutionEvent> findJobExecutionEvents(final Condition condition) {
        return new Result<>(getEventCount(TABLE_JOB_EXECUTION_LOG, FIELDS_JOB_EXECUTION_LOG, condition), getJobExecutionEvents(condition));
    }
    
    /**
     * 检索作业运行状态轨迹.
     * 
     * @param condition 查询条件
     * @return 作业状态轨迹检索结果
     */
    @Override
    public Result<JobStatusTraceEvent> findJobStatusTraceEvents(final Condition condition) {
        return new Result<>(getEventCount(TABLE_JOB_STATUS_TRACE_LOG, FIELDS_JOB_STATUS_TRACE_LOG, condition), getJobStatusTraceEvents(condition));
    }
    
    private List<JobExecutionEvent> getJobExecutionEvents(final Condition condition) {
        List<JobExecutionEvent> result = new LinkedList<>();
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement preparedStatement = createDataPreparedStatement(conn, TABLE_JOB_EXECUTION_LOG, FIELDS_JOB_EXECUTION_LOG, condition);
                ResultSet resultSet = preparedStatement.executeQuery()
                ) {
            while (resultSet.next()) {
                JobExecutionEvent jobExecutionEvent = new JobExecutionEvent(resultSet.getString(1), resultSet.getString(2), resultSet.getString(3), resultSet.getString(4),
                        resultSet.getString(5), JobExecutionEvent.ExecutionSource.valueOf(resultSet.getString(6)), Integer.valueOf(resultSet.getString(7)), 
                        new Date(resultSet.getTimestamp(8).getTime()), resultSet.getTimestamp(9) == null ? null : new Date(resultSet.getTimestamp(9).getTime()), 
                        resultSet.getBoolean(10), new JobExecutionEventThrowable(null, resultSet.getString(11)) 
                        );
                result.add(jobExecutionEvent);
            }
        } catch (final SQLException ex) {
            // TODO 记录失败直接输出日志,未来可考虑配置化
            log.error("Fetch JobExecutionEvent from DB error:", ex);
        }
        return result;
    }
    
    private List<JobStatusTraceEvent> getJobStatusTraceEvents(final Condition condition) {
        List<JobStatusTraceEvent> result = new LinkedList<>();
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement preparedStatement = createDataPreparedStatement(conn, TABLE_JOB_STATUS_TRACE_LOG, FIELDS_JOB_STATUS_TRACE_LOG, condition);
                ResultSet resultSet = preparedStatement.executeQuery()
                ) {
            while (resultSet.next()) {
                JobStatusTraceEvent jobStatusTraceEvent = new JobStatusTraceEvent(resultSet.getString(1), resultSet.getString(2), resultSet.getString(3), resultSet.getString(4),
                        resultSet.getString(5), Source.valueOf(resultSet.getString(6)), ExecutionType.valueOf(resultSet.getString(7)), resultSet.getString(8),
                        State.valueOf(resultSet.getString(9)), resultSet.getString(10), new Date(resultSet.getTimestamp(11).getTime()));
                result.add(jobStatusTraceEvent);
            }
        } catch (final SQLException ex) {
            // TODO 记录失败直接输出日志,未来可考虑配置化
            log.error("Fetch JobStatusTraceEvent from DB error:", ex);
        }
        return result;
    }

    protected String getTableTimeField(final String tableName) {
        String result = "";
        if (TABLE_JOB_EXECUTION_LOG.equals(tableName)) {
            result = "start_time";
        } else if (TABLE_JOB_STATUS_TRACE_LOG.equals(tableName)) {
            result = "creation_time";
        }
        return result;
    }


}
