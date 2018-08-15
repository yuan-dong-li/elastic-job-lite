package io.elasticjob.lite.event.rdb;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import io.elasticjob.lite.context.ExecutionType;
import io.elasticjob.lite.event.type.JobExecutionEvent;
import io.elasticjob.lite.event.type.JobExecutionEventThrowable;
import io.elasticjob.lite.event.type.JobStatusTraceEvent;
import lombok.extern.slf4j.Slf4j;

import javax.sql.DataSource;
import java.sql.*;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Created by Brian on 8/10/18
 */
@Slf4j
public final class OrientDBJobEventRdbSearch extends AbstractJobEventRdbSearch {

    private static final List<String> FIELDS_JOB_EXECUTION_LOG =
            Lists.newArrayList("id", "hostname", "ip", "taskId", "jobName", "executionSource", "shardingItem", "startTime", "completeTime", "isSuccess", "failureCause");

    private static final List<String> FIELDS_JOB_STATUS_TRACE_LOG =
            Lists.newArrayList("id", "jobName", "originalTaskId", "taskId", "slaveId", "source", "executionType", "shardingItem", "state", "message", "creationTime");

    public OrientDBJobEventRdbSearch(DataSource dataSource) {
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
        return new Result<>(getEventCount(OrientDBJobEventRdbStorage.TABLE_JOB_EXECUTION_LOG, FIELDS_JOB_EXECUTION_LOG, condition), getJobExecutionEvents(condition));
    }

    /**
     * 检索作业运行状态轨迹.
     *
     * @param condition 查询条件
     * @return 作业状态轨迹检索结果
     */
    @Override
    public Result<JobStatusTraceEvent> findJobStatusTraceEvents(final Condition condition) {
        return new Result<>(getEventCount(OrientDBJobEventRdbStorage.TABLE_JOB_STATUS_TRACE_LOG, FIELDS_JOB_STATUS_TRACE_LOG, condition), getJobStatusTraceEvents(condition));
    }

    private List<JobExecutionEvent> getJobExecutionEvents(final Condition condition) {
        List<JobExecutionEvent> result = new LinkedList<>();
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement preparedStatement = createDataPreparedStatement(conn, OrientDBJobEventRdbStorage.TABLE_JOB_EXECUTION_LOG, FIELDS_JOB_EXECUTION_LOG, condition);
                ResultSet resultSet = preparedStatement.executeQuery()
        ) {
            while (resultSet.next()) {
                JobExecutionEvent jobExecutionEvent = new JobExecutionEvent(resultSet.getString(1), resultSet.getString(2), resultSet.getString(3), resultSet.getString(4),
                        resultSet.getString(5), JobExecutionEvent.ExecutionSource.valueOf(resultSet.getString(6)), Integer.valueOf(resultSet.getString(7)),
                        new Date(resultSet.getDate(8).getTime()), resultSet.getDate(9) == null ? null : new Date(resultSet.getDate(9).getTime()),
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
                PreparedStatement preparedStatement = createDataPreparedStatement(conn, OrientDBJobEventRdbStorage.TABLE_JOB_STATUS_TRACE_LOG, FIELDS_JOB_STATUS_TRACE_LOG, condition);
                ResultSet resultSet = preparedStatement.executeQuery()
        ) {
            while (resultSet.next()) {
                JobStatusTraceEvent jobStatusTraceEvent = new JobStatusTraceEvent(resultSet.getString(1), resultSet.getString(2), resultSet.getString(3), resultSet.getString(4),
                        resultSet.getString(5), JobStatusTraceEvent.Source.valueOf(resultSet.getString(6)), ExecutionType.valueOf(resultSet.getString(7)), resultSet.getString(8),
                        JobStatusTraceEvent.State.valueOf(resultSet.getString(9)), resultSet.getString(10), new Date(resultSet.getDate(11).getTime()));
                result.add(jobStatusTraceEvent);
            }
        } catch (final SQLException ex) {
            // TODO 记录失败直接输出日志,未来可考虑配置化
            log.error("Fetch JobStatusTraceEvent from DB error:", ex);
        }
        return result;
    }

    @Override
    protected String getTableTimeField(final String tableName) {
        String result = "";
        if (OrientDBJobEventRdbStorage.TABLE_JOB_EXECUTION_LOG.equals(tableName)) {
            result = "startTime";
        } else if (OrientDBJobEventRdbStorage.TABLE_JOB_STATUS_TRACE_LOG.equals(tableName)) {
            result = "creationTime";
        }
        return result;
    }

    @Override
    protected String buildLimit(final int page, final int perPage) {
        StringBuilder sqlBuilder = new StringBuilder();
        if (page > 0 && perPage > 0) {
            sqlBuilder.append(" SKIP ").append((page - 1) * perPage).append(" LIMIT ").append(perPage);
        } else {
            sqlBuilder.append(" LIMIT ").append(Condition.DEFAULT_PAGE_SIZE);
        }
        return sqlBuilder.toString();
    }

    @Override
    protected void setBindValue(final PreparedStatement preparedStatement, final Collection<String> tableFields, final Condition condition) throws SQLException {
        int index = 1;
        if (null != condition.getFields() && !condition.getFields().isEmpty()) {
            for (Map.Entry<String, Object> entry : condition.getFields().entrySet()) {
                if (null != entry.getValue() && tableFields.contains(entry.getKey())) {
                    if("isSuccess".equals(entry.getKey())) {
                        String val = String.valueOf(entry.getValue());
                        preparedStatement.setBoolean(index++, ("1".equals(val) || "true".equals(val))? true: false);
                    } else {
                        preparedStatement.setString(index++, String.valueOf(entry.getValue()));
                    }
                }
            }
        }
        if (null != condition.getStartTime()) {
            preparedStatement.setDate(index++, new Date(condition.getStartTime().getTime()));
        }
        if (null != condition.getEndTime()) {
            preparedStatement.setDate(index, new Date(condition.getEndTime().getTime()));
        }
    }

    @Override
    protected String buildWhere(final String tableName, final Collection<String> tableFields, final Condition condition) {
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append(" WHERE 1=1");
        if (null != condition.getFields() && !condition.getFields().isEmpty()) {
            for (Map.Entry<String, Object> entry : condition.getFields().entrySet()) {
                if (null != entry.getValue() && tableFields.contains(entry.getKey())) {
                    sqlBuilder.append(" AND ").append(entry.getKey()).append("=?");
                }
            }
        }
        if (null != condition.getStartTime()) {
            sqlBuilder.append(" AND ").append(getTableTimeField(tableName)).append(">=?");
        }
        if (null != condition.getEndTime()) {
            sqlBuilder.append(" AND ").append(getTableTimeField(tableName)).append("<=?");
        }
        return sqlBuilder.toString();
    }

    @Override
    protected String buildOrder(final Collection<String> tableFields, final String sortName, final String sortOrder) {
        if (Strings.isNullOrEmpty(sortName)) {
            return "";
        }

        if (!tableFields.contains(sortName)) {
            return "";
        }
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append(" ORDER BY ").append(sortName);
        switch (sortOrder.toUpperCase()) {
            case "ASC":
                sqlBuilder.append(" ASC");
                break;
            case "DESC":
                sqlBuilder.append(" DESC");
                break;
            default :
                sqlBuilder.append(" ASC");
        }
        return sqlBuilder.toString();
    }
}
