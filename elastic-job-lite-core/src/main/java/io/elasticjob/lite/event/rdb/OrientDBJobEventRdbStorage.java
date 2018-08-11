package io.elasticjob.lite.event.rdb;

/**
 * Created by Brian on 8/10/18
 */

import com.google.common.base.Strings;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;
import io.elasticjob.lite.event.type.JobStatusTraceEvent.Source;
import io.elasticjob.lite.event.type.JobStatusTraceEvent.State;
import io.elasticjob.lite.context.ExecutionType;
import io.elasticjob.lite.event.type.JobExecutionEvent;
import io.elasticjob.lite.event.type.JobStatusTraceEvent;
import lombok.extern.slf4j.Slf4j;

import javax.sql.DataSource;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * 运行痕迹事件数据库存储.
 *
 * @author Brian
 */
@Slf4j
final class OrientDBJobEventRdbStorage implements IJobEventRdbStorage {

    public static final String TABLE_JOB_EXECUTION_LOG = "JobExecutionLog";

    public static final String TABLE_JOB_STATUS_TRACE_LOG = "JobStatusTraceLog";

    private static final String TASK_ID_STATE_INDEX = "TaskIdStateIndex";

    private final DataSource dataSource;

    private DatabaseType databaseType;

    OrientDBJobEventRdbStorage(final DataSource dataSource) throws SQLException {
        this.dataSource = dataSource;
        initTablesAndIndexes();
    }

    private void initTablesAndIndexes() throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            createJobExecutionTableAndIndexIfNeeded(conn);
            createJobStatusTraceTableAndIndexIfNeeded(conn);
            databaseType = DatabaseType.valueFrom(conn.getMetaData().getDatabaseProductName());
        }
    }

    private void createJobExecutionTableAndIndexIfNeeded(final Connection conn) throws SQLException {
        DatabaseMetaData dbMetaData = conn.getMetaData();
        try (ResultSet resultSet = dbMetaData.getTables(null, null, TABLE_JOB_EXECUTION_LOG, null)) {
            if (!resultSet.next()) {
                createJobExecutionTable(conn);
            }
        }
    }

    private void createJobStatusTraceTableAndIndexIfNeeded(final Connection conn) throws SQLException {
        DatabaseMetaData dbMetaData = conn.getMetaData();
        try (ResultSet resultSet = dbMetaData.getTables(null, null, TABLE_JOB_STATUS_TRACE_LOG, null)) {
            if (!resultSet.next()) {
                createJobStatusTraceTable(conn);
            }
        }
        createTaskIdIndexIfNeeded(conn, TABLE_JOB_STATUS_TRACE_LOG, TASK_ID_STATE_INDEX);
    }

    private void createTaskIdIndexIfNeeded(final Connection conn, final String tableName, final String indexName) throws SQLException {
        DatabaseMetaData dbMetaData = conn.getMetaData();
        try (ResultSet resultSet = dbMetaData.getIndexInfo(null, null, tableName, false, false)) {
            boolean hasTaskIdIndex = false;
            while (resultSet.next()) {
                if (indexName.equals(resultSet.getString("INDEX_NAME"))) {
                    hasTaskIdIndex = true;
                }
            }
            if (!hasTaskIdIndex) {
                createTaskIdAndStateIndex(conn, tableName);
            }
        }
    }

    public void clearJobTables() throws SQLException {
        Statement stmt = dataSource.getConnection().createStatement();
        stmt.addBatch("DELETE vertex " + TABLE_JOB_EXECUTION_LOG);
        stmt.addBatch("DELETE vertex " + TABLE_JOB_STATUS_TRACE_LOG);
        stmt.executeBatch();
    }

    private void createJobExecutionTable(final Connection conn) throws SQLException {
        Statement stmt = conn.createStatement();
        stmt.addBatch("CREATE CLASS " + TABLE_JOB_EXECUTION_LOG + " IF NOT EXISTS EXTENDS V ");
        stmt.addBatch("CREATE PROPERTY " + TABLE_JOB_EXECUTION_LOG + ".id IF NOT EXISTS STRING ");
        stmt.addBatch("CREATE PROPERTY " + TABLE_JOB_EXECUTION_LOG + ".jobName IF NOT EXISTS STRING ");
        stmt.addBatch("CREATE PROPERTY " + TABLE_JOB_EXECUTION_LOG + ".taskId IF NOT EXISTS STRING ");
        stmt.addBatch("CREATE PROPERTY " + TABLE_JOB_EXECUTION_LOG + ".hostname IF NOT EXISTS STRING ");
        stmt.addBatch("CREATE PROPERTY " + TABLE_JOB_EXECUTION_LOG + ".ip IF NOT EXISTS STRING ");
        stmt.addBatch("CREATE PROPERTY " + TABLE_JOB_EXECUTION_LOG + ".shardingItem IF NOT EXISTS STRING ");
        stmt.addBatch("CREATE PROPERTY " + TABLE_JOB_EXECUTION_LOG + ".executionSource IF NOT EXISTS STRING ");
        stmt.addBatch("CREATE PROPERTY " + TABLE_JOB_EXECUTION_LOG + ".failureCause IF NOT EXISTS STRING ");
        stmt.addBatch("CREATE PROPERTY " + TABLE_JOB_EXECUTION_LOG + ".isSuccess IF NOT EXISTS BOOLEAN ");
        stmt.addBatch("CREATE PROPERTY " + TABLE_JOB_EXECUTION_LOG + ".startTime IF NOT EXISTS DATETIME ");
        stmt.addBatch("CREATE PROPERTY " + TABLE_JOB_EXECUTION_LOG + ".completeTime IF NOT EXISTS DATETIME ");
        stmt.addBatch("DROP INDEX " + TABLE_JOB_EXECUTION_LOG + ".id ");
        stmt.addBatch("CREATE INDEX " + TABLE_JOB_EXECUTION_LOG + ".id ON " + TABLE_JOB_EXECUTION_LOG + "(id) UNIQUE_HASH_INDEX METADATA { ignoreNullValues : true }");
        stmt.executeBatch();
    }

    private void createJobStatusTraceTable(final Connection conn) throws SQLException {
        Statement stmt = conn.createStatement();
        stmt.addBatch("CREATE CLASS " + TABLE_JOB_STATUS_TRACE_LOG + " IF NOT EXISTS EXTENDS V ");
        stmt.addBatch("CREATE PROPERTY " + TABLE_JOB_STATUS_TRACE_LOG + ".id IF NOT EXISTS STRING ");
        stmt.addBatch("CREATE PROPERTY " + TABLE_JOB_STATUS_TRACE_LOG + ".jobName IF NOT EXISTS STRING ");
        stmt.addBatch("CREATE PROPERTY " + TABLE_JOB_STATUS_TRACE_LOG + ".originalTaskId IF NOT EXISTS STRING ");
        stmt.addBatch("CREATE PROPERTY " + TABLE_JOB_STATUS_TRACE_LOG + ".taskId IF NOT EXISTS STRING ");
        stmt.addBatch("CREATE PROPERTY " + TABLE_JOB_STATUS_TRACE_LOG + ".slaveId IF NOT EXISTS STRING ");
        stmt.addBatch("CREATE PROPERTY " + TABLE_JOB_STATUS_TRACE_LOG + ".source IF NOT EXISTS STRING ");
        stmt.addBatch("CREATE PROPERTY " + TABLE_JOB_STATUS_TRACE_LOG + ".executionType IF NOT EXISTS STRING ");
        stmt.addBatch("CREATE PROPERTY " + TABLE_JOB_STATUS_TRACE_LOG + ".shardingItem IF NOT EXISTS STRING ");
        stmt.addBatch("CREATE PROPERTY " + TABLE_JOB_STATUS_TRACE_LOG + ".state IF NOT EXISTS STRING ");
        stmt.addBatch("CREATE PROPERTY " + TABLE_JOB_STATUS_TRACE_LOG + ".message IF NOT EXISTS STRING ");
        stmt.addBatch("CREATE PROPERTY " + TABLE_JOB_STATUS_TRACE_LOG + ".creationTime IF NOT EXISTS DATETIME ");
        stmt.addBatch("DROP INDEX " + TABLE_JOB_STATUS_TRACE_LOG + ".id ");
        stmt.addBatch("CREATE INDEX " + TABLE_JOB_STATUS_TRACE_LOG + ".id ON " + TABLE_JOB_STATUS_TRACE_LOG + "(id) UNIQUE_HASH_INDEX METADATA { ignoreNullValues : true }");

        stmt.executeBatch();
    }

    private void createTaskIdAndStateIndex(final Connection conn, final String tableName) throws SQLException {
        Statement stmt = conn.createStatement();
        stmt.addBatch("DROP INDEX " + tableName + "." + TASK_ID_STATE_INDEX);
        stmt.addBatch("CREATE INDEX " + tableName + "." + TASK_ID_STATE_INDEX + " ON " + tableName + "(taskId, state) NOTUNIQUE_HASH_INDEX METADATA { ignoreNullValues : true }");
        stmt.executeBatch();
    }

    @Override
    public boolean addJobExecutionEvent(final JobExecutionEvent jobExecutionEvent) {
        if (null == jobExecutionEvent.getCompleteTime()) {
            return insertJobExecutionEvent(jobExecutionEvent);
        } else {
            if (jobExecutionEvent.isSuccess()) {
                return updateJobExecutionEventWhenSuccess(jobExecutionEvent);
            } else {
                return updateJobExecutionEventFailure(jobExecutionEvent);
            }
        }
    }

    private boolean insertJobExecutionEvent(final JobExecutionEvent jobExecutionEvent) {
        boolean result = false;
        String sql = "INSERT INTO " + TABLE_JOB_EXECUTION_LOG + "(id, jobName, taskId, hostname, ip, shardingItem, executionSource, isSuccess, startTime) "
                + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
            preparedStatement.setString(1, jobExecutionEvent.getId());
            preparedStatement.setString(2, jobExecutionEvent.getJobName());
            preparedStatement.setString(3, jobExecutionEvent.getTaskId());
            preparedStatement.setString(4, jobExecutionEvent.getHostname());
            preparedStatement.setString(5, jobExecutionEvent.getIp());
            preparedStatement.setInt(6, jobExecutionEvent.getShardingItem());
            preparedStatement.setString(7, jobExecutionEvent.getSource().toString());
            preparedStatement.setBoolean(8, jobExecutionEvent.isSuccess());
            preparedStatement.setDate(9, new Date(jobExecutionEvent.getStartTime().getTime()));
            preparedStatement.execute();
            result = true;
        } catch (final SQLException ex) {
            // TODO 记录失败直接输出日志,未来可考虑配置化
            log.error(ex.getMessage());
        }
        return result;
    }

    private boolean updateJobExecutionEventWhenSuccess(final JobExecutionEvent jobExecutionEvent) {
        boolean result = false;
        String sql = "UPDATE " + TABLE_JOB_EXECUTION_LOG + " SET isSuccess = ?, completeTime = ? WHERE id = ?";
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
            preparedStatement.setBoolean(1, jobExecutionEvent.isSuccess());
            preparedStatement.setDate(2, new Date(jobExecutionEvent.getCompleteTime().getTime()));
            preparedStatement.setString(3, jobExecutionEvent.getId());
            if (0 == preparedStatement.executeUpdate()) {
                return insertJobExecutionEventWhenSuccess(jobExecutionEvent);
            }
            result = true;
        } catch (final SQLException ex) {
            // TODO 记录失败直接输出日志,未来可考虑配置化
            log.error(ex.getMessage());
        }
        return result;
    }

    private boolean insertJobExecutionEventWhenSuccess(final JobExecutionEvent jobExecutionEvent) {
        boolean result = false;
        String sql = "INSERT INTO " + TABLE_JOB_EXECUTION_LOG + " (id, jobName, taskId, hostname, ip, shardingItem, executionSource, isSuccess, startTime, completeTime) "
                + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
            preparedStatement.setString(1, jobExecutionEvent.getId());
            preparedStatement.setString(2, jobExecutionEvent.getJobName());
            preparedStatement.setString(3, jobExecutionEvent.getTaskId());
            preparedStatement.setString(4, jobExecutionEvent.getHostname());
            preparedStatement.setString(5, jobExecutionEvent.getIp());
            preparedStatement.setInt(6, jobExecutionEvent.getShardingItem());
            preparedStatement.setString(7, jobExecutionEvent.getSource().toString());
            preparedStatement.setBoolean(8, jobExecutionEvent.isSuccess());
            preparedStatement.setDate(9, new Date(jobExecutionEvent.getStartTime().getTime()));
            preparedStatement.setDate(10, new Date(jobExecutionEvent.getCompleteTime().getTime()));
            preparedStatement.execute();
            result = true;
        } catch(final ORecordDuplicatedException ex) {
            return updateJobExecutionEventWhenSuccess(jobExecutionEvent);
        } catch (final SQLException ex) {
            // TODO 记录失败直接输出日志,未来可考虑配置化
            log.error(ex.getMessage());
        }
        return result;
    }

    private boolean updateJobExecutionEventFailure(final JobExecutionEvent jobExecutionEvent) {
        boolean result = false;
        String sql = "UPDATE " + TABLE_JOB_EXECUTION_LOG + " SET isSuccess = ?, completeTime = ?, failureCause = ? WHERE id = ?";
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
            preparedStatement.setBoolean(1, jobExecutionEvent.isSuccess());
            preparedStatement.setDate(2, new Date(jobExecutionEvent.getCompleteTime().getTime()));
            preparedStatement.setString(3, truncateString(jobExecutionEvent.getFailureCause()));
            preparedStatement.setString(4, jobExecutionEvent.getId());
            if (0 == preparedStatement.executeUpdate()) {
                return insertJobExecutionEventWhenFailure(jobExecutionEvent);
            }
            result = true;
        } catch (final SQLException ex) {
            // TODO 记录失败直接输出日志,未来可考虑配置化
            log.error(ex.getMessage());
        }
        return result;
    }

    private boolean insertJobExecutionEventWhenFailure(final JobExecutionEvent jobExecutionEvent) {
        boolean result = false;
        String sql = "INSERT INTO " + TABLE_JOB_EXECUTION_LOG + " (id, jobName, taskId, hostname, ip, shardingItem, executionSource, failureCause, isSuccess, startTime) "
                + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
            preparedStatement.setString(1, jobExecutionEvent.getId());
            preparedStatement.setString(2, jobExecutionEvent.getJobName());
            preparedStatement.setString(3, jobExecutionEvent.getTaskId());
            preparedStatement.setString(4, jobExecutionEvent.getHostname());
            preparedStatement.setString(5, jobExecutionEvent.getIp());
            preparedStatement.setInt(6, jobExecutionEvent.getShardingItem());
            preparedStatement.setString(7, jobExecutionEvent.getSource().toString());
            preparedStatement.setString(8, truncateString(jobExecutionEvent.getFailureCause()));
            preparedStatement.setBoolean(9, jobExecutionEvent.isSuccess());
            preparedStatement.setDate(10, new Date(jobExecutionEvent.getStartTime().getTime()));
            preparedStatement.execute();
            result = true;
        } catch (final ORecordDuplicatedException ex) {
            return updateJobExecutionEventFailure(jobExecutionEvent);
        } catch (final SQLException ex) {
            // TODO 记录失败直接输出日志,未来可考虑配置化
            log.error(ex.getMessage());
        }
        return result;
    }

    @Override
    public boolean addJobStatusTraceEvent(final JobStatusTraceEvent jobStatusTraceEvent) {
        String originalTaskId = jobStatusTraceEvent.getOriginalTaskId();
        if (State.TASK_STAGING != jobStatusTraceEvent.getState()) {
            originalTaskId = getOriginalTaskId(jobStatusTraceEvent.getTaskId());
        }
        boolean result = false;
        String sql = "INSERT INTO " + TABLE_JOB_STATUS_TRACE_LOG + " (id, jobName, originalTaskId, taskId, slaveId, source, executionType, shardingItem,  "
                + "state, message, creationTime) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
            preparedStatement.setString(1, UUID.randomUUID().toString());
            preparedStatement.setString(2, jobStatusTraceEvent.getJobName());
            preparedStatement.setString(3, originalTaskId);
            preparedStatement.setString(4, jobStatusTraceEvent.getTaskId());
            preparedStatement.setString(5, jobStatusTraceEvent.getSlaveId());
            preparedStatement.setString(6, jobStatusTraceEvent.getSource().toString());
            preparedStatement.setString(7, jobStatusTraceEvent.getExecutionType().name());
            preparedStatement.setString(8, jobStatusTraceEvent.getShardingItems());
            preparedStatement.setString(9, jobStatusTraceEvent.getState().toString());
            preparedStatement.setString(10, truncateString(jobStatusTraceEvent.getMessage()));
            preparedStatement.setDate(11, new Date(jobStatusTraceEvent.getCreationTime().getTime()));
            preparedStatement.execute();
            result = true;
        } catch (final SQLException ex) {
            // TODO 记录失败直接输出日志,未来可考虑配置化
            log.error(ex.getMessage());
        }
        return result;
    }

    private String getOriginalTaskId(final String taskId) {
        String sql = String.format("SELECT originalTaskId FROM %s WHERE taskId = '%s' and state='%s' LIMIT 1", TABLE_JOB_STATUS_TRACE_LOG, taskId, State.TASK_STAGING);
        String result = "";
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement preparedStatement = conn.prepareStatement(sql);
                ResultSet resultSet = preparedStatement.executeQuery()
        ) {
            if (resultSet.next()) {
                return resultSet.getString("originalTaskId");
            }
        } catch (final SQLException ex) {
            // TODO 记录失败直接输出日志,未来可考虑配置化
            log.error(ex.getMessage());
        }
        return result;
    }

    private String truncateString(final String str) {
        return !Strings.isNullOrEmpty(str) && str.length() > 4000 ? str.substring(0, 4000) : str;
    }

    @Override
    public List<JobStatusTraceEvent> getJobStatusTraceEvents(final String taskId) {
        String sql = String.format("SELECT FROM %s WHERE taskId = '%s'", TABLE_JOB_STATUS_TRACE_LOG, taskId);
        List<JobStatusTraceEvent> result = new ArrayList<>();
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement preparedStatement = conn.prepareStatement(sql);
                ResultSet resultSet = preparedStatement.executeQuery()
        ) {
            while (resultSet.next()) {
                JobStatusTraceEvent jobStatusTraceEvent = new JobStatusTraceEvent(resultSet.getString(1), resultSet.getString(2), resultSet.getString(3), resultSet.getString(4),
                        resultSet.getString(5), Source.valueOf(resultSet.getString(6)), ExecutionType.valueOf(resultSet.getString(7)), resultSet.getString(8),
                        State.valueOf(resultSet.getString(9)), resultSet.getString(10), resultSet.getDate(11));
                result.add(jobStatusTraceEvent);
            }
        } catch (final SQLException ex) {
            // TODO 记录失败直接输出日志,未来可考虑配置化
            log.error(ex.getMessage());
        }
        return result;
    }
}

