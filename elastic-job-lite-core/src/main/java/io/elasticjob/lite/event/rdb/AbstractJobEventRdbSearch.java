package io.elasticjob.lite.event.rdb;

import com.google.common.base.CaseFormat;
import com.google.common.base.Strings;
import lombok.extern.slf4j.Slf4j;

import javax.sql.DataSource;
import java.sql.*;
import java.util.Collection;
import java.util.Map;

/**
 * Created by Brian on 8/10/18
 */
@Slf4j
public abstract class AbstractJobEventRdbSearch implements IJobEventRdbSearch {

    protected final DataSource dataSource;

    AbstractJobEventRdbSearch(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    protected int getEventCount(final String tableName, final Collection<String> tableFields, final Condition condition) {
        int result = 0;
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement preparedStatement = createCountPreparedStatement(conn, tableName, tableFields, condition);
                ResultSet resultSet = preparedStatement.executeQuery()
        ) {
            resultSet.next();
            result = resultSet.getInt(1);
        } catch (final SQLException ex) {
            // TODO 记录失败直接输出日志,未来可考虑配置化
            log.error("Fetch EventCount from DB error:", ex);
        }
        return result;
    }

    protected PreparedStatement createDataPreparedStatement(final Connection conn, final String tableName, final Collection<String> tableFields, final Condition condition) throws SQLException {
        String sql = buildDataSql(tableName, tableFields, condition);
        PreparedStatement preparedStatement = conn.prepareStatement(sql);
        setBindValue(preparedStatement, tableFields, condition);
        return preparedStatement;
    }

    protected PreparedStatement createCountPreparedStatement(final Connection conn, final String tableName, final Collection<String> tableFields, final Condition condition) throws SQLException {
        String sql = buildCountSql(tableName, tableFields, condition);
        PreparedStatement preparedStatement = conn.prepareStatement(sql);
        setBindValue(preparedStatement, tableFields, condition);
        return preparedStatement;
    }

    protected String buildDataSql(final String tableName, final Collection<String> tableFields, final Condition condition) {
        StringBuilder sqlBuilder = new StringBuilder();
        String selectSql = buildSelect(tableName, tableFields);
        String whereSql = buildWhere(tableName, tableFields, condition);
        String orderSql = buildOrder(tableFields, condition.getSort(), condition.getOrder());
        String limitSql = buildLimit(condition.getPage(), condition.getPerPage());
        sqlBuilder.append(selectSql).append(whereSql).append(orderSql).append(limitSql);
        return sqlBuilder.toString();
    }

    protected String buildCountSql(final String tableName, final Collection<String> tableFields, final Condition condition) {
        StringBuilder sqlBuilder = new StringBuilder();
        String selectSql = buildSelectCount(tableName);
        String whereSql = buildWhere(tableName, tableFields, condition);
        sqlBuilder.append(selectSql).append(whereSql);
        return sqlBuilder.toString();
    }

    protected String buildSelectCount(final String tableName) {
        return String.format("SELECT COUNT(*) FROM %s", tableName);
    }

    protected String buildSelect(final String tableName, final Collection<String> tableFields) {
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT ");
        for (String each : tableFields) {
            sqlBuilder.append(each).append(",");
        }
        sqlBuilder.deleteCharAt(sqlBuilder.length() - 1);
        sqlBuilder.append(" FROM ").append(tableName);
        return sqlBuilder.toString();
    }

    protected String buildWhere(final String tableName, final Collection<String> tableFields, final Condition condition) {
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append(" WHERE 1=1");
        if (null != condition.getFields() && !condition.getFields().isEmpty()) {
            for (Map.Entry<String, Object> entry : condition.getFields().entrySet()) {
                String lowerUnderscore = CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, entry.getKey());
                if (null != entry.getValue() && tableFields.contains(lowerUnderscore)) {
                    sqlBuilder.append(" AND ").append(lowerUnderscore).append("=?");
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

    protected void setBindValue(final PreparedStatement preparedStatement, final Collection<String> tableFields, final Condition condition) throws SQLException {
        int index = 1;
        if (null != condition.getFields() && !condition.getFields().isEmpty()) {
            for (Map.Entry<String, Object> entry : condition.getFields().entrySet()) {
                String lowerUnderscore = CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, entry.getKey());
                if (null != entry.getValue() && tableFields.contains(lowerUnderscore)) {
                    preparedStatement.setString(index++, String.valueOf(entry.getValue()));
                }
            }
        }
        if (null != condition.getStartTime()) {
            preparedStatement.setTimestamp(index++, new Timestamp(condition.getStartTime().getTime()));
        }
        if (null != condition.getEndTime()) {
            preparedStatement.setTimestamp(index, new Timestamp(condition.getEndTime().getTime()));
        }
    }


    protected String buildOrder(final Collection<String> tableFields, final String sortName, final String sortOrder) {
        if (Strings.isNullOrEmpty(sortName)) {
            return "";
        }
        String lowerUnderscore = CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, sortName);
        if (!tableFields.contains(lowerUnderscore)) {
            return "";
        }
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append(" ORDER BY ").append(lowerUnderscore);
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

    protected String buildLimit(final int page, final int perPage) {
        StringBuilder sqlBuilder = new StringBuilder();
        if (page > 0 && perPage > 0) {
            sqlBuilder.append(" LIMIT ").append((page - 1) * perPage).append(",").append(perPage);
        } else {
            sqlBuilder.append(" LIMIT ").append(Condition.DEFAULT_PAGE_SIZE);
        }
        return sqlBuilder.toString();
    }

    protected abstract String getTableTimeField(final String tableName);
}
