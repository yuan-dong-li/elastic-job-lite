package io.elasticjob.lite.event.rdb;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * Created by Brian on 8/10/18
 */
public class JobEventRdbStorageFactory {
    public static IJobEventRdbStorage getInstance(DataSource dataSource) throws SQLException {
        IJobEventRdbStorage repository = null;
        try (Connection conn = dataSource.getConnection()) {
            DatabaseType databaseType = DatabaseType.valueFrom(conn.getMetaData().getDatabaseProductName());
            if(databaseType == DatabaseType.OrientDB){
                repository = new OrientDBJobEventRdbStorage(dataSource);
            } else {
                repository = new JobEventRdbStorage(dataSource);
            }
        }
        return repository;
    }
}
