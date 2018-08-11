package io.elasticjob.lite.event.rdb;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * Created by Brian on 8/10/18
 */
public class JobEventRdbSearchFactory {

   public static IJobEventRdbSearch getInstance(DataSource dataSource) throws SQLException {
        IJobEventRdbSearch search = null;
        try (Connection conn = dataSource.getConnection()) {
            DatabaseType databaseType = DatabaseType.valueFrom(conn.getMetaData().getDatabaseProductName());
            if(databaseType == DatabaseType.OrientDB){
                search = new OrientDBJobEventRdbSearch(dataSource);
            } else {
                search = new JobEventRdbSearch(dataSource);
            }
        }
        return search;
    }
}
