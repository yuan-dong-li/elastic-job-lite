package io.elasticjob.lite.event.rdb;

import io.elasticjob.lite.event.type.JobExecutionEvent;
import io.elasticjob.lite.event.type.JobStatusTraceEvent;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * Created by Brian on 8/10/18
 */
public interface IJobEventRdbSearch {

    IJobEventRdbSearch.Result<JobExecutionEvent> findJobExecutionEvents( JobEventRdbSearch.Condition condition);
    IJobEventRdbSearch.Result<JobStatusTraceEvent> findJobStatusTraceEvents( JobEventRdbSearch.Condition condition);

    /**
     * 查询条件对象.
     *
     * @author liguangyun
     */
    @RequiredArgsConstructor
    @Getter
    public static class Condition {

        public static final int DEFAULT_PAGE_SIZE = 10;

        private final int perPage;

        private final int page;

        private final String sort;

        private final String order;

        private final Date startTime;

        private final Date endTime;

        private final Map<String, Object> fields;
    }

    @RequiredArgsConstructor
    @Getter
    public static class Result<T> {

        private final Integer total;

        private final List<T> rows;
    }
}
