package io.elasticjob.lite.event.rdb;

import io.elasticjob.lite.event.type.JobExecutionEvent;
import io.elasticjob.lite.event.type.JobStatusTraceEvent;

import java.util.List;

/**
 * Created by Brian on 8/10/18
 */
public interface IJobEventRdbStorage {
    boolean addJobExecutionEvent( JobExecutionEvent jobExecutionEvent);
    boolean addJobStatusTraceEvent( JobStatusTraceEvent jobStatusTraceEvent);
    List<JobStatusTraceEvent> getJobStatusTraceEvents( String taskId);
}
