package io.elasticjob.lite.event.rdb;

import io.elasticjob.lite.context.ExecutionType;
import io.elasticjob.lite.event.type.JobExecutionEvent;
import io.elasticjob.lite.event.type.JobStatusTraceEvent;
import org.apache.commons.dbcp.BasicDataSource;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.SQLException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * Created by Brian on 8/10/18
 */
public class OrientDBJobEventRdbSearchTest {

    private static IJobEventRdbStorage storage;

    private static IJobEventRdbSearch  repository;

    @BeforeClass
    public static void setUpClass() throws SQLException {
        BasicDataSource dataSource = new BasicDataSource();
        dataSource.setDriverClassName(com.orientechnologies.orient.jdbc.OrientJdbcDriver.class.getName());
        dataSource.setUrl("jdbc:orient:remote:localhost/mes");
        dataSource.setUsername("root");
        dataSource.setPassword("Sailheader&Mes@2018");
        storage = JobEventRdbStorageFactory.getInstance(dataSource);
        repository = JobEventRdbSearchFactory.getInstance(dataSource);
        initStorage();
    }

    private static void initStorage() throws SQLException {
        ((OrientDBJobEventRdbStorage)storage).clearJobTables();
        for (int i = 1; i <= 500; i++) {
            JobExecutionEvent startEvent = new JobExecutionEvent("fake_task_id", "test_job_" + i, JobExecutionEvent.ExecutionSource.NORMAL_TRIGGER, 0);
            storage.addJobExecutionEvent(startEvent);
            if (i % 2 == 0) {
                JobExecutionEvent successEvent = startEvent.executionSuccess();
                storage.addJobExecutionEvent(successEvent);
            }
            storage.addJobStatusTraceEvent(
                    new JobStatusTraceEvent("test_job_" + i, "fake_failed_failover_task_id", "fake_slave_id",
                            JobStatusTraceEvent.Source.LITE_EXECUTOR, ExecutionType.FAILOVER, "0", JobStatusTraceEvent.State.TASK_FAILED, "message is empty."));
        }
    }

    @Test
    public void assertFindJobExecutionEventsWithPageSizeAndNumber() {
        JobEventRdbSearch.Result<JobExecutionEvent> result = repository.findJobExecutionEvents(new JobEventRdbSearch.Condition(10, 1, null, null, null, null, null));
        assertThat(result.getTotal(), is(500));
        assertThat(result.getRows().size(), is(10));
        result = repository.findJobExecutionEvents(new JobEventRdbSearch.Condition(50, 1, null, null, null, null, null));
        assertThat(result.getTotal(), is(500));
        assertThat(result.getRows().size(), is(50));
        result = repository.findJobExecutionEvents(new JobEventRdbSearch.Condition(100, 5, null, null, null, null, null));
        assertThat(result.getTotal(), is(500));
        assertThat(result.getRows().size(), is(100));
        result = repository.findJobExecutionEvents(new JobEventRdbSearch.Condition(100, 6, null, null, null, null, null));
        assertThat(result.getTotal(), is(500));
        assertThat(result.getRows().size(), is(0));
    }

    @Test
    public void assertFindJobExecutionEventsWithErrorPageSizeAndNumber() {
        JobEventRdbSearch.Result<JobExecutionEvent> result = repository.findJobExecutionEvents(new JobEventRdbSearch.Condition(-1, -1, null, null, null, null, null));
        assertThat(result.getTotal(), is(500));
        assertThat(result.getRows().size(), is(10));
    }

    @Test
    public void assertFindJobExecutionEventsWithSort() {
        JobEventRdbSearch.Result<JobExecutionEvent> result = repository.findJobExecutionEvents(new JobEventRdbSearch.Condition(10, 1, "jobName", "ASC", null, null, null));
        assertThat(result.getTotal(), is(500));
        assertThat(result.getRows().size(), is(10));
        assertThat(result.getRows().get(0).getJobName(), is("test_job_1"));
        result = repository.findJobExecutionEvents(new JobEventRdbSearch.Condition(10, 1, "jobName", "DESC", null, null, null));
        assertThat(result.getTotal(), is(500));
        assertThat(result.getRows().size(), is(10));
        assertThat(result.getRows().get(0).getJobName(), is("test_job_99"));
    }

    @Test
    public void assertFindJobExecutionEventsWithErrorSort() {
        JobEventRdbSearch.Result<JobExecutionEvent> result = repository.findJobExecutionEvents(new JobEventRdbSearch.Condition(10, 1, "jobName", "ERROR_SORT", null, null, null));
        assertThat(result.getTotal(), is(500));
        assertThat(result.getRows().size(), is(10));
        assertThat(result.getRows().get(0).getJobName(), is("test_job_1"));
        result = repository.findJobExecutionEvents(new JobEventRdbSearch.Condition(10, 1, "notExistField", "ASC", null, null, null));
        assertThat(result.getTotal(), is(500));
        assertThat(result.getRows().size(), is(10));
    }

    @Test
    public void assertFindJobExecutionEventsWithTime() {
        Date now = new Date();
        Date tenMinutesBefore = new Date(now.getTime() - 10 * 60 * 1000);
        JobEventRdbSearch.Result<JobExecutionEvent> result = repository.findJobExecutionEvents(new JobEventRdbSearch.Condition(10, 1, null, null, tenMinutesBefore, null, null));
        assertThat(result.getTotal(), is(500));
        assertThat(result.getRows().size(), is(10));
        result = repository.findJobExecutionEvents(new JobEventRdbSearch.Condition(10, 1, null, null, now, null, null));
        assertThat(result.getTotal(), is(0));
        assertThat(result.getRows().size(), is(0));
        result = repository.findJobExecutionEvents(new JobEventRdbSearch.Condition(10, 1, null, null, null, tenMinutesBefore, null));
        assertThat(result.getTotal(), is(0));
        assertThat(result.getRows().size(), is(0));
        result = repository.findJobExecutionEvents(new JobEventRdbSearch.Condition(10, 1, null, null, null, now, null));
        assertThat(result.getTotal(), is(500));
        assertThat(result.getRows().size(), is(10));
        result = repository.findJobExecutionEvents(new JobEventRdbSearch.Condition(10, 1, null, null, tenMinutesBefore, now, null));
        assertThat(result.getTotal(), is(500));
        assertThat(result.getRows().size(), is(10));
    }

    @Test
    public void assertFindJobExecutionEventsWithFields() {
        Map<String, Object> fields = new HashMap<>();
        fields.put("isSuccess", "1");
        JobEventRdbSearch.Result<JobExecutionEvent> result = repository.findJobExecutionEvents(new JobEventRdbSearch.Condition(10, 1, null, null, null, null, fields));
        assertThat(result.getTotal(), is(250));
        assertThat(result.getRows().size(), is(10));
        fields.put("isSuccess", null);
        fields.put("jobName", "test_job_1");
        result = repository.findJobExecutionEvents(new JobEventRdbSearch.Condition(10, 1, null, null, null, null, fields));
        assertThat(result.getTotal(), is(1));
        assertThat(result.getRows().size(), is(1));
    }

    @Test
    public void assertFindJobExecutionEventsWithErrorFields() {
        Map<String, Object> fields = new HashMap<>();
        fields.put("notExistField", "some value");
        JobEventRdbSearch.Result<JobExecutionEvent> result = repository.findJobExecutionEvents(new JobEventRdbSearch.Condition(10, 1, null, null, null, null, fields));
        assertThat(result.getTotal(), is(500));
        assertThat(result.getRows().size(), is(10));
    }

    @Test
    public void assertFindJobStatusTraceEventsWithPageSizeAndNumber() {
        JobEventRdbSearch.Result<JobStatusTraceEvent> result = repository.findJobStatusTraceEvents(new JobEventRdbSearch.Condition(10, 1, null, null, null, null, null));
        assertThat(result.getTotal(), is(500));
        assertThat(result.getRows().size(), is(10));
        result = repository.findJobStatusTraceEvents(new JobEventRdbSearch.Condition(50, 1, null, null, null, null, null));
        assertThat(result.getTotal(), is(500));
        assertThat(result.getRows().size(), is(50));
        result = repository.findJobStatusTraceEvents(new JobEventRdbSearch.Condition(100, 5, null, null, null, null, null));
        assertThat(result.getTotal(), is(500));
        assertThat(result.getRows().size(), is(100));
        result = repository.findJobStatusTraceEvents(new JobEventRdbSearch.Condition(100, 6, null, null, null, null, null));
        assertThat(result.getTotal(), is(500));
        assertThat(result.getRows().size(), is(0));
    }

    @Test
    public void assertFindJobStatusTraceEventsWithErrorPageSizeAndNumber() {
        JobEventRdbSearch.Result<JobStatusTraceEvent> result = repository.findJobStatusTraceEvents(new JobEventRdbSearch.Condition(-1, -1, null, null, null, null, null));
        assertThat(result.getTotal(), is(500));
        assertThat(result.getRows().size(), is(10));
    }

    @Test
    public void assertFindJobStatusTraceEventsWithSort() {
        JobEventRdbSearch.Result<JobStatusTraceEvent> result = repository.findJobStatusTraceEvents(new JobEventRdbSearch.Condition(10, 1, "jobName", "ASC", null, null, null));
        assertThat(result.getTotal(), is(500));
        assertThat(result.getRows().size(), is(10));
        assertThat(result.getRows().get(0).getJobName(), is("test_job_1"));
        result = repository.findJobStatusTraceEvents(new JobEventRdbSearch.Condition(10, 1, "jobName", "DESC", null, null, null));
        assertThat(result.getTotal(), is(500));
        assertThat(result.getRows().size(), is(10));
        assertThat(result.getRows().get(0).getJobName(), is("test_job_99"));
    }

    @Test
    public void assertFindJobStatusTraceEventsWithErrorSort() {
        JobEventRdbSearch.Result<JobStatusTraceEvent> result = repository.findJobStatusTraceEvents(new JobEventRdbSearch.Condition(10, 1, "jobName", "ERROR_SORT", null, null, null));
        assertThat(result.getTotal(), is(500));
        assertThat(result.getRows().size(), is(10));
        assertThat(result.getRows().get(0).getJobName(), is("test_job_1"));
        result = repository.findJobStatusTraceEvents(new JobEventRdbSearch.Condition(10, 1, "notExistField", "ASC", null, null, null));
        assertThat(result.getTotal(), is(500));
        assertThat(result.getRows().size(), is(10));
    }

    @Test
    public void assertFindJobStatusTraceEventsWithTime() {
        Date now = new Date();
        Date tenMinutesBefore = new Date(now.getTime() - 10 * 60 * 1000);
        JobEventRdbSearch.Result<JobStatusTraceEvent> result = repository.findJobStatusTraceEvents(new JobEventRdbSearch.Condition(10, 1, null, null, tenMinutesBefore, null, null));
        assertThat(result.getTotal(), is(500));
        assertThat(result.getRows().size(), is(10));
        result = repository.findJobStatusTraceEvents(new JobEventRdbSearch.Condition(10, 1, null, null, now, null, null));
        assertThat(result.getTotal(), is(0));
        assertThat(result.getRows().size(), is(0));
        result = repository.findJobStatusTraceEvents(new JobEventRdbSearch.Condition(10, 1, null, null, null, tenMinutesBefore, null));
        assertThat(result.getTotal(), is(0));
        assertThat(result.getRows().size(), is(0));
        result = repository.findJobStatusTraceEvents(new JobEventRdbSearch.Condition(10, 1, null, null, null, now, null));
        assertThat(result.getTotal(), is(500));
        assertThat(result.getRows().size(), is(10));
        result = repository.findJobStatusTraceEvents(new JobEventRdbSearch.Condition(10, 1, null, null, tenMinutesBefore, now, null));
        assertThat(result.getTotal(), is(500));
        assertThat(result.getRows().size(), is(10));
    }

    @Test
    public void assertFindJobStatusTraceEventsWithFields() {
        Map<String, Object> fields = new HashMap<>();
        fields.put("jobName", "test_job_1");
        JobEventRdbSearch.Result<JobStatusTraceEvent> result = repository.findJobStatusTraceEvents(new JobEventRdbSearch.Condition(10, 1, null, null, null, null, fields));
        assertThat(result.getTotal(), is(1));
        assertThat(result.getRows().size(), is(1));
    }

    @Test
    public void assertFindJobStatusTraceEventsWithErrorFields() {
        Map<String, Object> fields = new HashMap<>();
        fields.put("notExistField", "some value");
        JobEventRdbSearch.Result<JobStatusTraceEvent> result = repository.findJobStatusTraceEvents(new JobEventRdbSearch.Condition(10, 1, null, null, null, null, fields));
        assertThat(result.getTotal(), is(500));
        assertThat(result.getRows().size(), is(10));
    }
}
