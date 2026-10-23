/*
 * Copyright (C) 2017-2026 Universidade da Coruña
 * 
 * This file is part of MarDRe.
 * 
 * MarDRe is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * MarDRe is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with MarDRe. If not, see <http://www.gnu.org/licenses/>.
 */
package es.udc.gac.mardre.util;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskCompletionEvent;
import org.apache.hadoop.mapreduce.TaskCompletionEvent.Status;
import org.junit.Test;

public class ErrorsTest {

	private static final String FAILURE = "Error: java.lang.IllegalArgumentException: Name '@r1' does not have :X:Y coordinates\n"
			+ "\tat es.udc.gac.mardre.util.Sequence.setCoordinates(Sequence.java:1)\n";

	private static TaskAttemptID attempt(int task, int attempt) {
		return TaskAttemptID.forName(String.format("attempt_1_0001_m_%06d_%d", task, attempt));
	}

	private static TaskCompletionEvent event(int id, TaskAttemptID attempt, Status status) {
		return new TaskCompletionEvent(id, attempt, attempt.getTaskID().getId(), true, status, "http://node");
	}

	@Test
	public void aFailedAttemptIsReportedWithTheFirstLineOfItsDiagnostics() throws Exception {
		List<TaskCompletionEvent> events = Arrays.asList(event(0, attempt(0, 0), Status.FAILED));

		List<String> failures = Errors.taskFailures(events, id -> new String[] { FAILURE });

		assertEquals(Arrays.asList("Task attempt attempt_1_0001_m_000000_0 failed: java.lang.IllegalArgumentException: Name '@r1' does not have :X:Y coordinates"), failures);
	}

	@Test
	public void theAttemptsThatDidNotFailAreNotReported() throws Exception {
		List<TaskCompletionEvent> events = Arrays.asList(event(0, attempt(0, 0), Status.SUCCEEDED), event(1, attempt(1, 0), Status.KILLED),
				event(2, attempt(2, 0), Status.OBSOLETE));

		assertEquals(Collections.emptyList(), Errors.taskFailures(events, id -> new String[] { FAILURE }));
	}

	@Test
	public void theRetriesOfATaskWithTheSameCauseAreReportedOnce() throws Exception {
		List<TaskCompletionEvent> events = new ArrayList<TaskCompletionEvent>();
		for (int retry = 0; retry < 4; retry++)
			events.add(event(retry, attempt(0, retry), retry == 3 ? Status.TIPFAILED : Status.FAILED));

		List<String> failures = Errors.taskFailures(events, id -> new String[] { FAILURE });

		assertEquals(1, failures.size());
		assertEquals(true, failures.get(0).startsWith("Task attempt attempt_1_0001_m_000000_0 failed: "));
	}

	@Test
	public void differentCausesAreReportedSeparately() throws Exception {
		List<TaskCompletionEvent> events = Arrays.asList(event(0, attempt(0, 0), Status.FAILED), event(1, attempt(1, 0), Status.FAILED));

		List<String> failures = Errors.taskFailures(events,
				id -> new String[] { id.getTaskID().getId() == 0 ? FAILURE : "Error: java.lang.OutOfMemoryError: Java heap space\n\tat x\n" });

		assertEquals(2, failures.size());
		assertEquals("Task attempt attempt_1_0001_m_000001_0 failed: java.lang.OutOfMemoryError: Java heap space", failures.get(1));
	}

	@Test
	public void aFailedAttemptWithoutDiagnosticsIsReportedWithoutCause() throws Exception {
		List<TaskCompletionEvent> events = Arrays.asList(event(0, attempt(0, 0), Status.FAILED));

		assertEquals(Arrays.asList("Task attempt attempt_1_0001_m_000000_0 failed"), Errors.taskFailures(events, id -> new String[0]));
	}
}
