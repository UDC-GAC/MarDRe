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


import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskCompletionEvent;
import org.slf4j.Logger;
import org.slf4j.helpers.MessageFormatter;

/**
 * Reports the errors that end a run. They are logged and also written to the
 * standard error, so that they are not lost when there is no logging configuration.
 */
public final class Errors {

	private static final int EVENTS_PER_REQUEST = 100;

	/** The diagnostics of a task attempt, as {@link Job#getTaskDiagnostics} gives them. */
	public interface Diagnostics {
		String[] of(TaskAttemptID attempt) throws IOException, InterruptedException;
	}

	private Errors() {}

	public static void report(Logger log, String format, Object... args) {
		log.error(format, args);
		System.err.println("MarDRe: " + MessageFormatter.arrayFormat(format, args).getMessage());
	}

	/**
	 * One message for each different cause of the failed task attempts: the first line of their diagnostics,
	 * which is the exception, without the stack trace. The retries of a task fail with the same cause.
	 */
	public static List<String> taskFailures(List<TaskCompletionEvent> events, Diagnostics diagnostics) throws IOException, InterruptedException {
		List<String> failures = new ArrayList<String>();
		Set<String> causes = new HashSet<String>();

		for (TaskCompletionEvent event : events) {
			if (event.getStatus() != TaskCompletionEvent.Status.FAILED && event.getStatus() != TaskCompletionEvent.Status.TIPFAILED)
				continue;

			String cause = "";
			for (String diagnostic : diagnostics.of(event.getTaskAttemptId())) {
				if (diagnostic != null && !diagnostic.trim().isEmpty()) {
					cause = diagnostic.trim().split("\\R", 2)[0];
					break;
				}
			}
			if (cause.startsWith("Error: "))
				cause = cause.substring("Error: ".length());

			if (causes.add(cause))
				failures.add("Task attempt " + event.getTaskAttemptId() + " failed" + (cause.isEmpty() ? "" : ": " + cause));
		}

		return failures;
	}

	/** Reports why the tasks of a failed job failed. It is best effort: the job has already failed. */
	public static void reportTaskFailures(Logger log, Job job) {
		try {
			List<TaskCompletionEvent> events = new ArrayList<TaskCompletionEvent>();
			TaskCompletionEvent[] page;
			do {
				page = job.getTaskCompletionEvents(events.size(), EVENTS_PER_REQUEST);
				events.addAll(Arrays.asList(page));
			} while (page.length == EVENTS_PER_REQUEST);

			for (String failure : taskFailures(events, job::getTaskDiagnostics))
				report(log, "{}", failure);
		} catch (IOException e) {
			log.debug("could not get the diagnostics of the failed tasks", e);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}
}
