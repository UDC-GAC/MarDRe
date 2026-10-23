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


import org.slf4j.Logger;
import org.slf4j.helpers.MessageFormatter;

/**
 * Reports the errors that end a run. They are logged and also written to the
 * standard error, so that they are not lost when there is no logging configuration.
 */
public final class Errors {

	private Errors() {}

	public static void report(Logger log, String format, Object... args) {
		log.error(format, args);
		System.err.println("MarDRe: " + MessageFormatter.arrayFormat(format, args).getMessage());
	}
}
