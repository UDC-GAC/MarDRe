package es.udc.gac.mardre.util;

public class Timer {

	public static final int MAX_TIMERS = 32;
	private double startTime[] = new double[MAX_TIMERS];
	private double elapsedTime[] = new double[MAX_TIMERS];
	private double totalTime[] = new double[MAX_TIMERS];
	private static final double TENTO9 = new Long(1000000000L).doubleValue();

	public Timer() {
		for(int i=0; i<MAX_TIMERS; i++) {
			startTime[i] = 0;
			elapsedTime[i] = 0;
			totalTime[i] = 0;
		}
	}

	public void start(int n) {
		startTime[n] = System.nanoTime();
	}

	public void stop(int n) {
		elapsedTime[n] = System.nanoTime()-startTime[n];
		elapsedTime[n] /= Timer.TENTO9;
		totalTime[n] += elapsedTime[n];
	}

	public double read(int n) {
		return totalTime[n];
	}

	public void reset(int n) {
		elapsedTime[n]=startTime[n]=totalTime[n] = 0;
	}

	public void resetAll() {
		for(int i=0; i<MAX_TIMERS; i++)
			reset(i);
	}
}