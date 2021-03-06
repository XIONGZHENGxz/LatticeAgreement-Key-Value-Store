package la.common;

public class MovingAverage {
    /** How important is the last value - range (0,1) */
    private final double convergenceFactor;

    /** Starting point if no given */
    private volatile double average = 0;

    public MovingAverage(double convergenceFactor, double firstAverage) {
        if (0 >= convergenceFactor || convergenceFactor >= 1) {
            throw new IllegalArgumentException("Incorrect convergence factor in moving average.");
        }
        this.convergenceFactor = convergenceFactor;
        average = firstAverage;
    }

    /** Calculates next average basing on next value */
    public double add(double value) {
        average = (1 - convergenceFactor) * average + convergenceFactor * value;
        return average;
    }

    /**
     * (re)Starts the calculation with newAverage, that is treats newAverage as
     * the current value
     */
    public void reset(double newAverage) {
        average = newAverage;
    }

    /** Returns the current value */
    public double get() {
        return average;
    }
}
