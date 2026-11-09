public class ReportTotalMessage {
    private final int counterSum;
    private final int valueSum;

    public ReportTotalMessage(int counterSum, int valueSum) {
        this.counterSum = counterSum;
        this.valueSum = valueSum;
    }

    public int getCounterSum() {
        return counterSum;
    }

    public int getValueSum() {
        return valueSum;
    }
}
