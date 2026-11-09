public class ReportStateCounterMessage extends Msg{

    private final int counter;
    private final int sum;

    public ReportStateCounterMessage(int counter, int sum) {
        this.counter = counter;
        this.sum = sum;
    }

    public int getCounter() {
        return counter;
    }

    public int getSum() {
        return sum;
    }
}
