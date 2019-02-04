import java.io.IOException;

public class Driver {
    public static void main(String[] args) throws IOException{
        if (args.length < 1) {
            System.out.println("Usage : kafka-producer.jar inputFilePath");
        } else {
            System.out.println("Starting Streaming Job..");
        }
        String input=args[0];
        Producer prod_object = new Producer();
        prod_object.producer(input);
    }
}
