package kafka.track.java;

import static java.lang.System.out;

public class App {
    public String getGreeting() {
        return "Hello world.";
    }

    public static void main(String[] args) {
        out.println(new App().getGreeting());
    }
}
