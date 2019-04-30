package test.Java;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.LocalTime;

public class TestRegex {

    public static void main (String[] args) {

        String regex = "[ T]";

        String t1 = "2019-04-12 09:30:45";
        String t2 = "2019-04-12T09:30:45";

//        System.out.println(t1.replace(regex, t1));
        System.out.println(t1.split(regex)[0]);




    }
}
