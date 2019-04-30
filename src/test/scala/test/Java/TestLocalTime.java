package test.Java;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

public class TestLocalTime {

    public static void main (String[] args) {

        LocalTime parse = LocalTime.parse("13:30:10.12123280");
        LocalTime parse1 = LocalTime.parse("13:30:10.12223280");
        LocalTime parse2 = LocalTime.parse("13:30");

        LocalDateTime parse3 = LocalDateTime.parse("2019-04-12T09:30:45");
//        LocalDateTime parse4 = LocalDateTime.parse("2019-04-12 09:30:45");
        LocalDateTime parse4 = LocalDateTime.parse("2019-04-12T09:30:45" );

        System.out.println(parse3);
        System.out.println(parse4);
        System.out.println(parse);
        System.out.println(parse2);
        System.out.println(parse2.getNano());


        Timestamp timestamp = Timestamp.valueOf("2019-04-12 09:30:45");
        System.out.println(timestamp);


        String dt = "2012.06.13";
        System.out.println(dt.replace(".", "-"));

    }
}
