package test.Java;

import java.time.LocalDateTime;

import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Random;

public class for12 {

    public static void main (String[] args) {

        LocalTime time = LocalTime.parse("17:08:52.945646");
        System.out.println(time);

        System.out.println(LocalDate.now());
        System.out.println(LocalTime.now());
        System.out.println(LocalDateTime.now());
        LocalDateTime now = LocalDateTime.parse("2019-04-11T16:55:15.7956");

        System.out.println(now.getNano());
        System.out.println(now);

//        LocalDateTime now = LocalDateTime.now();
        String we = now.toString().split("T")[1];//.split(".")[0];//.substring(0,2);
        System.out.println(we);
        String[] split = we.split(":");
        System.out.println(split[2].substring(0,2));
        System.out.println(split[2].split("."));
        System.out.println(now.getSecond());
        System.out.println(now.getNano());
//        System.out.println(split[2].split(".")[0]);



        Random random = new Random();
//        for (int i =0; i < 100; i++ ) {
//            int ra = random.nextInt(3);
//            System.out.println(ra);
//        }


    }
}
