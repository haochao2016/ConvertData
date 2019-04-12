package test.Java;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Random;

public class for13 {

    public static void main (String[] args) {

       String as = "sdf";
        char c = as.charAt(0);

        System.out.println(c);
//        System.out.println(Byte.parseByte(c+""));

        Character character = new Character(c);
        System.out.println(character.charValue());
        System.out.println(character);


    }
}
