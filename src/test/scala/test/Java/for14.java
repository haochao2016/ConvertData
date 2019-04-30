package test.Java;

public class for14 {

    public static void main(String[] agrs) {

        int[] numList1 ={1,2,3,4,5};
        int[] numList2 = {11,2,12,13};

        for (int i1 : numList1) {
            for (int i2 : numList2) {

                if(i1 == i2){
                    break;
                } else {
                    System.out.println(i1 + "    "+ i2);
                }

            }
        }


    }

}
