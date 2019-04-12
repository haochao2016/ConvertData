package dolphindb.mutil;

import com.xxdb.DBConnection;
import com.xxdb.data.BasicTable;

import java.io.IOException;
import java.util.Random;

public class TestMutilByJAVA {

    public static void main (String[] args) throws IOException {

        Random random = new Random();
        String[] iparr = {"115.239.209.224", "115.239.209.189"};
        int[] portarr = {16961, 16962, 16963};
        int ipaddr = random.nextInt(2);
        int portaddr = random.nextInt(3);

        String ip = iparr[ipaddr];
        int port = portarr[portaddr];
        String user = "admin";
        String passwd = "123456";
        String dbPath = "\"dfs://chao/TAQ/db\"";
        String table = "\"taq\"";

        DBConnection conn = new DBConnection();
        conn.connect(ip, port, user, passwd);

        String existDB = "existsDatabase(" + dbPath + ")";
        if (conn.run(existDB) == null) {
            throw  new IOException("There'is no database");
        }

        conn.run("taq = database("+ dbPath +").loadTable("+ table +")");

        long startTime = System.currentTimeMillis();

//        String stab = "select * from taq where symbol='IBM' ,date=2007.08.10 ,  time >= 09:30:00";
//        String stab = "select symbol , time, bid, ofr from taq where symbol in (`IBM,`MSFT,`GOOG,`YHOO), " +
//                "date=2007.08.01 ,time between 09:30:00 : 09:30:59 ,BID > 0, ofr>bid";

//        String stab = "select * from taq where date=2007.08.27, symbol=`EBAY order by (ofr - bid) as spread desc";

//        String stab = "select avg((ofr - bid)/(ofr + bid)) as spread from taq where date=2007.08.01, bid > 0, ofr > bid, time>=09:30:00, time<=16:00:00 group by time.minute() as minute";

//        String stab = "select max(ofr) - min(bid) as gap from taq where date=2007.08.03, bid > 0, ofr > bid group by  symbol, minute(time) as minute";

//        String stab = "select avg(ofr + bid)/2.0 as mid from taq where symbol=`IBM, time>=09:30:00, time<=16:00:00 group by date, minute(time) as minute";

        String stab = "select wavg(bid, bidsiz) as vwab from taq group by date, symbol having sum(bidsiz)> 0 order by date desc, symbol";

        BasicTable tblen = (BasicTable)conn.run(stab);

        int row = 1000;
        if (tblen.rows() < 1000) {
            row = tblen.rows();
        }

        for (int i = 0; i < row; i++ ){
            StringBuffer sb = new StringBuffer();
            for (int j = 0; j< tblen.columns(); j++) {
                sb.append(tblen.getColumn(j).get(i)).append("\t");
            }
            System.out.println(sb);
        }

        long endTime = System.currentTimeMillis();
        System.out.println("==== ALL === "  + (endTime - startTime));
    }
}
