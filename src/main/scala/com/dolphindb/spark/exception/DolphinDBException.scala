package com.dolphindb.spark.exception

 class DolphinDBException (msg : String) extends RuntimeException (msg : String){
}

 class TableException (msg : String) extends DolphinDBException (msg : String){
}

 class NoDataBaseException (msg : String) extends DolphinDBException  (msg : String){
}
