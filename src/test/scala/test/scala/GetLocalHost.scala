package test.scala

import java.net.InetAddress

object GetLocalHost {

  def main(args: Array[String]): Unit = {
    val address = InetAddress.getLocalHost
    println(address.getAddress)
    println(address.getHostAddress)
    System.out.println(address.getHostAddress());
    System.out.println(address.getAddress());
    System.out.println(address.getCanonicalHostName());
    System.out.println(address.getHostName());
  }

}
