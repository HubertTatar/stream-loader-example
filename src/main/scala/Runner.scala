package io.huta.sle

import proto.greet.GreetRequest


object Runner {
  def main(args: Array[String]): Unit = {
    println(GreetRequest("asd"))
  }
}
