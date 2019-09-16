package com.lcy.scala.demo.implicitDemo

class SignPen(){
  def writer(name:String) = println(name)
}

object ImplicitContext{
  implicit val signPen = new SignPen
}

object Demo3 {

  def signPenExam(name:String)(implicit signPen: SignPen) = {
    signPen.writer(name)
  }

  def main(args: Array[String]): Unit = {
    import ImplicitContext._
    signPenExam("tom")
  }

}
