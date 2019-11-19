package com.lcy.scala.spark.implicitDemo

class Car(val name:String)

class SuperMan(val name:String){
  def emitLaser() = println("emit a pingpang ball !!!")
}

object Demo2 {

  implicit def CatToSuperman(car:Car):SuperMan={
    new SuperMan(car.name)
  }

  def main(args: Array[String]): Unit = {
    val car = new Car("car")
    car.emitLaser()
  }

}
