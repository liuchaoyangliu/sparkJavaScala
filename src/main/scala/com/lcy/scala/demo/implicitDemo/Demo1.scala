package com.lcy.scala.demo.implicitDemo

class SpecialPerson(var name :String)
class Older(val name :String)
class Child(val name:String)
class Teacher(val name:String)

object Demo1 {

  implicit def objectToSpecialPerson(obj :Object):SpecialPerson={
    if(obj.getClass == classOf[Older]){
      val older = obj.asInstanceOf[Older]
      new SpecialPerson(older.name)
    }else if(obj.getClass == classOf[Child]){
      val child = obj.asInstanceOf[Child]
      new SpecialPerson(child.name)
    }else{
      null
    }
  }

  var sumTickits = 0

  def buySpecialPerson(specialPerson: SpecialPerson) ={
    sumTickits += 1
    println("sumTickits的值" + sumTickits )
  }

  def main(args: Array[String]): Unit = {

    val older = new Older("laowang")
    buySpecialPerson(older)

    val child = new Child("xaioming")
    buySpecialPerson(child)

    val thacher = new Teacher("teacher")
    buySpecialPerson(thacher)

    buySpecialPerson(null)
  }

}



