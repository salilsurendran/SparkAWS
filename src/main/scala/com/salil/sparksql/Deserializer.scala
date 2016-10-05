package com.salil.sparksql

import java.io.{FileInputStream, ObjectInputStream}

/**
  * Created by salilsurendran on 9/18/16.
  */
object Deserializer extends App{
  val des = new ObjectInputStream(new FileInputStream("/home/salilsurendran/WORK/lineage/nflx1474086913377"))
  val logicalPlan = des.readObject()
  println(logicalPlan.toString)
}
