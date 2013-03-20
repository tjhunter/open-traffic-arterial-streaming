package arterial_research.socc_test

import arterial_research.socc._
import arterial_research.socc.gamma._
import org.junit._
import org.junit.Assert._
import math._
import java.io.File
import org.scalatest.junit.AssertionsForJUnit
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.joda.time.LocalDateTime
import com.codahale.jerkson.Json._
import core_extensions.MMLogging
import org.joda.time.DateTime
import org.scala_tools.time.Imports._
import spark.JavaSerializerInstance
import java.io.ByteArrayOutputStream
import spark.JavaSerializationStream
import java.io.ByteArrayInputStream
import spark.JavaDeserializationStream

//@RunWith(classOf[JUnitRunner])
class GammaTest extends FunSuite with MMLogging {

  @Test def ser1:Unit = {
    val t = new LocalDateTime
    val obs = PartialObservation(ObservationMask(Array(0)), 1.0)
    val rec = RecordRepr(t,obs)
    val s = generate(rec)
    println("xxxx")
    logInfo(s)
    val rec2 = parse[RecordRepr](s)
    logInfo(rec.toString)
    println(""+generate(new DateTime))
//    assert(false)
  }
  
  @Test def time1:Unit = {
    val t1 = new LocalDateTime
    val t2 = new LocalDateTime
    assert(t1 <= t2)
    
  }
  
  
//  def testSerialization[T](x:T) {
//    val ser = new JavaSerializerInstance
//    val s = ser.serialize(x)
//    val x2 = ser.deserialize[T](s)
//    
//    val out_stream = new ByteArrayOutputStream
//    val ser2 = new JavaSerializationStream(out_stream)
//    ser2.writeObject(x)
//    val in_stream = new ByteArrayInputStream(out_stream.toByteArray())
//    val deser = new JavaDeserializationStream(in_stream, this.getClass.getClassLoader)
//    val x3 = deser.readObject[T]()
//    logInfo("x3:"+x3)
//  }
//
//  @Test def ser3:Unit = {
////    val t = new LocalDateTime
//    val t = parse[LocalDateTime]("[2012,9,14,15,49,14,490]")
//    val obs = PartialObservation(ObservationMask(Array(0)), 1.0)
//    val rec = RecordRepr(t,obs)
//    val ser = new JavaSerializerInstance
//    val s = ser.serialize(rec)
//    val x = ser.deserialize[RecordRepr](s).toRecord
//    
//    val out_stream = new ByteArrayOutputStream
//    val ser2 = new JavaSerializationStream(out_stream)
//    ser2.writeObject(rec)
//    val in_stream = new ByteArrayInputStream(out_stream.toByteArray())
//    val deser = new JavaDeserializationStream(in_stream, this.getClass.getClassLoader)
//    val rec2 = deser.readObject[RecordRepr]().toRecord
//    
////    assertEquals(x,rec)
//  }
//
//  @Test def ser4:Unit = {
//    val t = parse[LocalDateTime]("[2012,9,14,15,49,14,490]")
//    logInfo("chrono:"+t.getChronology())
//    val t2 = t.toDateTime()
//    val t3 = t2.toLocalDateTime()
//    testSerialization(t3)
//  }
  
  @Test
  def ser5:Unit = {
    val s = """{"nodeId":113,"state":{"k":1.247614512293216,"theta":17.680335795619083}}"""
    val (a,b) = InputOutput.readLine(s)
  }
}
