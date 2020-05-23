package org.trident.projectlearn;
import org.apache.log4j.Logger;
import org.apache.spark.sql.SparkSession;


import org.apache.spark.sql.functions._;
import org.apache.spark.sql.types._;

object driver {

  //p
 def main(args: Array[String])
 {
 val ss=SparkSession.builder().getOrCreate();
 import ss.implicits._
 
 //val logr=Logger.getLogger()
 println("Hello-1")
 val df=ss.read.format("csv").option("header","true").load("C:/Bigdata/Traning_Data/CSV/Pokemon")
 df.printSchema()
 val st=df.columns
  println("COLUMNS:::  "+ st(0))
 
 Thread.sleep(1000)
 var stt=st.mkString(",")
 println("COLUMNS_LIST::  "+ stt)
  Thread.sleep(1000)
//df.select(st.head,st.tail:_*).show(false)
println("st:_*")
Thread.sleep(2000)
val ll=List("a","b")
//df.groupBy(ll:_*).agg(max("_c3")).show(false)
// df.show(false)
val ch="_c3" 
println("_c22: "+st.contains("_c22"))
println("_c2: "+st.contains("_c2"))
var ssx="_c47"
var sss =""
var ssn= ""
for (x <- st)
{
  if (stt.contains(ssx))
  {
    println("-"+x+"-:"+stt.contains(ssx))
    sss=sss+x
  }
  else
    ssn=ssn+x
}
 
 println("sss: " + sss)
println("ssn: " + ssn)


 df.select("Type 1").distinct.show(false)
 df.select($"Type 1".name("Type")).distinct.write.mode("overwrite").save("C:/Bigdata/Traning_Data/Output/parquet_1/")
 
 Thread.sleep(100000)
 
 }
}