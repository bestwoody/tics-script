///*
// * Copyright 2017 PingCAP, Inc.
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// *      http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package org.apache.spark.sql.ch
//import org.apache.arrow.memory.RootAllocator
//import org.apache.arrow.vector.file.ReadChannel
//import org.apache.arrow.memory.BufferAllocator
//import org.apache.arrow.vector.file.WriteChannel
//import org.apache.arrow.vector.stream.MessageSerializer
//import java.io.IOException
//import java.nio.channels.Channels
//import java.util.Arrays.asList
//import java.io.ByteArrayInputStream
//import java.io.ByteArrayOutputStream
//import io.netty.buffer.ArrowBuf
//import org.apache.arrow.vector.schema.{ArrowFieldNode, ArrowRecordBatch}
//import org.apache.spark.sql.{Row, SparkSession}
//
//
//object CHArrowDecode {
//
//  @throws[IOException]
//  def buf(alloc: BufferAllocator,bytes:Array[Byte]) : ArrowBuf = {
//    val buffer = alloc.buffer(bytes.length)
//    buffer.writeBytes(bytes)
//    return buffer
//
//  }
//
//  @throws[IOException]
//  def array(buf: ArrowBuf): Array[Byte] = {
//    val bytes = new Array[Byte](buf.readableBytes)
//    buf.readBytes(bytes)
//    return bytes
//  }
//
//  @throws[IOException]
//  def encode(out: ByteArrayOutputStream): Iterator[Row] = new Iterator[Row] {
//    val alloc = new RootAllocator(Long.MaxValue)
//    val in = new ByteArrayInputStream(out.toByteArray())
//    val channel = new ReadChannel(Channels.newChannel(in))
//    val deserialized = MessageSerializer.deserializeMessageBatch(channel, alloc)
//    val encode_batch = deserialized.asInstanceOf[ArrowRecordBatch]
//    val buffers = encode_batch.getBuffers()
//    val iterator = array(buffers.get(1)).toList.iterator
//    override def hasNext: Boolean = iterator.hasNext
//    override def next(): Row = Row.fromSeq(Seq(iterator.next))
//  }
//
//  @throws[IOException]
//  def code(values: Array[Byte]): ByteArrayOutputStream = {
//    val validity = Array[Byte](255.toByte, 0)
//    val alloc = new RootAllocator(Long.MaxValue)
//    val validityb = buf(alloc, validity)
//    val valuesb = buf(alloc, values)
//    val batch = new ArrowRecordBatch(16, asList(new ArrowFieldNode(16, 8)), asList(validityb, valuesb))
//    val out = new ByteArrayOutputStream();
//    MessageSerializer.serialize(new WriteChannel(Channels.newChannel(out)), batch)
//    return out
//  }
//
//  def verifyBatch(batch:ArrowRecordBatch): Unit ={
//    val nodes = batch.getNodes()
//    val buffers = batch.getBuffers
//    println(array(buffers.get(1)).toList.toString())
//  }
//
//
//  def main(args: Array[String]): Unit = {
//    // Serializer
//    val validity = Array[Byte](255.toByte, 0)
//    val values = Array[Byte](1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16)
//    val out = code(values)
//    // deserialize
//    encode(out)
//  }
//}
//
//class CHArrowDecode  {
//
//}