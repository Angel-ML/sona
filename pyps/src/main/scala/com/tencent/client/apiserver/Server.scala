package com.tencent.client.apiserver

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.SelectionKey
import java.nio.channels.Selector
import java.nio.channels.ServerSocketChannel
import java.nio.channels.SocketChannel
import java.io.IOException
import java.util.concurrent.Executors
import com.tencent.client.apiserver.protos.MSGProtos._

class Server(host: String, port: Int, handler: Handler) {
  private val selector: Selector = Selector.open() // 通过open()方法找到Selector
  private val pool = Executors.newCachedThreadPool

  def start(): Unit = {
    // 打开服务器套接字通道
    val ssc: ServerSocketChannel = ServerSocketChannel.open()
    // 服务器配置为非阻塞
    ssc.configureBlocking(false)
    // 进行服务的绑定
    ssc.bind(new InetSocketAddress(host, port))

    // 注册到selector，等待连接
    ssc.register(selector, SelectionKey.OP_ACCEPT)

    while (!Thread.currentThread.isInterrupted) {
      try {
        selector.select()
        val keys = selector.selectedKeys()
        val keyIterator = keys.iterator()
        while (keyIterator.hasNext) {
          val key = keyIterator.next()
          keyIterator.remove()

          try {
            if (key.isAcceptable) {
              accept(key)
            } else if (key.isReadable) {
              read(key)
            } else if (key.isWritable) {
              write(key)
            }
          } catch {
            case e: Exception =>
              if (key != null) {
                key.cancel()
                if(key.channel() != null){
                  key.channel().close()
                }
              }

              e.printStackTrace()
          }
        }
      } catch {
        case e: Exception => e.printStackTrace()
      }
    }

    ssc.close()
    selector.close()
    pool.shutdown()
  }

  private def write(key: SelectionKey): Unit = {
    val channel = key.channel().asInstanceOf[SocketChannel]
    val resp = key.attachment().asInstanceOf[Response]
    val writeBuf = ByteBuffer.wrap(resp.toByteArray)
    channel.write(writeBuf)
    channel.register(selector, SelectionKey.OP_READ)
  }

  private def read(key: SelectionKey): Unit = {
    val socketChannel = key.channel().asInstanceOf[SocketChannel]
    key.selector()
    // Clear out our read buffer so it's ready for new data
    val readBuffer = ByteBuffer.allocate(2048)

    try {
      // Attempt to read off the channel
      socketChannel.read(readBuffer)
      readBuffer.flip()
      val req:Request = Request.parseFrom(readBuffer)
      pool.execute(new Handle(req, key, handler))
    } catch {
      case e: IOException =>
        if (key != null) {
          key.cancel()
          socketChannel.close()
        }
        e.printStackTrace()
      case e: Exception =>
        e.printStackTrace()
        throw e
    }
  }

  private class Handle(req: Request, key: SelectionKey, handler: Handler) extends Runnable {
    override def run(): Unit = {
      val socketChannel = key.channel().asInstanceOf[SocketChannel]
      val selector: Selector = key.selector()
      val resp = handler.handle(req)
      socketChannel.register(selector, SelectionKey.OP_WRITE, resp)
    }
  }

  private def accept(key: SelectionKey): Unit = {
    val ssc = key.channel().asInstanceOf[ServerSocketChannel]
    val clientChannel = ssc.accept()
    clientChannel.configureBlocking(false)
    clientChannel.register(selector, SelectionKey.OP_READ)
  }
}

object Server {

  @throws[IOException]
  def main(args: Array[String]): Unit = {
    System.out.println("server started...")
    val handler =  new Handler(null, 12345)
    handler.register(null)
    val server = new Server("localhost", 8001,handler)
    server.start()
  }
}