package io.github.sanojmg.jmetrics.common

import cats.effect.{Blocker, ContextShift, IO, Resource, Sync, Timer}

import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{Executors, ThreadFactory}
import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContext.global

object IOThreadPool {

//  private val threadPool = Executors.newCachedThreadPool(
//    new ThreadFactory {
//      private val counter = new AtomicLong(0L)
//
//      def newThread(r: Runnable) = {
//        val th = new Thread(r)
//        th.setName("io-thread-" +
//          counter.getAndIncrement.toString)
//        th.setDaemon(true)
//        th
//      }
//    })

//  private val ioEC: ExecutionContext = ExecutionContext.fromExecutor(threadPool)
//  implicit val cs: ContextShift[IO] = IO.contextShift(ioEC)

  val blocker: Resource[IO, Blocker] = Blocker[IO]

//  implicit val cs: ContextShift[IO] = IO.contextShift(global)

//  implicit val timer: Timer[IO] = IO.timer(global)

}
