package io.github.sanojmg.jmetrics.util


import cats.data.Kleisli

import java.nio.charset.{Charset, StandardCharsets}
import java.io._
import cats.effect.concurrent.Semaphore
import cats.effect.{Concurrent, ExitCode, IO, IOApp, Resource, Sync}
import cats.syntax.all._
import io.github.sanojmg.jmetrics.config.AppEnv
import io.github.sanojmg.jmetrics.types.Common._

import java.nio.file.{Files, Path}

object CatsUtil {
  def putStrLn(value: Any) = IO(println(value))

  def readLn = IO(scala.io.StdIn.readLine())

  def putStrLn_[F[_]: Sync](value: Any) = Sync[F].delay(println(value))

  def readLn_[F[_]: Sync] = Sync[F].delay(scala.io.StdIn.readLine())

  def writeToOutFile[F[_]: Sync: Concurrent](data: String): Action[F, Unit] = for {
    env              <- Kleisli.ask[F, AppEnv]
    outFile          = env.appConf.outFile
    _                <- Kleisli.liftF(outFile.traverse(writeFile[F](_,data)))
    _                <- printC(scala.Console.GREEN, s"Output has been written to " +
      s"${outFile.getOrElse("--")}")
  } yield ()

  def outputStream[F[_]: Sync](path: Path, guard: Semaphore[F]): Resource[F, BufferedWriter] =
    Resource.make {
      Sync[F].delay(Files.newBufferedWriter(path, StandardCharsets.UTF_8))
    } { writer =>
      guard.withPermit {
        Sync[F].delay(writer.close()).handleErrorWith(_ => Sync[F].unit)
      }
    }

  def writeFile[F[_]: Sync: Concurrent](fileName: Path, dataStr: String): F[Unit] = for {
    guard <- Semaphore[F](1)
    _     <- outputStream(fileName, guard).use { case out =>
                  guard.withPermit(
                    Sync[F].delay(out.write(dataStr))
                  )
              }
  } yield ()

  def printC[F[_]: Sync](color: String, value: Any): Action[F, Unit] =
    Kleisli.liftF(putStrLn_(color + value + scala.Console.RESET))

  def printC[F[_]: Sync](value: Any): Action[F,Unit] =
    Kleisli.liftF(putStrLn_(value))

  def logA[F[_]: Sync](value: Any): Action[F,Unit] =
    Kleisli.liftF(putStrLn_(value))
}
