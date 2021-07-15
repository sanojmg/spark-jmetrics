package io.github.sanojmg.jmetrics.util


import java.nio.charset.{Charset, StandardCharsets}
import java.io._
import cats.effect.concurrent.Semaphore
import cats.effect.{Concurrent, ExitCode, IO, IOApp, Resource, Sync}
import cats.syntax.all._

import java.nio.file.{Files, Path}

object CatsUtil {
  def putStrLn(value: Any) = IO(println(value))

  def readLn = IO(scala.io.StdIn.readLine())


  def putStrLn_[F[_]: Sync](value: Any) = Sync[F].delay(println(value))

  def readLn_[F[_]: Sync] = Sync[F].delay(scala.io.StdIn.readLine())


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
}
