package io.github.sanojmg.jmetrics.config

import java.nio.file.Path
//import pureconfig._
//import pureconfig.generic.auto._
//import pureconfig.module.catseffect2.syntax._
//import cats.effect.{ Blocker, ContextShift, IO }


case class AppConfig(
                  restEndpoint: String,
                  appId: String,
                  outFile: Option[Path]
                )


//object AppConfig {
//
//  def loadConf(blocker: Blocker)(implicit cs: ContextShift[IO]): IO[AppConfig] = {
//    ConfigSource.default.loadF[IO, AppConfig](blocker)
//  }
//
//  private implicit val cs: ContextShift[IO] =
//    IO.contextShift(scala.concurrent.ExecutionContext.global)
//
//  val load: IO[AppConfig] = Blocker[IO].use(blocker => loadConf(blocker))
//}
