package io.github.sanojmg.jmetrics.types

import cats.data.Kleisli
import io.github.sanojmg.jmetrics.config.AppEnv

object Common {

  type Action[F[_], T] = Kleisli[F, AppEnv, T]

  type JobId = Int

  type StageId = Int
}
