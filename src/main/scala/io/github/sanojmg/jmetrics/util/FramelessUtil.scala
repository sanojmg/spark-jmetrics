package io.github.sanojmg.jmetrics.util

import cats.effect.Sync
import frameless.TypedDataset
import frameless.cats.implicits._
import io.github.sanojmg.jmetrics.types.Common.Action

object FramelessUtil {

  def collect[F[_]: Sync, T](tds: TypedDataset[T]): Action[F, Seq[T]] =
  // Use type lambda to collect Action from TypedDataset
  // Refer: https://stackoverflow.com/a/8736360/7279631
    tds.collect[({type λ[α] = Action[F, α]})#λ]()

  def count[F[_]: Sync, T](tds: TypedDataset[T]): Action[F, Long] =
    tds.count[({type λ[α] = Action[F, α]})#λ]()

}
