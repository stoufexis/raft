package com.stoufexis.raft.typeclass

import doobie.util.*
import com.stoufexis.raft.typeclass.Storeable.StoreableType
import doobie.util.meta.Meta

trait Storeable[A]:
  /** Names, Types
    */
  val columns: Map[String, StoreableType[?]]


object Storeable:
  case class StoreableType[A](typeName: String, get: Get[A], put: Put[A])

  given [A: Storeable]: Get[A] = ???

  given [A: Storeable]: Put[A] = ???