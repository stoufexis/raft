package com.stoufexis.raft.typeclass

import doobie.util.*
import doobie.util.meta.Meta

import scala.compiletime.*
import scala.deriving.Mirror

trait Storeable[A]:
  /** Names, Types
    */
  val columns: Map[String, String]

  val read: Read[A]

  val write: Write[A]

object Storeable:
  inline def derived[A <: Product](using M: Mirror.ProductOf[A]): Storeable[A] =
    new:
      val names: List[String] =
        constValueTuple[M.MirroredElemLabels]
          .productIterator
          .asInstanceOf[Iterator[String]]
          .toList

      val types: List[String] =
        summonAll[Tuple.Map[M.MirroredElemTypes, Field]]
          .productIterator
          .asInstanceOf[Iterator[Field[?]]]
          .toList
          .map(x => s"${x.typ}" + (if x.nullable then "" else " NOT NULL"))

      val columns: Map[String, String] =
        names.zip(types).toMap

      val read: Read[A] = summonInline[Read[M.MirroredElemTypes]].map(M.fromTuple)

      val write: Write[A] = summonInline[Write[M.MirroredElemTypes]].contramap(Tuple.fromProductTyped)

  trait FieldMeta[A]:
    val typ:      String
    val nullable: Boolean
    val meta:     Meta[A]

  given FieldMeta[Int] with
    val typ:      String    = "INTEGER"
    val nullable: Boolean   = false
    val meta:     Meta[Int] = Meta[Int]

  given FieldMeta[String] with
    val typ:      String       = "TEXT"
    val nullable: Boolean      = false
    val meta:     Meta[String] = Meta[String]

  trait Field[A]:
    val typ:      String
    val nullable: Boolean
    val read:     Read[A]
    val write:    Write[A]

  given [A](using fa: FieldMeta[A]): Field[A] with
    given Meta[A] = fa.meta

    val typ:      String   = fa.typ
    val nullable: Boolean  = false
    val read:     Read[A]  = summon
    val write:    Write[A] = summon

  given [A](using fa: FieldMeta[A]): Field[Option[A]] with
    given Meta[A] = fa.meta

    val typ:      String           = fa.typ
    val nullable: Boolean          = true
    val read:     Read[Option[A]]  = summon
    val write:    Write[Option[A]] = summon
