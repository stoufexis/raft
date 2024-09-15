package com.stoufexis.raft.kvstore.statemachine

import scodec.*
import scodec.bits.BitVector

given [K: Codec, V: Codec]: Codec[Map[K, V]] with
  val codec : Codec[(K, V)] = summon

  def encode(value: Map[K, V]): Attempt[BitVector] =
    codec.encodeAll(value)

  def decode(bits: BitVector): Attempt[DecodeResult[Map[K, V]]] =
    codec.collect(bits, None).map(_.map(_.toMap))

  def sizeBound: SizeBound = SizeBound.unknown

  override def toString = s"set($codec)"

given [A](using codec: Codec[A]): Codec[Set[A]] with
  def encode(value: Set[A]): Attempt[BitVector] =
    codec.encodeAll(value)

  def decode(bits: BitVector): Attempt[DecodeResult[Set[A]]] =
    codec.collect(bits, None)

  def sizeBound: SizeBound = SizeBound.unknown

  override def toString = s"set($codec)"