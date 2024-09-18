package com.stoufexis.raft.statemachine

case class Automaton[In, Out, S](f: (S, In) => (S, Out)):
  def apply(s: S, in: In): (S, Out) = f(s, in)
