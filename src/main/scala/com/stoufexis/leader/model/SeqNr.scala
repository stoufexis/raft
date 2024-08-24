package com.stoufexis.leader.model

import com.stoufexis.leader.typeclass.Increasing

opaque type Index = Int

object Index:
  given Increasing[Index]      = Increasing.IncreasingInt
  given CanEqual[Index, Index] = CanEqual.derived
