package com.stoufexis.leader.model

import com.stoufexis.leader.typeclass.Counter

opaque type Index = Int

object Index:
  given Counter[Index] = Counter.IncreasingInt
  given CanEqual[Index, Index] = CanEqual.derived
