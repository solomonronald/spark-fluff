package com.solomonronald.spark.fluff.exceptions

final case class InvalidFluffyFunction(private val message: String = "Invalid Fluffy Function",
                                       private val cause: Throwable = None.orNull)
  extends Exception(message, cause)
