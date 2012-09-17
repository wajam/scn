package com.wajam.scn.storage

/**
 * Enumeration of SCN storage types
 */
object StorageType extends Enumeration {
  type StorageType = Value
  val MEMORY, ZOOKEEPER = Value
}