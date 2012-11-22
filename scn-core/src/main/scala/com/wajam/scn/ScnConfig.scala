package com.wajam.scn

/**
 * Configuration classes for SCN and SCNClient
 *
 * @author : Jerome Gagnon <jerome@wajam.com>
 * @copyright Copyright (c) Wajam inc.
 *
 */
case class ScnConfig(timestampSaveAheadMs: Int = 6000, timestampSaveAheadRenewalMs: Int = 1000,
                     sequenceSaveAheadSize: Int = 1000, maxMessageQueueSize: Int = 1000,
                     messageExpirationMs: Int = 250 ,sequenceSeeds: Map[String, Long] = Map())


