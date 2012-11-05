package com.wajam.scn

/**
 * Configuration classes for SCN and SCNClient
 *
 * @author : Jerome Gagnon <jerome@wajam.com>
 * @copyright Copyright (c) Wajam inc.
 *
 */
case class ScnConfig(timestampSaveAheadMs: Int = 6000, timestampSaveAheadRenewalMs: Int = 1000,
                     sequenceSaveAheadSize: Int = 100, sequenceSeeds: Map[String, Long] = Map())

case class ScnClientConfig(executionRateInMs: Int = 10, timeoutInMs: Int = 1000)
