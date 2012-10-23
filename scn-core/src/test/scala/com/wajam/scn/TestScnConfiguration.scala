package com.wajam.scn

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.apache.commons.configuration.PropertiesConfiguration
import org.scalatest.FunSuite

/**
 * 
 */
@RunWith(classOf[JUnitRunner])
class TestScnConfiguration extends FunSuite {
  val SEED_PREFIX = "scn.sequence.seed."

  test("get sequence seed map") {
    val seq1Name = "seq1"
    val seq2Name = "seq2"
    val seq1Seed = 10
    val seq2Seed = 111

    val config = new PropertiesConfiguration()
    config.addProperty(SEED_PREFIX+seq1Name, seq1Seed)
    config.addProperty(SEED_PREFIX+seq2Name, seq2Seed)
    config.addProperty("other", "othervalue")

    val seeds = new ScnConfiguration(config).getScnSequenceSeeds

    assert(seq1Seed === seeds(seq1Name))
    assert(seq2Seed === seeds(seq2Name))
    assert(2 === seeds.size)
  }

  test("get sequence empty seed map") {
    val config = new PropertiesConfiguration()
    val seeds = new ScnConfiguration(config).getScnSequenceSeeds
    assert(seeds.isEmpty)
  }

}
