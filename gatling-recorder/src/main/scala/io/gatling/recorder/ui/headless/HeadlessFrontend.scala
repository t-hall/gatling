package io.gatling.recorder.ui.headless

import java.lang.management.ManagementFactory

import io.gatling.recorder.config.{RecorderConfiguration, RecorderMode}
import io.gatling.recorder.controller.RecorderController
import io.gatling.recorder.ui.{EventInfo, RecorderFrontend}

class HeadlessFrontend(controller: RecorderController)(implicit configuration: RecorderConfiguration) extends RecorderFrontend(controller) {

  override def selectedRecorderMode: RecorderMode = configuration.core.mode

  override def receiveEventInfo(eventInfo: EventInfo): Unit = ???

  override def init(): Unit = ???

  override def handleHarExportFailure(message: String): Unit = ???

  override def harFilePath: String = ???

  override def handleHarExportSuccess(): Unit = ???

  override def recordingStarted(): Unit = ???

  override def handleFilterValidationFailures(failures: Seq[String]): Unit = ???

  override def askSimulationOverwrite: Boolean = ???

  override def recordingStopped(): Unit = ???

  override def handleMissingHarFile(path: String): Unit = ???

  private def getProcessId: String =
    ManagementFactory.getRuntimeMXBean.getName.split("@").head
}
