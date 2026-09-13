#                Chronos Test Suite
#            (c) Copyright 2026-Present
#         Status Research & Development GmbH
#
#              Licensed under either of
#  Apache License, version 2.0, (LICENSE-APACHEv2)
#              MIT license (LICENSE-MIT)

# Standalone integration tests for the illumos event-port dispatcher.
when defined(solaris):
  import std/unittest
  import ../chronos
  import ../chronos/osdefs

  proc exerciseStreams() {.async.} =
    var sockets: array[2, cint]
    doAssert socketpair(AF_UNIX, SOCK_STREAM or SOCK_NONBLOCK or SOCK_CLOEXEC,
                        0, sockets) == 0
    let client = fromPipe(AsyncFD(sockets[0]))
    let peer = fromPipe(AsyncFD(sockets[1]))
    try:
      var message = newSeq[byte](128 * 1024)
      for i in 0 ..< message.len:
        message[i] = byte(i mod 251)
      var buffer = newSeq[byte](message.len)
      for i in 0 ..< 10:
        let reader = peer.readExactly(addr buffer[0], buffer.len)
        let writer = client.write(message)
        await reader.wait(2.seconds)
        doAssert (await writer.wait(2.seconds)) == message.len
        doAssert buffer == message
        # Exercise both directions and interest changes on the same sockets.
        let replyReader = client.readExactly(addr buffer[0], buffer.len)
        let replyWriter = peer.write(message)
        await replyReader.wait(2.seconds)
        doAssert (await replyWriter.wait(2.seconds)) == message.len
    finally:
      await client.closeWait()
      await peer.closeWait()

  when compileOption("threads"):
    from std/os import sleep

    type WakeArg = object
      dispatcher: DispatcherHandle
      future: pointer

    proc finish(udata: pointer) {.gcsafe, raises: [].} =
      cast[ptr Future[void]](udata)[].complete()

    proc wakeDispatcher(arg: WakeArg) {.thread.} =
      sleep(20)
      arg.dispatcher.callSoon(finish, arg.future)

  suite "Event-port dispatcher integration":
    test "Userspace timers":
      for i in 0 ..< 5:
        waitFor sleepAsync(5.milliseconds)

    test "Repeated bidirectional stream transfers and close":
      waitFor exerciseStreams()

    when compileOption("threads"):
      test "Repeated cross-thread dispatcher wakeups":
        for i in 0 ..< 5:
          var
            future = newFuture[void]("event-port wakeup")
            thread: Thread[WakeArg]
          createThread(thread, wakeDispatcher,
                       WakeArg(dispatcher: getThreadDispatcher().handle(),
                               future: addr future))
          try:
            waitFor future.wait(2.seconds)
          finally:
            joinThread(thread)
          check future.finished()
