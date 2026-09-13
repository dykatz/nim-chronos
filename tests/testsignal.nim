#                Chronos Test Suite
#            (c) Copyright 2018-Present
#         Status Research & Development GmbH
#
#              Licensed under either of
#  Apache License, version 2.0, (LICENSE-APACHEv2)
#              MIT license (LICENSE-MIT)
import unittest2
import ../chronos, ../chronos/[oserrno, config]

{.used.}

when not defined(windows):
  import posix

suite "Signal handling test suite":
  proc testSignal(signal, value: int): Future[bool] {.async.} =
    var
      signalCounter = 0
      sigFd: SignalHandle
      handlerFut = newFuture[void]("signal.handler")

    proc signalHandler(udata: pointer) {.gcsafe.} =
      signalCounter = cast[int](udata)
      let res = removeSignal2(sigFd)
      if res.isErr():
        handlerFut.fail(newException(ValueError, osErrorMsg(res.error())))
      else:
        handlerFut.complete()

    sigFd =
      block:
        let res = addSignal2(signal, signalHandler, cast[pointer](value))
        if res.isErr():
          raiseAssert osErrorMsg(res.error())
        res.get()

    when defined(windows):
      discard raiseSignal(cint(signal))
    else:
      discard posix.kill(posix.getpid(), cint(signal))

    await handlerFut.wait(5.seconds)
    return signalCounter == value

  proc testWait(signal: int): Future[bool] {.async.} =
    var fut = waitSignal(signal)
    when defined(windows):
      discard raiseSignal(cint(signal))
    else:
      discard posix.kill(posix.getpid(), cint(signal))
    await fut.wait(5.seconds)
    return true

  when defined(windows):
    proc testCtrlC(): Future[bool] {.async, used.} =
      var fut = waitSignal(SIGINT)
      let res = raiseConsoleCtrlSignal()
      if res.isErr():
        raiseAssert osErrorMsg(res.error())
      await fut.wait(5.seconds)
      return true

  test "SIGINT test":
    let res = waitFor testSignal(SIGINT, 31337)
    check res == true

  test "SIGTERM test":
    let res = waitFor testSignal(SIGTERM, 65537)
    check res == true

  test "waitSignal(SIGINT) test":
    let res = waitFor testWait(SIGINT)
    check res == true

  test "waitSignal(SIGTERM) test":
    let res = waitFor testWait(SIGTERM)
    check res == true

  when chronosEventEngine == "event_port":
    proc sendLocalSignal(sig: cint) =
      # Other tests may have worker threads with different signal masks.
      when compileOption("threads"):
        doAssert pthread_kill(pthread_self(), sig) == 0
      else:
        doAssert posix.kill(posix.getpid(), sig) == 0

    proc changeMask(how: cint, mask: var Sigset) =
      var previous: Sigset
      when compileOption("threads"):
        doAssert pthread_sigmask(how, mask, previous) == 0
      else:
        doAssert sigprocmask(how, mask, previous) == 0

    proc currentMask(): Sigset =
      var empty: Sigset
      doAssert sigemptyset(empty) == 0
      when compileOption("threads"):
        doAssert pthread_sigmask(SIG_BLOCK, empty, result) == 0
      else:
        doAssert sigprocmask(SIG_BLOCK, empty, result) == 0

    proc isBlocked(sig: cint): bool =
      var mask = currentMask()
      sigismember(mask, sig) == 1

    test "Persistent callbacks fire once per delivered signal":
      var
        count = 0
        notification = newFuture[void]("signal.notification")
      proc handler(udata: pointer) {.gcsafe.} =
        inc(count)
        if not notification.finished():
          notification.complete()
      let handle = addSignal(int(SIGUSR1), handler)
      var registered = true
      defer:
        if registered:
          discard removeSignal2(handle)
      for i in 1 .. 5:
        sendLocalSignal(SIGUSR1)
        waitFor notification.wait(2.seconds)
        waitFor sleepAsync(2.milliseconds)
        check count == i
        if i < 5:
          notification = newFuture[void]("signal.notification")
      removeSignal(handle)
      registered = false
      check not getThreadDispatcher().contains(AsyncFD(cint(handle)))

    test "Multiple signals share the dispatcher without losing notifications":
      let first = waitSignal(int(SIGUSR1))
      let second = waitSignal(int(SIGUSR2))
      sendLocalSignal(SIGUSR1)
      sendLocalSignal(SIGUSR2)
      waitFor first.wait(2.seconds)
      waitFor second.wait(2.seconds)
      check first.completed() and second.completed()

    test "Repeated waitSignal completion removes its registration":
      for i in 0 ..< 10:
        let future = waitSignal(int(SIGUSR1))
        sendLocalSignal(SIGUSR1)
        waitFor future.wait(2.seconds)
        check future.completed()

    test "Cancellation unregisters the signal and permits another wait":
      let wasBlocked = isBlocked(SIGUSR1)
      let future = waitSignal(int(SIGUSR1))
      waitFor future.cancelAndWait()
      check future.cancelled()
      check isBlocked(SIGUSR1) == wasBlocked
      let next = waitSignal(int(SIGUSR1))
      sendLocalSignal(SIGUSR1)
      waitFor next.wait(2.seconds)
      check next.completed()
      check isBlocked(SIGUSR1) == wasBlocked

    test "Timeout removes a pending signal wait":
      let wasBlocked = isBlocked(SIGUSR1)
      let future = waitSignal(int(SIGUSR1))
      expect AsyncTimeoutError:
        waitFor future.wait(10.milliseconds)
      check future.cancelled()
      check isBlocked(SIGUSR1) == wasBlocked
      let next = waitSignal(int(SIGUSR1))
      sendLocalSignal(SIGUSR1)
      waitFor next.wait(2.seconds)
      check next.completed()

    test "Cancellation with a blocked pending signal does not notify the old wait":
      var original = currentMask()
      defer: changeMask(SIG_SETMASK, original)
      var mask: Sigset
      require sigemptyset(mask) == 0
      require sigaddset(mask, SIGUSR1) == 0
      changeMask(SIG_BLOCK, mask)
      let future = waitSignal(int(SIGUSR1))
      sendLocalSignal(SIGUSR1)
      # Cancel synchronously, before the dispatcher retrieves the event. The
      # pre-existing block keeps the pending signal from running its default
      # handler when the old signalfd is closed.
      require future.tryCancel()
      check future.cancelled()
      check isBlocked(SIGUSR1)
      let next = waitSignal(int(SIGUSR1))
      waitFor next.wait(2.seconds)
      check next.completed()
      check future.cancelled()
      check isBlocked(SIGUSR1)

    test "Completion preserves pre-existing and unrelated signal-mask bits":
      var original = currentMask()
      defer: changeMask(SIG_SETMASK, original)
      for preBlocked in [false, true]:
        var mask: Sigset
        require sigemptyset(mask) == 0
        require sigaddset(mask, SIGUSR1) == 0
        changeMask(if preBlocked: SIG_BLOCK else: SIG_UNBLOCK, mask)
        let future = waitSignal(int(SIGUSR1))
        require sigemptyset(mask) == 0
        require sigaddset(mask, SIGUSR2) == 0
        changeMask(SIG_BLOCK, mask)
        sendLocalSignal(SIGUSR1)
        waitFor future.wait(2.seconds)
        check isBlocked(SIGUSR1) == preBlocked
        check isBlocked(SIGUSR2)

    test "Registration errors produce a failed future instead of a defect":
      for sig in [0, int(SIGKILL), int(SIGSTOP)]:
        let future = waitSignal(sig)
        check future.failed()
        expect AsyncError:
          waitFor future
      let next = waitSignal(int(SIGUSR1))
      sendLocalSignal(SIGUSR1)
      waitFor next.wait(2.seconds)
      check next.completed()

  # This test doesn't work well in test suite, because it generates CTRL+C
  # event in Windows console, parent process receives this signal and stops
  # test suite execution.

  # test "Windows [CTRL+C] test":
  #   when defined(windows):
  #     let res = waitFor testCtrlC()
  #     check res == true
  #   else:
  #     skip()
