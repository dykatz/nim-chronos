#                Chronos Test Suite
#            (c) Copyright 2026-Present
#         Status Research & Development GmbH
#
#              Licensed under either of
#  Apache License, version 2.0, (LICENSE-APACHEv2)
#              MIT license (LICENSE-MIT)

# Standalone selector tests: nim c -r tests/testeventportprocess.nim
when defined(solaris):
  import std/[unittest, sets, monotimes, times]
  import ../chronos/[selectors2, osdefs, osutils]
  from std/posix import WNOHANG, WUNTRACED, WIFSTOPPED, WIFEXITED, WEXITSTATUS,
                        WIFSIGNALED, WTERMSIG

  {.compile: "testeventportprocess.c".}
  proc spawnFixture(nonchild: cint, gate, ack, target: ptr cint): cint {.
    cdecl, importc: "chronos_process_fixture".}
  proc waitZombie(pid: cint): cint {.cdecl, importc: "chronos_wait_zombie".}

  type Child = object
    owner, pid: cint
    gate, ack: cint
    reaped: bool

  proc spawn(nonchild = false): Child =
    result.owner = spawnFixture(cint(nonchild), addr result.gate,
                                addr result.ack, addr result.pid)
    doAssert result.owner > 0, osErrorMsg(osLastError())

  proc command(child: Child, value: char) =
    var data = value
    doAssert handleEintr(osdefs.write(child.gate, addr data, 1)) == 1

  proc release(child: Child) =
    child.command('\n')

  proc execChild(child: Child) =
    child.command('E')
    var data: char
    doAssert handleEintr(osdefs.read(child.ack, addr data, 1)) == 1
    doAssert data == 'E'

  proc reap(child: var Child): cint =
    doAssert not child.reaped
    var status: cint
    doAssert handleEintr(waitpid(Pid(child.owner), status, 0)) == child.owner
    child.reaped = true
    status

  proc cleanup(child: var Child) =
    discard closeFd(child.gate)
    discard closeFd(child.ack)
    if not child.reaped:
      if child.owner == child.pid:
        # The unreaped child still owns its PID, so it cannot have been reused.
        discard kill(Pid(child.pid), SIGKILL)
      # Non-child fixtures exit when their input closes; their parent reaps
      # them. Do not signal a non-child PID which might already have been reused.
      discard child.reap()

  proc checkExit(child: var Child, code: cint = 23) =
    let status = child.reap()
    doAssert WIFEXITED(status)
    doAssert WEXITSTATUS(status) == code

  proc waitProcess(s: Selector[int], fd: cint) =
    let ready = s.select(2000)
    doAssert ready.len == 1
    doAssert ready[0] == ReadyKey(fd: fd,
      events: {Event.Process, Event.Oneshot, Event.Finished})

  proc sigchldBlocked(): bool =
    var mask, previous: Sigset
    doAssert sigemptyset(mask) == 0
    when compileOption("threads"):
      doAssert pthread_sigmask(SIG_BLOCK, mask, previous) == 0
    else:
      doAssert sigprocmask(SIG_BLOCK, mask, previous) == 0
    sigismember(previous, SIGCHLD) == 1

  suite "Event-port process monitoring":
    test "Live child exits exactly once without being reaped by the selector":
      var child = spawn()
      defer: child.cleanup()
      let s = newSelector[int]()
      defer: s.close()
      let wasBlocked = sigchldBlocked()
      let fd = s.registerProcess2(int(child.pid), 12).get()
      check s.contains(fd)
      check (fcntl(fd, F_GETFD) and FD_CLOEXEC) != 0
      check sigchldBlocked() == wasBlocked
      let start = getMonoTime()
      check s.select(20).len == 0
      check (getMonoTime() - start).inMilliseconds >= 10
      check s.setData(fd, 34)
      child.release()
      s.waitProcess(fd)
      s.withData(fd, data):
        check data[] == 34
      let finished = getMonoTime()
      check s.select(20).len == 0
      check (getMonoTime() - finished).inMilliseconds >= 10
      child.checkExit()
      s.unregister(fd)
      check not s.contains(fd)
      check fcntl(fd, F_GETFD) == -1
      check osLastError() == EBADF
      check sigchldBlocked() == wasBlocked

    test "Stops and exec are not reported as exits":
      var child = spawn()
      defer: child.cleanup()
      let s = newSelector[int]()
      defer: s.close()
      let fd = s.registerProcess(int(child.pid), 0).get()
      require kill(Pid(child.pid), SIGSTOP) == 0
      var status: cint
      require handleEintr(waitpid(Pid(child.pid), status, WUNTRACED)) == child.pid
      require WIFSTOPPED(status)
      check s.select(20).len == 0
      require kill(Pid(child.pid), SIGCONT) == 0
      child.execChild()
      check s.select(20).len == 0
      child.release()
      s.waitProcess(fd)
      s.unregister(fd)
      child.checkExit()

    test "Termination by signal":
      var child = spawn()
      defer: child.cleanup()
      let s = newSelector[int]()
      defer: s.close()
      let fd = s.registerProcess(int(child.pid), 0).get()
      require kill(Pid(child.pid), SIGTERM) == 0
      s.waitProcess(fd)
      s.unregister(fd)
      let status = child.reap()
      check WIFSIGNALED(status)
      check WTERMSIG(status) == SIGTERM

    test "A zombie can be registered before it is reaped":
      var child = spawn()
      defer: child.cleanup()
      child.release()
      require waitZombie(child.pid) == 0
      let s = newSelector[int]()
      defer: s.close()
      let fd = s.registerProcess(int(child.pid), 0).get()
      s.waitProcess(fd)
      s.unregister(fd)
      child.checkExit()
      check s.registerProcess(int(child.pid), 0).error() == ESRCH

    test "Exit and reaping before select do not lose the notification":
      var child = spawn()
      defer: child.cleanup()
      let s = newSelector[int]()
      defer: s.close()
      let fd = s.registerProcess(int(child.pid), 0).get()
      child.release()
      child.checkExit()
      s.waitProcess(fd)
      s.unregister(fd)

    test "A non-child process can be monitored":
      var child = spawn(nonchild = true)
      defer: child.cleanup()
      var status: cint
      require waitpid(Pid(child.pid), status, WNOHANG) == -1
      require osLastError() == ECHILD
      let s = newSelector[int]()
      defer: s.close()
      let fd = s.registerProcess(int(child.pid), 0).get()
      check s.select(20).len == 0
      child.release()
      s.waitProcess(fd)
      s.unregister(fd)
      child.checkExit(0) # Fixture parent reaped the monitored grandchild.

    test "Unregister does not terminate or reap a live process":
      var child = spawn()
      defer: child.cleanup()
      let s = newSelector[int]()
      defer: s.close()
      let fd = s.registerProcess(int(child.pid), 0).get()
      s.unregister(fd)
      check kill(Pid(child.pid), 0) == 0
      child.release()
      child.checkExit()
      check s.select(0).len == 0

    test "Queued events and deferred rearming cannot follow FD reuse":
      for delivered in [false, true]:
        var first = spawn()
        defer: first.cleanup()
        var second = spawn()
        defer: second.cleanup()
        let s = newSelector[int]()
        defer: s.close()
        let old = s.registerProcess(int(first.pid), 1).get()
        first.release()
        require waitZombie(first.pid) == 0
        if delivered:
          s.waitProcess(old)
        s.unregister(old)
        let replacement = s.registerProcess(int(second.pid), 2).get()
        check replacement == old
        check s.select(20).len == 0
        second.release()
        s.waitProcess(replacement)
        s.withData(replacement, data):
          check data[] == 2
        s.unregister(replacement)
        first.checkExit()
        second.checkExit()

    test "Batched exits respect output capacity":
      var children: seq[Child]
      defer:
        for child in children.mitems:
          child.cleanup()
      let s = newSelector[int]()
      defer: s.close()
      var expected = initHashSet[int]()
      for i in 0 ..< 12:
        children.add(spawn())
        expected.incl(int(s.registerProcess(int(children[^1].pid), i).get()))
      for child in children:
        child.release()
      var ready: array[3, ReadyKey]
      while expected.len > 0:
        let count = s.selectInto(2000, ready)
        require count > 0
        for i in 0 ..< count:
          check ready[i].fd in expected
          expected.excl(ready[i].fd)
          check ready[i].events == {Event.Process, Event.Oneshot, Event.Finished}
          s.unregister(cint(ready[i].fd))
      for child in children.mitems:
        child.checkExit()
      check s.select(0).len == 0

    test "Close releases live and finished process descriptors":
      var live = spawn()
      defer: live.cleanup()
      var done = spawn()
      defer: done.cleanup()
      let s = newSelector[int]()
      let liveFd = s.registerProcess(int(live.pid), 0).get()
      let doneFd = s.registerProcess(int(done.pid), 0).get()
      done.release()
      s.waitProcess(doneFd)
      s.close()
      for fd in [liveFd, doneFd]:
        check fcntl(fd, F_GETFD) == -1
        check osLastError() == EBADF
      live.release()
      live.checkExit()
      done.checkExit()

    test "Duplicate, invalid and failed registrations":
      var child = spawn()
      defer: child.cleanup()
      let s = newSelector[int]()
      check s.registerProcess(0, 0).error() == EINVAL
      check s.registerProcess(-1, 0).error() == EINVAL
      when sizeof(int) > sizeof(cint):
        check s.registerProcess(int(high(cint)) + 1, 0).error() == EINVAL
      let fd = s.registerProcess(int(child.pid), 0).get()
      expect AssertionDefect:
        discard s.registerProcess(int(child.pid), 1)
      s.unregister(fd)
      let portFd = s.getFd()
      require closeFd(portFd) == 0 # Inject port_associate failure after open.
      check s.registerProcess(int(child.pid), 0).isErr()
      check fcntl(portFd, F_GETFD) == -1 # The failed registration closed psinfo.
      check osLastError() == EBADF
      check s.close2().error() == EBADF
      check s.registerProcess(int(child.pid), 0).error() == EBADF
      child.release()
      child.checkExit()
