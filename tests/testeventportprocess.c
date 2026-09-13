/*
 * Chronos test helpers, licensed under MIT or Apache-2.0.
 * Keep the fork child entirely in C: no Nim runtime/GC operations after fork.
 */
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <fcntl.h>
#include <signal.h>
#include <stdlib.h>
#include <errno.h>

static int read_exact(int fd, void *buf, size_t len) {
  char *p = buf;
  while (len != 0) {
    ssize_t n = read(fd, p, len);
    if (n < 0 && errno == EINTR) continue;
    if (n <= 0) return -1;
    p += n;
    len -= n;
  }
  return 0;
}

static void run_child(int gate, int ack) {
  int pid = (int)getpid();
  char command;
  if (dup2(gate, STDIN_FILENO) < 0 || dup2(ack, STDOUT_FILENO) < 0 ||
      fcntl(STDIN_FILENO, F_SETFD, 0) < 0 ||
      fcntl(STDOUT_FILENO, F_SETFD, 0) < 0)
    _exit(126);
  closefrom(3);
  if (write(STDOUT_FILENO, &pid, sizeof(pid)) != sizeof(pid) ||
      write(STDOUT_FILENO, "R", 1) != 1)
    _exit(126);
  if (read_exact(STDIN_FILENO, &command, 1) == 0 && command == 'E') {
    /* Acknowledge exec, then stay alive until the test releases stdin. */
    execl("/bin/sh", "sh", "-c", "printf E; read line; exit 23", (char *)0);
    _exit(127);
  }
  _exit(23);
}

int chronos_process_fixture(int nonchild, int *gate, int *ack, int *target) {
  int g[2], a[2];
  pid_t owner;
  if (pipe2(g, O_CLOEXEC) < 0) return -1;
  if (pipe2(a, O_CLOEXEC) < 0) {
    int error = errno;
    close(g[0]); close(g[1]);
    errno = error;
    return -1;
  }
  owner = fork();
  if (owner == 0) {
    if (nonchild) {
      pid_t child = fork();
      if (child < 0) _exit(126);
      if (child != 0) {
        int status;
        closefrom(3);
        while (waitpid(child, &status, 0) < 0 && errno == EINTR) {}
        _exit(0);
      }
    }
    run_child(g[0], a[1]);
  }
  int error = errno;
  close(g[0]); close(a[1]);
  if (owner < 0) {
    close(g[1]); close(a[0]);
    errno = error;
    return -1;
  }
  char ready;
  if (read_exact(a[0], target, sizeof(*target)) < 0 ||
      read_exact(a[0], &ready, 1) < 0 || ready != 'R') {
    int status;
    close(g[1]); close(a[0]);
    while (waitpid(owner, &status, 0) < 0 && errno == EINTR) {}
    errno = EIO;
    return -1;
  }
  *gate = g[1];
  *ack = a[0];
  return (int)owner;
}

int chronos_wait_zombie(int pid) {
  siginfo_t info;
  int res;
  do {
    res = waitid(P_PID, (id_t)pid, &info, WEXITED | WNOWAIT);
  } while (res < 0 && errno == EINTR);
  return res;
}
