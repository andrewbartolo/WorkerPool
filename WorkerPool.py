#!/usr/bin/env python3
#
# ---- WorkerPool ----
# Schedule jobs in parallel across multiple remote hosts using SSH.
# Monitors hosts to make sure that they don't run out of memory.
# TODO: better failed-job handling (don't just hang at end)
#

from concurrent.futures import ThreadPoolExecutor
from subprocess import PIPE
import random
import shutil
import signal
import subprocess
import sys, os
import threading
import time



#### Helper functions
def getTerminalWidth():
    return shutil.get_terminal_size().columns

def getProgressBar(i, n):
    termWidth = getTerminalWidth()
    nNonBarChars = len('[XYZ%% ]')
    intPct = int((i / n) * 100)
    barWidth100Pct = termWidth - nNonBarChars
    nBarChars = int((i/n) * barWidth100Pct)
    nSpaceChars = termWidth - nNonBarChars - nBarChars

    return '[' + ' %3d%% ' % intPct + '#'*nBarChars + ' '*nSpaceChars + ']'

# returns a possibly-ellipsized string
def getFittingString(s):
    termWidth = getTerminalWidth()
    if len(s) <= termWidth or len(s) <= 6:
        return s

    # need to ellipsize
    sep = ' ... '
    parLen = int((termWidth - len(sep))/2)
    s = s[:parLen] + sep + s[-parLen:]
    return s

def prettyPrintWithProgress(s, i, n):
    sys.stdout.write('\r' + ' '*getTerminalWidth() + '\r') # clear current line
    sys.stdout.write(getFittingString(s))
    sys.stdout.write('\n')
    sys.stdout.write(getProgressBar(i, n))
    sys.stdout.flush()


class Job:
    def __init__(self, command, sourceDir, outputDir):
        self.command = command
        self.sourceDir = sourceDir
        self.outputDir = outputDir
        self.pid = -1

    def __str__(self):
        # NOTE: for fd redirects, we need to use 1>file, not just >file!
        # This is because command can end in a number, causing ambiguity
        return '\'cd %s && %s 1>%s/out.log 2>%s/err.log\'' % (self.sourceDir,
                self.command, self.outputDir, self.outputDir)


class Host:
    def __init__(self, hostname, nSlots, workerPool, lowMemThreshGiB=5):
        self.hostname = hostname
        self.pool = ThreadPoolExecutor(nSlots)
        self.workerPool = workerPool    # backreference to our owner WorkerPool
        self.memWatchdog = threading.Thread(target=self._mem_watchdog,
                args=(lowMemThreshGiB,), daemon=True)
        self.memWatchdog.start()
        self.youngestJob = None

        # counters
        self.jobsSubmitted = 0
        self.jobsOOMed = 0
        self.jobsCompleted = 0


    def submitToHost(self, job, wasRestarted):
        self.pool.submit(self._run_wrapper, job)

        # only count "original" non-restarted jobs toward the submitted tally
        if not wasRestarted:
            self.jobsSubmitted += 1
            self.workerPool.jobsSubmitted += 1


    def _mem_watchdog(self, lowMemThreshGiB):
        # poll the host and make sure it has enough memory
        while True:
            memCmd = ['ssh', self.hostname, '--', 'free', '-g']
            res = subprocess.run(memCmd, stdout=PIPE, stderr=PIPE)
            out = res.stdout
            gibFree = int(out.split(b'\n')[1].decode('utf-8').split()[6])

            if gibFree <= lowMemThreshGiB:
                job = self.youngestJob
                if job and job.pid != -1:   # if actually launched
                    print("WARNING: %s OOM -- killing and re-queueing youngest"
                            " job" % host)

                    # kill and re-enqueue (possibly on a different host!)
                    os.kill(job.pid, signal.SIGTERM)
                    self.workerPool._submit(job, wasRestarted=True)
                    self.jobsOOMed += 1

            time.sleep(20)

    # wrapper for subprocess.Popen() that records the youngest job
    def _run_wrapper(self, job):
        self.youngestJob = job
        sshCmd = ['ssh', self.hostname, '--', 'bash', '-c', str(job)]
        p = subprocess.Popen(sshCmd, stdout=PIPE, stderr=PIPE)
        job.pid = p.pid
        p.wait()


        ## begin critical region
        self.workerPool.cv.acquire()
        if (p.returncode == 0):
            # track completed jobs both per-host and WorkerPool-wide
            self.jobsCompleted += 1
            self.workerPool.jobsCompleted += 1
        else: print("WARNING: job exited with failure code %d"
            " (possibly restarted)" % p.returncode)

        if self.workerPool.progress:
            prettyPrintWithProgress(job.command, self.workerPool.jobsCompleted,
                    self.workerPool.jobsSubmitted)

        # if our (host's) job was the final job, signal the WorkerPool
        if self.workerPool.final and \
                self.workerPool.jobsCompleted >= self.workerPool.jobsFinal:
            self.workerPool.cv.notify()
        self.workerPool.cv.release()
        ## end critical region



class WorkerPool:
    # clusterHosts is a dict with hostnames as keys, and the number of CPU slots
    # as values. progress prints a status bar.
    def __init__(self, clusterHosts={'localhost': 1}, progress=True):
        self.hosts = [Host(k, v, self) for k, v in clusterHosts.items()]
        self.jobsSubmitted = 0
        self.jobsCompleted = 0
        self.final = False
        self.jobsFinal = 0
        self.cv = threading.Condition()
        self.progress = progress

    # internal WorkerPool-wide submission method.
    def _submit(self, job, wasRestarted):
        # Submits asynchronously to a node (round-robin).
        host = self.hosts[self.jobsSubmitted % len(self.hosts)]
        host.submitToHost(job, wasRestarted)


    # external client-facing submission interface
    def submit(self, command, sourceDir, outputDir):
        job = Job(command, sourceDir, outputDir)
        self._submit(job, wasRestarted=False)


    # indicate that we're done submitting (new) jobs, and join the WorkerPool.
    def join(self):
        ## begin critical region
        self.cv.acquire()
        self.final = True
        self.jobsFinal = self.jobsSubmitted

        while self.jobsCompleted < self.jobsFinal:
            self.cv.wait()
        self.cv.release()
        ## end critical region

        for h in self.hosts:
            h.pool.shutdown()
            # kill mem watchdog too (not strictly necessary, as daemon thread)


if __name__ == '__main__':
    WorkerPool = WorkerPool(clusterHosts={'rsg3.stanford.edu': 2})

    for i in range(10):
        WorkerPool.submit('uptime && sleep 2', '.', '.')
        #WorkerPool.submit('cat /tmp/nosuchfile', '.', '.')

    WorkerPool.join()

    print('done.')
