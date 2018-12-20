import threading
import signal
import sys
from queue import Queue, Empty
import traceback


class ThreadedHarvester():
    def __init__(self, scraper, num_workers=20, queue_timeout=10):
        self.scraper = scraper
        self.num_workers = num_workers
        self.queue_timeout = queue_timeout

    def process_queued_item(self, item):
        try:
            self.scraper.process_queued_item(item)
        except Exception as e:
            print("[ERROR] process_queued_item", str(item))
            print(e)
            traceback.print_tb(e.__traceback__)


    def worker_loop(self):
        thread_name = threading.current_thread().getName()
        print("%s started" % thread_name)

        while True:
            try:
                # print("%s get queue (%s)" % (thread_name,self.scraper.queue.qsize()))
                item = self.scraper.queue.get(timeout=self.queue_timeout)
                self.process_queued_item(item)
                self.scraper.queue.task_done()
                # print("%s  task done" % thread_name)
            except Empty:
                print("%s timed out" % thread_name)
                return

    def harvest(self, items):
        # Ctrl+\
        # signal.signal(signal.SIGQUIT, self.dump_thread_stacks)

        for item in items:
            self.scraper.queue.put(item)

        self.threads = []
        print("Starting %d threads" % self.num_workers)
        for _ in range(self.num_workers):
            t = threading.Thread(target=self.worker_loop)
            self.threads.append(t)
            t.daemon = True
            t.start()

        self.scraper.queue.join()



    # def dump_thread_stacks(self, sig, main_thread_frame):
    #     print("\n*** STACKTRACE - START ***\n")
    #     print("queue size = %s" % self.scraper.queue.qsize())
    #     print(threading.enumerate())

    #     # set to True to print stack traces for every single thread
    #     if False:
    #         code = []
    #         for threadId, stack in sys._current_frames().items():
    #             code.append("\n# ThreadID: %s" % threadId)
    #             for filename, lineno, name, line in traceback.extract_stack(stack):
    #                 code.append('File: "%s", line %d, in %s' % (filename,
    #                                                             lineno, name))
    #                 if line:
    #                     code.append("  %s" % (line.strip()))

    #         for line in code:
    #             print(line)
    #     print("\n*** STACKTRACE - END ***\n")
