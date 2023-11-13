# initial code by Gaurav Aggarwal on stackoverflow question
# https://stackoverflow.com/questions/76171191/multithreading-vs-linear-execution-of-python-code-showing-absurd-results/76187049#76187049

import datetime
import os
import threading
import time

import extrainterpreters as ei

workers = os.cpu_count() // 2


def test(iterations, wait, mode="increment"):
    for _ in range(iterations):
        if mode == "increment":
            a = 0
            while a <= 100000:
                a+=1
        elif mode == "time_polling":
            t = datetime.datetime.now()
            while datetime.datetime.now() <= t + datetime.timedelta(seconds=wait):
                pass
        else:
            raise ValueError(f"Unknown benchmark mode: {mode!r}")


def main():
    for iteration, wait in ((150, .001),): #((1000, .001), (10, 1)):
        for mode in ("increment", "time_polling"):
            print(f"Running {iteration} iteration, wait {wait}, {mode}, threaded")
            threads = [threading.Thread(target = test, args=(iteration, wait, mode)) for _ in range(workers)]
            start = time.time()
            [t.start() for t in threads]
            [t.join() for t in threads]
            multi_thread = time.time()-start

            print(f"Running {iteration} iteration, wait {wait}, {mode}, multi-interpreter")
            interps = [ei.Interpreter(target=test, args=(iteration, wait, mode)) for _ in range(workers)]
            start = time.time()
            [i.start() for i  in interps]
            [i.join() for i in interps]
            multi_interpreter = time.time()-start

            print(f"Running {iteration} iteration, wait {wait}, {mode}, single-worker,")
            start = time.time()
            test(iteration*2, wait, mode)
            linear = time.time() - start

            print(f"Mode: {mode}\n\tworkers: {workers}\n\tmulti-threaded time: {multi_thread:.4f}\n\tMulti-interpreter time: {multi_interpreter:.4f}\n\tSingle worker, in thread, wall time: {linear:.4f}\n\n")

if __name__ =="__main__":
    main()
