import argparse
import random
import subprocess
import threading


def process_output(out_pipe):
    """
    Capture the output from the given ``processes``

    :param out_pipe: pipe from which to read bytes
    """
    for line in out_pipe:
        print(line)


def get_arguments():
    """
    Parse command line arguments
    """
    argp = argparse.ArgumentParser(prog='houzz_prog')
    argp.add_argument('-p', '--pool', help='Process pool length', dest='pool', default=1, type=int)
    argp.add_argument('-m', '--max', help='Max amount of profiles to process', dest='max_', default=200, type=int)
    argp.add_argument('-s', '--start', help='From which profile to start', dest='start', default=0, type=int)

    return argp.parse_args()


def run(pool, max_, start_from):
    """
    Run scrapy subprocesses

    :param pool: amount of subprocesses to run
    :param max: max amount of profiles to process
    :param start_from: from which profile to start
    """
    process_hash = hex(random.getrandbits(128))[2:]
    total_max = max_
    step = total_max // pool
    it = start_from

    children = []
    for i in range(pool):
        process = subprocess.Popen(['scrapy', 'crawl', 'api',
                                    '-a',
                                    f'process_hash={process_hash}',
                                    '-a',
                                    f'start_from={it}',
                                    '-a',
                                    f'max_count={it + step}'], stdout=subprocess.PIPE)
        children.append(process)
        it += step

    out_thread = threading.Thread(target=process_output, args=(process.stdout,), daemon=True)
    out_thread.start()

    for v in children:
        v.wait()


if __name__ == '__main__':
    args = get_arguments()
    run(args.pool, args.max_, args.start)


