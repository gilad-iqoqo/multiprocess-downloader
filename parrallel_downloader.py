import logging
from multiprocessing import Process
import os
import urllib.request
import pdb


class MultiProcDownloader(object):

    def __init__(self, source_target_list, num_proc=16, overwrite_existing=False, buffering=1024*1024, log_file='/tmp/mpd.log'):
        self.source_target_list = source_target_list
        self.num_proc = num_proc
        self.overwrite_existing = overwrite_existing
        self.logger = logging.getLogger("mpd")
        self.buffering = buffering
        self.log_file = log_file
        self.set_logger(log_file)
        self.procs = []

        self.logger.debug("setup done")

    def set_logger(self, log_file):
        """ one file per worker"""
        self.logger = logging.getLogger("mpd")
        self.logger.setLevel(logging.DEBUG)

        ch = logging.StreamHandler()
        ch.setLevel(logging.ERROR)

        main_fh = logging.FileHandler("{}.{}".format(log_file, "main"))
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        main_fh.setFormatter(formatter)
        ch.setFormatter(formatter)

        self.logger.addHandler(main_fh)
        self.logger.addHandler(ch)

        fhs = [logging.FileHandler("{}.{}".format(log_file, w)) for w in range(self.num_proc)]
        for w, fh in enumerate(fhs):
            fh.setLevel(logging.DEBUG)
            fh.setFormatter(formatter)
            logger = logging.getLogger("mpd.{}".format(w))
            logger.addHandler(ch)
            logger.addHandler(fh)

    def download_list(self, source_target_list, w_id):
        OK = 0
        SKIPPED = 1
        FAILED = 2

        logger = logging.getLogger("mpd.{}".format(w_id))
        logger.info("starting download list with {} items".format(len(source_target_list)))

        def download_single(url, target):
            if not self.overwrite_existing:
                if os.path.isfile(target):
                    self.logger.debug("file {} exists. skipping".format(target))
                    return SKIPPED
            else:
                try:
                    bytes = 0
                    with open(target, 'wb') as f_target:
                        with urllib.request.urlopen(url) as f_source:
                            while True:
                                buff = f_source.read(self.buffering)
                                if len(buff) == 0:
                                    break
                                bytes += len(buff)
                                f_target.write(buff)
                    logger.debug("Done downloading {} bytes. {}  ===> {}".format(bytes, url, target))
                except:
                    logger.info("error downloading {}  ===> {}".format(url, target))
                    return FAILED

            return OK

        count = [0, 0, 0]
        for (url, target) in source_target_list:
            res = download_single(url, target)
            count[res] += 1

        logger.info("Download list done OK {} SKIPPED {} FAILED {}".format(count[OK],
                                                                                count[SKIPPED],
                                                                                count[FAILED]))
        return 0

    def run(self, wait=True):
        _len = len(self.source_target_list)
        part_size = max(_len // self.num_proc, 1)
        list_splits = [self.source_target_list[min(start, _len): min(start+part_size, _len)]
                       for start in range(0, _len, part_size)]
        w_ids = range(self.num_proc)
        for args in zip(list_splits, w_ids):
            p = Process(target=self.download_list, args=args)
            self.procs.append(p)
            p.start()
        if wait:
            self.wait_done()

    def wait_done(self):
        for p in self.procs:
            p.join()

    def check_done(self):
        return [not p.is_alive for p in self.procs]


def test():
    import timeit
    import csv
    with open('download_list.csv', 'r') as f:
        reader = csv.reader(f)
        download_list = [(_line[1], "/tmp/image.{}.jpg".format(e)) for e, _line in enumerate(reader)]

    mpd = MultiProcDownloader(download_list, overwrite_existing=True)

    t = timeit.Timer()
    pdb.set_trace()
    print("Download single")
    mpd.download_list([download_list[1]], 0)
    print("elapsed {}".format(t.timer()))

    print("Download one by one")
    mpd.download_list(download_list, 0)
    print("elapsed {}".format(t.timer()))

    print("Download in par")
    mpd.run(True)
    print("elapsed {}".format(t.timer()))


if __name__ == "__main__":
    test()

