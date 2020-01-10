from pprint import pprint
from statistics import mean, stdev

from models import Server, Scheduler
from utils.random import get_poisson_sample, get_exp, get_bernouli
from utils.report import *


class Simulator:
    INPUT_DELIMITER = ","
    INITIAL_COUNT = 50
    JOB_COUNT = 10000
    CLASS_ONE_RATIO = 0.1
    TIME_STEP = 1
    MIN_ACCURACY = 0.95

    def init_jobs(self):
        start_times = get_poisson_sample(rate=self.job_rate, n=self.JOB_COUNT)
        deadlines = [get_exp(self.job_deadline_rate) for i in range(self.JOB_COUNT)]
        types = [get_bernouli(1 - self.CLASS_ONE_RATIO) + 1 for i in range(self.JOB_COUNT)]

        self.jobs = [Job(i, types[i], start_times[i], start_times[i] + deadlines[i]) for i in range(self.JOB_COUNT)]

    def __init__(self, file_address):
        self.time = -1
        self.scheduler = None
        self.servers = []
        self.jobs = None
        self.job_deadline_rate = 0
        self.job_rate = 0
        self.parse_input(file_address)
        self.init_jobs()
        self.total_jobs_started = 0
        self.total_jobs_finished = 0
        self.report_final_phase_id = None

        self.servers_queue_lengths = [[] for i in range(len(self.servers))]
        self.scheduler_queue_lengths = []

        self.reports = []

    def parse_input(self, file_address):
        def parse_line(line):
            return map(lambda x: float(x.strip()), line.split(self.INPUT_DELIMITER))

        with open(file_address) as input_file:
            servers_count, self.job_rate, self.job_deadline_rate, scheduler_rate = parse_line(input_file.readline())

            servers_count = int(servers_count)
            for i in range(servers_count):
                cores_rate = list(parse_line(input_file.readline()))[1:]
                server = Server(i, cores_rate)
                self.servers.append(server)

            self.scheduler = Scheduler(scheduler_rate, self.servers)

    def run(self):
        while True:
            self.time += self.TIME_STEP
            self.remove_expired_jobs()
            self.finish_done_jobs()
            self.add_new_jobs()
            self.schedule_jobs()
            self.assign_to_cores()
            self.update_queue_lengths()
            self.gen_report()

            if self.check_finish_criteria():
                break

        pprint(self.reports[-1])
        print(self.get_accuracy())

    def remove_expired_jobs(self):
        def remove_expired_from_queue(server):
            removable_jobs = []
            for job in server.queue:
                if job.deadline < self.time:
                    job.expired = True
                    self.total_jobs_finished += 1
                    print(f'{job.id} expired')
                    removable_jobs.append(job)
                    job.expired_time = self.time
            server.queue = [j for j in server.queue if j not in removable_jobs]

        remove_expired_from_queue(self.scheduler)
        for s in self.servers:
            remove_expired_from_queue(s)

    def add_new_jobs(self):
        for job in self.jobs:
            if not job.is_active and job.start_time < self.time:
                self.scheduler.queue.append(job)
                self.total_jobs_started += 1
                job.is_active = True

    def schedule_jobs(self):
        while self.scheduler.queue and self.scheduler.ready_time < self.time:
            self.scheduler.schedule(self.time)

    def assign_to_cores(self):
        for server in self.servers:
            for core in server.cores:
                if core.ready_time < self.time and server.queue:
                    server.queue.sort(key=lambda x: (x.type, x.start_time))
                    job: Job = server.queue[0]
                    job.process_start_time = self.time
                    core.ready_time = self.time + get_exp(core.service_rate)
                    job.finish_time = core.ready_time
                    server.queue = server.queue[1:]

    def check_finish_criteria(self):
        return self.get_accuracy() >= self.MIN_ACCURACY or self.total_jobs_finished == self.JOB_COUNT

    def get_accuracy(self):
        if self.total_jobs_started <= self.INITIAL_COUNT:
            return 0

        if not self.report_final_phase_id:
            self.report_final_phase_id = len(self.reports) - 1

        if len(self.reports[self.report_final_phase_id:]) < 2:
            return 0

        min_acc = 1

        for k in self.reports[-1].keys():
            vals = list(map(lambda x: x[k], self.reports[self.report_final_phase_id:]))
            avg = mean(vals)
            dev = stdev(vals)

            err = 1.96 * dev / (len(vals)**.5) / avg if avg != 0 else 0

            min_acc = min(min_acc, 1 - err)

        return min_acc

    def finish_done_jobs(self):
        for job in self.jobs:
            if job.finish_time and job.finish_time < self.time and not job.is_done and not job.expired:
                print(f'{job.id} is  done')
                self.total_jobs_finished += 1
                job.is_done = True

    def update_queue_lengths(self):
        for i, s in enumerate(self.servers):
            self.servers_queue_lengths[i].append(len(s.queue))
        self.scheduler_queue_lengths.append(len(self.scheduler.queue))

    def gen_report(self):
        jobs = self.jobs
        jobs_t1 = list(filter(lambda x: x.type == 1, jobs))
        jobs_t2 = list(filter(lambda x: x.type == 2, jobs))
        report = {
            'avg_spent_tot': report_average_spent_time(jobs),
            'avg_spent_t1': report_average_spent_time(jobs_t1),
            'avg_spent_t2': report_average_spent_time(jobs_t2),
            'avg_wait_tot': report_average_wait_time(jobs),
            'avg_wait_t1': report_average_wait_time(jobs_t1),
            'avg_wait_t2': report_average_wait_time(jobs_t2),
            'avg_rate_expired_tot': report_rate_of_expired(jobs),
            'avg_rate_expired_t1': report_rate_of_expired(jobs_t1),
            'avg_rate_expired_t2': report_rate_of_expired(jobs_t2),
            'avg_scheduler_queue_len': report_mean_queue_length(self.scheduler_queue_lengths)
        }

        for i, server_queue_length in enumerate(self.servers_queue_lengths):
            report[f'avg_server_{i}_queue_lens'] = report_mean_queue_length(server_queue_length)

        self.reports.append(report)


if __name__ == '__main__':
    simulator = Simulator('input.csv')
    simulator.run()
