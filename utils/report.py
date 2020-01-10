from typing import List

from models import Job


def report_average_spent_time(jobs: List[Job]):
    sum = 0
    for job in jobs:
        if job.expired:
            sum += (job.expired_time - job.start_time)
        elif job.is_done:
            sum += (job.finish_time - job.start_time)

    return sum / len(jobs)


def report_average_wait_time(jobs: List[Job]):
    sum = 0
    for job in jobs:
        if job.expired:
            sum += (job.expired_time - job.start_time)
        elif job.is_done:
            sum += (job.process_start_time - job.start_time)

    return sum / len(jobs)


def report_rate_of_expired(jobs):
    expired_sum = 0
    done_sum = 0
    for job in jobs:
        if job.expired:
            expired_sum += 1
        elif job.is_done:
            done_sum += 1

    if expired_sum + done_sum == 0:
        return 0

    return expired_sum / (done_sum + expired_sum)


def report_mean_queue_length(queue_lengths):
    return sum(queue_lengths) / len(queue_lengths)
