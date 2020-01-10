from utils.random import get_uniform, get_exp


class Server:
    class Core:
        def __init__(self, service_rate):
            self.service_rate = service_rate
            self.ready_time = 0

    def __init__(self, id, cores):
        self.id = id
        self.cores = [self.Core(r) for r in cores]
        self.queue = []


class Scheduler:
    def __init__(self, service_rate, servers):
        self.ready_time = 0
        self.queue = []
        self.servers = servers
        self.service_rate = service_rate

    def schedule(self, time):

        if not self.queue:
            return
        self.queue.sort(key=lambda x: (x.type, x.start_time))
        self.servers.sort(key=lambda x: (len(x.queue), get_uniform()))
        job = self.queue[0]
        job.schedule_time = time
        job.process_server_id = self.servers[0].id
        self.servers[0].queue.append(self.queue[0])
        self.queue = self.queue[1:]
        self.ready_time += get_exp(self.service_rate)


class Job:
    def __init__(self, id, type, start_time, deadline):
        self.type = type
        self.id = id
        self.start_time = start_time
        self.deadline = deadline
        self.process_start_time = None
        self.schedule_time = None
        self.finish_time = None
        self.process_server_id = None
        self.is_active = False
        self.is_done = False
        self.expired = False
        self.expired_time = 0
