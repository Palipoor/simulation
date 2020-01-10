import math
import random
from itertools import accumulate

random.seed(12345)


def get_uniform(a=0, b=1):
    return random.uniform(a, b)


def get_exp(alpha):
    return -math.log(get_uniform()) / alpha


def get_poisson_sample(rate, n=1):
    return list(accumulate([get_exp(rate) for i in range(n)]))


def get_bernouli(p):
    r = get_uniform()
    if r <= p:
        return 1
    else:
        return 0
