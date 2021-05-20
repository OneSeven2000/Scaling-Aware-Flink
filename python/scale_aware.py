import flink_control
from BrainDQN_NIPS import BrainDQN
import numpy as np
import time


def adjust():
    # init BrainDQN and flink_control
    actions = 8
    brain = BrainDQN(actions)
    flink = flink_control.Control()
    # adjust
    # get init state first
    action0 = np.array([1, 0, 0, 0, 0, 0, 0, 0])  # do nothing
    observation0, reward0, terminal = flink.step(action0)
    brain.setInitState(observation0)

    while 1 != 0:
        action = brain.getAction()
        nextObservation, reward, terminal = flink.step(action)
        brain.setPerception(nextObservation, action, reward, terminal)


def main():
    adjust()


if __name__ == "__main__":
    main()
