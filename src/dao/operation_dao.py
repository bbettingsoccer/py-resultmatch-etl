from abc import ABCMeta, abstractmethod


class OperationDAO(metaclass=ABCMeta):

    @abstractmethod
    def get_collection(self, pipeline):
        pass

    @abstractmethod
    def save_collection(self, join_dt):
        pass
