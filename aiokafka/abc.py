import abc


class ConsumerRebalanceListener(abc.ABC):

    @abc.abstractmethod
    def on_partitions_revoked(self, revoked):
        """
        A coroutine or function the user can implement to provide cleanup or
        custom state save on the start of a rebalance operation.

        This method will be called *before* a rebalance operation starts and
        *after* the consumer stops fetching data.

        If you are using manual commit you have to commit all consumed offsets
        here, to avoid duplicate message delivery after rebalance is finished.

        .. note:: This method is only called before rebalances. It is not
            called prior to :meth:`.AIOKafkaConsumer.stop`

        Arguments:
            revoked (list(TopicPartition)): the partitions that were assigned
                to the consumer on the last rebalance
        """

    @abc.abstractmethod
    def on_partitions_assigned(self, assigned):
        """
        A coroutine or function the user can implement to provide load of
        custom consumer state or cache warmup on completion of a successful
        partition re-assignment.

        This method will be called *after* partition re-assignment completes
        and *before* the consumer starts fetching data again.

        It is guaranteed that all the processes in a consumer group will
        execute their :meth:`on_partitions_revoked` callback before any instance
        executes its :meth:`on_partitions_assigned` callback.

        Arguments:
            assigned (list(TopicPartition)): the partitions assigned to the
                consumer (may include partitions that were previously assigned)
        """


class AbstractTokenProvider(abc.ABC):

    @abc.abstractmethod
    async def token(self):
        """
        An async callback returning a :class:`str` ID/Access Token to be sent to
        the Kafka client. In case where a synchronous callback is needed,
        implementations like following can be used:

        .. code-block:: python

            from aiokafka.abc import AbstractTokenProvider

            class CustomTokenProvider(AbstractTokenProvider):
                async def token(self):
                    return await asyncio.get_running_loop().run_in_executor(
                        None, self._token)

                def _token(self):
                    # The actual synchronous token callback.
        """

    def extensions(self):
        """
        This is an OPTIONAL method that may be implemented.

        Returns a map of key-value pairs that can be sent with the
        SASL/OAUTHBEARER initial client request. If not implemented, the values
        are ignored

        This feature is only available in Kafka >= 2.1.0.
        """
        return {}


__all__ = [
    "AbstractTokenProvider",
    "ConsumerRebalanceListener",
]
